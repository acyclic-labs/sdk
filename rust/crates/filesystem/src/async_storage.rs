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

/// Commits a workspace fork as ordered single-authority steps: the retention record first, so the source generation is retained before any workspace can depend on it, then the destination authority and its creation record.
pub async fn commit_workspace_fork_in_steps<S: AsyncAuthorityStore + ?Sized>(
    store: &S,
    fork: WorkspaceForkCommit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> AuthorityResult<WorkspaceForkOutcome> {
    authority_projection(commit_workspace_fork_steps(
        store, fork, PublicationPermit::Unrestricted, budget, cancellation,
        None::<&mut fn(WorkBudget) -> std::future::Ready<crate::facade::FsResult<()>>>,
    ).await.map_err(|(error, work)| crate::performance::OperationFailure::new(error, work)))
}

/// Guarded ordered fork recovery, borrowing the same external input custody as [`commit_workspace_fork_in_steps`].
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
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
{
    commit_workspace_fork_steps(store, fork, permit, budget, cancellation, Some(guard))
        .await.map_err(|(error, work)| crate::performance::OperationFailure::new(error, work))
}

fn accept_fork_step<T>(
    result: crate::facade::FsResult<T>, work: &mut WorkCounters, budget: WorkBudget,
) -> Result<T, (crate::FsError, WorkCounters)> {
    let receipt = result.map_err(|failure| {
        let failure = failure.map_with_prior_work(*work, std::convert::identity);
        (failure.error, *failure.work)
    })?;
    *work = work.checked_add(receipt.work).map_err(|error| (crate::FsError::from(error), *work))?;
    work.verify(budget).map_err(|error| (crate::FsError::from(error), *work))?;
    Ok(receipt.value)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn commit_workspace_fork_in_steps_with_native_work<S: AsyncAuthorityStore + ?Sized>(
    store: &S, fork: WorkspaceForkCommit, original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<WorkspaceForkOutcome, crate::storage::AuthorityStoreError> {
    original.admit(WorkCounters::UNCHARGED)?;
    cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
    let WorkspaceForkCommit { lineage, destination, creation, retention, retained } = fork;
    if !store.create_authority_with_first_record_with_native_work(
        retention, retained, original, cancellation,
    ).await? { return Ok(WorkspaceForkOutcome::RetentionConflict); }
    match lineage {
        Some(source) => store.fork_generation_authority_with_native_work(
            source, destination, creation.operation_id, original, cancellation,
        ).await?,
        None => store.create_authority_with_native_work(
            destination, Epoch::GENESIS, original, cancellation,
        ).await?,
    };
    Ok(if store.create_authority_with_first_record_with_native_work(
        destination, creation, original, cancellation,
    ).await? { WorkspaceForkOutcome::Committed } else { WorkspaceForkOutcome::CreationRejected })
}

async fn commit_workspace_fork_steps<S, F, Fut>(
    store: &S, fork: WorkspaceForkCommit, permit: PublicationPermit,
    budget: WorkBudget, cancellation: &CancellationToken, mut guard: Option<&mut F>,
) -> Result<crate::facade::FsReceipt<WorkspaceForkOutcome>, (crate::FsError, WorkCounters)>
where S: AsyncAuthorityStore + ?Sized, F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
{
    let mut work = WorkCounters::UNCHARGED;
    if permit != PublicationPermit::Unrestricted {
        return Err((crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend cannot atomically evaluate fork publication leases",
        ).into(), work));
    }
    let WorkspaceForkCommit { lineage, destination, creation, retention, retained } = fork;
    let result = append_first_record(
        store, retention, retained, WorkCounters::UNCHARGED, budget, cancellation,
    ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs);
    if !accept_fork_step(result, &mut work, budget)? {
        return Ok(crate::FsReceipt { value: WorkspaceForkOutcome::RetentionConflict, work });
    }
    let remaining = work.remaining(budget).map_err(|error| (crate::FsError::from(error), work))?;
    let result = match (lineage, guard.as_deref_mut()) {
        (Some(source), Some(guard)) => store.fork_generation_authority_guarded(
            source, destination, creation.operation_id, remaining, cancellation, guard,
        ).await,
        (None, Some(guard)) => store.create_authority_guarded(
            destination, Epoch::GENESIS, remaining, cancellation, guard,
        ).await,
        (Some(source), None) => store.fork_generation_authority(
            source, destination, creation.operation_id, remaining, cancellation,
        ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
        (None, None) => store.create_authority(
            destination, Epoch::GENESIS, remaining, cancellation,
        ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
    };
    accept_fork_step(result, &mut work, budget)?;
    let remaining = work.remaining(budget).map_err(|error| (crate::FsError::from(error), work))?;
    let result = match guard {
        Some(guard) => store.create_authority_with_first_record_guarded(
            destination, creation, remaining, cancellation, guard,
        ).await,
        None => append_first_record(
            store, destination, creation, WorkCounters::UNCHARGED, remaining, cancellation,
        ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
    };
    let value = if accept_fork_step(result, &mut work, budget)? {
        WorkspaceForkOutcome::Committed
    } else {
        WorkspaceForkOutcome::CreationRejected
    };
    Ok(crate::FsReceipt { value, work })
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn commit_workspace_fork_in_steps_guarded_with_native_work<S, E, F, Fut>(
    store: &S, fork: WorkspaceForkCommit, permit: PublicationPermit,
    original: &crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
) -> Result<WorkspaceForkOutcome, E>
where S: AsyncAuthorityStore + ?Sized, E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static {
    original.admit(WorkCounters::UNCHARGED).map_err(crate::FsError::from)?;
    cancellation.check().map_err(crate::FsError::from)?;
    if permit != PublicationPermit::Unrestricted {
        return Err(crate::FsError::from(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend cannot atomically evaluate fork publication leases",
        )).into());
    }
    let WorkspaceForkCommit { lineage, destination, creation, retention, retained } = fork;
    if !store.create_authority_with_first_record_with_native_work(
        retention, retained, original, cancellation,
    ).await.map_err(crate::FsError::from)? { return Ok(WorkspaceForkOutcome::RetentionConflict); }
    match lineage {
        Some(source) => store.fork_generation_authority_guarded_with_native_work(
            source, destination, creation.operation_id, original, cancellation, guard,
        ).await?,
        None => store.create_authority_guarded_with_native_work(
            destination, Epoch::GENESIS, original, cancellation, guard,
        ).await?,
    };
    Ok(if store.create_authority_with_first_record_guarded_with_native_work(
        destination, creation, original, cancellation, guard,
    ).await? { WorkspaceForkOutcome::Committed } else { WorkspaceForkOutcome::CreationRejected })
}

/// Makes `commit` the first record of `authority`, creating the authority if needed.
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

/// Guarded first-record recovery with the same caller-held payload custody as [`append_first_record`].
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
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
{
    let receipt = guard(remaining_authority(*work, budget).map_err(authority_to_fs)?)
        .await
        .map_err(|failure| failure.map_with_prior_work(*work, std::convert::identity))?;
    *work = add_authority(*work, receipt.work, budget).map_err(authority_to_fs)?;
    Ok(crate::facade::FsReceipt { value: (), work: *work })
}

/// Nonblocking authority-store contract.
pub trait AsyncAuthorityStore: ProviderPlatform {
    /// Seals every actual destination publication against the same original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, E>> + ProviderTask
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (fork, permit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Creates an authority after its original-work guard.
    fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + ProviderTask
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, epoch, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Publishes the first record after its original-work guard.
    fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<bool, E>> + ProviderTask
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, commit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Forks an authority after its original-work guard.
    fn fork_generation_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + ProviderTask
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
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, crate::storage::AuthorityStoreError>> + ProviderTask {
        commit_workspace_fork_in_steps_with_native_work(self, fork, original, cancellation)
    }

    /// Retains the exact source lineage and operation identity in original custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn fork_generation_authority_with_native_work(
        &self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError>> + ProviderTask {
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
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError>> + ProviderTask {
        let _ = (authority, epoch, original, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Whether this backend can retain an immutable published-generation lineage prefix when it creates a child authority.
    fn supports_generation_lineage_prefix(&self) -> bool {
        false
    }

    /// Creates a destination authority whose canonical generation lineage is either an immutable native prefix of the source or an explicit independent lineage rooted at an unpublished authenticated generation.
    fn fork_generation_authority(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<CreateAuthorityOutcome>> + ProviderTask;

    /// Seals destination creation.
    fn fork_generation_authority_guarded<F, Fut>(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<CreateAuthorityOutcome>> + ProviderTask
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
    ) -> impl Future<Output = AuthorityResult<CreateAuthorityOutcome>> + ProviderTask;

    /// Guarded authority creation.
    fn create_authority_guarded<F, Fut>(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<CreateAuthorityOutcome>> + ProviderTask
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
    fn retire_authority(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<()>> + ProviderTask {
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
    ) -> impl Future<Output = AuthorityResult<Head>> + ProviderTask;

    /// Asynchronously compares and appends one durable operation.
    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<AppendOutcome>> + ProviderTask;

    /// Asynchronously compares and appends under an optional operation-window permit.
    fn compare_and_append_guarded(
        &self,
        request: GuardedAppend,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<AppendOutcome>> + ProviderTask {
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
    >> + ProviderTask {
        let _ = (request, original, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Seals the actual canonical publication after planning at every provider CAS/retry.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self,
        request: GuardedAppend,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = Result<(AppendOutcome, crate::NativeAllocation), E>> + ProviderTask
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<crate::storage::AuthorityStoreError> + Send + 'static,
    {
        let _ = (request, original, cancellation, guard);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported.into()))
    }

    /// Durably creates an authority whose first record is `commit`, or confirms that it already exists with exactly that first record.
    fn create_authority_with_first_record(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<bool>> + ProviderTask {
        append_first_record(
            self,
            authority,
            commit,
            WorkCounters::default(),
            budget,
            cancellation,
        )
    }

    /// Creates the same initial authority record without a separate operation, work root, journal, or unchecked output allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_with_native_work(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<bool, crate::storage::AuthorityStoreError>> + ProviderTask {
        let _ = (authority, commit, original, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Guarded first-record publication.
    fn create_authority_with_first_record_guarded<F, Fut>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<bool>> + ProviderTask
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
    {
        append_first_record_guarded(
            self, authority, commit, WorkCounters::default(), budget, cancellation, guard,
        )
    }

    /// Durably creates a forked workspace's authority state: the retention authority with its record and the destination authority with its creation record.
    fn commit_workspace_fork(
        &self,
        fork: WorkspaceForkCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<WorkspaceForkOutcome>> + ProviderTask {
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
    ) -> impl Future<Output = crate::facade::FsResult<WorkspaceForkOutcome>> + ProviderTask
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
    {
        commit_workspace_fork_in_steps_guarded(self, fork, permit, budget, cancellation, guard)
    }

    /// Reads only an existing reservation for the original operation identity.
    fn publication_reservation(
        &self,
        _authority_id: AuthorityId,
        _operation_id: OperationId,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Option<PublicationReservation>>> + ProviderTask {
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
    ) -> impl Future<Output = AuthorityResult<ReservationOutcome>> + ProviderTask {
        async move {
            Err(crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Rejected(
                    "authority backend does not support durable publication reservations"
                        .to_owned(),
                ),
            ))
        }
    }

    /// Releases an exact publication reservation.
    fn release_publication(
        &self,
        _reservation: PublicationReservation,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<()>> + ProviderTask {
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
    ) -> impl Future<Output = Result<Option<PublicationReservation>, crate::storage::AuthorityStoreError>> + ProviderTask {
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
    ) -> impl Future<Output = Result<PublicationReservation, crate::storage::AuthorityStoreError>> + ProviderTask {
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
    ) -> impl Future<Output = Result<(), crate::storage::AuthorityStoreError>> + ProviderTask {
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
    ) -> impl Future<Output = AuthorityResult<Vec<crate::foundation::DurableCommit>>> + ProviderTask;

    /// Asynchronously advances a writer fence.
    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<FenceOutcome>> + ProviderTask;

    /// Asynchronously resolves one idempotent operation identity.
    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Option<crate::foundation::DurableCommit>>> + ProviderTask;

    /// Reads the canonical head against the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn head_with_native_work(
        &self,
        authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<Head, crate::storage::AuthorityStoreError>> + ProviderTask {
        let _ = (authority_id, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Produces a canonical replay page funded in the original ledger.
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
    >> + ProviderTask {
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
    ) -> impl Future<Output = Result<FenceOutcome, crate::storage::AuthorityStoreError>> + ProviderTask {
        let _ = (authority_id, expected, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Resolves the original operation without allocating outside its ledger.
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
    >> + ProviderTask {
        let _ = (authority_id, operation_id, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
pub trait ImmediateAuthorityStore: AuthorityStore {
    /// Reads an existing publication reservation.
    fn publication_reservation(
        &self, authority: AuthorityId, operation: OperationId, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<PublicationReservation>> {
        let _ = (authority, operation, budget);
        cancellation.check().map_err(|_| crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Cancelled))?;
        Err(crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend cannot query original publication reservation evidence")))
    }

    /// Reserves publication against the expected head.
    fn reserve_publication(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<ReservationOutcome> {
        let _ = (authority, expected, operation, budget);
        cancellation.check().map_err(|_| crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Cancelled))?;
        Err(crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend does not support durable publication reservations")))
    }

    /// Releases the exact publication reservation.
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
    /// Reads a reservation using original work.
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
    /// Reserves publication using original work.
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
    /// Releases a reservation using original work.
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
    /// Commits guarded fork steps using original work.
    fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, E>> + ProviderTask
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (fork, permit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Creates an authority after its original-work guard.
    fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + ProviderTask
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, epoch, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Publishes the first record after its original-work guard.
    fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<bool, E>> + ProviderTask
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

    /// Canonical first-record producer.
    fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<bool>> + ProviderTask {
        append_first_record(self, authority, commit, WorkCounters::default(), budget, cancellation)
    }

    /// Canonical guarded first-record producer, including successful recovery.
    fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<bool>> + ProviderTask
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
    ) -> impl Future<Output = Result<(AppendOutcome, crate::NativeAllocation), E>> + ProviderTask
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
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, E>> + ProviderTask
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
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + ProviderTask
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
    ) -> impl Future<Output = Result<bool, E>> + ProviderTask
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
    ) -> impl Future<Output = AuthorityResult<bool>> + ProviderTask {
        ImmediateAuthorityStore::create_authority_with_first_record(self.as_ref(), authority, commit, budget, cancellation)
    }

    fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<bool>> + ProviderTask
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
    ) -> impl Future<Output = Result<(AppendOutcome, crate::NativeAllocation), E>> + ProviderTask
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
            let standing = observation.map_or(0, |observation| observation.custody.operation_cache_bytes());
            let mut remaining = budget;
            remaining.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(standing)
                .ok_or_else(|| crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes", observed: standing,
                        maximum: budget.peak_allocation_bytes,
                    }),
                ))?;
            let result = self.read(object_id, maximum_bytes, remaining, cancellation).await;
            let result = match observation {
                Some(observation) => observation.complete(result),
                None => result,
            };
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
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
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
