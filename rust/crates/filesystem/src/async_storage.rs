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
use bytes::Bytes;
use std::any::{Any, TypeId};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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
pub async fn commit_workspace_fork_in_steps<S: AsyncAuthorityStore + ?Sized>(
    store: &S,
    fork: WorkspaceForkCommit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> AuthorityResult<WorkspaceForkOutcome> {
    let WorkspaceForkCommit {
        lineage,
        destination,
        creation,
        retention,
        retained,
    } = fork;
    let retained = append_first_record(
        store,
        retention,
        retained,
        WorkCounters::default(),
        budget,
        cancellation,
    )
    .await?;
    let mut work = retained.work;
    if !retained.value {
        return Ok(crate::storage::AuthorityReceipt {
            value: WorkspaceForkOutcome::RetentionConflict,
            work,
        });
    }
    let remaining = remaining_authority(work, budget)?;
    let created = match lineage {
        Some(source) => {
            store
                .fork_generation_authority(
                    source,
                    destination,
                    creation.operation_id,
                    remaining,
                    cancellation,
                )
                .await
        }
        None => {
            store
                .create_authority(destination, Epoch::GENESIS, remaining, cancellation)
                .await
        }
    }
    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
    work = add_authority(work, created.work)?;
    let created =
        append_first_record(store, destination, creation, work, budget, cancellation).await?;
    Ok(crate::storage::AuthorityReceipt {
        value: if created.value {
            WorkspaceForkOutcome::Committed
        } else {
            WorkspaceForkOutcome::CreationRejected
        },
        work: created.work,
    })
}

/// Makes `commit` the first record of `authority`, creating the authority
/// if needed. Answers whether the authority's first record is exactly that
/// commit, including one an earlier attempt or a combined commit wrote under
/// another retry identity. The receipt's work includes `prior`.
///
/// This is the default
/// [`AsyncAuthorityStore::create_authority_with_first_record`], for backends
/// that resolve an existing authority here.
pub async fn append_first_record<S: AsyncAuthorityStore + ?Sized>(
    store: &S,
    authority: AuthorityId,
    commit: ProposedCommit,
    prior: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> AuthorityResult<bool> {
    let created = store
        .create_authority(
            authority,
            Epoch::GENESIS,
            remaining_authority(prior, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(prior, std::convert::identity))?;
    let mut work = add_authority(prior, created.work)?;
    let head = match created.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let identity = (commit.operation_id, commit.fingerprint);
    let appended = store
        .compare_and_append(
            authority,
            head.epoch,
            Head::genesis(head.epoch),
            commit,
            remaining_authority(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
    work = add_authority(work, appended.work)?;
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
                    remaining_authority(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add_authority(work, first.work)?;
            first.value.first().is_some_and(|record| {
                record.sequence == Sequence::new(1)
                    && (record.operation_id, record.fingerprint) == identity
            })
        }
        AppendOutcome::Fenced { .. } | AppendOutcome::IdempotencyConflict { .. } => false,
    };
    Ok(crate::storage::AuthorityReceipt {
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
) -> Result<WorkCounters, crate::storage::AuthorityFailure> {
    work.checked_add(more)
        .map_err(|error| crate::storage::AuthorityFailure::new(error.into(), work))
}

/// Nonblocking authority-store contract. Futures are sendable on native
/// targets and may remain JavaScript-thread-affine in browsers.
pub trait AsyncAuthorityStore: StorageProvider {
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

    /// Asynchronously creates one authority.
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<CreateAuthorityOutcome>> + StorageFuture;

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
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
///
/// Implementing this marker makes the synchronous authority methods satisfy
/// [`AsyncAuthorityStore`] without yielding. Blocking native backends must use
/// a provider-owned bounded blocking executor instead.
pub trait ImmediateAuthorityStore: AuthorityStore {}

impl<T: ImmediateAuthorityStore + ?Sized> ImmediateAuthorityStore for Arc<T> {}

/// The objects one authority record makes reachable, which must be durable
/// before the record is appended.
#[derive(Clone, Copy, Debug)]
pub enum PublicationScope<'a> {
    /// The complete authenticated closure of one published generation.
    Closure(&'a [ObjectId]),
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

    /// Asynchronously admits one verified immutable object.
    fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture;

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

    /// Asynchronously admits an ordered bounded group of objects whose
    /// identities their construction already proved. A store that verifies
    /// digests on admission may skip hashing the same bytes again; the
    /// default verifies through [`Self::put_many`].
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
    /// authority record may reference it. Ordinary stores already provide
    /// that guarantee from `put`/`put_many`; a bounded staging adapter overrides
    /// this boundary to group physical writes without changing publication.
    fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture {
        async {
            Ok(crate::storage::ObjectReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Asynchronously reads one complete bounded object.
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + StorageFuture;

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
pub trait ImmediateObjectStore: ObjectStore {}

impl<T: ImmediateObjectStore + ?Sized> ImmediateObjectStore for Arc<T> {}

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
    cancellation.check().map_err(|_| {
        crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
    })?;
    if requests.is_empty() {
        return Err(crate::storage::ObjectFailure::before_work(
            crate::storage::ObjectStoreError::Rejected("object read batch is empty".to_owned()),
        ));
    }
    let item_count = u64::try_from(requests.len()).unwrap_or(u64::MAX);
    let vector_bytes = u64::try_from(requests.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(size_of::<ObjectRead>()).unwrap_or(u64::MAX));
    let mut work = crate::performance::WorkCounters {
        items_examined: item_count,
        allocation_operations: u64::from(!requests.is_empty()),
        peak_allocation_bytes: vector_bytes,
        ..crate::performance::WorkCounters::default()
    };
    let mut admission = work;
    admission.items_returned = item_count;
    admission
        .verify(budget)
        .map_err(|error| crate::storage::ObjectFailure::before_work(error.into()))?;
    let mut values = Vec::new();
    values.try_reserve_exact(requests.len()).map_err(|_| {
        crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Rejected(
            "object batch result allocation failed".to_owned(),
        ))
    })?;
    let mut retained_bytes = vector_bytes;
    for request in requests {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::new(crate::storage::ObjectStoreError::Cancelled, work)
        })?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| crate::storage::ObjectFailure::new(error.into(), work))?;
        let receipt = store
            .read(
                request.object_id,
                request.maximum_bytes,
                remaining,
                cancellation,
            )
            .await
            .map_err(|failure| {
                let nested = *failure.work;
                let peak = work
                    .peak_allocation_bytes
                    .max(retained_bytes.saturating_add(nested.peak_allocation_bytes));
                match work.checked_add(nested) {
                    Ok(mut combined) => {
                        combined.peak_allocation_bytes = peak;
                        crate::storage::ObjectFailure::new(failure.error, combined)
                    }
                    Err(error) => crate::storage::ObjectFailure::new(error.into(), work),
                }
            })?;
        let nested_peak = retained_bytes.saturating_add(receipt.work.peak_allocation_bytes);
        work = work
            .checked_add(receipt.work)
            .map_err(|error| crate::storage::ObjectFailure::new(error.into(), work))?;
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(nested_peak);
        retained_bytes = retained_bytes.saturating_add(match receipt.value.retention {
            crate::storage::ObjectReadRetention::Shared => 0,
            crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        });
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(retained_bytes);
        work.verify(budget)
            .map_err(|error| crate::storage::ObjectFailure::new(error.into(), work))?;
        values.push(receipt.value);
    }
    work.items_returned = item_count;
    Ok(crate::storage::ObjectReceipt {
        value: values,
        work,
    })
}

impl<T: ImmediateAuthorityStore + ?Sized> AsyncAuthorityStore for T {
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
}

impl<T: ImmediateObjectStore + ?Sized> AsyncObjectStore for T {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
        })?;
        ObjectStore::put(self, object_id, bytes, budget)
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
