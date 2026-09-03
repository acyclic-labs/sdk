//! Runtime-neutral asynchronous storage contracts for browser and remote use.

use crate::cancellation::CancellationToken;
use crate::foundation::{AuthorityId, Epoch, Head, OperationId, ProposedCommit, Sequence};
use crate::performance::WorkBudget;
use crate::storage::{
    AppendOutcome, AuthorityResult, AuthorityStore, CreateAuthorityOutcome, FenceOutcome, ObjectId,
    ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore, ReplayLimit,
};
use bytes::Bytes;
use std::any::{Any, TypeId};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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
/// This keeps synchronous compatibility wrappers free of executors while
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

/// Nonblocking authority-store contract without a native-thread `Send` bound.
///
/// Browser implementations may retain JavaScript transaction handles inside
/// returned futures. Native fleet implementations may choose stronger `Send`
/// guarantees in their own adapters.
pub trait AsyncAuthorityStore {
    /// Asynchronously creates one authority.
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<CreateAuthorityOutcome>>;

    /// Asynchronously reads the linearizable head.
    fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Head>>;

    /// Asynchronously compares and appends one durable operation.
    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<AppendOutcome>>;

    /// Asynchronously replays one bounded contiguous page.
    fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Vec<crate::foundation::DurableCommit>>>;

    /// Asynchronously advances a writer fence.
    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<FenceOutcome>>;

    /// Asynchronously resolves one idempotent operation identity.
    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Option<crate::foundation::DurableCommit>>>;
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
///
/// Implementing this marker makes the synchronous authority methods satisfy
/// [`AsyncAuthorityStore`] without yielding. Blocking native backends must use
/// [`crate::NativeStore`] instead.
pub trait ImmediateAuthorityStore: AuthorityStore {}

impl<T: ImmediateAuthorityStore + ?Sized> ImmediateAuthorityStore for Arc<T> {}

/// Nonblocking immutable-object contract suitable for `IndexedDB` and remote I/O.
pub trait AsyncObjectStore {
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
    ) -> impl Future<Output = ObjectResult<()>>;

    /// Asynchronously reads one complete bounded object.
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<ObjectRead>>;

    /// Asynchronously reads an ordered batch of complete bounded objects.
    ///
    /// Backends with transactional multi-get or batched I/O should override
    /// this method. The default is cancellation-aware and retains exact
    /// per-object work rather than pretending a sequential fallback was one
    /// physical operation.
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Vec<ObjectRead>>>;

    /// Asynchronously probes one exact object identity.
    fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<bool>>;
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
///
/// This marker also admits the synchronous kernel convenience functions.
/// Blocking native backends must use [`crate::NativeStore`] for async access.
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
