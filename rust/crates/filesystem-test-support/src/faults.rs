//! Deterministic cuts before backend work and after a durable write.

use acyclic_fs::async_storage::seal_test_guard;
use acyclic_fs::foundation::{DurableCommit, Epoch, Head, Sequence};
use acyclic_fs::storage::ObjectWrite;
use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityStoreError,
    CreateAuthorityOutcome, FenceOutcome, HashedObject, MemoryAuthorityStore, MemoryObjectStore,
    ObjectRead, ObjectReadRequest, ObjectStoreError, PublicationPermit, ReplayLimit,
};
use acyclic_fs_format::identity::{AuthorityId, OperationId};
use acyclic_fs_format::object_reference::ObjectId;
use acyclic_fs_work::performance::{OperationReceipt, WorkCounters};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Selects the next deterministic backend call to fail.
pub struct FaultControl {
    operation: AtomicUsize,
    fail_at: AtomicUsize,
}
/// Fails one selected object operation before reaching storage.
pub struct FaultObjectStore {
    /// The actual in-memory backend.
    pub inner: MemoryObjectStore,
    /// Controls the configured failure point.
    pub control: Arc<FaultControl>,
}
/// Fails one selected authority operation before applying it.
pub struct FaultAuthorityStore {
    /// The actual in-memory backend.
    pub inner: MemoryAuthorityStore,
    /// Controls the configured failure point.
    pub control: Arc<FaultControl>,
}
/// Fails one selected acknowledgement after writing the object.
pub struct PostPutObjectStore {
    /// The actual in-memory backend.
    pub inner: Arc<MemoryObjectStore>,
    /// Controls the configured failure point.
    pub control: Arc<FaultControl>,
}
/// Loses one selected acknowledgement after appending the authority record.
pub struct PostAppendAuthorityStore {
    /// The actual in-memory backend.
    pub inner: Arc<MemoryAuthorityStore>,
    /// Controls the configured failure point.
    pub control: Arc<FaultControl>,
}

impl FaultControl {
    /// Starts without an injected failure.
    pub fn disabled() -> Self {
        Self {
            operation: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(usize::MAX),
        }
    }

    /// Fails the requested one-based call after resetting the count.
    pub fn arm(&self, fail_at: usize) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(fail_at, Ordering::Relaxed);
    }

    /// Resets the invocation count and disables injection.
    pub fn disable(&self) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(usize::MAX, Ordering::Relaxed);
    }

    fn should_fail(&self) -> bool {
        self.operation.fetch_add(1, Ordering::Relaxed) + 1 == self.fail_at.load(Ordering::Relaxed)
    }
}

fn authority_context(work: &acyclic_fs::AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
    #[cfg(target_arch = "wasm32")]
    let _ = work;
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() {
        return Err(AuthorityStoreError::NativeOriginalUnsupported);
    }
    Ok(())
}
fn object_context(work: &acyclic_fs::AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() {
        return Err(ObjectStoreError::NativeUnavailable);
    }
    work.work.remaining(work.budget)?;
    work.cancellation
        .check()
        .map_err(|_| ObjectStoreError::Cancelled)
}
fn authority_permit(permit: PublicationPermit) -> Result<(), AuthorityStoreError> {
    if permit == PublicationPermit::Unrestricted {
        Ok(())
    } else {
        Err(AuthorityStoreError::Rejected(
            "authority backend cannot atomically evaluate operation leases".into(),
        ))
    }
}
impl FaultAuthorityStore {
    fn fail(
        &self,
        read: bool,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            work.accept::<(), AuthorityStoreError>(Ok(OperationReceipt {
                value: (),
                work: WorkCounters {
                    authority_records_read: u64::from(read),
                    authority_records_appended: u64::from(!read),
                    ..WorkCounters::UNCHARGED
                },
            }))?;
            return Err(AuthorityStoreError::Corrupt(
                "deterministic test fault".into(),
            ));
        }
        Ok(())
    }
}
impl FaultObjectStore {
    fn fail(
        &self,
        read: bool,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        object_context(work)?;
        if self.control.should_fail() {
            work.work = work.work.checked_add(WorkCounters {
                backend_read_operations: u64::from(read),
                backend_write_operations: u64::from(!read),
                ..WorkCounters::UNCHARGED
            })?;
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(())
    }
}
impl AsyncAuthorityStore for FaultAuthorityStore {
    fn retains_operation_payload(&self) -> bool {
        self.inner.retains_operation_payload()
    }

    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }
    fn actor_scope(&self) -> [u8; 16] {
        self.inner.actor_scope()
    }

    async fn create_authority<G: acyclic_fs::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, acyclic_fs::AuthorityError<G::Error>> {
        authority_context(work)?;
        seal_test_guard(guard.as_deref_mut(), work).await?;
        self.fail(false, work)?;
        let value = self
            .inner
            .create_authority(authority, epoch, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                acyclic_fs::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            seal_test_guard(guard, work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: acyclic_fs::AuthorityGuard>(
        &self,
        request: acyclic_fs::GuardedAppend,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<AppendOutcome, acyclic_fs::AuthorityError<G::Error>> {
        authority_context(work)?;
        authority_permit(request.permit)?;
        seal_test_guard(guard, work).await?;
        self.fail(false, work)?;
        self.inner
            .compare_and_append(request, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                acyclic_fs::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        self.inner.head(authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, acyclic_fs::AuthorityAllocation), AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        self.inner.replay(authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(false, work)?;
        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        self.inner.find_operation(authority, operation, work).await
    }
}
impl AsyncAuthorityStore for PostAppendAuthorityStore {
    fn retains_operation_payload(&self) -> bool {
        self.inner.retains_operation_payload()
    }

    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }
    fn actor_scope(&self) -> [u8; 16] {
        self.inner.actor_scope()
    }

    async fn create_authority<G: acyclic_fs::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, acyclic_fs::AuthorityError<G::Error>> {
        authority_context(work)?;
        self.inner
            .create_authority(authority, epoch, work, guard)
            .await
    }
    async fn compare_and_append<G: acyclic_fs::AuthorityGuard>(
        &self,
        request: acyclic_fs::GuardedAppend,
        work: &mut acyclic_fs::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<AppendOutcome, acyclic_fs::AuthorityError<G::Error>> {
        authority_context(work)?;
        authority_permit(request.permit)?;
        let value = self.inner.compare_and_append(request, work, guard).await?;
        if matches!(value, AppendOutcome::Committed(_)) && self.control.should_fail() {
            return Err(AuthorityStoreError::Indeterminate {
                operation: "compare-and-append",
                source: std::io::Error::other("deterministic acknowledgement loss"),
            }
            .into());
        }
        Ok(value)
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        authority_context(work)?;
        self.inner.head(authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, acyclic_fs::AuthorityAllocation), AuthorityStoreError> {
        authority_context(work)?;
        self.inner.replay(authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        authority_context(work)?;
        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        authority_context(work)?;
        self.inner.find_operation(authority, operation, work).await
    }
}
impl AsyncObjectStore for FaultObjectStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fail(false, work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fail(false, work)?;
        self.inner.put_many(writes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.fail(true, work)?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, acyclic_fs::AuthorityAllocation), ObjectStoreError> {
        self.fail(true, work)?;
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.fail(true, work)?;
        self.inner.contains(id, work).await
    }
}
impl AsyncObjectStore for PostPutObjectStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(id, bytes, work).await?;
        if self.control.should_fail() {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(())
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many(writes, work).await?;
        if self.control.should_fail() {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(())
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, acyclic_fs::AuthorityAllocation), ObjectStoreError> {
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
    async fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            self.inner.put_hashed(object, work).await?;
            if self.control.should_fail() {
                return Err(ObjectStoreError::Corrupt);
            }
            return Ok(());
        }
        let (id, bytes) = object.into_parts();
        self.put(id, bytes, work).await
    }
}
