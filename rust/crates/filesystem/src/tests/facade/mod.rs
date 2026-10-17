use super::*;
use crate::kernel::{InlineFileData, LogicalName, NameEncoding};
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
};
use crate::storage::{
    CreateAuthorityOutcome, ObjectRead, ObjectReadRequest, ObjectWrite, object_digest,
};
use acyclic_stream::OwnedBytes;
use std::sync::atomic::{AtomicUsize, Ordering};

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..config()
    }
}

fn exclusive_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::ExclusiveWriter,
        ..config()
    }
}

fn pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::None,
    }
}

fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn manual() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::None,
    }
}

fn tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::None,
    }
}

fn writable_manual() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn writable_tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    }
}

fn writable_live() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Live,
        mutations: MutationMode::DirectLive,
    }
}

fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            value.as_bytes().to_vec(),
            config().limits.maximum_component_bytes,
        )?],
        config().limits,
    )?)
}

fn path_parts(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let components = values
        .iter()
        .map(|value| {
            LogicalName::new(
                NameEncoding::Utf8,
                value.as_bytes().to_vec(),
                config().limits.maximum_component_bytes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespacePath::new(components, config().limits)?)
}

fn root_path() -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(Vec::new(), config().limits)?)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CheckoutState {
    base_generation_root: ObjectId,
    generation_root: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

// --- Case-folding / Unicode-normalization ---
//
// `CaseSensitivity::ProfileFolded` rejects same-directory siblings whose
// names collide once case-folded (`FsError::NameCollision`) and resolves
// lookups case-insensitively; `UnicodePolicy::RequireNfc` rejects any new
// name that is not canonical NFC (`FsError::NonNormalizedName`). Both are
// admitted at volume creation (pinned above).

fn folded_config() -> VolumeConfig {
    VolumeConfig {
        case_sensitivity: CaseSensitivity::ProfileFolded,
        ..config()
    }
}

fn nfc_config() -> VolumeConfig {
    VolumeConfig {
        unicode: UnicodePolicy::RequireNfc,
        ..config()
    }
}

fn checkout_state<A, O>(checkout: &Checkout<A, O>) -> CheckoutState {
    CheckoutState {
        base_generation_root: checkout.root.base,
        generation_root: checkout.generation_root,
        base_root: checkout.base_root.clone(),
        root: checkout.root.clone(),
        authority_head: checkout.authority_head,
        prepared_merge_parent: checkout.prepared_merge_parent,
        dependencies: checkout.dependencies.proof().dependencies.clone(),
        mode: checkout.mode,
    }
}

struct FaultControl {
    operation: AtomicUsize,
    fail_at: AtomicUsize,
}
struct FaultObjectStore {
    inner: crate::MemoryObjectStore,
    control: Arc<FaultControl>,
}
struct FaultAuthorityStore {
    inner: crate::MemoryAuthorityStore,
    control: Arc<FaultControl>,
}
struct PostPutObjectStore {
    inner: Arc<crate::MemoryObjectStore>,
    control: Arc<FaultControl>,
}
struct PostAppendAuthorityStore {
    inner: Arc<crate::MemoryAuthorityStore>,
    control: Arc<FaultControl>,
}
struct CountingObjectStore {
    inner: crate::MemoryObjectStore,
    tracked: Arc<std::sync::Mutex<Option<ObjectId>>>,
    reads: Arc<AtomicUsize>,
    unreadable: Arc<std::sync::Mutex<Option<ObjectKind>>>,
}

impl FaultControl {
    fn disabled() -> Self {
        Self {
            operation: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(usize::MAX),
        }
    }

    fn arm(&self, fail_at: usize) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(fail_at, Ordering::Relaxed);
    }

    fn disable(&self) {
        self.operation.store(0, Ordering::Relaxed);
        self.fail_at.store(usize::MAX, Ordering::Relaxed);
    }

    fn should_fail(&self) -> bool {
        self.operation.fetch_add(1, Ordering::Relaxed) + 1 == self.fail_at.load(Ordering::Relaxed)
    }
}

fn authority_context(work: &crate::AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
    if work.is_native() {
        return Err(AuthorityStoreError::NativeOriginalUnsupported);
    }
    Ok(())
}
fn object_context(work: &crate::AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
    if work.is_native() {
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
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            work.record(WorkCounters {
                authority_records_read: u64::from(read),
                authority_records_appended: u64::from(!read),
                ..WorkCounters::UNCHARGED
            })?;
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
        work: &mut crate::AuthorityWork<'_>,
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
impl CountingObjectStore {
    fn record(&self, id: ObjectId) -> Result<(), ObjectStoreError> {
        if self
            .tracked
            .lock()
            .is_ok_and(|tracked| *tracked == Some(id))
        {
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .unreadable
            .lock()
            .is_ok_and(|kind| *kind == Some(id.kind))
        {
            return Err(ObjectStoreError::Missing);
        }
        Ok(())
    }
}
impl AsyncAuthorityStore for FaultAuthorityStore {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        self.fail(false, work)?;
        let value = self
            .inner
            .create_authority(authority, epoch, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard, work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        authority_permit(request.permit)?;
        crate::async_storage::seal(guard, work).await?;
        self.fail(false, work)?;
        self.inner
            .compare_and_append(request, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
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
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        self.inner.replay(authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(false, work)?;
        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        self.inner.find_operation(authority, operation, work).await
    }
}
impl AsyncAuthorityStore for PostAppendAuthorityStore {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        self.inner
            .create_authority(authority, epoch, work, guard)
            .await
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
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
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        authority_context(work)?;
        self.inner.head(authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        authority_context(work)?;
        self.inner.replay(authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        authority_context(work)?;
        self.inner.fence(authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
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
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fail(false, work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fail(false, work)?;
        self.inner.put_many(writes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.fail(true, work)?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.fail(true, work)?;
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.fail(true, work)?;
        self.inner.contains(id, work).await
    }
}
impl AsyncObjectStore for CountingObjectStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        object_context(work)?;
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        object_context(work)?;
        self.inner.put_many(writes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        object_context(work)?;
        self.record(id)?;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        object_context(work)?;
        for request in requests {
            self.record(request.object_id)?;
        }
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        object_context(work)?;
        self.inner.contains(id, work).await
    }
}
impl AsyncObjectStore for PostPutObjectStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
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
        work: &mut crate::AuthorityWork<'_>,
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
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
    async fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        if work.is_native() {
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
mod adoption;
mod attributes;
mod content;
mod creation;
mod detached;
mod fault_creation;
mod fault_mutations;
mod fault_publication;
mod generation_position;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod heterogeneous_mount;
mod identity;
mod live;
mod local;
mod merge;
mod paths;
mod profiles;
mod protocol_helpers;
mod publication;
mod read;
mod selection;
mod special;
mod tracking;
mod transactions;
mod transfer;
use protocol_helpers::*;
mod fixtures;
use fixtures::*;
const B: WorkBudget = WorkBudget::UNBOUNDED;
fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<Result<OperationReceipt<()>, OperationFailure<E>>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}
