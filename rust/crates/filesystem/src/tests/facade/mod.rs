use super::*;
use crate::kernel::{InlineFileData, LogicalName, MAXIMUM_GENERATION_ROOT_BYTES, NameEncoding};
use crate::memory::MemoryObjectStore as TestMemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
};
use crate::storage::CreateAuthorityOutcome;
use crate::storage::object_digest;
use crate::storage::{AuthorityFailure, AuthorityResult};
use crate::storage::{ObjectFailure, ObjectRead, ObjectReadRequest, ObjectResult, ObjectWrite};
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

fn checkout_state<A, O>(checkout: &Checkout<A, O>) -> CheckoutState {
    CheckoutState {
        base_generation_root: checkout.root.base(),
        generation_root: checkout.generation_root,
        base_file_table: checkout.base_root.file_table,
        base_root: checkout.base_root.clone(),
        root: checkout.root.clone(),
        authority_head: checkout.authority_head,
        prepared_merge_parent: checkout.prepared_merge_parent,
        dependencies: checkout.dependencies.proof().dependencies.clone(),
        mode: checkout.mode,
    }
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct CheckoutState {
    base_generation_root: ObjectId,
    generation_root: ObjectId,
    base_file_table: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

struct FaultControl {
    operation: AtomicUsize,
    fail_at: AtomicUsize,
}

struct FaultObjectStore {
    inner: crate::memory::MemoryObjectStore,
    control: Arc<FaultControl>,
}

struct FaultAuthorityStore {
    inner: crate::memory::MemoryAuthorityStore,
    control: Arc<FaultControl>,
}

struct PostPutObjectStore {
    inner: Arc<crate::memory::MemoryObjectStore>,
    control: Arc<FaultControl>,
}

struct PostAppendAuthorityStore {
    inner: Arc<crate::memory::MemoryAuthorityStore>,
    control: Arc<FaultControl>,
}

struct CountingObjectStore {
    inner: crate::memory::MemoryObjectStore,
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
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() {
        return Err(AuthorityStoreError::NativeOriginalUnsupported);
    }
    Ok(())
}
fn object_context(work: &crate::AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() {
        return Err(ObjectStoreError::NativeUnavailable);
    }
    work.work.remaining(work.budget)?;
    work.cancellation
        .check()
        .map_err(|_| ObjectStoreError::Cancelled)
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
                "deterministic test fault".to_owned(),
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
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        self.fail(false, work)?;
        let value = AsyncAuthorityStore::create_authority(
            &self.inner,
            authority,
            epoch,
            work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        if request.permit != PublicationPermit::Unrestricted {
            return Err(AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        self.fail(false, work)?;
        let value =
            AsyncAuthorityStore::compare_and_append(&self.inner, request, work, None::<&mut ()>)
                .await
                .map_err(|error| {
                    crate::AuthorityError::Authority(error.into_error(std::convert::identity))
                })?;
        Ok(value)
    }
    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        AsyncAuthorityStore::head(&self.inner, authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        AsyncAuthorityStore::replay(&self.inner, authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(false, work)?;
        AsyncAuthorityStore::fence(&self.inner, authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        authority_context(work)?;
        self.fail(true, work)?;
        AsyncAuthorityStore::find_operation(&self.inner, authority, operation, work).await
    }
}

impl AsyncAuthorityStore for PostAppendAuthorityStore {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let value = AsyncAuthorityStore::create_authority(
            self.inner.as_ref(),
            authority,
            epoch,
            work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        authority_context(work)?;
        if request.permit != PublicationPermit::Unrestricted {
            return Err(AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let value = AsyncAuthorityStore::compare_and_append(
            self.inner.as_ref(),
            request,
            work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(&value, AppendOutcome::Committed(_)) && self.control.should_fail() {
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
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        AsyncAuthorityStore::head(self.inner.as_ref(), authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        authority_context(work)?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        AsyncAuthorityStore::replay(self.inner.as_ref(), authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, AuthorityStoreError> {
        authority_context(work)?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        AsyncAuthorityStore::fence(self.inner.as_ref(), authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        authority_context(work)?;
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        AsyncAuthorityStore::find_operation(self.inner.as_ref(), authority, operation, work).await
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
        let value = AsyncObjectStore::put(&self.inner, id, bytes, work).await?;
        Ok(value)
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.fail(false, work)?;
        let value = AsyncObjectStore::put_many(&self.inner, writes, work).await?;
        Ok(value)
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.fail(true, work)?;
        let value = AsyncObjectStore::read(&self.inner, id, maximum, work).await?;
        Ok(value)
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.fail(true, work)?;
        let value = AsyncObjectStore::read_many(&self.inner, requests, work).await?;
        Ok(value)
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.fail(true, work)?;
        let value = AsyncObjectStore::contains(&self.inner, id, work).await?;
        Ok(value)
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
        let value = AsyncObjectStore::put(&self.inner, id, bytes, work).await?;
        Ok(value)
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        object_context(work)?;
        let value = AsyncObjectStore::put_many(&self.inner, writes, work).await?;
        Ok(value)
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        object_context(work)?;
        self.record(id)?;
        let value = AsyncObjectStore::read(&self.inner, id, maximum, work).await?;
        Ok(value)
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
        let value = AsyncObjectStore::read_many(&self.inner, requests, work).await?;
        Ok(value)
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        object_context(work)?;
        let value = AsyncObjectStore::contains(&self.inner, id, work).await?;
        Ok(value)
    }
}

impl AsyncObjectStore for PostPutObjectStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let value = AsyncObjectStore::put(self.inner.as_ref(), id, bytes, work).await?;
        if self.control.should_fail() {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(value)
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let value = AsyncObjectStore::put_many(self.inner.as_ref(), writes, work).await?;
        if self.control.should_fail() {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(value)
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let value = AsyncObjectStore::read(self.inner.as_ref(), id, maximum, work).await?;
        Ok(value)
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let value = AsyncObjectStore::read_many(self.inner.as_ref(), requests, work).await?;
        Ok(value)
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        object_context(work)?;
        let value = AsyncObjectStore::contains(self.inner.as_ref(), id, work).await?;
        Ok(value)
    }
    async fn put_hashed(
        &self,
        object: HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            AsyncObjectStore::put_hashed(self.inner.as_ref(), object, work).await?;
            if self.control.should_fail() {
                return Err(ObjectStoreError::Corrupt);
            }
            return Ok(());
        }
        let (id, bytes) = object.into_parts();
        self.put(id, bytes, work).await
    }
}

#[path = "adoption.rs"]
mod adoption;
#[path = "attributes.rs"]
mod attributes;
#[path = "content.rs"]
mod content;
#[path = "creation.rs"]
mod creation;
#[path = "detached.rs"]
mod detached;
#[path = "fault_creation.rs"]
mod fault_creation;
#[path = "fault_mutations.rs"]
mod fault_mutations;
#[path = "fault_publication.rs"]
mod fault_publication;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[path = "heterogeneous_mount.rs"]
mod heterogeneous_mount;
#[path = "identity.rs"]
mod identity;
#[path = "live.rs"]
mod live;
#[path = "local.rs"]
mod local;
#[path = "merge.rs"]
mod merge;
#[path = "paths.rs"]
mod paths;
#[path = "profiles.rs"]
mod profiles;
#[path = "publication.rs"]
mod publication;
#[path = "read.rs"]
mod read;
#[path = "selection.rs"]
mod selection;
#[path = "special.rs"]
mod special;
#[path = "tracking.rs"]
mod tracking;
mod transactions;
#[path = "transfer.rs"]
mod transfer;

mod protocol_helpers;
use protocol_helpers::*;

const B: WorkBudget = WorkBudget::UNBOUNDED;
fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<Result<OperationReceipt<()>, OperationFailure<E>>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}
