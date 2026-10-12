use super::*;
use crate::kernel::{InlineFileData, LogicalName, MAXIMUM_GENERATION_ROOT_BYTES, NameEncoding};
use crate::memory::MemoryObjectStore as TestMemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
};
use crate::storage::CreateAuthorityOutcome;
use crate::storage::object_digest;
use crate::storage::{AuthorityFailure, AuthorityResult, };
use crate::storage::{
    ObjectFailure, ObjectRead, ObjectReadRequest, ObjectResult, ObjectWrite,
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
    base_file_table: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

fn checkout_state<A, O>(checkout: &Checkout<A, O>) -> CheckoutState {
    CheckoutState {
        base_generation_root: checkout.root.base(),
        generation_root: checkout.generation_root,
        base_file_table: checkout.base_file_table,
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
    /// Every object of this kind reads as missing.
    unreadable: Arc<std::sync::Mutex<Option<ObjectKind>>>,
}

impl CountingObjectStore {
    fn record(&self, object_id: ObjectId) -> Result<(), ObjectFailure> {
        if self
            .tracked
            .lock()
            .is_ok_and(|tracked| tracked.as_ref() == Some(&object_id))
        {
            self.reads.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .unreadable
            .lock()
            .is_ok_and(|kind| *kind == Some(object_id.kind))
        {
            return Err(ObjectFailure::before_work(ObjectStoreError::Missing));
        }
        Ok(())
    }
}

impl FaultAuthorityStore {
    fn failure(read: bool) -> AuthorityFailure {
        AuthorityFailure::new(
            AuthorityStoreError::Corrupt("deterministic test fault".to_owned()),
            WorkCounters {
                authority_records_read: u64::from(read),
                authority_records_appended: u64::from(!read),
                ..WorkCounters::default()
            },
        )
    }
}

impl crate::AsyncAuthorityStore for FaultAuthorityStore {
async fn create_authority<G: crate::AuthorityGuard>(&self, authority: crate::AuthorityId, epoch: crate::Epoch, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
if self.control.should_fail() { let failure = Self::failure(false); work.record(*failure.work)?; return Err(failure.error.into()); }
let value = crate::AsyncAuthorityStore::create_authority(&self.inner, authority, epoch, work, None::<&mut ()>).await.map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: crate::GuardedAppend, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
if request.permit != crate::PublicationPermit::Unrestricted { return Err(crate::AuthorityStoreError::Rejected("authority backend cannot atomically evaluate operation leases".to_owned()).into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
if self.control.should_fail() { let failure = Self::failure(false); work.record(*failure.work)?; return Err(failure.error.into()); }
crate::AsyncAuthorityStore::compare_and_append(&self.inner, request, work, None::<&mut ()>).await.map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
}
async fn head(&self, authority: crate::AuthorityId, work: &mut crate::AuthorityWork<'_>) -> Result<crate::Head, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
if self.control.should_fail() { let failure = Self::failure(true); work.record(*failure.work)?; return Err(failure.error.into()); }
crate::AsyncAuthorityStore::head(&self.inner, authority, work).await
}
async fn replay(&self, authority: crate::AuthorityId, after: crate::Sequence, limit: crate::ReplayLimit, work: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
if self.control.should_fail() { let failure = Self::failure(true); work.record(*failure.work)?; return Err(failure.error.into()); }
crate::AsyncAuthorityStore::replay(&self.inner, authority, after, limit, work).await
}
async fn fence(&self, authority: crate::AuthorityId, expected: crate::Head, work: &mut crate::AuthorityWork<'_>) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
if self.control.should_fail() { let failure = Self::failure(false); work.record(*failure.work)?; return Err(failure.error.into()); }
crate::AsyncAuthorityStore::fence(&self.inner, authority, expected, work).await
}
async fn find_operation(&self, authority: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError,
    > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
if self.control.should_fail() { let failure = Self::failure(true); work.record(*failure.work)?; return Err(failure.error.into()); }
crate::AsyncAuthorityStore::find_operation(&self.inner, authority, operation, work).await
}
}

impl FaultObjectStore {
    fn failure(read: bool) -> ObjectFailure {
        ObjectFailure::new(
            ObjectStoreError::Corrupt,
            WorkCounters {
                backend_read_operations: u64::from(read),
                backend_write_operations: u64::from(!read),
                ..WorkCounters::default()
            },
        )
    }
}

impl crate::async_storage::AsyncObjectStore for FaultObjectStore {
    async fn put(&self, object_id: ObjectId, bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
            {
                let __object_args = (object_id, bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::put(&self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn put_many(&self, writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
            {
                let __object_args = (writes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::put_many(&self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read(&self, object_id: ObjectId, maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
            {
                let __object_args = (object_id, maximum_bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::read(&self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read_many(&self, requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
            {
                let __object_args = (requests, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::read_many(&self.inner, __object_args.0, &mut __object_work)
                        .await
                        .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result).map(|values| (values, None))
    }
    async fn contains(&self, object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
            {
                let __object_args = (object_id, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::contains(&self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}

impl crate::async_storage::AsyncObjectStore for CountingObjectStore {
    async fn put(&self, object_id: ObjectId, bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let __object_args = (object_id, bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::put(&self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn put_many(&self, writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let __object_args = (writes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::put_many(&self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read(&self, object_id: ObjectId, maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        self.record(object_id)?;
            {
                let __object_args = (object_id, maximum_bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::read(&self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read_many(&self, requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
        cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
        for request in requests {
            self.record(request.object_id)?;
        }
            {
                let __object_args = (requests, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::read_many(&self.inner, __object_args.0, &mut __object_work)
                        .await
                        .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result).map(|values| (values, None))
    }
    async fn contains(&self, object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let __object_args = (object_id, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::contains(&self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}

impl crate::async_storage::AsyncObjectStore for PostPutObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            let cancellation = context.cancellation;
            return async {
                {
                    let __object_args = (object_id, bytes, original, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.2, __object_args.3);
                    let __object_result = crate::async_storage::AsyncObjectStore::put(
                        &*self.inner,
                        __object_args.0,
                        __object_args.1,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }?;
                if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
                Ok(())
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            let receipt = {
                let __object_args = (object_id, bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::put(
                    &*self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }?;
            if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
            Ok(receipt)
        }
        .await;
        context.accept(result)
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            let cancellation = context.cancellation;
            return async {
                {
                    let __object_args = (writes, original, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.1, __object_args.2);
                    let __object_result = crate::async_storage::AsyncObjectStore::put_many(
                        &*self.inner,
                        __object_args.0,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }?;
                if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
                Ok(())
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            let receipt = {
                let __object_args = (writes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::put_many(&*self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }?;
            if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
            Ok(receipt)
        }
        .await;
        context.accept(result)
    }
    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            let cancellation = context.cancellation;
            return async {
                {
                    let __object_args = (object_id, maximum_bytes, original, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.2, __object_args.3);
                    let __object_result = AsyncObjectStore::read(
                        &*self.inner,
                        __object_args.0,
                        __object_args.1,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }
            }
            .await;
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let __object_args = (object_id, maximum_bytes, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = AsyncObjectStore::read(
                    &*self.inner,
                    __object_args.0,
                    __object_args.1,
                    &mut __object_work,
                )
                .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            let cancellation = context.cancellation;
            return async {
                {
                    let __object_args = (requests, original, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.1, __object_args.2);
                    let __object_result = AsyncObjectStore::read_many(
                        &*self.inner,
                        __object_args.0,
                        &mut __object_work,
                    )
                    .await
                    .map(|(values, allocation)| {
                        (values, allocation.expect("native batch allocation"))
                    });
                    __object_result
                }
            }
            .await
            .map(|(values, allocation)| (values, Some(allocation)));
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let __object_args = (requests, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::read_many(&*self.inner, __object_args.0, &mut __object_work)
                        .await
                        .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result).map(|values| (values, None))
    }
    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let __object_args = (object_id, budget, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result =
                    AsyncObjectStore::contains(&*self.inner, __object_args.0, &mut __object_work)
                        .await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }

    async fn put_hashed(
        &self,
        object: HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            let cancellation = context.cancellation;
            return async {
                {
                    let __object_args = (object, original, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.1, __object_args.2);
                    let __object_result = crate::async_storage::AsyncObjectStore::put_hashed(
                        &*self.inner,
                        __object_args.0,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
            }
            .await;
        }
        let (id, bytes) = object.into_parts();
        self.put(id, bytes, context).await
    }
}

impl crate::AsyncAuthorityStore for PostAppendAuthorityStore {
async fn create_authority<G: crate::AuthorityGuard>(&self, authority: crate::AuthorityId, epoch: crate::Epoch, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
let value = crate::AsyncAuthorityStore::create_authority(self.inner.as_ref(), authority, epoch, work, None::<&mut ()>).await.map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: crate::GuardedAppend, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
if request.permit != crate::PublicationPermit::Unrestricted { return Err(crate::AuthorityStoreError::Rejected("authority backend cannot atomically evaluate operation leases".to_owned()).into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
let value = crate::AsyncAuthorityStore::compare_and_append(self.inner.as_ref(), request, work, None::<&mut ()>).await.map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
if matches!(&value.0, crate::AppendOutcome::Committed(_)) && self.control.should_fail() {
    return Err(crate::AuthorityStoreError::Indeterminate { operation: "compare-and-append", source: std::io::Error::other("deterministic acknowledgement loss") }.into());
}
Ok(value)
}
async fn head(&self, authority: crate::AuthorityId, work: &mut crate::AuthorityWork<'_>) -> Result<crate::Head, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::head(self.inner.as_ref(), authority, work).await
}
async fn replay(&self, authority: crate::AuthorityId, after: crate::Sequence, limit: crate::ReplayLimit, work: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::replay(self.inner.as_ref(), authority, after, limit, work).await
}
async fn fence(&self, authority: crate::AuthorityId, expected: crate::Head, work: &mut crate::AuthorityWork<'_>) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::fence(self.inner.as_ref(), authority, expected, work).await
}
async fn find_operation(&self, authority: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError,
    > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
crate::AsyncAuthorityStore::find_operation(self.inner.as_ref(), authority, operation, work).await
}
}

async fn sparse_rebase_across_remote_ranges(native: bool)
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([86; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("file")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (seed.commit(OperationId::from_bytes([87; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (local.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"LOCAL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"REMOTE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.commit(OperationId::from_bytes([88; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    #[cfg(not(target_arch = "wasm32"))]
    if native {
        let before = (
            local.generation_id(), local.root.file_table, local.base_file_table,
            local.authority_head, local.authored_operation_id, local.live_operation_id,
            local.dependencies.proof().dependencies.clone(),
        );
        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        assert!((local.rebase_head_with_native_work(8, &denied, &cancellation)).await.is_err());
        assert_eq!(before, (
            local.generation_id(), local.root.file_table, local.base_file_table,
            local.authority_head, local.authored_operation_id, local.live_operation_id,
            local.dependencies.proof().dependencies.clone(),
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    #[cfg(not(target_arch = "wasm32"))]
    let safe = if native {
        let result = (local.rebase_head_with_native_work(8, &original, &cancellation)).await?;
        (result.0, Some(result.1))
    } else {
        ((local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?.value, None)
    };
    #[cfg(target_arch = "wasm32")]
    let safe = ((local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?.value, ());
    assert!(matches!(safe.0, RebaseDecision::Safe { .. }));
    let local_bytes = (local.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&local_bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&local_bytes.value.bytes[64..70], b"REMOTE");

    let mut overlapping_local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut overlapping_remote = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (overlapping_local.change_content(file.clone(), crate::ContentChange::Write { offset: 10, bytes: (bytes::Bytes::from_static(b"local")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping_remote.change_content(file, crate::ContentChange::Write { offset: 12, bytes: (bytes::Bytes::from_static(b"remote")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping_remote.commit(OperationId::from_bytes([89; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let unchanged = (
        overlapping_local.generation_id(), overlapping_local.root.file_table,
        overlapping_local.base_file_table, overlapping_local.authority_head,
        overlapping_local.authored_operation_id, overlapping_local.live_operation_id,
        overlapping_local.dependencies.proof().dependencies.clone(),
    );
    #[cfg(not(target_arch = "wasm32"))]
    let conflict = if native {
        let result = (overlapping_local.rebase_head_with_native_work(8, &original, &cancellation)).await?;
        assert!(result.1.work().shares_ledger(&original));
        assert!(result.1.bytes() >= (std::mem::size_of::<crate::kernel::RebaseConflict>()) as u64);
        (result.0, Some(result.1))
    } else {
        ((overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?.value, None)
    };
    #[cfg(target_arch = "wasm32")]
    let conflict = ((overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?.value, ());
    assert_eq!(unchanged, (
        overlapping_local.generation_id(), overlapping_local.root.file_table,
        overlapping_local.base_file_table, overlapping_local.authority_head,
        overlapping_local.authored_operation_id, overlapping_local.live_operation_id,
        overlapping_local.dependencies.proof().dependencies.clone(),
    ));
    assert!(matches!(
        conflict.0,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_authored_creates(
    requests: &[(&NamespacePath, OwnedBytes)],
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<AuthoredMutation>, crate::NativeAllocation), FsError> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let mut accounting = KernelAccounting::native(original, cancellation);
    let mut input = crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(requests.len(), &mut accounting)?;
    for (requested, content) in requests {
        let mut path = clone_authored_path(requested, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<AuthoredMutation>() as u64, ..WorkCounters::UNCHARGED
        })?;
        let bytes = input.bytes.checked_add(path.bytes).ok_or(WorkError::Overflow)?;
        input.allocation.absorb(&mut path.allocation)?;
        input.value.push(AuthoredMutation::CreateFile { path: path.value, bytes: content.clone().into(), metadata, });
        input.bytes = bytes;
    }
    let KernelOwnedAllocation::NativeOriginal(allocation) = input.allocation else {
        unreachable!("original producer keeps its original guard")
    };
    Ok((input.value, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
fn original_borrowed_path_changes(
    source: &NamespacePath,
    destination: &NamespacePath,
    removed: &NamespacePath,
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<AuthoredMutation>, crate::NativeAllocation, crate::NativeAllocation), FsError,
> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let mut accounting = KernelAccounting::native(original, cancellation);
    let input = crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(4, &mut accounting)?;
    let mut source_allocation = accounting.claim_owned(0, 0)?;
    let mut copy_path = |requested: &NamespacePath| -> Result<NamespacePath, FsError> {
        let mut path = clone_authored_path(requested, &mut accounting)?;
        source_allocation.absorb(&mut path.allocation)?;
        Ok(path.value)
    };
    let metadata_path = copy_path(source)?;
    let resize_path = copy_path(source)?;
    let rename_source = copy_path(source)?;
    let rename_destination = copy_path(destination)?;
    let remove_path = copy_path(removed)?;
    accounting.charge(WorkCounters {
        bytes_copied: (4 * size_of::<AuthoredMutation>()) as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let crate::kernel::Owned { mut value, allocation, .. } = input;
    value.push(AuthoredMutation::SetMetadata { path: metadata_path, metadata });
    value.push(AuthoredMutation::Resize { path: resize_path, logical_bytes: 4 });
    value.push(AuthoredMutation::Rename {
        source: rename_source, destination: rename_destination, replace: true,
    });
    value.push(AuthoredMutation::Remove { path: remove_path, expected_file_id: None });
    let KernelOwnedAllocation::NativeOriginal(slots) = allocation else {
        unreachable!("native producer retains original slots")
    };
    let KernelOwnedAllocation::NativeOriginal(source) = source_allocation else {
        unreachable!("native producer retains original paths")
    };
    Ok((value, slots, source))
}

async fn assert_authority_volume_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let generation_root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([221; 32]),
    };
    let expected_volume = VolumeId::from_bytes([222; 16]);
    let wrong_volume = VolumeId::from_bytes([223; 16]);
    let creation = DurableCommit {
        settled_at_micros: None,
        epoch: Epoch::new(1)?,
        sequence: Sequence::new(1),
        operation_id: OperationId::from_bytes([224; 16]),
        fingerprint: Digest::from_bytes([225; 32]),
        previous_digest: Digest::ZERO,
        digest: Digest::from_bytes([226; 32]),
        payload: bytes::Bytes::from(crate::kernel::encode_volume_created(VolumeCreated {
            volume_id: wrong_volume,
            config: config(),
            initial_generation_root: generation_root,
        }, |_| Ok::<_, crate::kernel::VolumeCreatedError>(()), |_| {
                crate::kernel::VolumeCreatedError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed)
            },
        )?),
    };
    assert!(matches!(
        generation_from_record(&creation, expected_volume, &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())),
        Err(FsError::VolumeMismatch)
    ));
    let publication = DurableCommit {
        sequence: Sequence::new(2),
        payload: {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"acyclic-fs-publish-generation-v1\0");
            bytes.extend_from_slice(&1_u16.to_le_bytes());
            bytes.extend_from_slice(&wrong_volume.into_bytes());
            bytes.push(generation_root.kind.canonical_tag());
            bytes.extend_from_slice(generation_root.digest.as_bytes());
            bytes::Bytes::from(bytes)
        },
        ..creation
    };
    assert!(matches!(
        generation_from_record(&publication, expected_volume, &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())),
        Err(FsError::VolumeMismatch)
    ));

    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        Arc::clone(&authority),
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let expected_authority = volume_authority_id(expected_volume);
    let active =({ let __authority_args = (expected_authority, Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =  AsyncAuthorityStore::create_authority(&*authority, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let head = match active.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let encoded = crate::kernel::encode(WorkBudget::UNBOUNDED, Some(&CancellationToken::new()), 0, |observe| {
            crate::kernel::encode_volume_created(VolumeCreated {
        volume_id: wrong_volume,
        config: config(),
        initial_generation_root: generation_root,
    }, observe, |_| {
                    VolumeCreatedError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed)
                },
            )
        },
    )?;
    let (proposal, _) = creation_commit(
        OperationId::from_bytes([227; 16]), encoded,
        WorkBudget::UNBOUNDED, &CancellationToken::new(),
    )?.value;({ let __authority_args = (expected_authority, head.epoch, Head::genesis(head.epoch), proposal, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result =
    AsyncAuthorityStore::compare_and_append(&*authority, crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
    let cancellation = CancellationToken::new();
    assert!(matches!(
        (fs.read_creation_record(expected_volume, WorkBudget::UNBOUNDED, &cancellation)).await,
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let empty_volume = VolumeId::from_bytes([228; 16]);({ let __authority_args = (volume_authority_id(empty_volume), Epoch::GENESIS, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
    AsyncAuthorityStore::create_authority(&*authority, __authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let empty = Volume {
        fs,
        id: empty_volume,
        config: config(),
    };
    assert!(matches!(
        (empty.resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)).await,
        Err(OperationFailure {
            error: FsError::EmptyAuthority,
            ..
        })
    ));
    Ok(())
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

#[path = "facade/adoption.rs"]
mod adoption;
#[path = "facade/attributes.rs"]
mod attributes;
#[path = "facade/content.rs"]
mod content;
#[path = "facade/creation.rs"]
mod creation;
#[path = "facade/detached.rs"]
mod detached;
#[path = "facade/fault_creation.rs"]
mod fault_creation;
#[path = "facade/fault_mutations.rs"]
mod fault_mutations;
#[path = "facade/fault_publication.rs"]
mod fault_publication;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[path = "facade/heterogeneous_mount.rs"]
mod heterogeneous_mount;
#[path = "facade/identity.rs"]
mod identity;
#[path = "facade/live.rs"]
mod live;
#[path = "facade/local.rs"]
mod local;
#[path = "facade/merge.rs"]
mod merge;
#[path = "facade/paths.rs"]
mod paths;
#[path = "facade/profiles.rs"]
mod profiles;
#[path = "facade/publication.rs"]
mod publication;
#[path = "facade/read.rs"]
mod read;
#[path = "facade/selection.rs"]
mod selection;
#[path = "facade/special.rs"]
mod special;
#[path = "facade/tracking.rs"]
mod tracking;
#[path = "facade/transactions.rs"]
mod transactions;
#[path = "facade/transfer.rs"]
mod transfer;
