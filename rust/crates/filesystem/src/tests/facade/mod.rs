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
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            let failure = Self::failure(false);
            work.record(*failure.work)?;
            return Err(failure.error.into());
        }
        let value = crate::AsyncAuthorityStore::create_authority(
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
        if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::AppendOutcome, crate::AuthorityError<G::Error>>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        if request.permit != crate::PublicationPermit::Unrestricted {
            return Err(crate::AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            let failure = Self::failure(false);
            work.record(*failure.work)?;
            return Err(failure.error.into());
        }
        crate::AsyncAuthorityStore::compare_and_append(&self.inner, request, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
    }
    async fn head(
        &self,
        authority: crate::AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::Head, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            let failure = Self::failure(true);
            work.record(*failure.work)?;
            return Err(failure.error.into());
        }
        crate::AsyncAuthorityStore::head(&self.inner, authority, work).await
    }
    async fn replay(
        &self,
        authority: crate::AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            let failure = Self::failure(true);
            work.record(*failure.work)?;
            return Err(failure.error.into());
        }
        crate::AsyncAuthorityStore::replay(&self.inner, authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: crate::AuthorityId,
        expected: crate::Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            let failure = Self::failure(false);
            work.record(*failure.work)?;
            return Err(failure.error.into());
        }
        crate::AsyncAuthorityStore::fence(&self.inner, authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        Option<crate::DurableCommit>,
        crate::AuthorityStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        if self.control.should_fail() {
            let failure = Self::failure(true);
            work.record(*failure.work)?;
            return Err(failure.error.into());
        }
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
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put(&self.inner, object_id, bytes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put_many(&self.inner, writes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
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
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            if self.control.should_fail() {
                return Err(Self::failure(true));
            }
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = AsyncObjectStore::read(
                    &self.inner,
                    object_id,
                    maximum_bytes,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::read_many(&self.inner, requests, &mut __object_work)
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
            if self.control.should_fail() {
                return Err(Self::failure(true));
            }
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::contains(&self.inner, object_id, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
        }
        .await;
        context.accept(result)
    }
}

impl crate::async_storage::AsyncObjectStore for CountingObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put(&self.inner, object_id, bytes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put_many(&self.inner, writes, &mut __object_work).await;
                __object_work.receipt(__object_result)
            }
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
        if context.original.is_some() {
            return Err(crate::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            self.record(object_id)?;
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = AsyncObjectStore::read(
                    &self.inner,
                    object_id,
                    maximum_bytes,
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::read_many(&self.inner, requests, &mut __object_work)
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::contains(&self.inner, object_id, &mut __object_work).await;
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
                    let mut __object_work = crate::AuthorityWork::native(original, cancellation);
                    let __object_result = crate::async_storage::AsyncObjectStore::put(
                        &*self.inner,
                        object_id,
                        bytes,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }?;
                if self.control.should_fail() {
                    return Err(ObjectStoreError::Corrupt);
                }
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put(&*self.inner, object_id, bytes, &mut __object_work).await;
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
                    let mut __object_work = crate::AuthorityWork::native(original, cancellation);
                    let __object_result = crate::async_storage::AsyncObjectStore::put_many(
                        &*self.inner,
                        writes,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }?;
                if self.control.should_fail() {
                    return Err(ObjectStoreError::Corrupt);
                }
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::put_many(&*self.inner, writes, &mut __object_work).await;
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
                    let mut __object_work = crate::AuthorityWork::native(original, cancellation);
                    let __object_result = AsyncObjectStore::read(
                        &*self.inner,
                        object_id,
                        maximum_bytes,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }
            }
            .await;
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation.check().map_err(|_| {
                crate::ObjectFailure::before_work(crate::ObjectStoreError::Cancelled)
            })?;
            {
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result = AsyncObjectStore::read(
                    &*self.inner,
                    object_id,
                    maximum_bytes,
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
                    let mut __object_work = crate::AuthorityWork::native(original, cancellation);
                    let __object_result =
                        AsyncObjectStore::read_many(&*self.inner, requests, &mut __object_work)
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::read_many(&*self.inner, requests, &mut __object_work)
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
                let mut __object_work = crate::AuthorityWork::new(budget, cancellation);
                let __object_result =
                    AsyncObjectStore::contains(&*self.inner, object_id, &mut __object_work).await;
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
                    let mut __object_work = crate::AuthorityWork::native(original, cancellation);
                    let __object_result = crate::async_storage::AsyncObjectStore::put_hashed(
                        &*self.inner,
                        object,
                        &mut __object_work,
                    )
                    .await;
                    __object_result
                }?;
                if self.control.should_fail() {
                    return Err(ObjectStoreError::Corrupt);
                }
                Ok(())
            }
            .await;
        }
        let (id, bytes) = object.into_parts();
        self.put(id, bytes, context).await
    }
}

impl crate::AsyncAuthorityStore for PostAppendAuthorityStore {
    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: crate::AuthorityId,
        epoch: crate::Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        let value = crate::AsyncAuthorityStore::create_authority(
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
        if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        }
        Ok(value)
    }
    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<crate::AppendOutcome, crate::AuthorityError<G::Error>>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        if request.permit != crate::PublicationPermit::Unrestricted {
            return Err(crate::AuthorityStoreError::Rejected(
                "authority backend cannot atomically evaluate operation leases".to_owned(),
            )
            .into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        let value = crate::AsyncAuthorityStore::compare_and_append(
            self.inner.as_ref(),
            request,
            work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
        if matches!(&value.0, crate::AppendOutcome::Committed(_)) && self.control.should_fail() {
            return Err(crate::AuthorityStoreError::Indeterminate {
                operation: "compare-and-append",
                source: std::io::Error::other("deterministic acknowledgement loss"),
            }
            .into());
        }
        Ok(value)
    }
    async fn head(
        &self,
        authority: crate::AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::Head, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        crate::AsyncAuthorityStore::head(self.inner.as_ref(), authority, work).await
    }
    async fn replay(
        &self,
        authority: crate::AuthorityId,
        after: crate::Sequence,
        limit: crate::ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError>
    {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        crate::AsyncAuthorityStore::replay(self.inner.as_ref(), authority, after, limit, work).await
    }
    async fn fence(
        &self,
        authority: crate::AuthorityId,
        expected: crate::Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        crate::AsyncAuthorityStore::fence(self.inner.as_ref(), authority, expected, work).await
    }
    async fn find_operation(
        &self,
        authority: crate::AuthorityId,
        operation: crate::OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<
        Option<crate::DurableCommit>,
        crate::AuthorityStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        crate::AsyncAuthorityStore::find_operation(self.inner.as_ref(), authority, operation, work)
            .await
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
