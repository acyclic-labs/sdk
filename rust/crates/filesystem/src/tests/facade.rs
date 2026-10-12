use super::*;
use acyclic_stream::OwnedBytes;
use crate::async_storage::{poll_immediate};
use crate::kernel::{InlineFileData, LogicalName, NameEncoding, MAXIMUM_GENERATION_ROOT_BYTES};
use crate::memory::MemoryObjectStore as TestMemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, UnicodePolicy, VolumeLimits,
};
use crate::storage::CreateAuthorityOutcome;
use crate::storage::object_digest;
use crate::storage::{AuthorityFailure, AuthorityResult, AuthorityStore};
use crate::storage::{
    ObjectFailure, ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore, ObjectWrite,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-identity-candidate", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/source", "actual source bytes").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout(
        GenerationSelector::Head, CheckoutMode::tracking_transaction(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let path = path("source")?;
    let source = checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual source record absent")?;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(
        &original, &cancellation,
    )?;
    let paid = |mutation| -> Result<(Vec<Mutation>, crate::NativeAllocation), FsError> {
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
        })?;
        operations.value.push(Mutation::File { file_id: source.file_id, mutation });
        let KernelOwnedAllocation::NativeOriginal(allocation) = operations.allocation else {
            unreachable!("native context returns original input custody")
        };
        Ok((operations.value, allocation))
    };
    let input = paid(FileMutation::Resize { logical_bytes: 0 })?;
    candidate.mutate_with_native_work(input.0, input.1, &original, &cancellation).await?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let current = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, candidate.root.file_table, source.file_id,
        DecodeLimits::for_volume(candidate.volume.config), &mut accounting, &cancellation, None,
    ).await?;
    let current = current.0.ok_or("mutated identity disappeared")?;
    assert_eq!(current.file_id, source.file_id);
    assert_ne!(current.payload, source.payload);
    assert!(candidate.root.same_original_scope(original.native_read_meter()));
    assert_eq!(candidate.base_file_table, checkout.root.file_table);
    assert_eq!(checkout.lookup_no_follow(
        &path, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record, Some(source));
    let before = (
        candidate.root.file_table, candidate.base_file_table,
        candidate.authored_operation_id, candidate.live_operation_id,
    );
    let baseline = original.live_allocation_bytes();
    let input = paid(FileMutation::Resize { logical_bytes: 7 })?;
    let before_io = original.receipt();
    cancellation.cancel();
    let failure = candidate.mutate_with_native_work(
        input.0, input.1, &original, &cancellation,
    ).await.err().ok_or("cancelled mutation changed candidate")?;
    assert!(matches!(failure,
        FsError::Cancelled(_) |
        FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled)));
    assert_eq!(before, (
        candidate.root.file_table, candidate.base_file_table,
        candidate.authored_operation_id, candidate.live_operation_id,
    ));
    assert_eq!(original.receipt().backend_read_operations, before_io.backend_read_operations);
    assert_eq!(original.receipt().backend_write_operations, before_io.backend_write_operations);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn prepared_candidate_adoption_rejects_new_observations_and_changed_base_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("candidate-adoption-seal", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let mut live = workspace.checkout(
        GenerationSelector::Head, tracking(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let base = live.private_candidate(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded private_candidate preserves a validated checkout").value;
    let mut candidate = base.private_candidate(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded private_candidate preserves a validated checkout").value;
    let prepared = live.prepare_candidate_adoption(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?;
    assert_eq!(prepared.work.allocation_operations, 0, "identical proof is only shared");
    assert_eq!(prepared.value.as_ref().ok_or("shared proof rejected")?.retained_bytes(), 0);
    live.lookup_no_follow(&path("later-reader")?, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert!(!live.adopt_prepared_candidate(
        &base, candidate.private_candidate(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded private_candidate preserves a validated checkout").value, prepared.value.ok_or("shared proof rejected")?,
    ));
    let prepared = live.prepare_candidate_adoption(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("reader observations rejected")?;
    candidate.lookup_no_follow(&path("later-candidate")?, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert!(!live.adopt_prepared_candidate(&base, candidate.private_candidate(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded private_candidate preserves a validated checkout").value, prepared));
    let prepared = live.prepare_candidate_adoption(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.ok_or("compatible candidate rejected")?;
    live.live_operation_id = Some(OperationId::from_bytes([209; 16]));
    assert!(!live.adopt_prepared_candidate(&base, candidate.private_candidate(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded private_candidate preserves a validated checkout").value, prepared));
    assert!(live.prepare_candidate_adoption(
        &base, &candidate, WorkBudget::UNBOUNDED, &cancellation,
    )?.value.is_none());
    assert!(!live.admits_candidate(&base, &candidate));
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod heterogeneous_mount {
    use super::*;
    use crate::foundation::{AuthorityId, Epoch, Head, OperationId, ProposedCommit, Sequence};
    use crate::model::VolumeLimits;
    use crate::storage::{
        AppendOutcome, AuthorityResult, CreateAuthorityOutcome, FenceOutcome, ObjectId, ObjectRead,
        ObjectReadRequest, ObjectResult, ObjectWrite, ReplayLimit,
    };
    use crate::{
        AsyncAuthorityStore, AsyncObjectStore, EmbeddedCapabilities, LocalFs, LocalOptions,
        MemoryFs,
    };
    use acyclic_stream::OwnedBytes;
    use std::sync::Arc;

    #[derive(Clone)]
    enum Stores {
        Memory(MemoryFs),
        Local(LocalFs),
    }

    macro_rules! delegate {
        ($self:ident, $trait:ident, $field:ident, $method:ident($($argument:expr),*)) => {
            match $self {
                Stores::Memory(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
                Stores::Local(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
            }
        };
    }
    impl AsyncAuthorityStore for Stores {
        async fn create_authority(
            &self,
            authority: AuthorityId,
            epoch: Epoch,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<CreateAuthorityOutcome> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                create_authority(authority, epoch, budget, cancellation)
            )
        }
        async fn head(
            &self,
            authority: AuthorityId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<Head> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                head(authority, budget, cancellation)
            )
        }
        async fn compare_and_append(
            &self,
            authority: AuthorityId,
            epoch: Epoch,
            expected: Head,
            commit: ProposedCommit,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<AppendOutcome> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                compare_and_append(authority, epoch, expected, commit, budget, cancellation)
            )
        }
        async fn replay(
            &self,
            authority: AuthorityId,
            after: Sequence,
            limit: ReplayLimit,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<Vec<crate::foundation::DurableCommit>> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                replay(authority, after, limit, budget, cancellation)
            )
        }
        async fn fence(
            &self,
            authority: AuthorityId,
            expected: Head,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<FenceOutcome> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                fence(authority, expected, budget, cancellation)
            )
        }
        async fn find_operation(
            &self,
            authority: AuthorityId,
            operation: OperationId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<Option<crate::foundation::DurableCommit>> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                find_operation(authority, operation, budget, cancellation)
            )
        }
        async fn fork_generation_authority(
            &self,
            source: crate::async_storage::GenerationForkSource,
            destination: AuthorityId,
            operation: OperationId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> AuthorityResult<CreateAuthorityOutcome> {
            delegate!(
                self,
                AsyncAuthorityStore,
                authority,
                fork_generation_authority(source, destination, operation, budget, cancellation)
            )
        }
        fn supports_generation_lineage_prefix(&self) -> bool {
            match self {
                Self::Memory(fs) => {
                    AsyncAuthorityStore::supports_generation_lineage_prefix(&fs.inner.authority)
                }
                Self::Local(fs) => {
                    AsyncAuthorityStore::supports_generation_lineage_prefix(&fs.inner.authority)
                }
            }
        }
    }

    impl AsyncObjectStore for Stores {
        async fn put(
            &self,
            id: ObjectId,
            bytes: OwnedBytes,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<()> {
            delegate!(
                self,
                AsyncObjectStore,
                objects,
                put(id, bytes, budget, cancellation)
            )
        }
        async fn put_many(
            &self,
            writes: &[ObjectWrite],
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<()> {
            delegate!(
                self,
                AsyncObjectStore,
                objects,
                put_many(writes, budget, cancellation)
            )
        }
        async fn read(
            &self,
            id: ObjectId,
            maximum: u64,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<ObjectRead> {
            delegate!(
                self,
                AsyncObjectStore,
                objects,
                read(id, maximum, budget, cancellation)
            )
        }
        async fn read_many(
            &self,
            requests: &[ObjectReadRequest],
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<Vec<ObjectRead>> {
            delegate!(
                self,
                AsyncObjectStore,
                objects,
                read_many(requests, budget, cancellation)
            )
        }
        async fn contains(
            &self,
            id: ObjectId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<bool> {
            delegate!(
                self,
                AsyncObjectStore,
                objects,
                contains(id, budget, cancellation)
            )
        }
        async fn flush_before_publish(
            &self,
            scope: crate::PublicationScope<'_>,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<crate::PublicationHold> {
            delegate!(
                self,
                AsyncObjectStore,
                objects,
                flush_before_publish(scope, budget, cancellation)
            )
        }
        fn collection(&self) -> Option<&Arc<crate::Collection>> {
            match self {
                Self::Memory(_) => None,
                Self::Local(fs) => fs.inner.objects.collection(),
            }
        }
    }

    #[tokio::test]
    async fn mounted_view_routes_memory_and_local_objects_through_existing_traits()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let local = Fs::local(LocalOptions::new(directory.path())).await?;
        let memory_store = Stores::Memory(Fs::memory());
        let local_store = Stores::Local(local);
        let memory = Fs::new(
            memory_store.clone(),
            memory_store,
            EmbeddedCapabilities::MEMORY,
        );
        let disk = Fs::new(
            local_store.clone(),
            local_store,
            EmbeddedCapabilities::MEMORY,
        );
        let root = memory.create_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let scratch = disk.create_workspace("scratch", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        root.write_text("/file", "memory").await?;
        scratch.write_text("/file", "disk").await?;
        let root_head = root.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value;
        let scratch_head = scratch.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value;
        assert!(matches!(
            scratch
                .fork(
                    "foreign",
                    crate::ForkOptions::from_generation(
                        root_head.clone(),
                        crate::IdempotencyKey::from_bytes([60; 16]),
                    ),
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error), Err(crate::WorkspaceError::ForeignGeneration)
        ));
        assert!(matches!(
            root.fork(
                "foreign",
                crate::ForkOptions::from_generation(
                    scratch_head.clone(),
                    crate::IdempotencyKey::from_bytes([61; 16]),
                ),
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error), Err(crate::WorkspaceError::ForeignGeneration)
        ));
        for (workspace, head) in [(&root, &root_head), (&scratch, &scratch_head)] {
            let fork = workspace
                .fork(
                    "child",
                    crate::ForkOptions::from_generation(
                        head.clone(),
                        crate::IdempotencyKey::from_bytes([62; 16]),
                    ),
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            fork.write_text("/file", "child only").await?;
            assert_eq!(workspace.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value.id(), head.id());
            assert_ne!(fork.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value.id(), head.id());
        }
        let mut view = crate::MountedView::builder()
            .mount(
                "/",
                root.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await?.value,
            )?
            .mount(
                "/.scratch",
                scratch
                    .checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await?.value,
            )?
            .build()?;
        let cancellation = CancellationToken::new();
        let root_path = crate::path::PortablePath::parse("/file", VolumeLimits::default())?;
        let scratch_path =
            crate::path::PortablePath::parse("/.scratch/file", VolumeLimits::default())?;
        for path in [&root_path, &scratch_path] {
            let route = view.route_mut(path)?;
            assert!(
                route
                    .checkout
                    .lookup_no_follow(&route.path, WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .record
                    .is_some()
            );
        }
        assert_eq!(view.snapshot().bindings.len(), 2);
        assert_eq!(
            view.resolve(&root_path)
                .ok_or("memory route")?
                .checkout()
                .generation_id(),
            root_head.id()
        );
        assert_eq!(
            view.resolve(&scratch_path)
                .ok_or("local route")?
                .checkout()
                .generation_id(),
            scratch_head.id()
        );
        assert_eq!(
            view.validate_rename(&root_path, &scratch_path),
            Err(crate::MountError::CrossVolume)
        );
        assert_eq!(root.read("/file", 32, WorkBudget::UNBOUNDED, &cancellation).await?.value.as_ref(), b"memory");
        assert_eq!(scratch.read("/file", 32, WorkBudget::UNBOUNDED, &cancellation).await?.value.as_ref(), b"disk");
        Ok(())
    }
}

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

impl AuthorityStore for FaultAuthorityStore {
    fn create_authority(&self, authority_id: crate::foundation::AuthorityId, genesis_epoch: Epoch, budget: WorkBudget) -> AuthorityResult<CreateAuthorityOutcome> {
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::create_authority(&self.inner, authority_id, genesis_epoch, budget)
    }

    fn head(&self, authority_id: crate::foundation::AuthorityId, budget: WorkBudget) -> AuthorityResult<Head> {
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::head(&self.inner, authority_id, budget)
    }

    fn compare_and_append(&self, authority_id: crate::foundation::AuthorityId, epoch: Epoch, expected: Head, commit: ProposedCommit, budget: WorkBudget) -> AuthorityResult<AppendOutcome> {
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::compare_and_append(&self.inner, authority_id, epoch, expected, commit, budget)
    }

    fn replay(&self, authority_id: crate::foundation::AuthorityId, after: Sequence, limit: ReplayLimit, budget: WorkBudget) -> AuthorityResult<Vec<DurableCommit>> {
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::replay(&self.inner, authority_id, after, limit, budget)
    }

    fn fence(&self, authority_id: crate::foundation::AuthorityId, expected: Head, budget: WorkBudget) -> AuthorityResult<FenceOutcome> {
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        AuthorityStore::fence(&self.inner, authority_id, expected, budget)
    }

    fn find_operation(&self, authority_id: crate::foundation::AuthorityId, operation_id: OperationId, budget: WorkBudget) -> AuthorityResult<Option<DurableCommit>> {
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        AuthorityStore::find_operation(&self.inner, authority_id, operation_id, budget)
    }
}

impl crate::async_storage::ImmediateAuthorityStore for FaultAuthorityStore {}

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

impl ObjectStore for FaultObjectStore {
    fn put(&self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget) -> ObjectResult<()> {
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    fn put_many(&self, writes: &[ObjectWrite], budget: WorkBudget) -> ObjectResult<()> {
        if self.control.should_fail() {
            return Err(Self::failure(false));
        }
        ObjectStore::put_many(&self.inner, writes, budget)
    }

    fn read(&self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget) -> ObjectResult<ObjectRead> {
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
    }

    fn read_many(&self, requests: &[ObjectReadRequest], budget: WorkBudget) -> ObjectResult<Vec<ObjectRead>> {
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool> {
        if self.control.should_fail() {
            return Err(Self::failure(true));
        }
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

impl crate::async_storage::ImmediateObjectStore for FaultObjectStore {}

impl ObjectStore for CountingObjectStore {
    fn put(&self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget) -> ObjectResult<()> {
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    fn put_many(&self, writes: &[ObjectWrite], budget: WorkBudget) -> ObjectResult<()> {
        ObjectStore::put_many(&self.inner, writes, budget)
    }

    fn read(&self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget) -> ObjectResult<ObjectRead> {
        self.record(object_id)?;
        ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
    }

    fn read_many(&self, requests: &[ObjectReadRequest], budget: WorkBudget) -> ObjectResult<Vec<ObjectRead>> {
        for request in requests {
            self.record(request.object_id)?;
        }
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool> {
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

impl crate::async_storage::ImmediateObjectStore for CountingObjectStore {}

impl ObjectStore for PostPutObjectStore {
    fn put(&self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget) -> ObjectResult<()> {
        let receipt = ObjectStore::put(&*self.inner, object_id, bytes, budget)?;
        if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
        Ok(receipt)
    }

    fn put_many(&self, writes: &[ObjectWrite], budget: WorkBudget) -> ObjectResult<()> {
        let receipt = ObjectStore::put_many(&*self.inner, writes, budget)?;
        if self.control.should_fail() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work));
        }
        Ok(receipt)
    }

    fn read(&self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget) -> ObjectResult<ObjectRead> {
        ObjectStore::read(&*self.inner, object_id, maximum_bytes, budget)
    }

    fn read_many(&self, requests: &[ObjectReadRequest], budget: WorkBudget) -> ObjectResult<Vec<ObjectRead>> {
        ObjectStore::read_many(&*self.inner, requests, budget)
    }

    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool> {
        ObjectStore::contains(&*self.inner, object_id, budget)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work(&self, object_id: ObjectId, maximum_bytes: u64, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<crate::storage::ObjectRead, ObjectStoreError> {
        ObjectStore::read_with_native_work(&*self.inner, object_id, maximum_bytes, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_many_with_native_work(&self, requests: &[ObjectReadRequest], original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<(Vec<crate::storage::ObjectRead>, crate::materializer::NativeAllocation), ObjectStoreError> {
        ObjectStore::read_many_with_native_work(&*self.inner, requests, original, cancellation)
    }
}

impl crate::async_storage::ImmediateObjectStore for PostPutObjectStore {
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        crate::async_storage::ImmediateObjectStore::put_with_native_work(&*self.inner, object_id, bytes, original, cancellation)?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self, object: HashedObject,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        crate::async_storage::ImmediateObjectStore::put_hashed_with_native_work(&*self.inner, object, original, cancellation)?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self, writes: &[ObjectWrite],
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        crate::async_storage::ImmediateObjectStore::put_many_with_native_work(&*self.inner, writes, original, cancellation)?;
        if self.control.should_fail() { return Err(ObjectStoreError::Corrupt); }
        Ok(())
    }
}

impl AuthorityStore for PostAppendAuthorityStore {
    fn create_authority(&self, authority_id: crate::foundation::AuthorityId, genesis_epoch: Epoch, budget: WorkBudget) -> AuthorityResult<CreateAuthorityOutcome> {
        AuthorityStore::create_authority(&*self.inner, authority_id, genesis_epoch, budget)
    }

    fn head(&self, authority_id: crate::foundation::AuthorityId, budget: WorkBudget) -> AuthorityResult<Head> {
        AuthorityStore::head(&*self.inner, authority_id, budget)
    }

    fn compare_and_append(&self, authority_id: crate::foundation::AuthorityId, epoch: Epoch, expected: Head, commit: ProposedCommit, budget: WorkBudget) -> AuthorityResult<AppendOutcome> {
        let receipt = AuthorityStore::compare_and_append(&*self.inner, authority_id, epoch, expected, commit, budget)?;
        if matches!(receipt.value, AppendOutcome::Committed(_)) && self.control.should_fail() {
            return Err(AuthorityFailure::new(
                AuthorityStoreError::Indeterminate {
                    operation: "compare-and-append",
                    source: std::io::Error::other("deterministic acknowledgement loss"),
                },
                receipt.work,
            ));
        }
        Ok(receipt)
    }

    fn replay(&self, authority_id: crate::foundation::AuthorityId, after: Sequence, limit: ReplayLimit, budget: WorkBudget) -> AuthorityResult<Vec<DurableCommit>> {
        AuthorityStore::replay(&*self.inner, authority_id, after, limit, budget)
    }

    fn fence(&self, authority_id: crate::foundation::AuthorityId, expected: Head, budget: WorkBudget) -> AuthorityResult<FenceOutcome> {
        AuthorityStore::fence(&*self.inner, authority_id, expected, budget)
    }

    fn find_operation(&self, authority_id: crate::foundation::AuthorityId, operation_id: OperationId, budget: WorkBudget) -> AuthorityResult<Option<DurableCommit>> {
        AuthorityStore::find_operation(&*self.inner, authority_id, operation_id, budget)
    }
}

impl crate::async_storage::ImmediateAuthorityStore for PostAppendAuthorityStore {}

#[test]
fn every_object_backend_cut_preserves_facade_atomicity_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;

    macro_rules! through_every_object_cut {
        ($operation:expr) => {{
            let original_state = checkout_state(&checkout);
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    poll_immediate($operation);
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(checkout_state(&checkout), original_state);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "facade operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let large = path("fault-large")?;
    let large_id = through_every_object_cut!(checkout.create(crate::AuthoredMutation::CreateFile { path: large.clone(), bytes: (bytes::Bytes::from(vec![b'x'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    let inline = path("fault-inline")?;
    let inline_id = through_every_object_cut!(checkout.create(crate::AuthoredMutation::CreateFile { path: inline.clone(), bytes: (bytes::Bytes::from_static(b"inline")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    through_every_object_cut!(checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("fault-directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let authored = path("fault-authored-large")?;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: authored.clone(),
            bytes: OwnedBytes::from(vec![b'a'; 4_096]),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateDirectory {
            path: path("fault-authored-directory")?,
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateSymbolicLink {
            path: path("fault-authored-link")?,
            target: OwnedBytes::from_static(b"fault-authored-large"),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: authored.clone(),
            offset: 4_096,
            bytes: OwnedBytes::from_static(b"-fault-swept"),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::SetMetadata {
            path: authored.clone(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    control.disable();
    let mut staged_source = std::io::Cursor::new(OwnedBytes::from_static(b"staged-fault-content"));
    let staged = poll_immediate(checkout.stage_content(
        &mut staged_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    through_every_object_cut!(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: path("fault-authored-content")?,
            content: staged,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let range = ByteRange {
        offset: 1,
        length: 64,
    };
    let path_read = through_every_object_cut!(checkout.read_file_range(
        &large,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!((path_read.value.bytes).as_ref(), OwnedBytes::from(vec![b'x'; 64]).as_ref());
    let identity_read = through_every_object_cut!(checkout.read_file_range_by_id(
        large_id,
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_read.value.bytes, path_read.value.bytes);
    let path_plan = through_every_object_cut!(checkout.plan_file_extents(
        &large,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_plan = through_every_object_cut!(checkout.plan_file_extents_by_id(
        large_id,
        range,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        path_plan.value.as_ref().map(|plan| &plan.spans),
        identity_plan.value.as_ref().map(|plan| &plan.spans)
    );
    assert_eq!(
        path_plan.value.as_ref().map(|plan| plan.spans.len()),
        Some(1)
    );
    let data_seek = through_every_object_cut!(checkout.seek_file_extent(
        &large,
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(data_seek.value, Some(1));
    let hole_seek = through_every_object_cut!(checkout.seek_file_extent_by_id(
        large_id,
        1,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(hole_seek.value, Some(4_096));
    let path_metadata = through_every_object_cut!(checkout.read_metadata(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let identity_metadata = through_every_object_cut!(checkout.read_metadata_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_metadata.value, path_metadata.value);
    let identity_record = through_every_object_cut!(checkout.read_file_record_by_id(
        large_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_record.value.file_id, large_id);
    let batch = through_every_object_cut!(checkout.lookup_batch_no_follow(
        &[large.clone(), inline.clone(), path("fault-missing")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(batch.value.entries.len(), 3);
    assert!(batch.value.entries[0].record.is_some());
    assert!(batch.value.entries[1].record.is_some());
    assert!(batch.value.entries[2].record.is_none());
    let directory_names = through_every_object_cut!(checkout.list_directory(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory_names.value.entries.len(), 7);
    let directory = through_every_object_cut!(checkout.list_directory_records(
        &root_path()?,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(directory.value.entries.len(), 7);
    assert!(!directory.value.has_more);
    assert!(
        directory
            .value
            .entries
            .iter()
            .any(|entry| entry.record.file_id == large_id)
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.change_named_attribute(large.clone(), &attribute, Some((&bytes::Bytes::from_static(b"fault-checked-value"), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    let attribute_value = through_every_object_cut!(checkout.read_named_attribute(
        &large,
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        attribute_value.value.map(|chunk| chunk.bytes),
        Some(OwnedBytes::from_static(b"fault-checked-value"))
    );
    let attributes = through_every_object_cut!(checkout.list_named_attributes(
        &large,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(attributes.value.entries.len(), 1);
    assert_eq!(attributes.value.entries[0].name, attribute);
    assert!(!attributes.value.has_more);
    let identity_attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity-fault-cut".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    through_every_object_cut!(checkout.change_named_attribute_by_id(large_id, &identity_attribute, Some((&OwnedBytes::from_static(b"identity-fault-checked-value"), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(
        through_every_object_cut!(checkout.read_named_attribute_by_id(
            large_id,
            &identity_attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value.map(|chunk| chunk.bytes),
        Some(OwnedBytes::from_static(b"identity-fault-checked-value"))
    );
    let identity_attributes = through_every_object_cut!(checkout.list_named_attributes_by_id(
        large_id,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(identity_attributes.value.entries.len(), 2);
    through_every_object_cut!(checkout.change_named_attribute_by_id(large_id, &identity_attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_named_attribute(large.clone(), &attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::Write { offset: 128, bytes: (bytes::Bytes::from_static(b"atomic-retry")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(inline_id, crate::ContentChange::CloneFrom { source: large_id, source_offset: 0, offset: 0, length: 4 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let cloned = through_every_object_cut!(checkout.read_file_range_by_id(
        inline_id,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(cloned.value.bytes.as_ref(), b"xxxxne");
    let mut changed_metadata = FileMetadata::default();
    changed_metadata.modified_ns = MetadataField::Value(101);
    through_every_object_cut!(checkout.set_attributes_by_id(large_id, changed_metadata, None, WorkBudget::UNBOUNDED, &cancellation));
    changed_metadata.modified_ns = MetadataField::Value(102);
    through_every_object_cut!(checkout.set_attributes_by_id(
        large_id,
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4_096,
            length: 32,
        }, allocated: true, extend: true }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::Preallocate { range: ByteRange {
            offset: 4_160,
            length: 32,
        }, keep_size: true }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large_id, crate::ContentChange::Resize { logical_bytes: 4_192 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    changed_metadata.modified_ns = MetadataField::Value(103);
    through_every_object_cut!(checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: large.clone(), metadata: changed_metadata }], WorkBudget::UNBOUNDED, &cancellation));
    changed_metadata.modified_ns = MetadataField::Value(104);
    through_every_object_cut!(checkout.set_attributes(
        large.clone(),
        changed_metadata,
        Some(4_224),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_object_cut!(checkout.change_content(large.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4_096,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large.clone(), crate::ContentChange::Preallocate { range: ByteRange {
            offset: 4_176,
            length: 16,
        }, keep_size: true }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(large.clone(), crate::ContentChange::Resize { logical_bytes: 4_192 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    through_every_object_cut!(checkout.change_content(inline.clone(), ContentChange::CloneFrom { source: large.clone(), source_offset: 0, offset: 1, length: 3 }, ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let mut detached = through_every_object_cut!(checkout.detach_regular_file(
        &large,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    macro_rules! through_every_detached_cut {
        ($operation:expr) => {{
            let original_record = detached.record;
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    poll_immediate($operation);
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(
                            failure.work.backend_read_operations
                                + failure.work.backend_write_operations
                                > 0
                        );
                        assert_eq!(detached.record, original_record);
                    }
                }
            }
            control.disable();
            success.ok_or_else(|| {
                format!(
                    "detached operation never passed cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let detached_read = through_every_detached_cut!(detached.read_range(
        range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!((detached_read.value.bytes).as_ref(), OwnedBytes::from(vec![b'x'; 64]).as_ref());
    let detached_seek = through_every_detached_cut!(detached.seek(
        1,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_seek.value, Some(1));
    let mut detached_metadata =
        through_every_detached_cut!(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation,))
            .value;
    detached_metadata.modified_ns = MetadataField::Value(42);
    through_every_detached_cut!(detached.set_attributes(detached_metadata, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_detached_cut!(detached.change_named_attribute(&attribute, Some((&OwnedBytes::from_static(b"detached fault value"), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    let detached_attribute = through_every_detached_cut!(detached.read_named_attribute(
        &attribute,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        detached_attribute.value.map(|chunk| chunk.bytes),
        Some(OwnedBytes::from_static(b"detached fault value"))
    );
    let detached_attributes = through_every_detached_cut!(detached.list_named_attributes(
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(detached_attributes.value.entries.len(), 1);
    assert_eq!(detached_attributes.value.entries[0].name, attribute);
    through_every_detached_cut!(detached.change_content(
        ContentChange::Write {
            offset: 256,
            bytes: OwnedBytes::from_static(b"detached atomic retry")
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 512,
                length: 32,
            },
            allocated: true,
            extend: false
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 768,
                length: 32,
            },
            keep_size: false
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_content(
        ContentChange::Resize {
            logical_bytes: 2_048
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    through_every_detached_cut!(detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    through_every_detached_cut!(detached.set_attributes(
        detached_metadata,
        Some(1_024),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    Ok(())
}

#[test]
fn pinned_reader_batches_ranges_in_request_order() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(writer.create(crate::AuthoredMutation::CreateFile { path: path("first")?, bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(writer.create(crate::AuthoredMutation::CreateFile { path: path("second")?, bytes: (bytes::Bytes::from_static(b"01234567")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(writer.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (bytes::Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let first_directory = path("first-directory")?;
    let second_directory = path("second-directory")?;
    poll_immediate(writer.create(crate::AuthoredMutation::CreateDirectory { path: first_directory.clone(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(writer.create(crate::AuthoredMutation::CreateDirectory { path: second_directory.clone(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    for name in ["a", "b"] {
        poll_immediate(writer.create(crate::AuthoredMutation::CreateFile { path: path_parts(&["first-directory", name])?, bytes: (bytes::Bytes::from_static(b"x")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    }
    poll_immediate(writer.create(crate::AuthoredMutation::CreateFile { path: path_parts(&["second-directory", "c"])?, bytes: (bytes::Bytes::from_static(b"y")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let reader = writer.pinned_reader()?;
    let request_paths = [path("second")?, path("first")?];
    let resolved_batch =
        poll_immediate(reader.resolve_files(&request_paths, WorkBudget::UNBOUNDED, &cancellation))?;
    let second_file = resolved_batch.value[0]
        .as_ref()
        .ok_or("second resolved file absent")?;
    let first_file = resolved_batch.value[1]
        .as_ref()
        .ok_or("first resolved file absent")?;
    let requests = [
        ResolvedFileRangeReadRequest {
            file: second_file,
            range: ByteRange {
                offset: 2,
                length: 3,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first_file,
            range: ByteRange {
                offset: 1,
                length: 4,
            },
        },
    ];
    let batch =
        poll_immediate(reader.read_resolved_ranges(&requests, 2, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(&batch.value[0].bytes[..], b"234");
    assert_eq!(&batch.value[1].bytes[..], b"bcde");

    let descriptions = poll_immediate(reader.describe_files(
        &[path("first")?, path("second")?, path("first")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(descriptions.value.len(), 3);
    assert_eq!(descriptions.value[0], descriptions.value[2]);
    assert!(descriptions.value.iter().all(Option::is_some));
    let resolved =
        poll_immediate(reader.resolve_files(&[path("first")?], WorkBudget::UNBOUNDED, &cancellation))?;
    let resolved_file = resolved.value[0].as_ref().ok_or("resolved file absent")?;
    let resolved_read = poll_immediate(resolved_file.read_range(
        ByteRange {
            offset: 1,
            length: 4,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&resolved_read.value.bytes[..], b"bcde");
    let other_volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut other_writer = poll_immediate(other_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(other_writer.create(crate::AuthoredMutation::CreateFile { path: path("foreign")?, bytes: (bytes::Bytes::from_static(b"WXYZ")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let other_reader = other_writer.pinned_reader()?;
    let other_resolved = poll_immediate(other_reader.resolve_files(
        &[path("foreign")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let other_file = other_resolved.value[0]
        .as_ref()
        .ok_or("other resolved file absent")?;
    let mixed_requests = [
        ResolvedFileRangeReadRequest {
            file: resolved_file,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
        },
        ResolvedFileRangeReadRequest {
            file: other_file,
            range: ByteRange {
                offset: 1,
                length: 2,
            },
        },
    ];
    let mixed = poll_immediate(reader.read_resolved_ranges(
        &mixed_requests,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&mixed.value[0].bytes[..], b"ab");
    assert_eq!(&mixed.value[1].bytes[..], b"XY");
    let bounded_mixed =
        poll_immediate(reader.read_resolved_ranges(&mixed_requests, 2, mixed.work, &cancellation))?;
    assert_eq!(bounded_mixed.value, mixed.value);
    let target =
        poll_immediate(reader.read_symbolic_link(&path("link")?, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(&target.value[..], b"first");
    assert!(matches!(
        poll_immediate(reader.read_symbolic_link(
            &path("first")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        Err(OperationFailure {
            error: FsError::NotSymbolicLink,
            ..
        })
    ));
    let pages = poll_immediate(reader.list_directory_pages(
        &[
            DirectoryPageRequest {
                path: second_directory,
                after: None,
                maximum_entries: 8,
            },
            DirectoryPageRequest {
                path: first_directory.clone(),
                after: Some(LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?),
                maximum_entries: 1,
            },
        ],
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let second_page = pages.value.first().ok_or("second directory page missing")?;
    let first_page = pages.value.get(1).ok_or("first directory page missing")?;
    assert_eq!(
        second_page
            .entries
            .first()
            .ok_or("second entry missing")?
            .name
            .as_bytes(),
        b"c"
    );
    assert_eq!(
        first_page
            .entries
            .first()
            .ok_or("first entry missing")?
            .name
            .as_bytes(),
        b"b"
    );
    assert!(!second_page.has_more);
    assert!(!first_page.has_more);
    let resolved_page = poll_immediate(reader.resolve_directory_page(
        &first_directory,
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(resolved_page.value.entries.len(), 1);
    assert!(resolved_page.value.has_more);
    let resolved_entry = resolved_page
        .value
        .entries
        .first()
        .ok_or("resolved directory entry missing")?;
    assert_eq!(resolved_entry.name.as_bytes(), b"a");
    assert_eq!(resolved_entry.file.description().kind, FileKind::Regular);
    let resolved_child = poll_immediate(resolved_entry.file.read_range(
        ByteRange {
            offset: 0,
            length: 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&resolved_child.value.bytes[..], b"x");
    assert!(
        poll_immediate(reader.list_directory_pages(&[], 0, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );

    let first = poll_immediate(reader.read_file_range(
        &request_paths[0],
        requests[0].range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let second = poll_immediate(reader.read_file_range(
        &request_paths[1],
        requests[1].range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let independent = first.work.checked_add(second.work)?;
    assert!(batch.work.object_probes < independent.object_probes);
    assert!(batch.work.backend_read_operations < independent.backend_read_operations);
    assert!(batch.work.page_reads < independent.page_reads);

    let Err(bounded_failure) =
        poll_immediate(reader.read_resolved_ranges(&requests, 2, first.work, &cancellation))
    else {
        return Err("the second range received the first range's full budget".into());
    };
    bounded_failure.work.verify(first.work)?;

    let concurrent_failure_requests = [
        ResolvedFileRangeReadRequest {
            file: second_file,
            range: ByteRange {
                offset: 0,
                length: u64::MAX,
            },
        },
        ResolvedFileRangeReadRequest {
            file: first_file,
            range: requests[1].range,
        },
    ];
    let Err(concurrent_failure) = poll_immediate(reader.read_resolved_ranges(
        &concurrent_failure_requests,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    else {
        return Err("the invalid concurrent range unexpectedly succeeded".into());
    };
    assert_eq!(
        concurrent_failure.work.backend_read_operations,
        batch.work.backend_read_operations
    );
    assert_eq!(concurrent_failure.work.page_reads, batch.work.page_reads);
    assert_eq!(
        concurrent_failure.work.output_bytes,
        second.work.output_bytes
    );

    let tracking = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert!(matches!(
        tracking.pinned_reader(),
        Err(FsError::MutationNotAllowed)
    ));
    Ok(())
}

#[test]
fn checkout_snapshot_reader_is_private_and_stable() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file_path = path("private")?;
    let file_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file_path.clone(), bytes: (bytes::Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    let snapshot = checkout.snapshot_reader();
    let resolved = poll_immediate(snapshot.resolve_files(
        std::slice::from_ref(&file_path),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .into_iter()
    .next()
    .flatten()
    .ok_or("private file missing from snapshot")?;
    assert_eq!(resolved.file_id(), file_id);

    poll_immediate(checkout.change_content(file_path, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"later")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    let bytes = poll_immediate(resolved.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .bytes;
    assert_eq!(&bytes[..], b"first");
    Ok(())
}

#[test]
fn every_authority_backend_cut_preserves_creation_checkout_and_commit_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        FaultAuthorityStore {
            inner: crate::memory::MemoryAuthorityStore::default(),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();

    macro_rules! through_every_authority_cut {
        ($operation:expr) => {{
            let mut success = None;
            for cut in 1..=64 {
                control.arm(cut);
                let outcome =
                    poll_immediate($operation);
                match outcome {
                    Ok(receipt) => {
                        success = Some(receipt);
                        break;
                    }
                    Err(failure) => assert!(
                        failure
                            .work
                            .authority_records_read
                            .saturating_add(failure.work.authority_records_appended)
                            > 0
                    ),
                }
            }
            control.disable();
            success.ok_or("authority operation never passed the complete cut-point sweep")?
        }};
    }

    let volume_id = VolumeId::from_bytes([31; 16]);
    let volume = through_every_authority_cut!(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert_eq!(volume.id(), volume_id);
    let reopened = through_every_authority_cut!(fs.open_volume(
        volume_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut checkout = through_every_authority_cut!(reopened.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    control.disable();
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("authority-cut-file")?, bytes: (bytes::Bytes::from_static(b"authority cut candidate")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let candidate = checkout.generation_id();
    assert!(checkout.has_pending_mutations());
    let commit_operation = OperationId::from_bytes([33; 16]);
    let mut committed = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome =
            poll_immediate(checkout.commit(commit_operation, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }));
        match outcome {
            Ok(receipt) => {
                committed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure
                        .work
                        .authority_records_read
                        .saturating_add(failure.work.authority_records_appended)
                        > 0
                );
                assert_eq!(checkout.generation_id(), candidate);
                assert!(checkout.has_pending_mutations());
            }
        }
    }
    control.disable();
    let committed = committed.ok_or("commit never passed the complete authority cut sweep")?;
    let committed_generation = match committed.value {
        CheckoutCommitOutcome::Committed { generation_id, .. }
        | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
        CheckoutCommitOutcome::Conflict { .. }
        | CheckoutCommitOutcome::Fenced { .. }
        | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
            return Err("exact retry returned a semantic authority conflict".into());
        }
    };
    assert_eq!(checkout.generation_id(), committed_generation);
    assert!(!checkout.has_pending_mutations());

    let live_id = VolumeId::from_bytes([32; 16]);
    let live_volume = through_every_authority_cut!(fs.create_volume_with_id(
        live_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut live = through_every_authority_cut!(live_volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let live_before = live.generation_id();
    control.disable();
    poll_immediate(live.create(crate::AuthoredMutation::CreateFile { path: path("live-authority-cut")?, bytes: (bytes::Bytes::from_static(b"live")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(live.has_pending_mutations());
    let live_operation = OperationId::from_bytes([34; 16]);
    let mut published = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome = poll_immediate(live.resume_live(
            live_operation,
            3,
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        match outcome {
            Ok(receipt) => {
                published = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure
                        .work
                        .authority_records_read
                        .saturating_add(failure.work.authority_records_appended)
                        > 0
                );
                assert_eq!(live.generation_id(), live_before);
                assert!(live.has_pending_mutations());
            }
        }
    }
    control.disable();
    let published = published.ok_or("live publication never passed the authority cut sweep")?;
    assert!(matches!(
        published.value,
        LiveMutationOutcome::Committed { .. } | LiveMutationOutcome::AlreadyCommitted { .. }
    ));
    assert_ne!(live.generation_id(), live_before);
    assert!(!live.has_pending_mutations());
    Ok(())
}

#[test]
fn every_post_put_creation_fault_retries_from_a_complete_authenticated_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut reached_clean_completion = false;
    for cut in 1..=32 {
        let control = Arc::new(FaultControl::disabled());
        control.arm(cut);
        let objects = Arc::new(crate::memory::MemoryObjectStore::default());
        let fs = Fs::new(
            crate::memory::MemoryAuthorityStore::default(),
            PostPutObjectStore {
                inner: Arc::clone(&objects),
                control: Arc::clone(&control),
            },
            EmbeddedCapabilities::MEMORY,
        );
        let volume_id = VolumeId::from_bytes(
            [u8::try_from(cut).map_err(|_| "post-put cut does not fit identity")?; 16],
        );
        let first = poll_immediate(fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        match first {
            Ok(created) => {
                assert_eq!(created.value.id(), volume_id);
                reached_clean_completion = true;
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(failure.error, FsError::Object(ObjectStoreError::Corrupt)),
                    "unexpected creation failure: {:#?}",
                    failure.error
                );
                assert_eq!(
                    failure.work.backend_write_operations,
                    u64::try_from(cut).map_err(|_| "post-put cut does not fit work counter")?
                );
                control.disable();
                let retried = poll_immediate(fs.create_volume_with_id(
                    volume_id,
                    config(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))?;
                assert_eq!(retried.value.id(), volume_id);
                let reopened =
                    poll_immediate(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))?;
                let checkout = poll_immediate(reopened.value.checkout(
                    GenerationSelector::Head,
                    pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))?;
                assert_eq!(checkout.value.volume_id(), volume_id);
                assert!(!checkout.value.has_pending_mutations());
            }
        }
    }
    assert!(reached_clean_completion);
    Ok(())
}

#[test]
fn every_pre_object_fault_preserves_creation_and_checkout_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([50; 16]);
    let mut volume = None;
    for cut in 1..=64 {
        control.arm(cut);
        match poll_immediate(fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        {
            Ok(receipt) => {
                volume = Some(receipt.value);
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(
                        failure.error,
                        FsError::Object(ObjectStoreError::Corrupt)
                            | FsError::Closure(ClosureError::Storage(ObjectStoreError::Corrupt))
                    ),
                    "unexpected creation failure: {:#?}",
                    failure.error
                );
                assert!(failure.work.backend_write_operations > 0);
            }
        }
    }
    control.disable();
    let volume = volume.ok_or("creation never passed the complete object cut sweep")?;
    assert_eq!(volume.id(), volume_id);

    let mut checkout = None;
    for cut in 1..=64 {
        control.arm(cut);
        match poll_immediate(volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        {
            Ok(receipt) => {
                checkout = Some(receipt.value);
                break;
            }
            Err(failure) => {
                assert!(
                    matches!(failure.error, FsError::Object(ObjectStoreError::Corrupt)),
                    "unexpected checkout failure: {:#?}",
                    failure.error
                );
                assert!(failure.work.backend_read_operations > 0);
            }
        }
    }
    control.disable();
    let checkout = checkout.ok_or("checkout never passed the complete object cut sweep")?;
    assert_eq!(checkout.volume_id(), volume_id);
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[test]
fn every_object_cut_preserves_sparse_rebase_retry_and_candidate_state()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut rebasing = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(rebasing.create(crate::AuthoredMutation::CreateFile { path: path("local")?, bytes: (bytes::Bytes::from_static(b"local sparse candidate")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(writer.create(crate::AuthoredMutation::CreateFile { path: path("remote")?, bytes: (bytes::Bytes::from_static(b"remote sparse head")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(writer.commit(OperationId::from_bytes([227; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let original = checkout_state(&rebasing);
    let mut completed = None;
    for cut in 1..=512 {
        control.arm(cut);
        match poll_immediate(rebasing.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(failure.work.backend_read_operations > 0);
                assert_eq!(checkout_state(&rebasing), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("rebase never passed the complete object cut sweep")?;
    assert!(matches!(completed.value, RebaseDecision::Safe { .. }));
    assert!(rebasing.has_pending_mutations());
    assert!(
        poll_immediate(rebasing.lookup_no_follow(
            &path("local")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_some()
    );
    assert!(
        poll_immediate(rebasing.lookup_no_follow(
            &path("remote")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_some()
    );
    Ok(())
}

#[test]
fn every_object_cut_preserves_diff_export_and_merge_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let base = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .generation_id();
    let mut ours = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut theirs = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(ours.create(crate::AuthoredMutation::CreateFile { path: path("ours")?, bytes: (bytes::Bytes::from(vec![b'o'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(theirs.create(crate::AuthoredMutation::CreateFile { path: path("theirs")?, bytes: (bytes::Bytes::from(vec![b't'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let ours_generation = poll_immediate(ours.checkpoint(WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let theirs_generation = poll_immediate(theirs.checkpoint(WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    poll_immediate(theirs.commit(OperationId::from_bytes([228; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    macro_rules! sweep_read_only {
        ($operation:expr) => {{
            let original = checkout_state(&ours);
            let mut completed = None;
            for cut in 1..=4_096 {
                control.arm(cut);
                match poll_immediate($operation) {
                    Ok(receipt) => {
                        completed = Some(receipt);
                        break;
                    }
                    Err(failure) => {
                        assert!(failure.work.backend_read_operations > 0);
                        assert_eq!(checkout_state(&ours), original);
                    }
                }
            }
            control.disable();
            completed.ok_or_else(|| {
                format!(
                    "operation never passed object cut sweep: {}",
                    stringify!($operation)
                )
            })?
        }};
    }

    let diff = sweep_read_only!(volume.diff_generations(
        base,
        ours_generation,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(diff.value.files.len() + diff.value.bindings.len(), 3);
    let manifest = sweep_read_only!(ours.export_manifest(WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(manifest.value.volume_id, volume.id());
    assert_eq!(
        manifest.value.generation_root,
        ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: ours_generation.digest(),
        }
    );
    let prepared = sweep_read_only!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    assert!(ours.has_pending_mutations());
    for expected in ["ours", "theirs"] {
        assert!(
            poll_immediate(ours.lookup_no_follow(
                &path(expected)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?
            .value
            .record
            .is_some()
        );
    }
    Ok(())
}

#[test]
fn every_object_cut_preserves_authenticated_publication_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        FaultObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("publish-large")?, bytes: (bytes::Bytes::from(vec![b'p'; 16_384])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.change_named_attribute(path("publish-large")?, &AttributeName::new(
            crate::kernel::AttributeClass::PosixXattr,
            b"user.publication".to_vec(),
            config().limits.maximum_component_bytes,
        )?, Some((&bytes::Bytes::from(vec![b'a'; 4_096]), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))?;

    let original = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([229; 16]);
    let mut completed = None;
    for cut in 1..=4_096 {
        control.arm(cut);
        match poll_immediate(checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
        {
            Ok(receipt) => {
                completed = Some(receipt);
                break;
            }
            Err(failure) => {
                assert!(
                    failure.work.backend_read_operations + failure.work.backend_write_operations
                        > 0
                );
                assert_eq!(checkout_state(&checkout), original);
            }
        }
    }
    control.disable();
    let completed = completed.ok_or("publication never passed the complete object cut sweep")?;
    assert!(matches!(
        completed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[test]
fn indeterminate_post_append_retry_resolves_one_durable_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let control = Arc::new(FaultControl::disabled());
    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        PostAppendAuthorityStore {
            inner: Arc::clone(&authority),
            control: Arc::clone(&control),
        },
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("indeterminate-file")?, bytes: (bytes::Bytes::from_static(b"durable exactly once")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let before = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([49; 16]);
    control.arm(1);
    let failure = poll_immediate(checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
        .err()
        .ok_or("post-append acknowledgement loss unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Publication(PublicationError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));
    assert_eq!(checkout_state(&checkout), before);
    assert!(
        AuthorityStore::find_operation(
            &*authority,
            volume_authority_id(volume.id()),
            operation_id,
            WorkBudget::UNBOUNDED,
        )?
        .value
        .is_some()
    );

    control.disable();
    let resolved = poll_immediate(checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        resolved.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let history = AuthorityStore::replay(
        &*authority,
        volume_authority_id(volume.id()),
        Sequence::GENESIS,
        ReplayLimit {
            records: 8,
            payload_bytes: 64 * 1024,
        },
        WorkBudget::UNBOUNDED,
    )?
    .value;
    assert_eq!(
        history
            .iter()
            .filter(|commit| commit.operation_id == operation_id)
            .count(),
        1
    );
    let mut reopened = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let bytes = poll_immediate(reopened.value.read_file_range(
        &path("indeterminate-file")?,
        ByteRange {
            offset: 0,
            length: 20,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(bytes.value.bytes.as_ref(), b"durable exactly once");
    Ok(())
}

#[test]
fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let file = path("open-file")?;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let detached =
        poll_immediate(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))?;
    let mut detached = detached.value;
    let file_id = detached.file_id();
    assert_eq!(
        poll_immediate(detached.seek(
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value,
        Some(0)
    );
    assert_eq!(
        poll_immediate(detached.seek(
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value,
        Some(128)
    );
    for (offset, target, expected) in [
        (128, ExtentSeekTarget::Data, None),
        (128, ExtentSeekTarget::Hole, Some(128)),
        (129, ExtentSeekTarget::Data, None),
        (129, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            poll_immediate(detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation,))?
                .value,
            expected
        );
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.detached".to_vec(),
        128,
    )?;
    assert!(
        poll_immediate(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )?
        .value
        .is_none()
    );
    assert!(
        poll_immediate(detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,))?
            .value
            .entries
            .is_empty()
    );
    let absent_remove = poll_immediate(detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("attribute-free detached removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = poll_immediate(detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"replacement"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("absent detached replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    poll_immediate(detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(
        poll_immediate(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        )?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        poll_immediate(detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,))?
            .value
            .entries
            .len(),
        1
    );
    assert!(
        poll_immediate(detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"duplicate"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );
    poll_immediate(detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation))?;

    let mut metadata = poll_immediate(detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    poll_immediate(detached.set_attributes(metadata, None, WorkBudget::UNBOUNDED, &cancellation))?;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    poll_immediate(detached.set_attributes(metadata, None, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(detached.logical_bytes(), 128);
    poll_immediate(detached.set_attributes(metadata, Some(160), WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(detached.logical_bytes(), 160);
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Remove { path: file.clone(), expected_file_id: (Some(file_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation))?;
    let absent = poll_immediate(checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(absent.value.record.is_none());

    poll_immediate(detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(b"open") },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(detached.change_content(
        ContentChange::Write { offset: 4, bytes: OwnedBytes::new() },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(detached.change_content(
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 128,
                length: 64,
            },
            keep_size: false,
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(detached.change_content(
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 64,
                length: 16,
            },
            allocated: false,
            extend: false,
        },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let read = poll_immediate(detached.read_range(
        ByteRange {
            offset: 0,
            length: 80,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&read.value.bytes[..4], b"open");
    assert_eq!(&read.value.bytes[64..80], &[0; 16]);
    poll_immediate(detached.change_content(
        ContentChange::Resize { logical_bytes: 192 },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(detached.logical_bytes(), 192);
    poll_immediate(detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(
        poll_immediate(detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );
    Ok(())
}

#[test]
fn tracking_reads_capture_only_their_exact_terminal_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([81; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("file")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(seed.commit(OperationId::from_bytes([82; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut content_reader = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(content_reader.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let mut metadata_writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut changed_metadata = FileMetadata::default();
    changed_metadata.modified_ns = MetadataField::Value(7);
    poll_immediate(metadata_writer.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: file.clone(), metadata: changed_metadata }], WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(metadata_writer.commit(OperationId::from_bytes([83; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let content_rebase =
        poll_immediate(content_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(content_rebase.value, RebaseDecision::Safe { .. }));

    let mut metadata_reader = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(metadata_reader.read_metadata(&file, WorkBudget::UNBOUNDED, &cancellation))?;
    let mut content_writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(content_writer.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"changed")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(content_writer.commit(OperationId::from_bytes([84; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let metadata_rebase =
        poll_immediate(metadata_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(metadata_rebase.value, RebaseDecision::Safe { .. }));

    let mut generic_reader = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(generic_reader.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation))?;
    let mut final_writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(final_writer.change_content(file, crate::ContentChange::Write { offset: 96, bytes: (bytes::Bytes::from_static(b"different")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(final_writer.commit(OperationId::from_bytes([85; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let generic_rebase =
        poll_immediate(generic_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(
        generic_rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileRecord(_)
            ))
    ));
    Ok(())
}

#[test]
fn sparse_seek_dependencies_track_base_semantics_and_exact_observed_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([101; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("sparse")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 256])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(seed.commit(OperationId::from_bytes([102; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(local.change_content(file.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 128,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    let local_observation = poll_immediate(local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(local_observation.value, Some(128));

    let mut disjoint = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(disjoint.change_content(file.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 0,
            length: 64,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(disjoint.commit(OperationId::from_bytes([103; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let safe = poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let retained = poll_immediate(local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(retained.value, Some(128));

    let mut reader = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let observation = poll_immediate(reader.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(observation.value, Some(256));

    let mut conflicting = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(conflicting.change_content(file, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 192,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(conflicting.commit(OperationId::from_bytes([104; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let conflict = poll_immediate(reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::SparseSeek {
                    offset: 128,
                    target: ExtentSeekTarget::Hole,
                    ..
                }
            ))
    ));

    Ok(())
}

#[test]
fn clipped_identity_read_at_eof_tracks_file_length() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([190; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("file")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file_id = poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abc")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    poll_immediate(seed.commit(OperationId::from_bytes([190; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut reader = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let at_eof = poll_immediate(reader.read_file_up_to_by_id(
        file_id,
        3,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(at_eof.value.bytes.is_empty());

    let mut writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(writer.change_content(file, crate::ContentChange::Write { offset: 3, bytes: (bytes::Bytes::from_static(b"d")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(writer.commit(OperationId::from_bytes([191; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let rebase = poll_immediate(reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(
        rebase.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::FileLength(actual) if actual == file_id
            ))
    ));
    Ok(())
}

#[test]
fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(false)
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_native_rebase_preserves_disjoint_ranges_and_is_atomic_on_denial_and_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(true)
}

fn sparse_rebase_across_remote_ranges(native: bool)
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([86; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("file")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(seed.commit(OperationId::from_bytes([87; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut remote = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(local.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"LOCAL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(remote.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"REMOTE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(remote.commit(OperationId::from_bytes([88; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
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
        assert!(poll_immediate(local.rebase_head_with_native_work(8, &denied, &cancellation)).is_err());
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
        let result = poll_immediate(local.rebase_head_with_native_work(8, &original, &cancellation))?;
        (result.0, Some(result.1))
    } else {
        (poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?.value, None)
    };
    #[cfg(target_arch = "wasm32")]
    let safe = (poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?.value, ());
    assert!(matches!(safe.0, RebaseDecision::Safe { .. }));
    let local_bytes = poll_immediate(local.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&local_bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&local_bytes.value.bytes[64..70], b"REMOTE");

    let mut overlapping_local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut overlapping_remote = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(overlapping_local.change_content(file.clone(), crate::ContentChange::Write { offset: 10, bytes: (bytes::Bytes::from_static(b"local")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(overlapping_remote.change_content(file, crate::ContentChange::Write { offset: 12, bytes: (bytes::Bytes::from_static(b"remote")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(overlapping_remote.commit(OperationId::from_bytes([89; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let unchanged = (
        overlapping_local.generation_id(), overlapping_local.root.file_table,
        overlapping_local.base_file_table, overlapping_local.authority_head,
        overlapping_local.authored_operation_id, overlapping_local.live_operation_id,
        overlapping_local.dependencies.proof().dependencies.clone(),
    );
    #[cfg(not(target_arch = "wasm32"))]
    let conflict = if native {
        let result = poll_immediate(overlapping_local.rebase_head_with_native_work(8, &original, &cancellation))?;
        assert!(result.1.work().shares_ledger(&original));
        assert!(result.1.bytes() >= (std::mem::size_of::<crate::kernel::RebaseConflict>()) as u64);
        (result.0, Some(result.1))
    } else {
        (poll_immediate(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?.value, None)
    };
    #[cfg(target_arch = "wasm32")]
    let conflict = (poll_immediate(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?.value, ());
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

#[test]
fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("identity")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file_id = poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    poll_immediate(seed.commit(OperationId::from_bytes([112; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut remote = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(local.change_content(file_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"LOCAL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(remote.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"REMOTE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(remote.commit(OperationId::from_bytes([113; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let safe = poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let bytes = poll_immediate(local.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&bytes.value.bytes[64..70], b"REMOTE");
    let plan = poll_immediate(local.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(plan.value.is_some());

    let mut overlapping_local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut overlapping_remote = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(overlapping_local.change_content(file_id, crate::ContentChange::Write { offset: 10, bytes: (bytes::Bytes::from_static(b"local")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(overlapping_remote.change_content(file, crate::ContentChange::Write { offset: 12, bytes: (bytes::Bytes::from_static(b"remote")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(overlapping_remote.commit(OperationId::from_bytes([114; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let conflict =
        poll_immediate(overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { file_id: actual, .. } if actual == file_id
            ))
    ));

    let mut extending = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(extending.change_content(file_id, crate::ContentChange::Write { offset: 128, bytes: (bytes::Bytes::from_static(b"TAIL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(extending.change_content(file_id, crate::ContentChange::CloneFrom { source: file_id, source_offset: 0, offset: 140, length: 4 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    let extended = poll_immediate(extending.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 128,
            length: 16,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(&extended.value.bytes[..4], b"TAIL");
    assert_eq!(&extended.value.bytes[4..12], &[0; 8]);
    assert_eq!(&extended.value.bytes[12..], b"aaaa");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_guarded_live_retry_preserves_both_real_identities()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = fs.create_volume_with_id(
        VolumeId::from_bytes([209; 16]), serialized_config(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut seed = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let left = seed.create(crate::AuthoredMutation::CreateFile { path: path("left")?, bytes: (bytes::Bytes::from_static(b"left real source")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
    let right = seed.create(crate::AuthoredMutation::CreateFile { path: path("right")?, bytes: (bytes::Bytes::from_static(b"distinct right real source")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation).await?.value;
    seed.commit(OperationId::from_bytes([210; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await?;
    let original = crate::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let mut local = volume.checkout_with_native_work(
        GenerationSelector::Head, writable_live(), &original, &cancellation,
    ).await?;
    let mut remote = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
    accounting.charge(WorkCounters {
        bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
    })?;
    operations.value.push(Mutation::File {
        file_id: left, mutation: FileMutation::Resize { logical_bytes: 0 },
    });
    let KernelOwnedAllocation::NativeOriginal(allocation) = operations.allocation else {
        unreachable!("native input retains original custody")
    };
    local.mutate_with_native_work(
        operations.value, allocation, &original, &cancellation,
    ).await?;
    remote.mutate(vec![Mutation::File {
        file_id: right, mutation: FileMutation::Resize { logical_bytes: 0 },
    }], WorkBudget::UNBOUNDED, &cancellation).await?;
    remote.commit(OperationId::from_bytes([211; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&callbacks);
    let callback_original = original.clone();
    let mut before_publication = move |meter: &crate::NativeMaterializationWork| {
        assert!(meter.shares_ledger(&callback_original));
        callback_calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok::<(), FsError>(()))
    };
    let outcome = local.resume_live_with_native_work(
        OperationId::from_bytes([212; 16]), 8, 16, PublicationPermit::Unrestricted,
        &original, &cancellation, &mut before_publication, None, None, None, None,
    ).await?;
    assert!(matches!(outcome.0, LiveMutationOutcome::Committed { .. }));
    assert_eq!(outcome.1.bytes(), 0);
    assert!(outcome.1.work().shares_ledger(&original));
    assert!(callbacks.load(Ordering::SeqCst) >= 2);
    assert!(!local.has_pending_mutations());
    assert_eq!(local.live_operation_id, None);
    assert!(local.root.same_original_scope(original.native_read_meter()));
    let left_record = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, local.root.file_table, left, DecodeLimits::for_volume(volume.config),
        &mut accounting, &cancellation, None,
    ).await?.0.ok_or("local identity disappeared during safe retry")?;
    let right_record = crate::kernel::lookup_file_record_async_with_accounting(
        &fs.inner.objects, local.root.file_table, right, DecodeLimits::for_volume(volume.config),
        &mut accounting, &cancellation, None,
    ).await?.0.ok_or("concurrent destination identity disappeared during safe retry")?;
    assert_eq!(left_record.file_id, left);
    assert_eq!(right_record.file_id, right);
    assert_eq!(left_record.payload, right_record.payload);
    Ok(())
}

#[test]
fn direct_live_mutations_retry_only_across_exactly_safe_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let optimistic = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([90; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let rejected = poll_immediate(optimistic.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("optimistic volume admitted direct live")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));

    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([91; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("file")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(seed.commit(OperationId::from_bytes([92; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut first = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut second = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let first_blob = poll_immediate(first.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"FIRST")), 5, WorkBudget::UNBOUNDED, &cancellation))?;
    let second_blob = poll_immediate(second.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"SECOND")), 6, WorkBudget::UNBOUNDED, &cancellation))?;
    let first_outcome = poll_immediate(first.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 0,
            length: first_blob.value.logical_bytes,
            content: first_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([93; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(matches!(
        first_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let second_outcome = poll_immediate(second.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 64,
            length: second_blob.value.logical_bytes,
            content: second_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([94; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(matches!(
        second_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));

    let mut overlapping = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut racing = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let overlap_blob = poll_immediate(overlapping.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"OVERLAP")), 7, WorkBudget::UNBOUNDED, &cancellation))?;
    let racing_blob = poll_immediate(racing.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"RACING")), 6, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(racing.mutate_live(
        vec![Mutation::Write {
            path: file.clone(),
            offset: 10,
            length: racing_blob.value.logical_bytes,
            content: racing_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([95; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let conflict = poll_immediate(overlapping.mutate_live(
        vec![Mutation::Write {
            path: file,
            offset: 12,
            length: overlap_blob.value.logical_bytes,
            content: overlap_blob.value.root,
            content_offset: 0,
        }],
        OperationId::from_bytes([96; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(matches!(
        conflict.value,
        LiveMutationOutcome::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    Ok(())
}

#[test]
fn explicit_live_refresh_advances_only_across_unobserved_regions()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let file = path("refresh")?;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    ready!(seed.commit(OperationId::from_bytes([98; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }));

    let mut live = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert_eq!(
        ready!(live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );

    let mut disjoint = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(disjoint.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"SAFE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let disjoint_generation = match ready!(disjoint.commit(OperationId::from_bytes([99; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected disjoint commit: {outcome:?}").into()),
    };
    assert_eq!(
        ready!(live.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).value,
        disjoint_generation
    );
    assert_eq!(live.generation_id(), disjoint_generation);

    assert_eq!(
        ready!(live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );
    let mut overlapping = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(overlapping.change_content(file, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"RACE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(overlapping.commit(OperationId::from_bytes([100; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }));
    let before_conflict = live.generation_id();
    let conflict = poll_immediate(live.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("conflicting live refresh unexpectedly advanced")?;
    assert!(matches!(
        conflict.error,
        FsError::LiveConflict { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { .. }
            ))
    ));
    assert_eq!(live.generation_id(), before_conflict);
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let native_conflict = poll_immediate(live.synchronize_live_with_accounting(&mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation), &cancellation))
            .err().ok_or("native conflicting live sync unexpectedly advanced")?;
        assert!(matches!(&native_conflict,
            FsError::NativeLiveConflict { conflicts, allocation, .. }
                if allocation.work().shares_ledger(&original)
                    && allocation.bytes() >= size_of::<RebaseConflict>() as u64
                    && conflicts.iter().any(|conflict| matches!(
                        conflict.region, DependencyRegion::ContentRange { .. }
                    ))
        ));
        assert_eq!(live.generation_id(), before_conflict);
    }
    Ok(())
}

#[test]
fn direct_live_resume_is_idempotent_retry_bounded_and_epoch_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([117; 16]);
    let volume = ready!(fs.create_volume_with_id(
        volume_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;

    let mut winner = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut retrying = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(winner.create(crate::AuthoredMutation::CreateFile { path: path("winner")?, bytes: (bytes::Bytes::from_static(b"winner")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    ready!(winner.resume_live(
        OperationId::from_bytes([118; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    ready!(retrying.create(crate::AuthoredMutation::CreateFile { path: path("retrying")?, bytes: (bytes::Bytes::from_static(b"retrying")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let retry_operation = OperationId::from_bytes([119; 16]);
    let limited =
        ready!(retrying.resume_live(retry_operation, 1, 8, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(matches!(
        limited.value,
        LiveMutationOutcome::RetryLimit { .. }
    ));
    assert!(retrying.has_pending_mutations());
    let mutation_while_unresolved = poll_immediate(retrying.create(crate::AuthoredMutation::CreateFile { path: path("must-remain-fenced")?, bytes: (bytes::Bytes::from_static(b"blocked")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("unresolved live mutation accepted another mutation")?;
    assert!(matches!(
        mutation_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let mismatched_resume = poll_immediate(retrying.resume_live(
        OperationId::from_bytes([120; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("mismatched live resume unexpectedly succeeded")?;
    assert!(matches!(
        mismatched_resume.error,
        FsError::PendingLiveMutation
    ));
    let rebase_while_unresolved =
        poll_immediate(retrying.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("unresolved live operation accepted public rebase")?;
    assert!(matches!(
        rebase_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let resumed =
        ready!(retrying.resume_live(retry_operation, 2, 8, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(matches!(
        resumed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    let clean_resume = poll_immediate(retrying.resume_live(
        retry_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("clean live resume unexpectedly succeeded")?;
    assert!(matches!(clean_resume.error, FsError::NoPendingMutations));
    assert_eq!(*clean_resume.work, WorkCounters::default());
    for failure in [
        poll_immediate(retrying.resume_live(
            retry_operation,
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("zero-limit live resume unexpectedly succeeded")?,
        poll_immediate(retrying.mutate_live(
            Vec::new(),
            OperationId::from_bytes([124; 16]),
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("zero-limit live mutation unexpectedly succeeded")?,
    ] {
        assert!(matches!(failure.error, FsError::ZeroLiveRetryLimit));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let mut pending_guard = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(pending_guard.create(crate::AuthoredMutation::CreateFile { path: path("pending-guard")?, bytes: (bytes::Bytes::from_static(b"pending")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let pending_failure = poll_immediate(pending_guard.mutate_live(
        Vec::new(),
        OperationId::from_bytes([125; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("second pending live mutation unexpectedly succeeded")?;
    assert!(matches!(
        pending_failure.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*pending_failure.work, WorkCounters::default());
    let authored_pending = poll_immediate(pending_guard.apply_authored_live(
        Vec::new(),
        OperationId::from_bytes([126; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("pending authored-live operation unexpectedly succeeded")?;
    assert!(matches!(
        authored_pending.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*authored_pending.work, WorkCounters::default());

    let mut first_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut exact_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let root = root_path()?;
    let metadata =
        ready!(first_retry.lookup_no_follow(&root, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let identical = Mutation::Create {
        path: path("idempotent")?,
        record: FileRecord {
            file_id: FileId::from_bytes([120; 16]),
            kind: FileKind::Regular,
            link_count: 1,
            metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"same")?),
        },
    };
    ready!(first_retry.mutate(
        vec![identical.clone()],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(exact_retry.mutate(vec![identical], WorkBudget::UNBOUNDED, &cancellation,));
    let idempotent_operation = OperationId::from_bytes([121; 16]);
    let committed = ready!(first_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        committed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let repeated = ready!(exact_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        repeated.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));

    let mut conflicting_retry = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(conflicting_retry.create(crate::AuthoredMutation::CreateFile { path: path("different-operation")?, bytes: (bytes::Bytes::from_static(b"different")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let conflicted = ready!(conflicting_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        conflicted.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(conflicting_retry.has_pending_mutations());

    let mut fenced = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(fenced.create(crate::AuthoredMutation::CreateFile { path: path("fenced")?, bytes: (bytes::Bytes::from_static(b"fenced")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let fence_expected = ready!(AsyncAuthorityStore::head(
        &fs.inner.authority,
        volume_authority_id(volume_id),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let fence = ready!(AsyncAuthorityStore::fence(
        &fs.inner.authority,
        volume_authority_id(volume_id),
        fence_expected,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let FenceOutcome::Advanced(fenced_head) = fence.value else {
        return Err("fresh facade fence conflicted".into());
    };
    let fenced_outcome = ready!(fenced.resume_live(
        OperationId::from_bytes([122; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        fenced_outcome.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == fenced_head.epoch
    ));

    let mut non_live = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        poll_immediate(non_live.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .is_err()
    );
    assert!(
        poll_immediate(non_live.mutate_live(
            Vec::new(),
            OperationId::from_bytes([123; 16]),
            1,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    Ok(())
}

#[test]
fn authored_live_transaction_preserves_create_ids_and_publishes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("authored-live.txt")?;
    let receipt = poll_immediate(checkout.apply_authored_live(
        vec![
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: OwnedBytes::from_static(b"initial"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: OwnedBytes::from_static(b"-published"),
            },
        ],
        OperationId::from_bytes([118; 16]),
        3,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(matches!(
        receipt.value.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert_eq!(receipt.value.created_file_ids.len(), 2);
    assert!(receipt.value.created_file_ids[0].is_some());
    assert!(receipt.value.created_file_ids[1].is_none());
    assert!(!checkout.has_pending_mutations());
    let read = poll_immediate(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(read.value.bytes.as_ref(), b"initial-published");
    Ok(())
}

#[test]
fn authored_transaction_is_atomic_and_preserves_result_positions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([113; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let directory = path("workspace")?;
    let file = path("file.txt")?;
    let transaction = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: directory,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: OwnedBytes::from_static(b"initial"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: file.clone(),
                offset: 7,
                bytes: OwnedBytes::from_static(b"-atomic"),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(transaction.value.created_file_ids.len(), 3);
    assert!(transaction.value.created_file_ids[0].is_some());
    assert!(transaction.value.created_file_ids[1].is_some());
    assert!(transaction.value.created_file_ids[2].is_none());
    let read = poll_immediate(checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 14,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(read.value.bytes.as_ref(), b"initial-atomic");

    let failed = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("orphan")?,
                bytes: OwnedBytes::from_static(b"never-visible"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Remove {
                path: path("absent")?,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(failed.is_err());
    let absent = poll_immediate(checkout.lookup_no_follow(
        &path("orphan")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(absent.value.record.is_none());
    Ok(())
}

#[test]
fn authored_bulk_transaction_is_bounded_and_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 2;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([244; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let created = (0..5)
        .map(|index| {
            Ok(AuthoredMutation::CreateFile { path: path(&format!("bulk{index}"))?, bytes: OwnedBytes::from_static(b"ok"), metadata: FileMetadata::default() })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    poll_immediate(checkout.apply_authored_bulk_transaction(
        created,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    for index in 0..5 {
        assert!(
            poll_immediate(checkout.lookup_no_follow(
                &path(&format!("bulk{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?
            .value
            .record
            .is_some()
        );
    }
    let failed = poll_immediate(checkout.apply_authored_bulk_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("should-not-appear")?,
                bytes: OwnedBytes::from_static(b"no"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("should-not-appear-either")?,
                bytes: OwnedBytes::from_static(b"no"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("bulk0")?,
                bytes: OwnedBytes::from_static(b"duplicate"),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(failed.is_err());
    assert!(
        poll_immediate(checkout.lookup_no_follow(
            &path("should-not-appear")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_none()
    );
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
#[tokio::test]
async fn original_authored_transaction_keeps_true_ids_and_original_result_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-authored-transaction", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/source", "transaction source").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let metadata = checkout.lookup_no_follow_with_metadata(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("real source metadata absent")?.metadata;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    candidate.bind_authored_operation(OperationId::new());
    let first = path("transaction-first")?;
    let second = path("transaction-second")?;
    let expected = [
        Some(candidate.authored_file_id(FileKind::Regular, &first)),
        Some(candidate.authored_file_id(FileKind::Regular, &second)),
    ];
    let input = original_authored_creates(
        &[(&first, OwnedBytes::from_static(b"first actual source")),
          (&second, OwnedBytes::from_static(b"second actual source"))],
        metadata, &original, &cancellation,
    )?;
    let result = candidate.apply_authored_transaction_with_native_work(
        input.0, input.1, &original, &cancellation,
    ).await?;
    assert_eq!(result.0.created_file_ids, expected);
    assert!(result.1.work().shares_ledger(&original));
    assert_eq!(result.1.bytes(),
        (result.0.created_file_ids.capacity() * size_of::<Option<FileId>>()) as u64);
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    for (requested, expected) in [(&first, expected[0]), (&second, expected[1])] {
        let actual = crate::kernel::lookup_path_async_with_accounting(
            &fs.inner.objects, &candidate.root, requested, candidate.volume.config,
            &mut accounting, &cancellation, None,
        ).await?.record.ok_or("created real record absent")?;
        assert_eq!(Some(actual.file_id), expected);
    }
    let live = original.live_allocation_bytes();
    let result_bytes = result.1.bytes();
    drop(result);
    assert_eq!(original.live_allocation_bytes(), live - result_bytes);
    assert!(checkout.lookup_no_follow(
        &first, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.is_none(), "private transaction cannot alter its source checkout");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_borrowed_path_changes(
    source: &NamespacePath,
    destination: &NamespacePath,
    removed: &NamespacePath,
    metadata: FileMetadata,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<AuthoredMutation>, crate::NativeAllocation, crate::NativeAllocation), FsError> {
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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_second_staged_write_failure_keeps_candidate_source_and_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let control = Arc::new(FaultControl::disabled());
    let objects = Arc::new(crate::memory::MemoryObjectStore::default());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        PostPutObjectStore { inner: Arc::clone(&objects), control: Arc::clone(&control) },
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs.create_volume_with_id(
        VolumeId::from_bytes([122; 16]), config(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut checkout = volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = path("source")?;
    let destination = path("destination")?;
    let removed = path("removed")?;
    for (requested, body) in [
        (&source, b"source body".as_slice()),
        (&destination, b"destination body".as_slice()),
        (&removed, b"removed body".as_slice()),
    ] {
        checkout.create(crate::AuthoredMutation::CreateFile { path: requested.clone(), bytes: (bytes::Bytes::copy_from_slice(body)).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation).await?;
    }
    let mut metadata = checkout.lookup_no_follow_with_metadata(
        &source, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("real source missing")?.metadata;
    metadata.modified_ns = MetadataField::Value(99);
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    let input = original_borrowed_path_changes(
        &source, &destination, &removed, metadata, &original, &cancellation,
    )?;
    let state = checkout_state(&candidate);
    let installed = checkout_state(&checkout);
    let live = original.live_allocation_bytes();
    control.arm(2);
    let failure = candidate.apply_borrowed_authored_transaction_with_native_work(
        &input.0, &input.1, &original, &cancellation,
    ).await.err().ok_or("second staged write unexpectedly succeeded")?;
    assert!(matches!(failure, FsError::Object(ObjectStoreError::Corrupt)));
    assert_eq!(control.operation.load(Ordering::Relaxed), 2);
    assert_eq!(original.receipt().backend_write_operations, 2);
    assert_eq!(checkout_state(&candidate), state);
    assert_eq!(checkout_state(&checkout), installed);
    assert_eq!(original.live_allocation_bytes(), live);
    assert!(matches!(&input.0[2], AuthoredMutation::Rename {
        source: old_source, destination: old_destination, ..
    } if old_source == &source && old_destination == &destination));
    control.disable();
    for requested in [&source, &destination, &removed] {
        let retained = candidate.lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.record.ok_or("failed candidate lost existing record")?;
        let installed = checkout.lookup_no_follow(requested, WorkBudget::UNBOUNDED, &cancellation)
            .await?.value.record.ok_or("installed source record disappeared")?;
        assert_eq!(retained, installed);
    }
    Ok(())
}

#[test]
fn inverse_private_mutations_restore_the_base_and_clear_replay_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([136; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let base = checkout.generation_id();
    let transient = path("transient")?;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: transient.clone(), bytes: (bytes::Bytes::from_static(b"temporary")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(checkout.has_pending_mutations());
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Remove { path: transient, expected_file_id: (None).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[test]
fn authored_transactions_preflight_expansion_noops_and_byte_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();

    let mut operation_limited = config();
    operation_limited.limits.maximum_mutations_per_batch = 1;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        operation_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let missing_noop = poll_immediate(checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("absent")?,
            offset: 0,
            bytes: OwnedBytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("authored missing noop succeeded")?;
    assert!(matches!(
        missing_noop.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(missing_noop.work.page_reads > 0);

    let noop_fs = Fs::memory();
    let noop_volume = poll_immediate(noop_fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut noop_checkout = poll_immediate(noop_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let ordered = poll_immediate(noop_checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::Write {
                path: path("ordered")?,
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            AuthoredMutation::CreateFile {
                path: path("ordered")?,
                bytes: OwnedBytes::new(),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("ordered noop bypassed its missing source")?;
    assert!(matches!(
        ordered.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(
        poll_immediate(noop_checkout.lookup_no_follow(
            &path("ordered")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_none()
    );

    let direct_missing = poll_immediate(noop_checkout.change_content(path("missing")?, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::new()).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("direct missing noop succeeded")?;
    assert!(matches!(
        direct_missing.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));

    let created = poll_immediate(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("present")?,
            bytes: OwnedBytes::new(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let file_id = created.value.created_file_ids[0].ok_or("created file id missing")?;
    let generation_before_noop = noop_checkout.generation_id();
    let empty = poll_immediate(noop_checkout.apply_authored_transaction(
        Vec::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(empty.value.created_file_ids.is_empty());
    assert_eq!(empty.work, WorkCounters::default());
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let noop = poll_immediate(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("present")?,
            offset: 0,
            bytes: OwnedBytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(noop.value.created_file_ids, vec![None]);
    assert!(noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let mut empty_source = std::io::Cursor::new(OwnedBytes::new());
    let empty_content = poll_immediate(noop_checkout.stage_content(
        &mut empty_source,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert_eq!(empty_content.logical_bytes(), 0);
    let content_noop = poll_immediate(noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::WriteFromContent {
            path: path("present")?,
            offset: u64::MAX,
            content: empty_content,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(content_noop.value.created_file_ids, vec![None]);
    assert!(content_noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let direct = poll_immediate(noop_checkout.change_content(path("present")?, crate::ContentChange::Write { offset: u64::MAX, bytes: (bytes::Bytes::new()).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(direct.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let identity = poll_immediate(noop_checkout.change_content(file_id, crate::ContentChange::Write { offset: u64::MAX, bytes: (bytes::Bytes::new()).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(identity.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);

    let expanded = poll_immediate(checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("large")?,
            bytes: OwnedBytes::from(vec![1; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("expanded authored transaction unexpectedly succeeded")?;
    assert!(matches!(expanded.error, FsError::TooManyPendingMutations));
    assert_eq!(*expanded.work, WorkCounters::default());
    assert!(
        poll_immediate(checkout.lookup_no_follow(
            &path("large")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_none()
    );

    let mut byte_limited = config();
    byte_limited.limits.maximum_read_bytes = 1;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        byte_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    for mutation in [
        AuthoredMutation::CreateFile { path: path("created-file")?, bytes: OwnedBytes::from_static(b"xx"), metadata: FileMetadata::default() },
        AuthoredMutation::Write { path: path("file")?, offset: 0, bytes: OwnedBytes::from_static(b"xx") },
        AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: OwnedBytes::from_static(b"xx"), metadata: FileMetadata::default() },
    ] {
        let failure = poll_immediate(checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("byte-limited authored transaction unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let direct = poll_immediate(checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("direct-link")?, target: (bytes::Bytes::from_static(b"xx")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("byte-limited direct symlink unexpectedly succeeded")?;
    assert!(matches!(
        direct.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*direct.work, WorkCounters::default());
    Ok(())
}

#[test]
fn opaque_payload_reads_enforce_the_volume_output_bound() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.profile = FilesystemProfile::Windows;
    limited.limits.maximum_read_bytes = 1;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([126; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(b"xx"));
    let staged =
        poll_immediate(checkout.stage_content(&mut source, 2, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    let metadata =
        poll_immediate(checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation))?
            .value.record.ok_or("root metadata missing")?.metadata;
    let symbolic = path("symbolic")?;
    let reparse = path("reparse")?;
    poll_immediate(checkout.mutate(
        vec![
            Mutation::Create {
                path: symbolic.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([127; 16]),
                    kind: FileKind::SymbolicLink,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::SymbolicLink {
                        target_bytes: staged.logical_bytes(),
                        target: staged.root(),
                    },
                },
            },
            Mutation::Create {
                path: reparse.clone(),
                record: FileRecord {
                    file_id: FileId::from_bytes([128; 16]),
                    kind: FileKind::ReparsePoint,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::ReparsePoint {
                        payload_bytes: staged.logical_bytes(),
                        payload: staged.root(),
                    },
                },
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    for failure in [
        poll_immediate(checkout.read_symbolic_link(&symbolic, WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("oversized symlink read unexpectedly succeeded")?,
        poll_immediate(checkout.read_reparse_point(&reparse, WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("oversized reparse read unexpectedly succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert!(failure.work.object_probes > 0);
    }
    Ok(())
}

#[test]
fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([114; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;

    let mut create_source = std::io::Cursor::new(OwnedBytes::from_static(b"streamed"));
    let staged_create = poll_immediate(checkout.stage_content(
        &mut create_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut write_source = std::io::Cursor::new(OwnedBytes::from_static(b"TAIL"));
    let staged_write = poll_immediate(checkout.stage_content(
        &mut write_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert_eq!(staged_create.logical_bytes(), 8);
    assert_eq!(staged_create.root().kind, ObjectKind::Blob);
    assert_eq!(staged_write.logical_bytes(), 4);
    assert_eq!(staged_write.root().kind, ObjectKind::Blob);

    let source = path("source")?;
    let destination = path("destination")?;
    let streamed = path("streamed")?;
    let streamed_identity = FileId::from_bytes([199; 16]);
    let hard_link = path("hard-link")?;
    let renamed = path("renamed")?;
    let mut replacement_metadata = FileMetadata::default();
    replacement_metadata.modified_ns = MetadataField::Value(42);
    let large = OwnedBytes::from(vec![0x5a; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]);
    let transaction = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("directory")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: source.clone(),
                bytes: OwnedBytes::from_static(b"abcdefgh"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: destination.clone(),
                bytes: OwnedBytes::from_static(b"................"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("large")?,
                bytes: large.clone(),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFileFromContent {
                path: streamed.clone(),
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            },
            AuthoredMutation::CreateSymbolicLink {
                path: path("symbolic")?,
                target: OwnedBytes::from_static(b"../source"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 0,
                bytes: OwnedBytes::new(),
            },
            AuthoredMutation::Write {
                path: source.clone(),
                offset: 6,
                bytes: OwnedBytes::from_static(b"XY"),
            },
            AuthoredMutation::WriteFromContent {
                path: streamed.clone(),
                offset: 8,
                content: staged_write,
            },
            AuthoredMutation::SetMetadata {
                path: source.clone(),
                metadata: replacement_metadata,
            },
            AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 24,
            },
            AuthoredMutation::ZeroRange {
                path: source.clone(),
                range: ByteRange {
                    offset: 2,
                    length: 2,
                },
                allocated: false,
                extend: false,
            },
            AuthoredMutation::Preallocate {
                path: destination.clone(),
                range: ByteRange {
                    offset: 16,
                    length: 8,
                },
                keep_size: false,
            },
            AuthoredMutation::CloneRange(FileCloneRequest {
                source: source.clone(),
                source_offset: 0,
                destination: destination.clone(),
                destination_offset: 4,
                length: 8,
            }),
            AuthoredMutation::HardLink {
                source: source.clone(),
                destination: hard_link.clone(),
            },
            AuthoredMutation::Rename {
                source: hard_link,
                destination: renamed.clone(),
                replace: false,
            },
            AuthoredMutation::Remove {
                path: renamed,
                expected_file_id: None,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(transaction.value.created_file_ids.len(), 17);
    assert_eq!(
        transaction.value.created_file_ids.get(4),
        Some(&Some(streamed_identity))
    );
    assert_eq!(
        transaction
            .value
            .created_file_ids
            .iter()
            .filter(|created| created.is_some())
            .count(),
        6
    );
    assert_eq!(
        poll_immediate(checkout.read_file_range(
            &streamed,
            ByteRange {
                offset: 0,
                length: 12,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .bytes
        .as_ref(),
        b"streamedTAIL"
    );
    assert!(
        poll_immediate(checkout.apply_authored_transaction(
            vec![AuthoredMutation::CreateFileFromContent {
                path: path("duplicate-source-identity")?,
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert_eq!(
        poll_immediate(checkout.read_file_range(
            &path("large")?,
            ByteRange {
                offset: 0,
                length: u64::try_from(large.len())?,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .bytes.as_ref(),
        large.as_ref()
    );
    assert_eq!(
        poll_immediate(checkout.read_symbolic_link(
            &path("symbolic")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value.bytes
        .as_ref(),
        b"../source"
    );
    assert_eq!(
        poll_immediate(checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))?
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}

#[test]
fn path_sdk_exposes_every_sparse_operation_with_one_authenticated_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert_eq!(volume.id(), VolumeId::from_bytes([117; 16]));
    assert_eq!(volume.config(), config());
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let source = path("source-path")?;
    let destination = path("destination-path")?;
    let source_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: source.clone(), bytes: (bytes::Bytes::from(vec![b's'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: destination.clone(), bytes: (bytes::Bytes::from(vec![b'd'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let mut metadata = FileMetadata::default();
    metadata.modified_ns = MetadataField::Value(99);
    poll_immediate(checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: source.clone(), metadata: metadata }], WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.change_content(destination.clone(), crate::ContentChange::Resize { logical_bytes: 160 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.change_content(source.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 64,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.change_content(destination.clone(), crate::ContentChange::Preallocate { range: ByteRange {
            offset: 128,
            length: 32,
        }, keep_size: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.change_content(destination.clone(), ContentChange::CloneFrom { source: source.clone(), source_offset: 0, offset: 32, length: 96 }, ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    let alias = path("source-alias")?;
    let renamed = path("source-renamed")?;
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Link { source: source.clone(), destination: alias.clone() }], WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Rename { source: alias, destination: renamed.clone(), replace: false }], WorkBudget::UNBOUNDED, &cancellation))?;
    let lookup = poll_immediate(checkout.lookup_no_follow_with_metadata(
        &renamed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .ok_or("renamed link is absent")?;
    assert_eq!(lookup.record.file_id, source_id);
    assert_eq!(lookup.metadata.modified_ns, MetadataField::Value(99));
    let batch = poll_immediate(checkout.lookup_batch_no_follow(
        &[source.clone(), renamed.clone(), path("absent-path")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(batch.value.entries.len(), 3);
    assert_eq!(
        batch.value.entries[0].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert_eq!(
        batch.value.entries[1].record.map(|record| record.file_id),
        Some(source_id)
    );
    assert!(batch.value.entries[2].record.is_none());
    let records = poll_immediate(checkout.list_directory_records(
        &root_path()?,
        None,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(records.value.entries.len(), 3);
    let plan = poll_immediate(checkout.plan_file_extents(
        &source,
        ByteRange {
            offset: 0,
            length: 128,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(plan.value.is_some());
    assert_eq!(
        poll_immediate(checkout.seek_file_extent(
            &source,
            64,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value,
        Some(64)
    );
    assert_eq!(
        poll_immediate(checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,))?
            .value,
        metadata
    );
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Remove { path: renamed, expected_file_id: (Some(source_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation))?;

    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    assert!(
        poll_immediate(checkout.stage_content(&mut empty, 0, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(checkout.stage_content(
            &mut empty,
            config().limits.maximum_generation_bytes + 1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let body = [b'n'; 256];
        let mut source = std::io::Cursor::new(body.as_slice());
        let staged = poll_immediate(checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        ))?;
        assert_eq!(staged.logical_bytes(), body.len() as u64);
        assert_eq!(original.receipt().source_bytes_read, body.len() as u64);
        assert!(original.receipt().backend_write_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&denied, &cancellation);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!(poll_immediate(checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(denied.receipt().backend_write_operations, 0);

        struct PrefixFailure(bool);
        impl crate::kernel::AsyncBlobSource for PrefixFailure {
            async fn read<'a>(
                &'a mut self, destination: &'a mut [u8], _budget: WorkBudget,
                _cancellation: &'a CancellationToken,
            ) -> std::io::Result<usize> {
                if self.0 { return Err(std::io::Error::other("content source failed")); }
                self.0 = true;
                let count = destination.len().min(4);
                destination[..count].fill(b'p');
                Ok(count)
            }
            async fn read_with_native_work<'a>(
                &'a mut self, destination: &'a mut [u8], original: &'a crate::NativeMaterializationWork,
                cancellation: &'a CancellationToken,
            ) -> Result<usize, FsError> {
                cancellation.check()?;
                let delta = WorkCounters {
                    source_bytes_read: destination.len().min(4) as u64,
                    bytes_copied: destination.len().min(4) as u64,
                    ..WorkCounters::UNCHARGED
                };
                original.admit(delta)?;
                let count = self.read(destination, original.remaining()?, cancellation).await
                    .map_err(crate::kernel::BlobBuildError::Source)?;
                original.spent(delta)?;
                Ok(count)
            }
        }
        let failed = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&failed, &cancellation);
        let mut source = PrefixFailure(false);
        assert!(poll_immediate(checkout.stage_content_with_accounting(
            &mut source, 64, &mut accounting, &cancellation,
        )).is_err());
        assert_eq!(failed.receipt().source_bytes_read, 4);

        let cancelled = CancellationToken::new();
        let stopped = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
            .map_err(|(cause, _)| cause)?;
        cancelled.cancel();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&stopped, &cancelled);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!(poll_immediate(checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancelled,
        )).is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(stopped.receipt().backend_write_operations, 0);
    }
    Ok(())
}

#[test]
fn public_query_and_identity_failures_preserve_the_checkout_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([118; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("regular")?;
    let directory = path("directory")?;
    let missing = path("missing")?;
    let file_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"data")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    let directory_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateDirectory { path: directory.clone(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    let empty_directory = poll_immediate(checkout.list_directory_records(
        &directory,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(empty_directory.value.entries.is_empty());
    assert!(!empty_directory.value.has_more);
    let unknown_id = FileId::from_bytes([250; 16]);
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.missing".to_vec(),
        128,
    )?;

    let oversized_identity_read = poll_immediate(checkout.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: config().limits.maximum_read_bytes + 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("oversized identity read unexpectedly succeeded")?;
    assert!(matches!(
        oversized_identity_read.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*oversized_identity_read.work, WorkCounters::default());
    let zero_span_plan = poll_immediate(checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 1,
        },
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("zero-span identity plan unexpectedly succeeded")?;
    assert!(matches!(
        zero_span_plan.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*zero_span_plan.work, WorkCounters::default());
    assert_eq!(
        poll_immediate(checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value,
        Some(0)
    );
    assert_eq!(
        poll_immediate(checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value,
        Some(4)
    );
    for (offset, target, expected) in [
        (4, ExtentSeekTarget::Data, None),
        (4, ExtentSeekTarget::Hole, Some(4)),
        (5, ExtentSeekTarget::Data, None),
        (5, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            poll_immediate(checkout.seek_file_extent_by_id(
                file_id,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?
            .value,
            expected
        );
        assert_eq!(
            poll_immediate(checkout.seek_file_extent(
                &file,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?
            .value,
            expected
        );
    }
    let wrong_kind_seek = poll_immediate(checkout.seek_file_extent_by_id(
        directory_id,
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("wrong-kind identity seek unexpectedly succeeded")?;
    assert!(matches!(
        wrong_kind_seek.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert!(
        poll_immediate(checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .is_none()
    );
    assert!(
        poll_immediate(checkout.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .entries
        .is_empty()
    );
    let missing_remove = poll_immediate(checkout.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("attribute-free identity removal unexpectedly succeeded")?;
    assert!(matches!(missing_remove.error, FsError::NotFound));
    let missing_replace = poll_immediate(checkout.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("missing identity attribute replacement unexpectedly succeeded")?;
    assert!(matches!(missing_replace.error, FsError::CreationRejected));
    poll_immediate(checkout.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))?;
    let duplicate_create = poll_immediate(checkout.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("duplicate identity attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        poll_immediate(checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    poll_immediate(checkout.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation))?;
    let repeated_remove = poll_immediate(checkout.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("repeated identity attribute removal unexpectedly succeeded")?;
    assert!(matches!(repeated_remove.error, FsError::NotFound));

    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut candidate = checkout.private_candidate(&original, &cancellation)?;
        let source = OwnedBytes::from_static(b"actual original-accounted attribute source");
        poll_immediate(candidate.change_named_attribute_by_id(file_id, &attribute, Some((&source, NamedAttributeWriteMode::Create)), &original, &cancellation))?;
        let created = poll_immediate(candidate.read_metadata_by_id(
            file_id, &original, &cancellation,
        ))?;
        let MetadataField::Value(root) = created.named_attributes else {
            return Err("native attribute metadata root absent".into());
        };
        let reader = candidate.pinned_reader_with_native_work(&original, &cancellation)?;
        let (values, allocation) = poll_immediate(reader.read_named_attributes_with_native_work(
            root, &original, &cancellation,
        ))?;
        assert_eq!(values.len(), 1);
        assert_eq!(&values[0].0, &attribute);
        assert_eq!(values[0].1, source);
        drop(values);
        drop(allocation);
        let duplicate = poll_immediate(candidate.change_named_attribute_by_id(file_id, &attribute, Some((&source, NamedAttributeWriteMode::Create)), &original, &cancellation))
            .err().ok_or("native duplicate attribute unexpectedly accepted")?;
        assert!(matches!(duplicate, FsError::CreationRejected));
        let mut updated = created;
        updated.modified_ns = MetadataField::Value(913);
        poll_immediate(candidate.set_attributes_by_id(
            file_id, updated, Some(7), &original, &cancellation,
        ))?;
        assert_eq!(poll_immediate(candidate.read_metadata_by_id(
            file_id, &original, &cancellation,
        ))?, updated);
        let resized = poll_immediate(candidate.read_file_record_by_id(
            file_id, &original, &cancellation,
        ))?;
        let resized_bytes = match resized.payload {
            FilePayload::InlineRegular(data) => data.as_bytes().len() as u64,
            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => return Err("native resize changed the regular file kind".into()),
        };
        assert_eq!(resized_bytes, 7);
        poll_immediate(candidate.change_named_attribute_by_id(file_id, &attribute, None, &original, &cancellation))?;
        assert!(original.receipt().source_bytes_read >= source.len() as u64);
        assert!(original.receipt().bytes_encoded > 0);
        assert!(original.receipt().allocation_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut denied_candidate = checkout.private_candidate(&denied, &cancellation)?;
        let before = denied_candidate.root.file_table;
        let failure = poll_immediate(denied_candidate.change_named_attribute_by_id(file_id, &attribute, Some((&source, NamedAttributeWriteMode::Upsert)), &denied, &cancellation))
            .err().ok_or("native attribute source denial ignored")?;
        assert!(matches!(failure, FsError::Work(_)));
        assert_eq!(denied_candidate.root.file_table, before);
        assert_eq!(denied.receipt().source_bytes_read, 0);

        let denied_metadata = crate::NativeMaterializationWork::new(
            WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut denied_candidate = checkout.private_candidate(&denied_metadata, &cancellation)?;
        let before = denied_candidate.root.file_table;
        let failure = poll_immediate(denied_candidate.set_attributes_by_id(
            file_id, updated, Some(3), &denied_metadata, &cancellation,
        ))
            .err().ok_or("native metadata encoding denial ignored")?;
        assert!(matches!(failure, FsError::Work(_)));
        assert_eq!(denied_candidate.root.file_table, before);
        assert_eq!(denied_metadata.receipt().bytes_encoded, 0);
    }

    assert!(
        poll_immediate(checkout.list_directory(&file, None, 8, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(checkout.list_directory_records(
            &file,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.read_file_range(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.plan_file_extents(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.seek_file_extent(
            &directory,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.read_symbolic_link(&file, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(checkout.read_reparse_point(&file, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(checkout.read_metadata(&missing, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(checkout.read_named_attribute(
            &missing,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.list_named_attributes(
            &missing,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.change_named_attribute(missing.clone(), &attribute, None, WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );

    assert!(
        poll_immediate(checkout.read_file_record_by_id(
            unknown_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.read_file_range_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.plan_file_extents_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.seek_file_extent_by_id(
            directory_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.read_metadata_by_id(unknown_id, WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(checkout.read_named_attribute_by_id(
            unknown_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.list_named_attributes_by_id(
            unknown_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.change_content(directory_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"x")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.change_named_attribute_by_id(unknown_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );
    assert!(checkout.has_pending_mutations());
    assert_eq!(
        poll_immediate(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .bytes
        .as_ref(),
        b"data"
    );
    Ok(())
}

#[test]
fn public_byte_boundaries_reject_before_allocation_or_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 4;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([189; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("bounded")?;
    let failure = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"12345")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("oversized create succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let file_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"1234")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    for failure in [
        poll_immediate(checkout.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"12345")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("oversized path write succeeded")?,
        poll_immediate(checkout.change_content(file_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"12345")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("oversized identity write succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    poll_immediate(checkout.change_content(file_id, crate::ContentChange::Write { offset: 4, bytes: (bytes::Bytes::from_static(b"5")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;

    for failure in [
        poll_immediate(checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("oversized path read succeeded")?,
        poll_immediate(checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("oversized identity read succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let excessive_range = ByteRange {
        offset: 0,
        length: limited.limits.maximum_generation_bytes + 1,
    };
    for failure in [
        poll_immediate(checkout.plan_file_extents(
            &file,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("oversized path plan succeeded")?,
        poll_immediate(checkout.plan_file_extents_by_id(
            file_id,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("oversized identity plan succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let symbolic = poll_immediate(checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (bytes::Bytes::from_static(b"12345")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("oversized symbolic link succeeded")?;
    assert!(matches!(
        symbolic.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*symbolic.work, WorkCounters::default());

    let mut detached =
        poll_immediate(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    let detached_read_failure = poll_immediate(detached.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("oversized detached read succeeded")?;
    assert!(matches!(
        detached_read_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_read_failure.work, WorkCounters::default());
    let detached_failure = poll_immediate(detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(b"12345") },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("oversized detached write succeeded")?;
    assert!(matches!(
        detached_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn checkout_generation_and_mode_guards_reject_without_candidate_damage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([119; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let second = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([120; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let first_head = poll_immediate(first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .generation_id();
    let second_head = poll_immediate(second.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .generation_id();
    assert!(
        poll_immediate(first.checkout(
            GenerationSelector::Exact(first_head),
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(matches!(
        poll_immediate(first.checkout(
            GenerationSelector::Exact(second_head), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )),
        Err(OperationFailure { error: FsError::VolumeMismatch, .. })
    ));
    assert!(
        poll_immediate(first.diff_generations(
            first_head,
            second_head,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );

    let mut read_only = poll_immediate(first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert!(
        poll_immediate(read_only.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation,))
            .is_err()
    );
    assert!(
        poll_immediate(read_only.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
            .is_err()
    );
    assert!(
        poll_immediate(read_only.discard(WorkBudget::UNBOUNDED, &cancellation))
            .is_err()
    );
    assert!(
        poll_immediate(read_only.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .is_err()
    );
    assert!(
        poll_immediate(read_only.refresh_live(WorkBudget::UNBOUNDED, &cancellation))
            .is_err()
    );
    assert!(
        poll_immediate(read_only.mutate_live(
            Vec::new(),
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(read_only.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );

    let mut writable = poll_immediate(first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let excessive_capacity = Vec::<Mutation>::with_capacity(
        usize::try_from(config().limits.maximum_mutations_per_batch)? + 1,
    );
    let failure =
        poll_immediate(writable.mutate(excessive_capacity, WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("excessive mutation capacity unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::ExcessiveMutationCapacity));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut clean = poll_immediate(first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert!(
        poll_immediate(clean.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
            .is_err()
    );
    assert!(
        poll_immediate(clean.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );

    let mut manual_checkout = poll_immediate(first.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(manual_checkout.create(crate::AuthoredMutation::CreateFile { path: path("dirty")?, bytes: (bytes::Bytes::from_static(b"dirty")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(
        poll_immediate(manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .is_err()
    );
    assert!(manual_checkout.has_pending_mutations());
    poll_immediate(manual_checkout.discard(WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(!manual_checkout.has_pending_mutations());
    assert_eq!(
        poll_immediate(manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))?
            .value,
        first_head
    );
    Ok(())
}

#[test]
fn authored_special_operations_are_profile_exact_and_fail_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([115; 16]),
        posix,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let receipt = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateEmptySpecial {
                path: path("fifo")?,
                kind: FileKind::Fifo,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("socket")?,
                kind: FileKind::Socket,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateEmptySpecial {
                path: path("mount")?,
                kind: FileKind::MountBoundary,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDevice {
                path: path("character")?,
                kind: FileKind::CharacterDevice,
                major: 1,
                minor: 3,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDevice {
                path: path("block")?,
                kind: FileKind::BlockDevice,
                major: 8,
                minor: 0,
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(receipt.value.created_file_ids.iter().all(Option::is_some));

    for (mutation, sentinel) in [
        (
            AuthoredMutation::CreateEmptySpecial {
                path: path("invalid-special")?,
                kind: FileKind::Regular,
                metadata: FileMetadata::default(),
            },
            path("invalid-special")?,
        ),
        (
            AuthoredMutation::CreateDevice {
                path: path("invalid-device")?,
                kind: FileKind::Regular,
                major: 0,
                minor: 0,
                metadata: FileMetadata::default(),
            },
            path("invalid-device")?,
        ),
    ] {
        let failure = poll_immediate(checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        assert!(failure.is_err());
        assert!(
            poll_immediate(checkout.lookup_no_follow(&sentinel, WorkBudget::UNBOUNDED, &cancellation,))?
                .value
                .record
                .is_none()
        );
    }

    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let windows_volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([116; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut windows_checkout = poll_immediate(windows_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let reparse = path("reparse")?;
    let reparse_payload = OwnedBytes::from_static(b"opaque-reparse-data");
    let receipt = poll_immediate(windows_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateReparsePoint {
            path: reparse.clone(),
            payload: reparse_payload.clone(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(receipt.value.created_file_ids[0].is_some());
    assert_eq!(
        poll_immediate(windows_checkout.read_reparse_point(
            &reparse,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value.as_ref(),
        reparse_payload.as_ref()
    );
    Ok(())
}

#[test]
fn manifest_transfer_restores_only_after_the_complete_closure_authenticates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Fs::memory();
    let destination = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(source.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("file")?, bytes: (bytes::Bytes::from(vec![42; 4096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let first_checkpoint = poll_immediate(checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation))?;
    let repeated_checkpoint = poll_immediate(checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(first_checkpoint.value, repeated_checkpoint.value);
    let manifest = poll_immediate(checkout.export_manifest(WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    assert_eq!(manifest.generation_id, first_checkpoint.value);
    assert!(manifest.objects.contains(&manifest.generation_root));

    let mut cursor = TransferCursor::START;
    loop {
        let exported = poll_immediate(source.export_generation_batch(
            &manifest,
            cursor,
            2,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?;
        let bodies = exported
            .value
            .objects
            .iter()
            .map(|object| object.bytes.clone())
            .collect::<Vec<_>>();
        let imported = poll_immediate(destination.import_generation_batch(
            &manifest,
            cursor,
            &bodies,
            2,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?;
        assert_eq!(
            imported.value.next_object(),
            cursor.next_object() + u64::try_from(bodies.len())?
        );
        let Some(next) = exported.value.next else {
            assert_eq!(
                imported.value.next_object(),
                u64::try_from(manifest.objects.len())?
            );
            break;
        };
        assert_eq!(imported.value, next);
        cursor = next;
    }
    let mut wrong_volume = manifest.clone();
    wrong_volume.volume_id = VolumeId::from_bytes([96; 16]);
    let mut wrong_generation = manifest.clone();
    wrong_generation.generation_id = GenerationId::new(Digest::from_bytes([0xee; 32]));
    let mut wrong_objects = manifest.clone();
    wrong_objects
        .objects
        .pop()
        .ok_or("manifest closure is empty")?;
    let mut wrong_file_count = manifest.clone();
    wrong_file_count.file_count = wrong_file_count
        .file_count
        .checked_add(1)
        .ok_or("fixture file count overflow")?;
    for (label, invalid, expected_backend_reads) in [
        ("volume", wrong_volume, true),
        ("generation", wrong_generation, false),
        ("objects", wrong_objects, true),
        ("file-count", wrong_file_count, true),
    ] {
        let failure = poll_immediate(destination.restore_volume(
            &invalid,
            OperationId::from_bytes([31; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .err()
        .ok_or("invalid manifest unexpectedly restored")?;
        assert!(matches!(failure.error, FsError::InvalidExportManifest));
        assert_eq!(
            failure.work.backend_read_operations > 0,
            expected_backend_reads,
            "unexpected invalid-manifest physical plan for {label}"
        );
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    let restore_operation = OperationId::from_bytes([32; 16]);
    let restored = poll_immediate(destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let retried = poll_immediate(destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert_eq!(retried.id(), restored.id());
    let changed_operation = poll_immediate(destination.restore_volume(
        &manifest,
        OperationId::from_bytes([33; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("changed-operation restore unexpectedly succeeded")?;
    assert!(matches!(changed_operation.error, FsError::CreationRejected));
    let mut restored_checkout = poll_immediate(restored.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let bytes = poll_immediate(restored_checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 0,
            length: 4096,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(bytes.value.bytes.as_ref(), vec![42; 4096]);
    Ok(())
}

#[test]
fn windows_reparse_payload_round_trips_without_interpretation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([98; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let payload = bytes::Bytes::from_static(b"opaque-reparse\0payload");
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateReparsePoint { path: path("junction")?, payload: (payload.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let read = poll_immediate(checkout.read_reparse_point(
        &path("junction")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(read.value.as_ref(), payload.as_ref());

    let empty_reparse = path("empty-reparse")?;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateReparsePoint { path: empty_reparse.clone(), payload: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let empty_reparse_read = poll_immediate(checkout.read_reparse_point(
        &empty_reparse,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(empty_reparse_read.value.is_empty());

    let empty_link = path("empty-link")?;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: empty_link.clone(), target: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let empty_link_read =
        poll_immediate(checkout.read_symbolic_link(&empty_link, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(empty_link_read.value.bytes.is_empty());
    Ok(())
}

#[test]
fn memory_create_open_and_head_checkout_reconstruct_from_two_stores()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([41; 16]);
    let created = poll_immediate(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let opened = poll_immediate(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(opened.value.config(), config());
    let mut checkout = poll_immediate(opened.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(checkout.value.volume_id(), volume_id);
    assert_eq!(checkout.value.root().parents, Vec::new());
    assert_eq!(checkout.work.object_probes, 1);
    assert_eq!(checkout.work.page_reads, 0);
    assert!(checkout.work.object_bytes_read <= MAXIMUM_GENERATION_ROOT_BYTES);
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let root = poll_immediate(checkout.value.lookup_no_follow(
        &root_path,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(
        root.value.record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    let roots = poll_immediate(checkout.value.lookup_batch_no_follow(
        &[root_path.clone(), root_path],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(roots.value.entries.len(), 2);
    assert_eq!(roots.value.entries[0], roots.value.entries[1]);
    assert!(created.work.backend_write_operations >= 4);
    Ok(())
}

#[test]
fn create_retry_is_idempotent_and_exact_selector_matches_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([42; 16]);
    let first = poll_immediate(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let retry = poll_immediate(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let head = poll_immediate(first.value.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let exact = poll_immediate(retry.value.checkout(
        GenerationSelector::Exact(head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(exact.value.generation_id(), head.value.generation_id());
    Ok(())
}

#[test]
fn unsupported_semantics_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let result = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([43; 16]),
        durable,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let Err(failure) = result else {
        return Err("durable memory volume succeeded".into());
    };
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn conflicting_creation_config_is_rejected_without_changing_opened_config()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([44; 16]);
    poll_immediate(fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation))?;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    let retry = poll_immediate(fs.create_volume_with_id(
        volume_id,
        conflicting,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let Err(failure) = retry else {
        return Err("conflicting creation succeeded".into());
    };
    assert!(matches!(failure.error, FsError::CreationRejected));
    let opened = poll_immediate(fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(opened.value.config(), config());
    Ok(())
}

#[test]
fn pre_cancelled_facade_operations_perform_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([47; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let Err(failure) = result else {
        return Err("cancelled create succeeded".into());
    };
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! cancelled {
        ($future:expr, $label:literal) => {{
            let result = poll_immediate($future);
            let failure = result
                .err()
                .ok_or(concat!($label, " unexpectedly succeeded"))?;
            assert!(
                format!("{:?}", failure.error).contains("Cancelled"),
                "{}: {:?}",
                $label,
                failure.error
            );
            assert_eq!(*failure.work, WorkCounters::default(), "{}", $label);
        }};
    }

    let fs = Fs::memory();
    let active = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([137; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &active,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &active,
    ))?
    .value;
    let file = path("cancelled-file")?;
    let file_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &active))?
    .value;
    let mut detached =
        poll_immediate(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &active))?
            .value;
    let cancelled_token = CancellationToken::new();
    cancelled_token.cancel();
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.cancelled".to_vec(),
        128,
    )?;

    cancelled!(
        volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled checkout"
    );
    cancelled!(
        checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled lookup"
    );
    cancelled!(
        checkout.lookup_batch_no_follow(
            std::slice::from_ref(&file),
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled batch lookup"
    );
    cancelled!(
        checkout.list_directory(
            &root_path()?,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled directory listing"
    );
    cancelled!(
        checkout.read_metadata(&file, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled metadata"
    );
    cancelled!(
        checkout.read_file_range(&file, range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled path read"
    );
    cancelled!(
        checkout.plan_file_extents(&file, range, 8, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path plan"
    );
    cancelled!(
        checkout.seek_file_extent(
            &file,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled path seek"
    );
    cancelled!(
        checkout.read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled identity record"
    );
    cancelled!(
        checkout.read_file_range_by_id(file_id, range, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled identity read"
    );
    cancelled!(
        checkout.read_named_attribute(&file, &attribute, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled path attribute"
    );
    cancelled!(
        checkout.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"x")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled path write"
    );
    cancelled!(
        checkout.create(crate::AuthoredMutation::CreateFile { path: path("cancelled-new-file")?, bytes: (bytes::Bytes::from_static(b"must-not-be-encoded")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled file creation"
    );
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    cancelled!(
        checkout.stage_content(&mut empty, 1, WorkBudget::UNBOUNDED, &cancelled_token,),
        "cancelled content staging"
    );
    cancelled!(
        checkout.commit(OperationId::from_bytes([138; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancelled_token, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }),
        "cancelled commit"
    );
    cancelled!(
        detached.read_range(range, WorkBudget::UNBOUNDED, &cancelled_token),
        "cancelled detached read"
    );
    cancelled!(
        detached.change_content(
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"x")
            },
            None,
            WorkBudget::UNBOUNDED,
            &cancelled_token,
        ),
        "cancelled detached write"
    );
    Ok(())
}

#[test]
fn private_overlay_commit_retry_and_conflict_are_generation_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let volume = poll_immediate(fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut first = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut retry = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut stale = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        poll_immediate(first.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation))?
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([7; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"hello")?),
    };
    for checkout in [&mut first, &mut retry] {
        poll_immediate(checkout.mutate(
            vec![Mutation::Create {
                path: path("a")?,
                record,
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?;
        assert_eq!(
            poll_immediate(checkout.lookup_no_follow(
                &path("a")?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?
            .value
            .record
            .map(|value| value.payload),
            Some(record.payload)
        );
    }
    assert!(
        poll_immediate(stale.lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,))?
            .value
            .record
            .is_none()
    );
    poll_immediate(stale.mutate(
        vec![Mutation::Create {
            path: path("b")?,
            record: FileRecord {
                file_id: FileId::from_bytes([8; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;

    let operation_id = OperationId::from_bytes([9; 16]);
    let committed = poll_immediate(first.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!first.has_pending_mutations());
    let repeated = poll_immediate(retry.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        repeated.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!retry.has_pending_mutations());
    let conflict = poll_immediate(stale.commit(OperationId::from_bytes([10; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        conflict.value,
        CheckoutCommitOutcome::Conflict { .. }
    ));
    assert!(stale.has_pending_mutations());

    let mut head = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(
        poll_immediate(
            head.value
                .lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,)
        )?
        .value
        .record
        .is_some()
    );
    let clean_failure = poll_immediate(first.commit(OperationId::from_bytes([11; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .err()
    .ok_or("clean checkout commit unexpectedly succeeded")?;
    assert!(matches!(clean_failure.error, FsError::NoPendingMutations));
    assert_eq!(*clean_failure.work, WorkCounters::default());

    let mut inverse = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let inverse_record = FileRecord {
        file_id: FileId::from_bytes([15; 16]),
        ..record
    };
    poll_immediate(inverse.mutate(
        vec![Mutation::Create {
            path: path("inverse")?,
            record: inverse_record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(inverse.has_pending_mutations());
    poll_immediate(inverse.mutate(
        vec![Mutation::Remove {
            path: path("inverse")?,
            expected_file_id: MetadataField::Value(inverse_record.file_id),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(!inverse.has_pending_mutations());
    assert!(inverse.dependencies.proof().is_empty());
    let inverse_commit = poll_immediate(inverse.commit(OperationId::from_bytes([16; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .err()
    .ok_or("inverse clean commit unexpectedly succeeded")?;
    assert!(matches!(inverse_commit.error, FsError::NoPendingMutations));
    assert_eq!(
        *inverse_commit.work,
        WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        }
    );
    Ok(())
}

#[test]
fn exclusive_writer_checkout_atomically_fences_every_prior_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([0xe1; 16]),
        exclusive_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut displaced = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut owner = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(displaced.create(crate::AuthoredMutation::CreateFile { path: path("displaced")?, bytes: (bytes::Bytes::from_static(b"stale")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let rejected = poll_immediate(displaced.commit(OperationId::from_bytes([0xe2; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        rejected.value,
        CheckoutCommitOutcome::Fenced { actual_epoch } if actual_epoch.get() == 3
    ));
    poll_immediate(owner.create(crate::AuthoredMutation::CreateFile { path: path("owner")?, bytes: (bytes::Bytes::from_static(b"current")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let committed = poll_immediate(owner.commit(OperationId::from_bytes([0xe3; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { head, .. } if head.epoch.get() == 3
    ));
    Ok(())
}

#[test]
fn manual_refresh_is_explicit_bounded_and_never_discards_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([50; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut clean = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut clean_tracking = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut dirty = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut conflicted = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut pinned_checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        poll_immediate(writer.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation))?
            .value
            .record
            .ok_or("root record absent")?
            .metadata;
    let record = FileRecord {
        file_id: FileId::from_bytes([12; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"new-head")?),
    };
    assert!(
        poll_immediate(conflicted.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_none()
    );
    poll_immediate(conflicted.mutate(
        vec![Mutation::Create {
            path: path("conflicted-local")?,
            record: FileRecord {
                file_id: FileId::from_bytes([16; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(dirty.mutate(
        vec![Mutation::Create {
            path: path("private")?,
            record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(writer.mutate(
        vec![Mutation::Create {
            path: path("published")?,
            record: FileRecord {
                file_id: FileId::from_bytes([13; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(writer.commit(OperationId::from_bytes([14; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let conflict = poll_immediate(conflicted.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.iter().any(|value| matches!(
            value.region,
            DependencyRegion::DirectoryName { .. }
        ))
    ));
    assert!(conflicted.has_pending_mutations());

    let pinned_failure =
        poll_immediate(pinned_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("pinned refresh unexpectedly succeeded")?;
    assert!(matches!(pinned_failure.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_failure.work, WorkCounters::default());
    let pinned_rebase =
        poll_immediate(pinned_checkout.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("pinned rebase unexpectedly succeeded")?;
    assert!(matches!(pinned_rebase.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_rebase.work, WorkCounters::default());

    let dirty_failure = poll_immediate(dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation))
        .err()
        .ok_or("dirty refresh unexpectedly succeeded")?;
    assert!(matches!(
        dirty_failure.error,
        FsError::PendingMutationsRequireRebase
    ));
    assert_eq!(*dirty_failure.work, WorkCounters::default());
    assert!(dirty.has_pending_mutations());
    assert!(
        poll_immediate(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )?
        .value
        .record
        .is_some()
    );
    let rebased = poll_immediate(dirty.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(rebased.value, RebaseDecision::Safe { .. }));
    assert!(dirty.has_pending_mutations());
    assert!(
        poll_immediate(dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_some()
    );
    assert!(
        poll_immediate(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )?
        .value
        .record
        .is_some()
    );

    let advanced = poll_immediate(clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(advanced.value, writer.generation_id());
    assert!(advanced.work.object_probes > 0);
    let tracking_advanced =
        poll_immediate(clean_tracking.refresh_head(WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(tracking_advanced.value, writer.generation_id());
    assert!(
        poll_immediate(clean.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_some()
    );
    let equal = poll_immediate(clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(equal.value, advanced.value);
    assert_eq!(equal.work.object_probes, 0);
    assert_eq!(equal.work.backend_read_operations, 3);

    assert!(!dirty.dependencies.proof().is_empty());
    poll_immediate(dirty.discard(WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(!dirty.has_pending_mutations());
    assert!(dirty.dependencies.proof().is_empty());
    assert!(
        poll_immediate(
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        )?
        .value
        .record
        .is_none()
    );
    assert!(
        poll_immediate(dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value
        .record
        .is_some()
    );
    let after_discard = poll_immediate(dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(after_discard.value, writer.generation_id());
    Ok(())
}

#[test]
fn private_candidate_rebases_more_changes_than_one_mutation_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 8;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([241; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut remote = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    for index in 0..20 {
        poll_immediate(local.create(crate::AuthoredMutation::CreateFile { path: path(&format!("local{index}"))?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    }
    poll_immediate(remote.create(crate::AuthoredMutation::CreateFile { path: path("remote")?, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(remote.commit(OperationId::from_bytes([242; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    assert!(matches!(
        poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?
            .value,
        RebaseDecision::Safe { .. }
    ));
    assert!(matches!(
        poll_immediate(local.commit(OperationId::from_bytes([243; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?
        .value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    for index in 0..20 {
        assert!(
            poll_immediate(local.lookup_no_follow(
                &path(&format!("local{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))?
            .value
            .record
            .is_some()
        );
    }
    assert!(
        poll_immediate(local.lookup_no_follow(&path("remote")?, WorkBudget::UNBOUNDED, &cancellation,))?
            .value
            .record
            .is_some()
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_reopens_durable_volume_and_exact_generation()
-> Result<(), Box<dyn std::error::Error>> {
    use fs2::FileExt as _;

    let directory = tempfile::tempdir()?;
    let volume_id = VolumeId::from_bytes([48; 16]);
    let cancellation = CancellationToken::new();
    let generation_id;
    let root_released;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    {
        let fs = Fs::local(LocalOptions::new(directory.path())).await?;
        root_released = fs
            .local_root_release_barrier()
            .ok_or("local root has no release barrier")?;
        let created = fs
            .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        let checkout = created
            .value
            .checkout(
                GenerationSelector::Head,
                writable_pinned(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        generation_id = checkout.value.generation_id();
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !root_released.is_released() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    for relative in ["stream/stream.lock", "objects/owner.lock"] {
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.path().join(relative))?;
        lock.try_lock_exclusive()?;
        lock.unlock()?;
    }
    let objects_journal = directory.path().join("objects").join("mutations.log");
    let journal_bytes_before_reopen = std::fs::metadata(&objects_journal)?.len();
    let reopened = Fs::local(LocalOptions::new(directory.path())).await?;
    assert_eq!(
        std::fs::metadata(objects_journal)?.len(),
        journal_bytes_before_reopen,
        "read-only reopen must not append an idempotent bucket mutation"
    );
    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let exact = volume
        .value
        .checkout(
            GenerationSelector::Exact(generation_id),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(exact.value.generation_id(), generation_id);
    assert_eq!(exact.value.volume_id(), volume_id);
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[test]
fn local_root_lifecycle_outlives_provider_destruction() -> Result<(), Box<dyn std::error::Error>> {
    struct BlockingProviderDrop {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Drop for BlockingProviderDrop {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }

    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).try_lock_owned()?;
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let inner = FsInner {
        authority: BlockingProviderDrop {
            entered: entered_tx,
            release: release_rx,
        },
        objects: (),
        capabilities: EmbeddedCapabilities::MEMORY,
        workspace_namespace: [0; 16],
        path_index: Arc::new(crate::path_index::MemoryGenerationPathIndex::default()),
        _local_root_lifecycle: Some(acyclic_native_runtime::OwnershipAnchor::new(ownership)),
    };
    let dropping = std::thread::spawn(move || drop(inner));

    entered_rx.recv()?;
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "replacement root ownership must remain fenced while providers are dropping"
    );
    release_tx.send(())?;
    dropping
        .join()
        .map_err(|_| "provider drop thread panicked")?;
    Arc::clone(&lifecycle).try_lock_owned()?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_provider_handles_retain_local_root_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).lock_owned().await;
    let fs = Fs::open_local_unshared(
        LocalOptions::new(directory.path()),
        Some(acyclic_native_runtime::OwnershipAnchor::new(ownership)),
    )
    .await?;
    let stream = fs.inner.authority.provider();
    let objects = Arc::clone(fs.inner.objects.inner().inner().provider());
    drop(fs);

    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "a detached Stream or Objects handle must retain the canonical root gate"
    );
    drop(stream);
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "the root gate must remain held until every detached provider is gone"
    );
    drop(objects);
    Arc::clone(&lifecycle).try_lock_owned()?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn cancelled_local_open_keeps_the_canonical_lifecycle_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let first = Fs::local(options.clone()).await?;
    let retained = first
        .inner
        ._local_root_lifecycle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("local root ownership"))?
        .clone();
    drop(first);

    let retry_options = options.clone();
    let opening = tokio::spawn(async move { Fs::local(retry_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!opening.is_finished());
    let cancelled_options = options.clone();
    let cancelled = tokio::spawn(async move { Fs::local(cancelled_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!cancelled.is_finished());
    cancelled.abort();
    assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));

    drop(retained);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(2), opening).await???;
    let coalesced = Fs::local(options).await?;
    assert!(reopened.same_deployment(&coalesced));
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn cancelled_local_initialization_keeps_root_owned_until_startup_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).lock_owned().await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let opening = tokio::spawn(run_local_initialization(
        ownership,
        crate::obs::span!(DEBUG, "test.local_initialization"),
        move |ownership| async move {
            let _ownership = ownership;
            let _ = started_tx.send(());
            let _ = release_rx.await;
            drop(_ownership);
        },
    ));

    started_rx.await?;
    opening.abort();
    let cancelled = opening.await;
    assert!(
        cancelled.as_ref().is_err_and(|error| error.is_cancelled()),
        "opening caller must be cancelled: {cancelled:?}"
    );
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "caller cancellation must not release ownership from an active startup worker"
    );
    let _ = release_tx.send(());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        Arc::clone(&lifecycle).lock_owned(),
    )
    .await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_shares_bounded_object_acceleration_across_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(LocalOptions::new(directory.path())).await?;
    let volume = fs
        .create_volume(durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    let baseline = fs.object_cache_stats()?;
    assert_eq!(baseline.resident_entries, 0);
    let first = fs
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let cloned = fs.clone();
    let warm = cloned
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let stats = fs.object_cache_stats()?;
    assert_eq!(stats.misses - baseline.misses, 1);
    assert_eq!(stats.hits - baseline.hits, 1);
    assert_eq!(stats.resident_entries, 1);
    assert!(stats.resident_bytes > 0);
    Ok(())
}

#[test]
fn public_identity_and_posix_special_surfaces_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }

    let mut volume_config = config();
    volume_config.profile = FilesystemProfile::Posix;
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(volume_config, WorkBudget::UNBOUNDED, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));

    let file = path("identity.bin")?;
    let clone = path("clone.bin")?;
    let file_id = ready!(checkout.value.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    let clone_id = ready!(checkout.value.create(crate::AuthoredMutation::CreateFile { path: clone.clone(), bytes: (bytes::Bytes::from_static(b"........")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;

    let record = ready!(checkout.value.read_file_record_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(record.value.file_id, file_id);
    assert_eq!(record.value.kind, FileKind::Regular);
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 2,
                length: 3,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        b"cde"
    );
    assert!(
        ready!(checkout.value.plan_file_extents_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 8,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(0)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        Some(8)
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent_by_id(
            file_id,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        None
    );
    assert_eq!(
        ready!(checkout.value.seek_file_extent(
            &file,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        None
    );

    let mut path_metadata = ready!(checkout.value.read_metadata(
        &file,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    path_metadata.posix_mode = MetadataField::Value(0o100_660);
    ready!(checkout.value.set_attributes(
        file.clone(),
        path_metadata,
        Some(10),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(
        ready!(checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 8,
                length: 2,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .bytes
        .as_ref(),
        &[0, 0]
    );

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity".to_vec(),
        128,
    )?;
    assert!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    assert!(
        ready!(checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .entries
        .is_empty()
    );
    ready!(checkout.value.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        ready!(checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .entries
        .len(),
        1
    );
    ready!(checkout.value.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation));

    let mut metadata = ready!(checkout.value.read_metadata_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    ready!(checkout.value.set_attributes_by_id(file_id, metadata, None, WorkBudget::UNBOUNDED, &cancellation));
    metadata.posix_mode = MetadataField::Value(0o100_600);
    ready!(checkout.value.set_attributes_by_id(
        file_id,
        metadata,
        Some(12),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    ready!(checkout.value.change_content(file_id, crate::ContentChange::Write { offset: 8, bytes: (bytes::Bytes::from_static(b"tail")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.change_content(file_id, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4,
            length: 2,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.change_content(file_id, crate::ContentChange::Preallocate { range: ByteRange {
            offset: 12,
            length: 4,
        }, keep_size: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(
        checkout
            .value
            .change_content(clone_id, crate::ContentChange::Resize { logical_bytes: 16 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)
    );
    ready!(checkout.value.change_content(clone_id, crate::ContentChange::CloneFrom { source: file_id, source_offset: 0, offset: 4, length: 8 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation));
    assert!(
        ready!(checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );

    let target = bytes::Bytes::from_static(b"../identity.bin");
    ready!(checkout.value.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (target.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    assert_eq!(
        ready!(checkout.value.read_symbolic_link(
            &path("link")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value.bytes.as_ref(),
        target.as_ref()
    );
    ready!(checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("fifo")?, kind: FileKind::Fifo, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("socket")?, kind: FileKind::Socket, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("character")?, kind: FileKind::CharacterDevice, major: 1, minor: 3, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("block")?, kind: FileKind::BlockDevice, major: 8, minor: 0, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    assert!(
        poll_immediate(checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("invalid")?, kind: FileKind::Regular, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );
    assert!(
        poll_immediate(checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("invalid-device")?, kind: FileKind::Regular, major: 0, minor: 0, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );

    ready!(checkout.value.mutate(vec![crate::kernel::Mutation::Link { source: file.clone(), destination: path("hard-link")? }], WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.mutate(vec![crate::kernel::Mutation::Rename { source: path("hard-link")?, destination: path("renamed-link")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.mutate(vec![crate::kernel::Mutation::Remove { path: path("renamed-link")?, expected_file_id: (Some(file_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation));
    let listing = ready!(checkout.value.list_directory_records(
        &root_path()?,
        None,
        32,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(listing.value.entries.len(), 7);
    Ok(())
}
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_open_leaves_no_provider_opening_behind_it()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    for iteration in 0..24_u32 {
        let fs = Fs::local(options.clone()).await?;
        assert!(
            Fs::open_local_unshared(options.clone(), None)
                .await
                .is_err(),
            "a live engine excludes a second owner"
        );
        drop(fs);
        drop(
            acyclic_objects::v1::local::LocalObjects::open(
                directory.path().join("objects"),
                acyclic_objects::LocalObjectsLimits::default(),
                None,
            )
            .await
            .map_err(|error| format!("objects still owned after iteration {iteration}: {error}"))?,
        );
        drop(
            acyclic_stream::LocalStream::open(
                directory.path().join("stream"),
                acyclic_stream::LocalStreamLimits::default(),
            )
            .await
            .map_err(|error| format!("stream still owned after iteration {iteration}: {error}"))?,
        );
    }
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_garbage_collection_keeps_live_state_while_the_root_is_open()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let file = path("retained.txt")?;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(options.clone()).await?;
    let volume = fs
        .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"retained")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    checkout
        .commit(OperationId::from_bytes([49; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
        .await?;
    let pending = path("pending.txt")?;
    checkout
        .create(crate::AuthoredMutation::CreateFile { path: pending.clone(), bytes: (bytes::Bytes::from(vec![b'p'; 256 * 1_024])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    checkout
        .commit(OperationId::from_bytes([50; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    drop(checkout);
    drop(volume);
    drop(fs);

    let orphan_bytes = OwnedBytes::from(vec![b'o'; 64 * 1_024 + 1]);
    let orphan = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &orphan_bytes),
    };
    let objects = acyclic_objects::v1::local::LocalObjects::open(
        directory.path().join("objects"),
        acyclic_objects::LocalObjectsLimits::default(),
        None,
    )
    .await?;
    let bucket = acyclic_objects::v1::wire::BucketRef {
        name: "filesystem-objects".to_owned(),
    };
    acyclic_objects::v1::ObjectsProvider::head_bucket(
        &objects,
        acyclic_objects::v1::wire::HeadBucketRequest {
            bucket: Some(bucket.clone()),
        },
    )
    .await?;
    acyclic_objects::v1::ObjectsProvider::put(
        &objects,
        acyclic_objects::v1::wire::PutObjectHeader {
            bucket: Some(bucket),
            object_key: crate::distributed::object_key(orphan),
            metadata: Some(acyclic_objects::v1::wire::ObjectMetadata::default()),
            preconditions: Some(acyclic_objects::v1::wire::Preconditions {
                condition: Some(
                    acyclic_objects::v1::wire::preconditions::Condition::IfAbsent(true),
                ),
            }),
            mutation: None,
        },
        orphan_bytes,
    )
    .await?;
    drop(objects);
    let reopened = Fs::local(options).await?;
    let collected = reopened.collect_local_garbage(None, &cancellation).await?;
    assert!(collected.removed >= 1);
    assert!(collected.segments_removed >= 1);

    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let mut checkout = volume
        .value
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let retained = checkout
        .value
        .read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(retained.value.bytes.as_ref(), b"retained");
    let pending = checkout
        .value
        .read_file_range(
            &pending,
            ByteRange {
                offset: 0,
                length: 256 * 1_024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(
        pending.value.bytes.as_ref(),
        vec![b'p'; 256 * 1_024].as_slice()
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_centralizes_same_process_owners_and_rejects_option_drift()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let first = Fs::local(options.clone()).await?;
    let second = Fs::local(options.clone()).await?;
    assert!(first.same_deployment(&second));

    let mut conflicting = options;
    conflicting.object_cache.maximum_entries =
        conflicting.object_cache.maximum_entries.saturating_add(1);
    assert!(matches!(
        Fs::local(conflicting).await,
        Err(FsError::LocalOptionsConflict)
    ));
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_waits_per_root_without_serializing_independent_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first_root = directory.path().join("first");
    let second_root = directory.path().join("second");
    let first = Fs::local(LocalOptions::new(&first_root)).await?;
    let retained = first
        .inner
        ._local_root_lifecycle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("local root ownership"))?
        .clone();
    drop(first);

    let same_root = first_root.clone();
    let reopening = tokio::spawn(async move { Fs::local(LocalOptions::new(first_root)).await });
    let concurrent = tokio::spawn(async move { Fs::local(LocalOptions::new(same_root)).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !reopening.is_finished(),
        "retained native owner must fence reopening"
    );
    assert!(!concurrent.is_finished());

    let second = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Fs::local(LocalOptions::new(second_root)),
    )
    .await??;
    drop(second);
    drop(retained);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(2), reopening).await???;
    let shared = tokio::time::timeout(std::time::Duration::from_secs(2), concurrent).await???;
    assert!(reopened.same_deployment(&shared));
    drop(shared);
    drop(reopened);
    Ok(())
}

#[test]
fn named_attributes_are_sparse_bounded_and_atomic_with_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation,));
    let mut checkout = ready!(volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let path = path("attributes.bin")?;
    let file_id = ready!(checkout.value.create(crate::AuthoredMutation::CreateFile { path: path.clone(), bytes: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).value;
    let name = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.acyclic".to_vec(),
        128,
    )?;
    assert!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    let empty_listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(empty_listing.value.entries.is_empty());
    assert!(!empty_listing.value.has_more);
    let absent_remove = poll_immediate(checkout.value.change_named_attribute(path.clone(), &name, None, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("attribute-free removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = poll_immediate(checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"replacement"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("absent attribute replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    ready!(checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation));
    let duplicate_create = poll_immediate(checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"duplicate"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("duplicate attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    let listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert_eq!(listing.value.entries.len(), 1);
    assert_eq!(listing.value.entries[0].name, name);
    assert!(!listing.value.has_more);
    ready!(checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.value.change_named_attribute(path.clone(), &name, None, WorkBudget::UNBOUNDED, &cancellation));
    assert!(
        ready!(checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value
        .is_none()
    );
    let removed_listing = ready!(checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(removed_listing.value.entries.is_empty());
    assert!(!removed_listing.value.has_more);
    ready!(checkout.value.change_named_attribute_by_id(file_id, &name, Some((&OwnedBytes::new(), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation));
    assert!(ready!(checkout.value.read_named_attribute_by_id(
        file_id, &name, WorkBudget::UNBOUNDED, &cancellation,
    )).value.ok_or("empty attribute missing")?.bytes.is_empty());
    Ok(())
}

#[test]
fn generation_diff_is_semantic_bounded_and_equal_root_constant_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([73; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut checkout = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let initial = checkout.generation_id();
    ready!(checkout.create(crate::AuthoredMutation::CreateFile { path: path("changed")?, bytes: (bytes::Bytes::from_static(b"one")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let first =
        match ready!(checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected first commit: {outcome:?}").into()),
        };
    let invalid = poll_immediate(volume.diff_generations(
        initial,
        first,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("zero-bound generation diff unexpectedly succeeded")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    ready!(checkout.change_content(path("changed")?, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"two")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let second =
        match ready!(checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected second commit: {outcome:?}").into()),
        };

    let created =
        ready!(volume.diff_generations(initial, first, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(!created.value.truncated);
    assert!(created.value.files.len() >= 2);
    assert_eq!(created.value.bindings.len(), 1);
    let bounded =
        ready!(volume.diff_generations(initial, first, 1, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(bounded.value.truncated);
    assert_eq!(bounded.value.files.len(), 1);
    assert!(bounded.value.bindings.is_empty());
    let modified =
        ready!(volume.diff_generations(first, second, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert_eq!(modified.value.files.len(), 1);
    assert!(modified.value.bindings.is_empty());
    let equal =
        ready!(volume.diff_generations(second, second, 16, WorkBudget::UNBOUNDED, &cancellation,));
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());

    let root_metadata =
        ready!(checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("diff fixture root is absent")?
            .metadata;
    ready!(checkout.mutate(
        vec![Mutation::Create {
            path: path("ordered-last")?,
            record: FileRecord {
                file_id: FileId::from_bytes([0xff; 16]),
                kind: FileKind::Regular,
                link_count: 1,
                metadata: root_metadata,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"last")?),
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let third =
        match ready!(checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected ordered diff commit: {outcome:?}").into()),
        };
    for maximum in [1_u32, 2] {
        let bounded = ready!(volume.diff_generations(
            second,
            third,
            maximum,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ));
        assert!(bounded.value.truncated);
        assert_eq!(
            bounded.value.files.len() + bounded.value.bindings.len(),
            usize::try_from(maximum)?
        );
    }
    let changed_id = ready!(checkout.lookup_no_follow(
        &path("changed")?, WorkBudget::UNBOUNDED, &cancellation,
    )).value.record.ok_or("changed record absent")?.file_id;
    ready!(checkout.mutate(vec![crate::kernel::Mutation::Link { source: path("changed")?, destination: path("alias")? }], WorkBudget::UNBOUNDED, &cancellation));
    ready!(checkout.mutate(vec![crate::kernel::Mutation::Rename { source: path("changed")?, destination: path("renamed")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation));
    let mut metadata = ready!(checkout.lookup_no_follow_with_metadata(
        &root_path()?, WorkBudget::UNBOUNDED, &cancellation,
    )).value.ok_or("root metadata absent")?.metadata;
    metadata.modified_ns = MetadataField::Value(123);
    ready!(checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: root_path()?, metadata: metadata }], WorkBudget::UNBOUNDED, &cancellation));
    let fourth = match ready!(checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).value {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected semantic diff commit: {outcome:?}").into()),
    };
    let semantic = ready!(volume.diff_generations(
        third, fourth, 32, WorkBudget::UNBOUNDED, &cancellation,
    )).value;
    let file = semantic.files.iter().find(|change| change.file_id == changed_id)
        .ok_or("hard-link identity change absent")?;
    assert_eq!(file.before.ok_or("original record absent")?.file_id, changed_id);
    assert_eq!(file.after.ok_or("linked record absent")?.link_count, 2);
    let root = ready!(checkout.lookup_no_follow_with_metadata(
        &root_path()?, WorkBudget::UNBOUNDED, &cancellation,
    )).value.ok_or("updated root metadata absent")?;
    assert_eq!(root.metadata, metadata);
    assert!(semantic.files.iter().any(|change|
        change.after.is_some_and(|record| record.kind == FileKind::Directory && record.metadata == root.record.metadata)));
    assert_eq!(semantic.bindings.len(), 3);
    assert!(semantic.bindings.iter().any(|change|
        change.name.as_bytes() == b"changed" && change.before.is_some() && change.after.is_none()));
    for name in [b"alias".as_slice(), b"renamed".as_slice()] {
        assert!(semantic.bindings.iter().any(|change|
            change.name.as_bytes() == name && change.before.is_none()
                && change.after.as_ref().is_some_and(|entry| entry.file_id == changed_id)));
    }
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    for (before, after) in [(initial, first), (first, second), (second, third), (third, fourth), (fourth, fourth)] {
        let before = ready!(volume.checkout(
            GenerationSelector::Exact(before), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).value;
        let after = ready!(volume.checkout(
            GenerationSelector::Exact(after), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).value;
        for maximum in [1, 2, 32] {
            let ordinary = ready!(before.candidate_diff(
                &after, maximum, WorkBudget::UNBOUNDED, &cancellation,
            )).value;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let (native, allocation) = ready!(before.candidate_diff_with_native_work(
                &after, maximum, &original, &cancellation,
            ));
            assert_eq!(ordinary, native);
            let actual = (native.files.capacity() * size_of::<FileRecordChange>()
                + native.bindings.capacity() * size_of::<DirectoryBindingChange>()) as u64
                + native.bindings.iter().map(|change| {
                    change.name.retained_bytes() as u64
                        + change.before.as_ref().map_or(0, |entry| entry.name.retained_bytes() as u64)
                        + change.after.as_ref().map_or(0, |entry| entry.name.retained_bytes() as u64)
                }).sum::<u64>();
            assert_eq!(original.live_allocation_bytes() - baseline, actual);
            drop(native);
            drop(allocation);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        if before.root.file_table != after.root.file_table {
            for budget in [
                WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
                WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED },
            ] {
                let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, _)| cause)?;
                let baseline = denied.live_allocation_bytes();
                let failure = poll_immediate(before.candidate_diff_with_native_work(
                    &after, 32, &denied, &cancellation,
                )).err().ok_or("native diff denial succeeded")?;
                assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
                assert_eq!(denied.receipt().bytes_copied, 0);
                if budget.allocation_operations == 0 {
                    assert_eq!(denied.receipt().backend_read_operations, 0);
                }
                drop(failure);
                assert_eq!(denied.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

#[test]
fn three_way_merge_combines_independent_directory_bindings_and_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([74; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(ours.create(crate::AuthoredMutation::CreateFile { path: path("ours")?, bytes: (bytes::Bytes::from_static(b"ours")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    ready!(theirs.create(crate::AuthoredMutation::CreateFile { path: path("theirs")?, bytes: (bytes::Bytes::from_static(b"theirs")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    let theirs_generation = match ready!(theirs.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected branch commit: {outcome:?}").into()),
    };
    let prepared = ready!(ours.prepare_merge(
        theirs_generation,
        32,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    let mutation_during_merge =
        poll_immediate(ours.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation))
            .err()
            .ok_or("prepared merge accepted a mutation")?;
    assert!(matches!(
        mutation_during_merge.error,
        FsError::PreparedMergePending
    ));
    assert!(
        poll_immediate(ours.prepare_merge(
            theirs_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(ours.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))
            .is_err()
    );
    let merged_generation = match ready!(ours.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected merge commit: {outcome:?}").into()),
    };
    let mut merged = ready!(volume.checkout(
        GenerationSelector::Exact(merged_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        ready!(merged.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert!(
        ready!(merged.lookup_no_follow(&path("theirs")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .is_some()
    );
    assert_eq!(merged.root().parents.len(), 2);

    let mut left = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut right = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    ready!(left.mutate(vec![crate::kernel::Mutation::Link { source: path("ours")?, destination: path("left-link")? }], WorkBudget::UNBOUNDED, &cancellation));
    ready!(right.mutate(vec![crate::kernel::Mutation::Link { source: path("ours")?, destination: path("right-link")? }], WorkBudget::UNBOUNDED, &cancellation));
    let right_generation =
        match ready!(right.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected hard-link commit: {outcome:?}").into()),
        };
    assert!(matches!(
        ready!(left.prepare_merge(
            right_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .value,
        MergePreparation::Prepared { .. }
    ));
    let links_generation = match ready!(left.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link merge: {outcome:?}").into()),
    };
    let mut links = ready!(volume.checkout(
        GenerationSelector::Exact(links_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let source =
        ready!(links.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,))
            .value
            .record
            .ok_or("merged hard-link source missing")?;
    assert_eq!(source.link_count, 3);
    for linked in ["left-link", "right-link"] {
        let record =
            ready!(links.lookup_no_follow(&path(linked)?, WorkBudget::UNBOUNDED, &cancellation,))
                .value
                .record
                .ok_or("merged hard link missing")?;
        assert_eq!(record.file_id, source.file_id);
    }
    Ok(())
}

#[test]
fn three_way_merge_rejects_guards_and_reports_exact_file_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! ready {
        ($future:expr) => {
            poll_immediate($future)?
        };
    }
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = ready!(fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut seed = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let file = path("conflict.txt")?;
    let file_id = ready!(seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"base")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    let second_file = path("second-conflict.txt")?;
    let second_file_id = ready!(seed.create(crate::AuthoredMutation::CreateFile { path: second_file.clone(), bytes: (bytes::Bytes::from_static(b"base-two")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .value;
    let base_generation = match ready!(seed.commit(OperationId::from_bytes([125; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected seed commit: {outcome:?}").into()),
    };
    let mut ours = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut theirs = ready!(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    let mut read_only = ready!(volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .value;
    assert!(
        poll_immediate(read_only.prepare_merge(
            base_generation,
            8,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .is_err()
    );
    assert!(
        poll_immediate(
            ours.prepare_merge(base_generation, 0, 8, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .is_err()
    );
    assert!(
        poll_immediate(
            ours.prepare_merge(base_generation, 8, 0, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .is_err()
    );
    ready!(ours.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"ours")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(ours.change_content(second_file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"ours-two")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(theirs.change_content(file, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"them")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    ready!(theirs.change_content(second_file, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"them-two")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation));
    let theirs_generation = match ready!(theirs.commit(OperationId::from_bytes([126; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected competing commit: {outcome:?}").into()),
    };
    assert!(
        poll_immediate(
            ours.prepare_merge(base_generation, 8, 8, WorkBudget::UNBOUNDED, &cancellation,)
        )
        .is_err()
    );
    let change_limited = poll_immediate(ours.prepare_merge(
        theirs_generation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("truncated merge inputs unexpectedly prepared")?;
    assert!(matches!(change_limited.error, FsError::MergeChangeLimit));
    assert!(change_limited.work.backend_read_operations > 0);
    let bounded_conflicts = ready!(ours.prepare_merge(
        theirs_generation,
        8,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        bounded_conflicts.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: true,
        } if conflicts.len() == 1
            && matches!(conflicts[0], MergeConflict::File(id) if id == file_id || id == second_file_id)
    ));
    let conflicted = ready!(ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    assert!(matches!(
        conflicted.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: false,
        } if conflicts.len() == 2
            && conflicts.contains(&MergeConflict::File(file_id))
            && conflicts.contains(&MergeConflict::File(second_file_id))
    ));
    assert!(ours.has_pending_mutations());
    Ok(())
}

#[test]
fn detached_inline_file_seek_and_attribute_absence_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let file = path("inline-detached")?;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abc")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let mut detached =
        poll_immediate(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    assert_eq!(detached.logical_bytes(), 3);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (3, ExtentSeekTarget::Data, None),
        (0, ExtentSeekTarget::Hole, Some(3)),
        (3, ExtentSeekTarget::Hole, Some(3)),
        (4, ExtentSeekTarget::Hole, None),
    ] {
        let receipt =
            poll_immediate(detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation))?;
        assert_eq!(receipt.value, expected);
        assert_eq!(receipt.work, WorkCounters::default());
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.inline".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    poll_immediate(detached.change_named_attribute(&attribute, Some((&OwnedBytes::from_static(b"value"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation))?;
    let absent =
        poll_immediate(detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(absent.value.is_none());

    detached.record.kind = FileKind::Fifo;
    detached.record.payload = FilePayload::Empty;
    assert_eq!(detached.logical_bytes(), 0);
    let rejected = poll_immediate(detached.seek(
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("non-regular detached seek succeeded")?;
    assert!(matches!(
        rejected.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[test]
fn remaining_facade_guards_are_fast_typed_and_non_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        validate_checkout(
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Live,
                mutations: MutationMode::PrivateOverlay,
            },
            serialized_config(),
        )
        .is_ok()
    );

    let prior = WorkCounters {
        bytes_copied: u64::MAX,
        peak_allocation_bytes: 1,
        ..WorkCounters::default()
    };
    let overflow = merge_simultaneous_failure(
        prior,
        WorkCounters {
            bytes_copied: 1,
            peak_allocation_bytes: 1,
            ..WorkCounters::default()
        },
        0,
        FsError::NotFound,
    );
    assert!(matches!(overflow.error, FsError::Work(WorkError::Overflow)));
    assert_eq!(*overflow.work, prior);

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 1;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([211; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;

    let baseline = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    assert!(matches!(
        checkout.validate_profile_kind(FileKind::ReparsePoint, baseline),
        Err(OperationFailure {
            error: FsError::UnsupportedFileKind,
            ..
        })
    ));

    poll_immediate(checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(
        poll_immediate(checkout.detach_regular_file(
            &path("directory")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    Ok(())
}

fn assert_authority_volume_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
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
        }, |_| Ok::<_, crate::kernel::VolumeCreatedError>(()), |_| crate::kernel::VolumeCreatedError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed))?),
    };
    assert!(matches!(
        generation_from_record(&creation, expected_volume, WorkCounters::default()),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
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
        generation_from_record(&publication, expected_volume, WorkCounters::default()),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let authority = Arc::new(crate::memory::MemoryAuthorityStore::default());
    let fs = Fs::new(
        Arc::clone(&authority),
        crate::memory::MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let expected_authority = volume_authority_id(expected_volume);
    let active = AuthorityStore::create_authority(
        &*authority,
        expected_authority,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let head = match active.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let encoded = crate::kernel::encode(WorkBudget::UNBOUNDED, Some(&CancellationToken::new()), 0, |observe| crate::kernel::encode_volume_created(VolumeCreated {
        volume_id: wrong_volume,
        config: config(),
        initial_generation_root: generation_root,
    }, observe, |_| VolumeCreatedError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed)))?;
    let (proposal, _) = creation_commit(
        OperationId::from_bytes([227; 16]), encoded,
        WorkBudget::UNBOUNDED, &CancellationToken::new(),
    )?.value;
    AuthorityStore::compare_and_append(
        &*authority,
        expected_authority,
        head.epoch,
        Head::genesis(head.epoch),
        proposal,
        WorkBudget::UNBOUNDED,
    )?;
    let cancellation = CancellationToken::new();
    assert!(matches!(
        poll_immediate(fs.read_creation_record(expected_volume, WorkBudget::UNBOUNDED, &cancellation)),
        Err(OperationFailure {
            error: FsError::VolumeMismatch,
            ..
        })
    ));

    let empty_volume = VolumeId::from_bytes([228; 16]);
    AuthorityStore::create_authority(
        &*authority,
        volume_authority_id(empty_volume),
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let empty = Volume {
        fs,
        id: empty_volume,
        config: config(),
    };
    assert!(matches!(
        poll_immediate(empty.resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)),
        Err(OperationFailure {
            error: FsError::EmptyAuthority,
            ..
        })
    ));
    Ok(())
}

#[test]
fn detached_payload_bounds_and_authority_volume_identity_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let file = path("bounded-detached")?;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    poll_immediate(checkout.change_named_attribute(file.clone(), &attribute, Some((&bytes::Bytes::from_static(b"xx"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation))?;
    let mut detached =
        poll_immediate(checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    detached.volume.config.limits.maximum_read_bytes = 1;
    assert!(matches!(
        poll_immediate(
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        ),
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert_authority_volume_identity_fails_closed()
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

#[test]
fn profile_folded_admission_covers_directories_links_and_renames()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_immediate(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;

    poll_immediate(checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("Directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let directory_collision = poll_immediate(checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("directory collision unexpectedly succeeded")?;
    assert!(matches!(directory_collision.error, FsError::NameCollision));

    poll_immediate(checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("Link")?, target: (bytes::Bytes::from_static(b"target")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let link_collision = poll_immediate(checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (bytes::Bytes::from_static(b"other")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("link collision unexpectedly succeeded")?;
    assert!(matches!(link_collision.error, FsError::NameCollision));

    let source = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("Source")?, bytes: (bytes::Bytes::from_static(b"body")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Rename { source: path("Source")?, destination: path("SOURCE")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation))?;
    let renamed = poll_immediate(checkout.lookup_no_follow(
        &path("source")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value
    .record
    .ok_or("renamed source absent")?;
    assert_eq!(renamed.file_id, source);

    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("Other")?, bytes: (bytes::Bytes::from_static(b"other")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile {
        path: path("other")?, bytes: bytes::Bytes::from_static(b"collision").into(),
        metadata: crate::kernel::FileMetadata::default(),
    }, WorkBudget::UNBOUNDED, &cancellation)), Err(OperationFailure { error: FsError::NameCollision, .. })));
    let rename_collision = poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Rename { source: path("SOURCE")?, destination: path("other")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("rename collision unexpectedly succeeded")?;
    assert!(matches!(rename_collision.error, FsError::NameCollision));

    let link_destination_collision = poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Link { source: path("SOURCE")?, destination: path("source")? }], WorkBudget::UNBOUNDED, &cancellation))
    .err()
    .ok_or("same-fold link unexpectedly succeeded")?;
    assert!(matches!(
        link_destination_collision.error,
        FsError::NameCollision
    ));

    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("Removed")?, bytes: (bytes::Bytes::from_static(b"removed")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.mutate(vec![crate::kernel::Mutation::Remove { path: path("Removed")?, expected_file_id: (None).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("removed")?, bytes: (bytes::Bytes::from_static(b"reused")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    Ok(())
}

#[test]
fn profile_folded_authored_batches_reject_intra_batch_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        poll_immediate(fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let failure = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Batch")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("batch")?,
                bytes: OwnedBytes::from_static(b"body"),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .err()
    .ok_or("authored collision unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::NameCollision));
    assert!(!checkout.has_pending_mutations());

    let ordered_aliases = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("Source")?,
                bytes: OwnedBytes::from_static(b"source"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Rename {
                source: path("source")?,
                destination: path("Renamed")?,
                replace: false,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let created_id = ordered_aliases.value.created_file_ids[0].ok_or("created id missing")?;
    let renamed = poll_immediate(checkout.lookup_no_follow(
        &path("renamed")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(
        renamed
            .value
            .record
            .ok_or("renamed record missing")?
            .file_id,
        created_id
    );

    let nested = poll_immediate(checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Nested")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path_parts(&["nested", "Child"])?,
                bytes: OwnedBytes::from_static(b"nested"),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(nested.value.created_file_ids.len(), 2);
    let nested_read = poll_immediate(checkout.read_file_range(
        &path_parts(&["NESTED", "child"])?,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(nested_read.value.bytes.as_ref(), b"nested");
    Ok(())
}

#[test]
fn sensitive_volume_still_permits_case_distinct_siblings() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("Foo")?, bytes: (bytes::Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("foo")?, bytes: (bytes::Bytes::from_static(b"second")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    Ok(())
}

#[test]
fn require_nfc_volume_rejects_non_normalized_names() -> Result<(), Box<dyn std::error::Error>> {
    let precomposed = "\u{00e9}";
    let decomposed = "e\u{0301}";
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let rejected = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path(decomposed)?, bytes: (bytes::Bytes::from_static(b"body")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation));
    assert!(
        rejected.is_err(),
        "a non-normalized (NFD) name must be rejected under RequireNfc"
    );
    poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path(precomposed)?, bytes: (bytes::Bytes::from_static(b"body")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    Ok(())
}

#[test]
fn stamped_content_change_equals_the_change_then_its_stamp_in_one_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([131; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let timed = path("timed")?;
    let untimed = path("untimed")?;
    let mut seed = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file_id = poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: timed.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    poll_immediate(seed.create(crate::AuthoredMutation::CreateFile { path: untimed.clone(), bytes: (bytes::Bytes::from(vec![b'b'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    let times = FileMetadata {
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(2),
        changed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    poll_immediate(seed.set_attributes_by_id(file_id, times, None, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(seed.commit(OperationId::from_bytes([132; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut stamped = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut separate = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(stamped.change_content(file_id, ContentChange::Write { offset: 4, bytes: OwnedBytes::from_static(b"STAMP") }, ContentTimes::Stamp(99), WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(separate.change_content(file_id, crate::ContentChange::Write { offset: 4, bytes: (bytes::Bytes::from_static(b"STAMP")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation))?;
    let mut expected =
        poll_immediate(separate.read_metadata_by_id(file_id, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    assert_eq!(expected, times, "a preserving write leaves metadata intact");
    assert!(expected.stamp_content_change(99));
    poll_immediate(separate.set_attributes_by_id(file_id, expected, None, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(stamped.root(), separate.root());
    let metadata =
        poll_immediate(stamped.read_metadata_by_id(file_id, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    assert_eq!(metadata.modified_ns, MetadataField::Value(99));
    assert_eq!(metadata.changed_ns, MetadataField::Value(99));
    assert_eq!(metadata.accessed_ns, MetadataField::Value(2));

    // The change and its stamp are one mutation: a change that fails stamps nothing.
    let before = stamped.root().clone();
    assert!(
        poll_immediate(stamped.change_content(file_id, ContentChange::CloneFrom {
                source: FileId::from_bytes([200; 16]),
                source_offset: 0,
                offset: 0,
                length: 1,
            }, ContentTimes::Stamp(100), WorkBudget::UNBOUNDED, &cancellation))
        .is_err()
    );
    assert_eq!(stamped.root(), &before);

    // A profile that represents no content times keeps its metadata object.
    let untimed_before =
        poll_immediate(stamped.read_metadata(&untimed, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    poll_immediate(stamped.change_content(
        untimed.clone(),
        ContentChange::Resize { logical_bytes: 7 },
        ContentTimes::Stamp(101),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    let untimed_after =
        poll_immediate(stamped.read_metadata(&untimed, WorkBudget::UNBOUNDED, &cancellation))?
            .value;
    assert_eq!(untimed_after, untimed_before);
    Ok(())
}

#[test]
fn grouped_changes_land_together_and_each_keeps_its_own_result()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([141; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let existing = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path("existing")?, bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
    .value;
    let content = |bytes: &'static [u8]| GroupedChange::Content {
        file_id: existing,
        change: ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(bytes) },
        times: ContentTimes::Stamp(7),
    };
    let create = |name: &str| -> Result<GroupedChange, Box<dyn std::error::Error>> {
        Ok(GroupedChange::CreateFile {
            path: path(name)?,
            metadata: Box::new(FileMetadata::default()),
            file_id: FileId::new(),
            bytes: OwnedBytes::new(),
        })
    };

    // Every change applies: one mutation, one result each.
    let applied = poll_immediate(checkout.apply_group(
        vec![create("first")?, content(b"ONE"), create("second")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let [
        Ok(GroupedOutcome::Created(first)),
        Ok(GroupedOutcome::Changed),
        Ok(GroupedOutcome::Created(second)),
    ] = applied.as_slice()
    else {
        return Err(format!("unexpected grouped outcomes {applied:?}").into());
    };
    assert_ne!(first, second);

    // A failing change fails alone; its neighbours still apply in order.
    let mixed = poll_immediate(checkout.apply_group(
        vec![
            create("third")?,
            create("first")?,
            GroupedChange::Content {
                file_id: FileId::from_bytes([142; 16]),
                change: ContentChange::Resize { logical_bytes: 1 },
                times: ContentTimes::Stamp(8),
            },
            content(b"TWO"),
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert!(matches!(mixed[0], Ok(GroupedOutcome::Created(_))));
    assert!(
        mixed[1].is_err(),
        "an existing name cannot be created again"
    );
    assert!(mixed[2].is_err(), "an absent identity cannot change");
    assert!(matches!(mixed[3], Ok(GroupedOutcome::Changed)));

    for name in ["existing", "first", "second", "third"] {
        assert!(
            poll_immediate(checkout.lookup_no_follow(
                &path(name)?,
                WorkBudget::UNBOUNDED,
                &cancellation
            ))?
            .value
            .record
            .is_some(),
            "{name} is bound"
        );
    }
    let read = poll_immediate(checkout.read_file_range_by_id(
        existing,
        ByteRange {
            offset: 0,
            length: 4,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert_eq!(read.bytes.as_ref(), b"TWOa");
    let streamed_id = FileId::new();
    let streamed = poll_immediate(checkout.apply_group(
        vec![
            GroupedChange::CreateFile {
                path: path("streamed")?,
                metadata: Box::new(FileMetadata::default()),
                file_id: streamed_id,
                bytes: OwnedBytes::from(vec![b's'; 4096]),
            },
            create("first")?,
            content(b"END"),
            content(b"LAST"),
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?.value;
    assert!(matches!(streamed[0], Ok(GroupedOutcome::Created(id)) if id == streamed_id));
    assert!(streamed[1].is_err());
    assert!(matches!(streamed[2], Ok(GroupedOutcome::Changed)));
    assert!(matches!(streamed[3], Ok(GroupedOutcome::Changed)));
    let streamed_read = poll_immediate(checkout.read_file_range_by_id(
        streamed_id, ByteRange { offset: 0, length: 4096 },
        WorkBudget::UNBOUNDED, &cancellation,
    ))?.value;
    assert_eq!(streamed_read.bytes.as_ref(), vec![b's'; 4096].as_slice());
    let ordered_read = poll_immediate(checkout.read_file_range_by_id(
        existing, ByteRange { offset: 0, length: 4 },
        WorkBudget::UNBOUNDED, &cancellation,
    ))?.value;
    assert_eq!(ordered_read.bytes.as_ref(), b"LAST");

    let denied = poll_immediate(checkout.apply_group(
        vec![create("denied-group")?],
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        &cancellation,
    ));
    assert!(denied.is_err(), "output admission must precede effects");
    assert!(poll_immediate(checkout.lookup_no_follow(
        &path("denied-group")?, WorkBudget::UNBOUNDED, &cancellation,
    ))?.value.record.is_none());
    Ok(())
}

#[test]
fn repeated_metadata_lookups_preserve_binding_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([151; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut local = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut remote = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    poll_immediate(local.create(crate::AuthoredMutation::CreateFile { path: path("authored")?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;

    let observed = path("observed")?;
    let first = poll_immediate(local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(first.value.is_none());
    let second = poll_immediate(local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(second.value.is_none());

    poll_immediate(remote.create(crate::AuthoredMutation::CreateFile { path: observed, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
    poll_immediate(remote.commit(OperationId::from_bytes([152; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let rebase = poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    Ok(())
}

#[test]
fn batch_lookups_preserve_each_binding_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    for (seed, authored) in [(171_u8, false), (181, true)] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = poll_immediate(fs.create_volume_with_id(
            VolumeId::from_bytes([seed; 16]),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value;
        let mut local = poll_immediate(volume.checkout(
            GenerationSelector::Head,
            writable_tracking(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value;
        let mut remote = poll_immediate(volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .value;
        if authored {
            poll_immediate(local.create(crate::AuthoredMutation::CreateFile { path: path("authored")?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
        }
        let paths = [path("first")?, path("second")?];
        let batch =
        poll_immediate(local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation))?;
        assert!(
            batch
                .value
                .entries
                .iter()
                .all(|entry| entry.record.is_none())
        );
        poll_immediate(local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation))?;

        poll_immediate(remote.create(crate::AuthoredMutation::CreateFile { path: path("second")?, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
        poll_immediate(remote.commit(OperationId::from_bytes([seed.wrapping_add(1); 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
        let rebase = poll_immediate(local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation))?;
        assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    }
    Ok(())
}

#[test]
fn one_reader_answers_a_batch_of_identities() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([191; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut writer = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut identities = Vec::new();
    for name in ["a", "b"] {
        poll_immediate(writer.create(crate::AuthoredMutation::CreateFile { path: path(name)?, bytes: (bytes::Bytes::from_static(b"x")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?;
        let record =
            poll_immediate(writer.lookup_no_follow(&path(name)?, WorkBudget::UNBOUNDED, &cancellation))?
                .value
                .record
                .ok_or("created file missing")?;
        identities.push(record);
    }
    let absent = FileId::from_bytes([192; 16]);
    let reader = writer.pinned_reader()?;
    assert_eq!(reader.generation_id(), writer.generation_id());
    let records = poll_immediate(reader.file_records_by_id(
        &[identities[1].file_id, absent, identities[0].file_id],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert_eq!(
        records.value,
        vec![Some(identities[1]), None, Some(identities[0])]
    );
    Ok(())
}

#[test]
fn each_grouped_change_has_its_own_budget() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([161; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut checkout = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let mut files = Vec::new();
    for (index, name) in ["one", "two", "three", "four"].into_iter().enumerate() {
        let file_id = poll_immediate(checkout.create(crate::AuthoredMutation::CreateFile { path: path(name)?, bytes: (bytes::Bytes::from(vec![u8::try_from(index)?; 16])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation))?
        .value;
        files.push(file_id);
    }
    let write = |file_id: FileId, fill: u8| GroupedChange::Content {
        file_id,
        change: ContentChange::Write { offset: 0, bytes: OwnedBytes::from(vec![fill; 4_096]) },
        times: ContentTimes::Preserve,
    };
    let single = poll_immediate(checkout.apply_group(
        vec![write(files[0], 1)],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    assert!(single.value.iter().all(Result::is_ok));
    // Enough for any one change, far from enough for three sharing it.
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_hashed = single.work.bytes_hashed * 3 / 2;
    let group = poll_immediate(checkout.apply_group(
        vec![write(files[1], 2), write(files[2], 3), write(files[3], 4)],
        budget,
        &cancellation,
    ))?;
    assert!(
        group.value.iter().all(Result::is_ok),
        "a change failed on its neighbours' work: {:?}",
        group.value
    );
    assert!(group.work.bytes_hashed > budget.bytes_hashed);
    Ok(())
}
#[test]
fn a_whole_file_rewrite_larger_than_the_read_bound_is_admitted()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([231; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("large")?;
    let stage = |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        poll_immediate(checkout.stage_content(&mut source, 64, WorkBudget::UNBOUNDED, &cancellation))
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };

    let mut first = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let original = stage(&first, b"0123456789abcdef0123456789abcdef")?;
    poll_immediate(first.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(first.commit(OperationId::from_bytes([231; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut second = poll_immediate(volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let replacement = stage(&second, b"ZYXWVUTSRQPONMLKzyxwvutsrqponmlk")?;
    poll_immediate(second.apply_authored_transaction(
        vec![
            AuthoredMutation::Resize {
                path: file.clone(),
                logical_bytes: 0,
            },
            AuthoredMutation::Resize {
                path: file.clone(),
                logical_bytes: 32,
            },
            AuthoredMutation::WriteFromContent {
                path: file.clone(),
                offset: 0,
                content: replacement,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .map_err(|failure| format!("whole-file rewrite refused: {:?}", failure.error))?;
    poll_immediate(second.commit(OperationId::from_bytes([232; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;
    let head = poll_immediate(second.read_file_range(
        &file,
        ByteRange {
            offset: 24,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    assert_eq!(head.bytes.as_ref(), b"rqponmlk");
    Ok(())
}
#[test]
fn a_stale_whole_file_rewrite_still_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume = poll_immediate(fs.create_volume_with_id(
        VolumeId::from_bytes([233; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .value;
    let file = path("large")?;
    let checkout = || {
        poll_immediate(volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .map(|checkout| checkout.value)
        .map_err(|failure| format!("checkout failed: {:?}", failure.error))
    };
    let stage = |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        poll_immediate(checkout.stage_content(&mut source, 64, WorkBudget::UNBOUNDED, &cancellation))
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };
    let rewrite = |checkout: &mut Checkout<_, _>, body: &'static [u8]| {
        let content = stage(checkout, body)?;
        poll_immediate(checkout.apply_authored_transaction(
            vec![
                AuthoredMutation::Resize {
                    path: file.clone(),
                    logical_bytes: 0,
                },
                AuthoredMutation::Resize {
                    path: file.clone(),
                    logical_bytes: 32,
                },
                AuthoredMutation::WriteFromContent {
                    path: file.clone(),
                    offset: 0,
                    content,
                },
            ],
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .map(|_| ())
        .map_err(|failure| format!("rewrite refused: {:?}", failure.error))
    };

    let mut seed = checkout()?;
    let original = stage(&seed, b"0123456789abcdef0123456789abcdef")?;
    poll_immediate(seed.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?;
    poll_immediate(seed.commit(OperationId::from_bytes([233; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?;

    let mut first = checkout()?;
    let mut stale = checkout()?;
    rewrite(&mut first, b"FIRSTFIRSTFIRSTFIRSTFIRSTFIRST12")?;
    let committed = poll_immediate(first.commit(OperationId::from_bytes([234; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?
    .value;
    assert!(
        matches!(committed, CheckoutCommitOutcome::Committed { .. }),
        "{committed:?}"
    );
    rewrite(&mut stale, b"STALESTALESTALESTALESTALESTALE12")?;
    let refused = poll_immediate(stale.commit(OperationId::from_bytes([235; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }))?
    .value;
    assert!(
        matches!(refused, CheckoutCommitOutcome::Conflict { .. }),
        "{refused:?}"
    );
    Ok(())
}

#[tokio::test]
async fn selected_fork_invalid_native_paths_preflight_before_allocation_and_reads()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::WorkspaceError;
    let fs = Fs::memory();
    let workspace = fs.create_workspace("selected-fork-invalid-paths", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/file", "body").await?;
    let source = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let cancellation = CancellationToken::new();
    let checkout = workspace.checkout(
        GenerationSelector::Exact(source.id()), CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let limits = source.workspace.volume.config().limits;
    for invalid in [
        "/../file".to_owned(),
        "/bad\0name".to_owned(),
        format!("/{}", "x".repeat(limits.maximum_component_bytes as usize + 1)),
        format!("/{}", "a/".repeat(limits.maximum_path_depth as usize + 1)),
        format!("/{}", "x".repeat(limits.maximum_path_bytes as usize + 1)),
    ] {
        let paths = vec![invalid];
        let owned = (paths.capacity() * std::mem::size_of::<String>()
            + paths[0].capacity()) as u64;
        let failure = fs.select_fork_paths(
            &source, checkout.root(), paths,
            WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).await.err().ok_or("invalid selected path unexpectedly succeeded")?;
        assert!(matches!(failure.error, WorkspaceError::NamespacePath(_)));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.page_reads, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.peak_allocation_bytes, owned);
    }
    Ok(())
}

#[tokio::test]
async fn selected_fork_nested_hardlink_closure_replays_finite_work_and_root_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::{ForkOptions, WorkspaceError};
    let fs = Fs::memory();
    let workspace = fs.create_workspace("selected-fork-closure-controls", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/tree/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/tree/nested/file", "linked body").await?;
    transaction.hard_link("/tree/nested/file", "/tree/nested/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.hard_link("/tree/nested/file", "/outside-alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/omitted", "not inherited").await?;
    transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let source = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let paths = vec!["/tree".to_owned(), "/tree/nested/file".to_owned(), "/tree".to_owned()];
    let child = workspace.fork(
        "selected-fork-closure-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(paths.clone()),
    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let initial = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!((child.read("/tree/nested/alias", 32, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value).as_ref(), b"linked body");
    assert!(child.read("/outside-alias", 32, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.is_err());
    assert!(child.read("/omitted", 32, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.is_err());
    let file = initial.stat("/tree/nested/file", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let alias = initial.stat("/tree/nested/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(file.file_id, alias.file_id);
    assert_eq!(file.link_count, 2);
    assert!(initial.matches_fork_selection(&source, Some(paths.clone()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert!(initial.matches_fork_selection(&source, Some(vec!["/tree".to_owned()]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    let cancellation = CancellationToken::new();
    let baseline = initial.matches_fork_selection(
        &source, Some(paths.clone()), WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert!(baseline.value);
    assert!(baseline.work.allocation_operations > 0);
    assert!(baseline.work.backend_read_operations > 0);
    let finite = WorkBudget {
        authority_records_read: 1_000_000, authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000, authority_bytes_written: 1_000_000,
        object_probes: 1_000_000, backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000, durability_operations: 1_000_000,
        page_reads: 1_000_000, page_writes: 1_000_000,
        object_bytes_read: 1_000_000, object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000, bytes_copied: 1_000_000, bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000, source_path_components: 1_000_000,
        source_entries_visited: 1_000_000, output_bytes: 1_000_000,
        items_examined: 1_000_000, items_returned: 1_000_000,
        allocation_operations: 1_000_000, peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    };
    let replay = initial.matches_fork_selection(
        &source, Some(paths.clone()), finite, &cancellation,
    ).await?;
    assert_eq!(replay.value, baseline.value);
    assert_eq!(replay.work, baseline.work);
    let denied = initial.matches_fork_selection(
        &source, Some(paths),
        WorkBudget { peak_allocation_bytes: baseline.work.peak_allocation_bytes - 1, ..finite },
        &cancellation,
    ).await.err().ok_or("tight peak unexpectedly admitted the entire selection")?;
    assert!(matches!(denied.error, WorkspaceError::Work(_)));
    assert!(denied.work.peak_allocation_bytes > 0);
    assert!(denied.work.backend_read_operations > 0);
    assert!(denied.work.allocation_operations > 0);
    let all = workspace.fork(
        "selected-fork-root-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(vec!["/tree".to_owned(), "/".to_owned(), "/".to_owned()]),
    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let all = all.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(all.matches_fork_selection(&source, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert!(all.matches_fork_selection(&source, Some(vec!["/".to_owned()]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert!(!initial.matches_fork_selection(&source, Some(vec!["/".to_owned()]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    Ok(())
}

#[test]
fn selected_fork_verification_zero_allocation_and_cancellation_keep_failed_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::{ForkOptions, WorkspaceError};
    let tracked = Arc::new(std::sync::Mutex::new(None));
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        CountingObjectStore {
            inner: crate::memory::MemoryObjectStore::default(),
            tracked: Arc::clone(&tracked),
            reads: Arc::clone(&reads),
            unreadable: Arc::default(),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = poll_immediate(fs.create_workspace("selected-fork-verification-gates", WorkBudget::UNBOUNDED, &CancellationToken::new())).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    poll_immediate(workspace.write_text("/file", "body"))?;
    let source = poll_immediate(async { workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) })?;
    let child = poll_immediate(workspace.fork(
        "selected-fork-verification-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(vec!["/file".to_owned()]),
    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let initial = poll_immediate(async { child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) })?;
    *tracked.lock().map_err(|_| "tracking lock poisoned")? = Some(ObjectId {
        kind: ObjectKind::GenerationRoot, digest: source.id().digest(),
    });
    reads.store(0, Ordering::Relaxed);
    let cancellation = CancellationToken::new();
    for budget in [
        WorkBudget::default(),
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let failed = poll_immediate(initial.matches_fork_selection(
            &source, Some(vec!["/file".to_owned()]), budget, &cancellation,
        )).err().ok_or("verification ignored its allocation gate")?;
        assert!(matches!(failed.error, WorkspaceError::Work(_)));
        assert_eq!(failed.work.allocation_operations, 0);
        assert_eq!(failed.work.backend_read_operations, 0);
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed = poll_immediate(initial.matches_fork_selection(
        &source, Some(vec!["/file".to_owned()]), WorkBudget::UNBOUNDED, &cancelled,
    )).err().ok_or("verification ignored cancellation")?;
    assert!(matches!(failed.error, WorkspaceError::Cancelled(_)));
    assert_eq!(failed.work.allocation_operations, 0);
    assert_eq!(failed.work.backend_read_operations, 0);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_content_stream_and_clone_keep_real_identity_and_atomic_stamps()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-content-stream", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/source", "authenticated source bytes").await?;
    workspace.write_text("/destination", "old destination").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = checkout.lookup_no_follow(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual source absent")?;
    let destination = checkout.lookup_no_follow(
        &path("destination")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("actual destination absent")?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(1), changed_ns: MetadataField::Value(2),
        accessed_ns: MetadataField::Value(3), ..FileMetadata::default()
    };
    checkout.set_attributes_by_id(destination.file_id, metadata, None, WorkBudget::UNBOUNDED, &cancellation).await?;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let reader = checkout.pinned_reader()?;
    let bytes = reader.read_record_range(
        source, ByteRange { offset: 0, length: 26 }, &original, &cancellation,
    ).await?.bytes;
    assert_eq!(bytes.as_ref(), b"authenticated source bytes");
    let mut candidate = checkout.private_candidate(&original, &cancellation)?;
    candidate.change_content(destination.file_id, ContentChange::Write { offset: 0, bytes }, ContentTimes::Stamp(40), &original, &cancellation).await?;
    candidate.change_content(destination.file_id, ContentChange::CloneFrom { source: source.file_id, source_offset: 14, offset: 0, length: 6 }, ContentTimes::Stamp(41), &original, &cancellation).await?;
    let actual = candidate.read_file_record_by_id(
        destination.file_id, &original, &cancellation,
    ).await?;
    assert_eq!(actual.file_id, destination.file_id);
    let content = reader.read_record_range(
        actual, ByteRange { offset: 0, length: 26 }, &original, &cancellation,
    ).await?;
    assert_eq!(content.bytes.as_ref(), b"sourceticated source bytes");
    let stamped = candidate.read_metadata_by_id(
        destination.file_id, &original, &cancellation,
    ).await?;
    assert_eq!(stamped.modified_ns, MetadataField::Value(41));
    assert_eq!(stamped.changed_ns, MetadataField::Value(41));
    assert_eq!(stamped.accessed_ns, MetadataField::Value(3));
    assert!(original.receipt().source_bytes_read >= 26);
    assert!(original.receipt().bytes_encoded > 0);
    assert_eq!(checkout.read_metadata_by_id(
        destination.file_id, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value, metadata, "private native content cannot stamp its source checkout");
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let destination_path = path("destination")?;
    let source_path = path("source")?;
    let mut destination_owned = clone_authored_path(&destination_path, &mut accounting)?;
    let mut source_owned = clone_authored_path(&source_path, &mut accounting)?;
    let input_bytes = destination_owned.bytes.checked_add(source_owned.bytes).ok_or(WorkError::Overflow)?;
    destination_owned.allocation.absorb(&mut source_owned.allocation)?;
    let compiled = candidate.compile_content_with_accounting(
        crate::kernel::Owned::new(
            (destination_owned.value, ContentChange::CloneFrom {
                source: source_owned.value, source_offset: 0, offset: 0, length: 6,
            }),
            destination_owned.allocation, input_bytes,
        ),
        ContentTimes::Stamp(42), &mut accounting, &cancellation,
    ).await?;
    assert!(matches!(&compiled.value[0], Mutation::CloneRange { source, destination, .. }
        if source == &source_path && destination == &destination_path));
    assert_eq!(compiled.bytes,
        compiled.value.capacity() as u64 * size_of::<Mutation>() as u64
        + source_path.retained_bytes() + 2 * destination_path.retained_bytes());
    compiled.retire(&mut accounting)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_detached_content_stream_keeps_atomic_metadata_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("original-detached-content", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace.write_text("/source", "real detached stream").await?;
    workspace.write_text("/destination", "before").await?;
    let cancellation = CancellationToken::new();
    let mut checkout = workspace.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let source = checkout.lookup_no_follow(
        &path("source")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("real detached source absent")?;
    let destination = checkout.lookup_no_follow(
        &path("destination")?, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.record.ok_or("real detached destination absent")?;
    let reader = checkout.pinned_reader()?;
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let source = reader.read_record_range(
        source, ByteRange { offset: 0, length: 20 }, &original, &cancellation,
    ).await?;
    let metadata = FileMetadata {
        modified_ns: MetadataField::Value(90), changed_ns: MetadataField::Value(90),
        ..FileMetadata::default()
    };
    let mut detached = DetachedFile::from_record(checkout.volume.clone(), destination, WorkBudget::UNBOUNDED, &cancellation)?.value;
    detached.change_content(
        ContentChange::Write { offset: 0, bytes: source.bytes }, Some(metadata),
        &original, &cancellation,
    ).await?;
    assert_eq!(detached.record.file_id, destination.file_id);
    assert_eq!(reader.read_record_range(
        detached.record, ByteRange { offset: 0, length: 20 }, &original, &cancellation,
    ).await?.bytes.as_ref(), b"real detached stream");
    let before = detached.record;
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(cause, _)| cause)?;
    let failure = detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::new() },
        Some(FileMetadata { modified_ns: MetadataField::Value(91), ..metadata }),
        &denied, &cancellation,
    ).await.err().ok_or("detached metadata encoding ignored original denial")?;
    assert!(matches!(failure, FsError::Work(_) | FsError::NativeKernel(_)));
    assert_eq!(detached.record, before);
    assert_eq!(denied.receipt().bytes_encoded, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);
    Ok(())
}
