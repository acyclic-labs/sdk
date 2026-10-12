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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::allocation::KernelAccounting;
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
    let paid = |mutation| -> Result<crate::kernel::Owned<Vec<Mutation>>, FsError> {
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(1, &mut accounting)?;
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
        })?;
        operations.value.push(Mutation::File { file_id: source.file_id, mutation });
        Ok(operations)
    };
    let input = paid(FileMutation::Resize { logical_bytes: 0 })?;
    candidate.mutate_owned_with_accounting(input, &mut KernelAccounting::native(&original, &cancellation), &cancellation).await?;
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
    let failure = candidate.mutate_owned_with_accounting(
        input, &mut KernelAccounting::native(&original, &cancellation), &cancellation,
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

        ($trait:ident, $field:ident, {$($extra:item)*},
            $(fn $method:ident($($argument:ident: $type:ty),*) -> $result:ty;)*
        ) => {
            impl $trait for Stores {
                $($extra)*
                $(async fn $method(&self, $($argument: $type),*) -> $result {
                    match self {
                        Stores::Memory(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
                        Stores::Local(fs) => $trait::$method(&fs.inner.$field, $($argument),*).await,
                    }
                })*
            }
        };
    }
impl AsyncAuthorityStore for Stores {
async fn create_authority<G: crate::AuthorityGuard>(&self, authority: crate::AuthorityId, epoch: crate::Epoch, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
let value = delegate!(self, AsyncAuthorityStore, authority, create_authority(authority, epoch, work, None::<&mut ()>)).map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn fork_authority<G: crate::AuthorityGuard>(&self, source: crate::GenerationForkSource, destination: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
let value = delegate!(self, AsyncAuthorityStore, authority, fork_authority(source, destination, operation, work, None::<&mut ()>)).map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: crate::GuardedAppend, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>,
        > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
if request.permit != crate::PublicationPermit::Unrestricted { return Err(crate::AuthorityStoreError::Rejected("authority backend cannot atomically evaluate operation leases".to_owned()).into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
delegate!(self, AsyncAuthorityStore, authority, compare_and_append(request, work, None::<&mut ()>)).map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
}
async fn head(&self, authority: crate::AuthorityId, work: &mut crate::AuthorityWork<'_>) -> Result<crate::Head, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, head(authority, work))

}
async fn replay(&self, authority: crate::AuthorityId, after: crate::Sequence, limit: crate::ReplayLimit, work: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError,
        > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, replay(authority, after, limit, work))

}
async fn fence(&self, authority: crate::AuthorityId, expected: crate::Head, work: &mut crate::AuthorityWork<'_>) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, fence(authority, expected, work))

}
async fn find_operation(&self, authority: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError,
        > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }

delegate!(self, AsyncAuthorityStore, authority, find_operation(authority, operation, work))

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

    delegate!(AsyncObjectStore, objects, {
        fn collection(&self) -> Option<&Arc<crate::Collection>> {
            match self {
                Self::Memory(_) => None,
                Self::Local(fs) => fs.inner.objects.collection(),
            }
        }
    },
        fn put(id: ObjectId, bytes: OwnedBytes, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<()>;,
        fn put_many(writes: &[ObjectWrite], budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<()>;,
        fn read(id: ObjectId, maximum: u64, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<ObjectRead>;,
        fn read_many(requests: &[ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<Vec<ObjectRead>>;,
        fn contains(id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<bool>;,
        fn flush_before_publish(scope: crate::PublicationScope<'_>, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<crate::PublicationHold>;
    );

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
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| {
                        std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))
                    },
                )
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



#[tokio::test]
async fn every_object_backend_cut_preserves_facade_atomicity_and_retry()
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
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;

    macro_rules! through_every_object_cut {
        ($operation:expr) => {{
            let original_state = checkout_state(&checkout);
            let mut success = None;
            for cut in 1..=256 {
                control.arm(cut);
                let outcome =
                    ($operation).await;
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
    let staged = (checkout.stage_content(
        &mut staged_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
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
        std::ops::Bound::Unbounded,
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
                    ($operation).await;
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

#[tokio::test]
async fn pinned_reader_batches_ranges_in_request_order() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (writer.create(crate::AuthoredMutation::CreateFile { path: path("first")?, bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.create(crate::AuthoredMutation::CreateFile { path: path("second")?, bytes: (bytes::Bytes::from_static(b"01234567")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (bytes::Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let first_directory = path("first-directory")?;
    let second_directory = path("second-directory")?;
    (writer.create(crate::AuthoredMutation::CreateDirectory { path: first_directory.clone(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.create(crate::AuthoredMutation::CreateDirectory { path: second_directory.clone(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    for name in ["a", "b"] {
        (writer.create(crate::AuthoredMutation::CreateFile { path: path_parts(&["first-directory", name])?, bytes: (bytes::Bytes::from_static(b"x")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    }
    (writer.create(crate::AuthoredMutation::CreateFile { path: path_parts(&["second-directory", "c"])?, bytes: (bytes::Bytes::from_static(b"y")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let reader = writer.pinned_reader()?;
    let request_paths = [path("second")?, path("first")?];
    let resolved_batch =
        (reader.resolve_files(&request_paths, WorkBudget::UNBOUNDED, &cancellation)).await?;
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
        (reader.read_resolved_ranges(&requests, 2, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(&batch.value[0].bytes[..], b"234");
    assert_eq!(&batch.value[1].bytes[..], b"bcde");

    let descriptions = (reader.describe_files(
        &[path("first")?, path("second")?, path("first")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(descriptions.value.len(), 3);
    assert_eq!(descriptions.value[0], descriptions.value[2]);
    assert!(descriptions.value.iter().all(Option::is_some));
    let resolved =
        (reader.resolve_files(&[path("first")?], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let resolved_file = resolved.value[0].as_ref().ok_or("resolved file absent")?;
    let resolved_read = (resolved_file.read_range(
        ByteRange {
            offset: 1,
            length: 4,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&resolved_read.value.bytes[..], b"bcde");
    let other_volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut other_writer = (other_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (other_writer.create(crate::AuthoredMutation::CreateFile { path: path("foreign")?, bytes: (bytes::Bytes::from_static(b"WXYZ")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let other_reader = other_writer.pinned_reader()?;
    let other_resolved = (other_reader.resolve_files(
        &[path("foreign")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
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
    let mixed = (reader.read_resolved_ranges(
        &mixed_requests,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&mixed.value[0].bytes[..], b"ab");
    assert_eq!(&mixed.value[1].bytes[..], b"XY");
    let bounded_mixed =
        (reader.read_resolved_ranges(&mixed_requests, 2, mixed.work, &cancellation)).await?;
    assert_eq!(bounded_mixed.value, mixed.value);
    let target =
        (reader.read_symbolic_link(&path("link")?, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(&target.value[..], b"first");
    assert!(matches!(
        (reader.read_symbolic_link(
            &path("first")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await,
        Err(OperationFailure {
            error: FsError::NotSymbolicLink,
            ..
        })
    ));
    let pages = (reader.list_directory_pages(
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
    )).await?;
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
    let resolved_page = (reader.resolve_directory_page(
        &first_directory,
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(resolved_page.value.entries.len(), 1);
    assert!(resolved_page.value.has_more);
    let resolved_entry = resolved_page
        .value
        .entries
        .first()
        .ok_or("resolved directory entry missing")?;
    assert_eq!(resolved_entry.name.as_bytes(), b"a");
    assert_eq!(resolved_entry.file.description().kind, FileKind::Regular);
    let resolved_child = (resolved_entry.file.read_range(
        ByteRange {
            offset: 0,
            length: 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&resolved_child.value.bytes[..], b"x");
    assert!(
        (reader.list_directory_pages(&[], 0, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );

    let first = (reader.read_file_range(
        &request_paths[0],
        requests[0].range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let second = (reader.read_file_range(
        &request_paths[1],
        requests[1].range,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let independent = first.work.checked_add(second.work)?;
    assert!(batch.work.object_probes < independent.object_probes);
    assert!(batch.work.backend_read_operations < independent.backend_read_operations);
    assert!(batch.work.page_reads < independent.page_reads);

    let Err(bounded_failure) =
        (reader.read_resolved_ranges(&requests, 2, first.work, &cancellation)).await
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
    let Err(concurrent_failure) = (reader.read_resolved_ranges(
        &concurrent_failure_requests,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
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

    let tracking = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert!(matches!(
        tracking.pinned_reader(),
        Err(FsError::MutationNotAllowed)
    ));
    Ok(())
}

#[tokio::test]
async fn checkout_snapshot_reader_is_private_and_stable() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file_path = path("private")?;
    let file_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: file_path.clone(), bytes: (bytes::Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let snapshot = checkout.snapshot_reader();
    let resolved = (snapshot.resolve_files(
        std::slice::from_ref(&file_path),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .into_iter()
    .next()
    .flatten()
    .ok_or("private file missing from snapshot")?;
    assert_eq!(resolved.file_id(), file_id);

    (checkout.change_content(file_path, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"later")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let bytes = (resolved.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .bytes;
    assert_eq!(&bytes[..], b"first");
    Ok(())
}

#[tokio::test]
async fn every_authority_backend_cut_preserves_creation_checkout_and_commit_retry()
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
                    ($operation).await;
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
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("authority-cut-file")?, bytes: (bytes::Bytes::from_static(b"authority cut candidate")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let candidate = checkout.generation_id();
    assert!(checkout.has_pending_mutations());
    let commit_operation = OperationId::from_bytes([33; 16]);
    let mut committed = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome =
            (checkout.commit(commit_operation, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await;
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
    (live.create(crate::AuthoredMutation::CreateFile { path: path("live-authority-cut")?, bytes: (bytes::Bytes::from_static(b"live")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(live.has_pending_mutations());
    let live_operation = OperationId::from_bytes([34; 16]);
    let mut published = None;
    for cut in 1..=64 {
        control.arm(cut);
        let outcome = (live.resume_live(
            live_operation,
            3,
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await;
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

#[tokio::test]
async fn every_post_put_creation_fault_retries_from_a_complete_authenticated_closure()
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
        let first = (fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await;
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
                let retried = (fs.create_volume_with_id(
                    volume_id,
                    config(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )).await?;
                assert_eq!(retried.value.id(), volume_id);
                let reopened =
                    (fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)).await?;
                let checkout = (reopened.value.checkout(
                    GenerationSelector::Head,
                    pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )).await?;
                assert_eq!(checkout.value.volume_id(), volume_id);
                assert!(!checkout.value.has_pending_mutations());
            }
        }
    }
    assert!(reached_clean_completion);
    Ok(())
}

#[tokio::test]
async fn every_pre_object_fault_preserves_creation_and_checkout_retry()
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
        match (fs.create_volume_with_id(
            volume_id,
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
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
        match (volume.checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
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

#[tokio::test]
async fn every_object_cut_preserves_sparse_rebase_retry_and_candidate_state()
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
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut rebasing = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (rebasing.create(crate::AuthoredMutation::CreateFile { path: path("local")?, bytes: (bytes::Bytes::from_static(b"local sparse candidate")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.create(crate::AuthoredMutation::CreateFile { path: path("remote")?, bytes: (bytes::Bytes::from_static(b"remote sparse head")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.commit(OperationId::from_bytes([227; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let original = checkout_state(&rebasing);
    let mut completed = None;
    for cut in 1..=512 {
        control.arm(cut);
        match (rebasing.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await
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
        (rebasing.lookup_no_follow(
            &path("local")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    assert!(
        (rebasing.lookup_no_follow(
            &path("remote")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn every_object_cut_preserves_diff_export_and_merge_preparation()
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
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let base = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .generation_id();
    let mut ours = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut theirs = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (ours.create(crate::AuthoredMutation::CreateFile { path: path("ours")?, bytes: (bytes::Bytes::from(vec![b'o'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (theirs.create(crate::AuthoredMutation::CreateFile { path: path("theirs")?, bytes: (bytes::Bytes::from(vec![b't'; 4_096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let ours_generation = (ours.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let theirs_generation = (theirs.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    (theirs.commit(OperationId::from_bytes([228; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    macro_rules! sweep_read_only {
        ($operation:expr) => {{
            let original = checkout_state(&ours);
            let mut completed = None;
            for cut in 1..=4_096 {
                control.arm(cut);
                match ($operation).await {
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
            (ours.lookup_no_follow(
                &path(expected)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value
            .record
            .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn every_object_cut_preserves_authenticated_publication_retry()
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
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("publish-large")?, bytes: (bytes::Bytes::from(vec![b'p'; 16_384])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_named_attribute(path("publish-large")?, &AttributeName::new(
            crate::kernel::AttributeClass::PosixXattr,
            b"user.publication".to_vec(),
            config().limits.maximum_component_bytes,
        )?, Some((&bytes::Bytes::from(vec![b'a'; 4_096]), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;

    let original = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([229; 16]);
    let mut completed = None;
    for cut in 1..=4_096 {
        control.arm(cut);
        match (checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
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

#[tokio::test]
async fn indeterminate_post_append_retry_resolves_one_durable_publication()
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
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("indeterminate-file")?, bytes: (bytes::Bytes::from_static(b"durable exactly once")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let before = checkout_state(&checkout);
    let operation_id = OperationId::from_bytes([49; 16]);
    control.arm(1);
    let failure = (checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
        .err()
        .ok_or("post-append acknowledgement loss unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Publication(PublicationError::Authority(
            AuthorityStoreError::Indeterminate { .. }
        ))
    ));
    assert_eq!(checkout_state(&checkout), before);
    assert!(({ let __authority_args = (volume_authority_id(volume.id()), operation_id, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result =
        AsyncAuthorityStore::find_operation(&*authority, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
        .value
        .is_some()
    );

    control.disable();
    let resolved = (checkout.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        resolved.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!checkout.has_pending_mutations());
    let history =({ let __authority_args = (volume_authority_id(volume.id()), Sequence::GENESIS, ReplayLimit {
            records: 8,
            payload_bytes: 64 * 1024,
        }, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result =  AsyncAuthorityStore::replay(&*authority, __authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?
    .value;
    assert_eq!(
        history
            .iter()
            .filter(|commit| commit.operation_id == operation_id)
            .count(),
        1
    );
    let mut reopened = (volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let bytes = (reopened.value.read_file_range(
        &path("indeterminate-file")?,
        ByteRange {
            offset: 0,
            length: 20,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(bytes.value.bytes.as_ref(), b"durable exactly once");
    Ok(())
}

#[tokio::test]
async fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let file = path("open-file")?;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let detached =
        (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut detached = detached.value;
    let file_id = detached.file_id();
    assert_eq!(
        (detached.seek(
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(0)
    );
    assert_eq!(
        (detached.seek(
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
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
            (detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation,)).await?
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
        (
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        ).await?
        .value
        .is_none()
    );
    assert!(
        (detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .entries
            .is_empty()
    );
    let absent_remove = (detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("attribute-free detached removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = (detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"replacement"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("absent detached replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    (detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(
        (
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        ).await?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        (detached.list_named_attributes(None, 8, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .entries
            .len(),
        1
    );
    assert!(
        (detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"duplicate"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    (detached.change_named_attribute(&attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await?;

    let mut metadata = (detached.read_metadata(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    (detached.set_attributes(metadata, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    (detached.set_attributes(metadata, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(detached.logical_bytes(), 128);
    (detached.set_attributes(metadata, Some(160), WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(detached.logical_bytes(), 160);
    (checkout.mutate(vec![crate::kernel::Mutation::Remove { path: file.clone(), expected_file_id: (Some(file_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let absent = (checkout.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(absent.value.record.is_none());

    (detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(b"open") },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (detached.change_content(
        ContentChange::Write { offset: 4, bytes: OwnedBytes::new() },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (detached.change_content(
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
    )).await?;
    (detached.change_content(
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
    )).await?;
    let read = (detached.read_range(
        ByteRange {
            offset: 0,
            length: 80,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&read.value.bytes[..4], b"open");
    assert_eq!(&read.value.bytes[64..80], &[0; 16]);
    (detached.change_content(
        ContentChange::Resize { logical_bytes: 192 },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(detached.logical_bytes(), 192);
    (detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn tracking_reads_capture_only_their_exact_terminal_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([81; 16]),
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
    (seed.commit(OperationId::from_bytes([82; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut content_reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (content_reader.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let mut metadata_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut changed_metadata = FileMetadata::default();
    changed_metadata.modified_ns = MetadataField::Value(7);
    (metadata_writer.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: file.clone(), metadata: changed_metadata }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (metadata_writer.commit(OperationId::from_bytes([83; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let content_rebase =
        (content_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(content_rebase.value, RebaseDecision::Safe { .. }));

    let mut metadata_reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (metadata_reader.read_metadata(&file, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut content_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (content_writer.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"changed")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (content_writer.commit(OperationId::from_bytes([84; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let metadata_rebase =
        (metadata_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(metadata_rebase.value, RebaseDecision::Safe { .. }));

    let mut generic_reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (generic_reader.lookup_no_follow(&file, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut final_writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (final_writer.change_content(file, crate::ContentChange::Write { offset: 96, bytes: (bytes::Bytes::from_static(b"different")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (final_writer.commit(OperationId::from_bytes([85; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let generic_rebase =
        (generic_reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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

#[tokio::test]
async fn sparse_seek_dependencies_track_base_semantics_and_exact_observed_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([101; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("sparse")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 256])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (seed.commit(OperationId::from_bytes([102; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (local.change_content(file.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 128,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let local_observation = (local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(local_observation.value, Some(128));

    let mut disjoint = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (disjoint.change_content(file.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 0,
            length: 64,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (disjoint.commit(OperationId::from_bytes([103; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let safe = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let retained = (local.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(retained.value, Some(128));

    let mut reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let observation = (reader.seek_file_extent(
        &file,
        128,
        ExtentSeekTarget::Hole,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(observation.value, Some(256));

    let mut conflicting = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (conflicting.change_content(file, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 192,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (conflicting.commit(OperationId::from_bytes([104; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let conflict = (reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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

#[tokio::test]
async fn clipped_identity_read_at_eof_tracks_file_length() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([190; 16]),
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
    let file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abc")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (seed.commit(OperationId::from_bytes([190; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut reader = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let at_eof = (reader.read_file_up_to_by_id(
        file_id,
        3,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(at_eof.value.bytes.is_empty());

    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (writer.change_content(file, crate::ContentChange::Write { offset: 3, bytes: (bytes::Bytes::from_static(b"d")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (writer.commit(OperationId::from_bytes([191; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let rebase = (reader.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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

#[tokio::test]
async fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_native_rebase_preserves_disjoint_ranges_and_is_atomic_on_denial_and_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    sparse_rebase_across_remote_ranges(true).await
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

#[tokio::test]
async fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([111; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("identity")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (seed.commit(OperationId::from_bytes([112; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

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
    (local.change_content(file_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"LOCAL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"REMOTE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.commit(OperationId::from_bytes([113; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let safe = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(safe.value, RebaseDecision::Safe { .. }));
    let bytes = (local.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(&bytes.value.bytes[..5], b"LOCAL");
    assert_eq!(&bytes.value.bytes[64..70], b"REMOTE");
    let plan = (local.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 70,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(plan.value.is_some());

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
    (overlapping_local.change_content(file_id, crate::ContentChange::Write { offset: 10, bytes: (bytes::Bytes::from_static(b"local")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping_remote.change_content(file, crate::ContentChange::Write { offset: 12, bytes: (bytes::Bytes::from_static(b"remote")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping_remote.commit(OperationId::from_bytes([114; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let conflict =
        (overlapping_local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(
        conflict.value,
        RebaseDecision::Conflicted { ref conflicts, .. }
            if conflicts.iter().any(|conflict| matches!(
                conflict.region,
                DependencyRegion::ContentRange { file_id: actual, .. } if actual == file_id
            ))
    ));

    let mut extending = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (extending.change_content(file_id, crate::ContentChange::Write { offset: 128, bytes: (bytes::Bytes::from_static(b"TAIL")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (extending.change_content(file_id, crate::ContentChange::CloneFrom { source: file_id, source_offset: 0, offset: 140, length: 4 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let extended = (extending.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 128,
            length: 16,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
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
    local.mutate_owned_with_accounting(operations, &mut accounting, &cancellation).await?;
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

#[tokio::test]
async fn direct_live_mutations_retry_only_across_exactly_safe_regions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let optimistic = (fs.create_volume_with_id(
        VolumeId::from_bytes([90; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let rejected = (optimistic.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("optimistic volume admitted direct live")?;
    assert!(matches!(
        rejected.error,
        FsError::LiveRequiresSerializedAuthority
    ));

    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([91; 16]),
        serialized_config(),
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
    (seed.commit(OperationId::from_bytes([92; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut first = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut second = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let first_blob = (first.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"FIRST")), 5, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let second_blob = (second.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"SECOND")), 6, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let first_outcome = (first.mutate_live(
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
    )).await?;
    assert!(matches!(
        first_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let second_outcome = (second.mutate_live(
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
    )).await?;
    assert!(matches!(
        second_outcome.value,
        LiveMutationOutcome::Committed { .. }
    ));

    let mut overlapping = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut racing = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let overlap_blob = (overlapping.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"OVERLAP")), 7, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let racing_blob = (racing.stage_content(&mut std::io::Cursor::new(bytes::Bytes::from_static(b"RACING")), 6, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (racing.mutate_live(
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
    )).await?;
    let conflict = (overlapping.mutate_live(
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
    )).await?;
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

#[tokio::test]
async fn explicit_live_refresh_advances_only_across_unobserved_regions()
-> Result<(), Box<dyn std::error::Error>> {

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("refresh")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (seed.commit(OperationId::from_bytes([98; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut live = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(
        (live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );

    let mut disjoint = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (disjoint.change_content(file.clone(), crate::ContentChange::Write { offset: 64, bytes: (bytes::Bytes::from_static(b"SAFE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let disjoint_generation = match (disjoint.commit(OperationId::from_bytes([99; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected disjoint commit: {outcome:?}").into()),
    };
    assert_eq!(
        (live.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).await?.value,
        disjoint_generation
    );
    assert_eq!(live.generation_id(), disjoint_generation);

    assert_eq!(
        (live.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes
        .as_ref(),
        b"aaaa"
    );
    let mut overlapping = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (overlapping.change_content(file, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"RACE")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (overlapping.commit(OperationId::from_bytes([100; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let before_conflict = live.generation_id();
    let conflict = (live.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).await
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
        let native_conflict = (live.synchronize_live_with_accounting(&mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation), &cancellation)).await
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

#[tokio::test]
async fn direct_live_resume_is_idempotent_retry_bounded_and_epoch_fenced()
-> Result<(), Box<dyn std::error::Error>> {

    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([117; 16]);
    let volume = (fs.create_volume_with_id(
        volume_id,
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;

    let mut winner = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut retrying = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (winner.create(crate::AuthoredMutation::CreateFile { path: path("winner")?, bytes: (bytes::Bytes::from_static(b"winner")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (winner.resume_live(
        OperationId::from_bytes([118; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;

    (retrying.create(crate::AuthoredMutation::CreateFile { path: path("retrying")?, bytes: (bytes::Bytes::from_static(b"retrying")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let retry_operation = OperationId::from_bytes([119; 16]);
    let limited =
        (retrying.resume_live(retry_operation, 1, 8, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    assert!(matches!(
        limited.value,
        LiveMutationOutcome::RetryLimit { .. }
    ));
    assert!(retrying.has_pending_mutations());
    let mutation_while_unresolved = (retrying.create(crate::AuthoredMutation::CreateFile { path: path("must-remain-fenced")?, bytes: (bytes::Bytes::from_static(b"blocked")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("unresolved live mutation accepted another mutation")?;
    assert!(matches!(
        mutation_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let mismatched_resume = (retrying.resume_live(
        OperationId::from_bytes([120; 16]),
        2,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("mismatched live resume unexpectedly succeeded")?;
    assert!(matches!(
        mismatched_resume.error,
        FsError::PendingLiveMutation
    ));
    let rebase_while_unresolved =
        (retrying.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await
            .err()
            .ok_or("unresolved live operation accepted public rebase")?;
    assert!(matches!(
        rebase_while_unresolved.error,
        FsError::PendingLiveMutation
    ));
    let resumed =
        (retrying.resume_live(retry_operation, 2, 8, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    assert!(matches!(
        resumed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    assert!(!retrying.has_pending_mutations());
    let clean_resume = (retrying.resume_live(
        retry_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("clean live resume unexpectedly succeeded")?;
    assert!(matches!(clean_resume.error, FsError::NoPendingMutations));
    assert_eq!(*clean_resume.work, WorkCounters::default());
    for failure in [
        (retrying.resume_live(
            retry_operation,
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("zero-limit live resume unexpectedly succeeded")?,
        (retrying.mutate_live(
            Vec::new(),
            OperationId::from_bytes([124; 16]),
            0,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("zero-limit live mutation unexpectedly succeeded")?,
    ] {
        assert!(matches!(failure.error, FsError::ZeroLiveRetryLimit));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let mut pending_guard = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (pending_guard.create(crate::AuthoredMutation::CreateFile { path: path("pending-guard")?, bytes: (bytes::Bytes::from_static(b"pending")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let pending_failure = (pending_guard.mutate_live(
        Vec::new(),
        OperationId::from_bytes([125; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("second pending live mutation unexpectedly succeeded")?;
    assert!(matches!(
        pending_failure.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*pending_failure.work, WorkCounters::default());
    let authored_pending = (pending_guard.apply_authored_live(
        Vec::new(),
        OperationId::from_bytes([126; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("pending authored-live operation unexpectedly succeeded")?;
    assert!(matches!(
        authored_pending.error,
        FsError::PendingLiveMutation
    ));
    assert_eq!(*authored_pending.work, WorkCounters::default());

    let mut first_retry = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut exact_retry = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let root = root_path()?;
    let metadata =
        (first_retry.lookup_no_follow(&root, WorkBudget::UNBOUNDED, &cancellation,)).await?
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
    (first_retry.mutate(
        vec![identical.clone()],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (exact_retry.mutate(vec![identical], WorkBudget::UNBOUNDED, &cancellation,)).await?;
    let idempotent_operation = OperationId::from_bytes([121; 16]);
    let committed = (first_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        committed.value,
        LiveMutationOutcome::Committed { .. }
    ));
    let repeated = (exact_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        repeated.value,
        LiveMutationOutcome::AlreadyCommitted { .. }
    ));

    let mut conflicting_retry = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (conflicting_retry.create(crate::AuthoredMutation::CreateFile { path: path("different-operation")?, bytes: (bytes::Bytes::from_static(b"different")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let conflicted = (conflicting_retry.resume_live(
        idempotent_operation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        conflicted.value,
        LiveMutationOutcome::IdempotencyConflict { .. }
    ));
    assert!(conflicting_retry.has_pending_mutations());

    let mut fenced = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (fenced.create(crate::AuthoredMutation::CreateFile { path: path("fenced")?, bytes: (bytes::Bytes::from_static(b"fenced")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let fence_expected = (async { let __authority_args = (volume_authority_id(volume_id), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = AsyncAuthorityStore::head(&fs.inner.authority, __authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?
    .value;
    let fence = (async { let __authority_args = (volume_authority_id(volume_id), fence_expected, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::fence(&fs.inner.authority, __authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?;
    let FenceOutcome::Advanced(fenced_head) = fence.value else {
        return Err("fresh facade fence conflicted".into());
    };
    let fenced_outcome = (fenced.resume_live(
        OperationId::from_bytes([122; 16]),
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        fenced_outcome.value,
        LiveMutationOutcome::Fenced { actual_epoch } if actual_epoch == fenced_head.epoch
    ));

    let mut non_live = (volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert!(
        (non_live.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).await
            .is_err()
    );
    assert!(
        (non_live.mutate_live(
            Vec::new(),
            OperationId::from_bytes([123; 16]),
            1,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn authored_live_transaction_preserves_create_ids_and_publishes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        serialized_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_live(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("authored-live.txt")?;
    let receipt = (checkout.apply_authored_live(
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
    )).await?;
    assert!(matches!(
        receipt.value.outcome,
        LiveMutationOutcome::Committed { .. }
    ));
    assert_eq!(receipt.value.created_file_ids.len(), 2);
    assert!(receipt.value.created_file_ids[0].is_some());
    assert!(receipt.value.created_file_ids[1].is_none());
    assert!(!checkout.has_pending_mutations());
    let read = (checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 17,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(read.value.bytes.as_ref(), b"initial-published");
    Ok(())
}

#[tokio::test]
async fn authored_transaction_is_atomic_and_preserves_result_positions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([113; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let directory = path("workspace")?;
    let file = path("file.txt")?;
    let transaction = (checkout.apply_authored_transaction(
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
    )).await?;
    assert_eq!(transaction.value.created_file_ids.len(), 3);
    assert!(transaction.value.created_file_ids[0].is_some());
    assert!(transaction.value.created_file_ids[1].is_some());
    assert!(transaction.value.created_file_ids[2].is_none());
    let read = (checkout.read_file_range(
        &file,
        ByteRange {
            offset: 0,
            length: 14,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(read.value.bytes.as_ref(), b"initial-atomic");

    let failed = (checkout.apply_authored_transaction(
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
    )).await;
    assert!(failed.is_err());
    let absent = (checkout.lookup_no_follow(
        &path("orphan")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(absent.value.record.is_none());
    Ok(())
}

#[tokio::test]
async fn authored_bulk_transaction_is_bounded_and_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 2;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([244; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let created = (0..5)
        .map(|index| {
            Ok(AuthoredMutation::CreateFile { path: path(&format!("bulk{index}"))?, bytes: OwnedBytes::from_static(b"ok"), metadata: FileMetadata::default() })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    (checkout.apply_authored_bulk_transaction(
        created,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    for index in 0..5 {
        assert!(
            (checkout.lookup_no_follow(
                &path(&format!("bulk{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value
            .record
            .is_some()
        );
    }
    let failed = (checkout.apply_authored_bulk_transaction(
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
    )).await;
    assert!(failed.is_err());
    assert!(
        (checkout.lookup_no_follow(
            &path("should-not-appear")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
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
    let result = candidate.apply_authored_transaction_with_accounting(
        crate::kernel::Owned { value: input.0, bytes: input.1.bytes(), allocation: KernelOwnedAllocation::NativeOriginal(input.1) },
        &mut KernelAccounting::native(&original, &cancellation), &cancellation,
    ).await?;
    assert_eq!(result.value.created_file_ids, expected);
    let KernelOwnedAllocation::NativeOriginal(allocation) = &result.allocation else {
        return Err("native authored result lost its original custody".into());
    };
    assert!(allocation.work().shares_ledger(&original));
    assert_eq!(allocation.bytes(),
        (result.value.created_file_ids.capacity() * size_of::<Option<FileId>>()) as u64);
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    for (requested, expected) in [(&first, expected[0]), (&second, expected[1])] {
        let actual = crate::kernel::lookup_path_async_with_accounting(
            &fs.inner.objects, &candidate.root, requested, candidate.volume.config,
            &mut accounting, &cancellation, None,
        ).await?.record.ok_or("created real record absent")?;
        assert_eq!(Some(actual.file_id), expected);
    }
    let live = original.live_allocation_bytes();
    let result_bytes = allocation.bytes();
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

#[tokio::test]
async fn inverse_private_mutations_restore_the_base_and_clear_replay_state()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([136; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let base = checkout.generation_id();
    let transient = path("transient")?;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: transient.clone(), bytes: (bytes::Bytes::from_static(b"temporary")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(checkout.has_pending_mutations());
    (checkout.mutate(vec![crate::kernel::Mutation::Remove { path: transient, expected_file_id: (None).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(!checkout.has_pending_mutations());
    assert_eq!(checkout.generation_id(), base);
    Ok(())
}

#[tokio::test]
async fn authored_transactions_preflight_expansion_noops_and_byte_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();

    let mut operation_limited = config();
    operation_limited.limits.maximum_mutations_per_batch = 1;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        operation_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let missing_noop = (checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("absent")?,
            offset: 0,
            bytes: OwnedBytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("authored missing noop succeeded")?;
    assert!(matches!(
        missing_noop.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(missing_noop.work.page_reads > 0);

    let noop_fs = Fs::memory();
    let noop_volume = (noop_fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut noop_checkout = (noop_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let ordered = (noop_checkout.apply_authored_transaction(
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
    )).await
    .err()
    .ok_or("ordered noop bypassed its missing source")?;
    assert!(matches!(
        ordered.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));
    assert!(
        (noop_checkout.lookup_no_follow(
            &path("ordered")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_none()
    );

    let direct_missing = (noop_checkout.change_content(path("missing")?, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::new()).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("direct missing noop succeeded")?;
    assert!(matches!(
        direct_missing.error,
        FsError::Mutation(GenerationMutationError::MissingSource)
    ));

    let created = (noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("present")?,
            bytes: OwnedBytes::new(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let file_id = created.value.created_file_ids[0].ok_or("created file id missing")?;
    let generation_before_noop = noop_checkout.generation_id();
    let empty = (noop_checkout.apply_authored_transaction(
        Vec::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(empty.value.created_file_ids.is_empty());
    assert_eq!(empty.work, WorkCounters::default());
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let noop = (noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::Write {
            path: path("present")?,
            offset: 0,
            bytes: OwnedBytes::new(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(noop.value.created_file_ids, vec![None]);
    assert!(noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let mut empty_source = std::io::Cursor::new(OwnedBytes::new());
    let empty_content = (noop_checkout.stage_content(
        &mut empty_source,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(empty_content.logical_bytes(), 0);
    let content_noop = (noop_checkout.apply_authored_transaction(
        vec![AuthoredMutation::WriteFromContent {
            path: path("present")?,
            offset: u64::MAX,
            content: empty_content,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(content_noop.value.created_file_ids, vec![None]);
    assert!(content_noop.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let direct = (noop_checkout.change_content(path("present")?, crate::ContentChange::Write { offset: u64::MAX, bytes: (bytes::Bytes::new()).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(direct.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);
    let identity = (noop_checkout.change_content(file_id, crate::ContentChange::Write { offset: u64::MAX, bytes: (bytes::Bytes::new()).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(identity.work.page_reads > 0);
    assert_eq!(noop_checkout.generation_id(), generation_before_noop);

    let expanded = (checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateFile {
            path: path("large")?,
            bytes: OwnedBytes::from(vec![1; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1]),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("expanded authored transaction unexpectedly succeeded")?;
    assert!(matches!(expanded.error, FsError::TooManyPendingMutations));
    assert_eq!(*expanded.work, WorkCounters::default());
    assert!(
        (checkout.lookup_no_follow(
            &path("large")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_none()
    );

    let mut byte_limited = config();
    byte_limited.limits.maximum_read_bytes = 1;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([125; 16]),
        byte_limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    for mutation in [
        AuthoredMutation::CreateFile { path: path("created-file")?, bytes: OwnedBytes::from_static(b"xx"), metadata: FileMetadata::default() },
        AuthoredMutation::Write { path: path("file")?, offset: 0, bytes: OwnedBytes::from_static(b"xx") },
        AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: OwnedBytes::from_static(b"xx"), metadata: FileMetadata::default() },
    ] {
        let failure = (checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("byte-limited authored transaction unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let direct = (checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("direct-link")?, target: (bytes::Bytes::from_static(b"xx")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("byte-limited direct symlink unexpectedly succeeded")?;
    assert!(matches!(
        direct.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*direct.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn opaque_payload_reads_enforce_the_volume_output_bound() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.profile = FilesystemProfile::Windows;
    limited.limits.maximum_read_bytes = 1;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([126; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(b"xx"));
    let staged =
        (checkout.stage_content(&mut source, 2, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    let metadata =
        (checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value.record.ok_or("root metadata missing")?.metadata;
    let symbolic = path("symbolic")?;
    let reparse = path("reparse")?;
    (checkout.mutate(
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
    )).await?;
    for failure in [
        (checkout.read_symbolic_link(&symbolic, WorkBudget::UNBOUNDED, &cancellation)).await
            .err()
            .ok_or("oversized symlink read unexpectedly succeeded")?,
        (checkout.read_reparse_point(&reparse, WorkBudget::UNBOUNDED, &cancellation)).await
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

#[tokio::test]
async fn authored_transactions_cover_every_portable_operation_without_hidden_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([114; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;

    let mut create_source = std::io::Cursor::new(OwnedBytes::from_static(b"streamed"));
    let staged_create = (checkout.stage_content(
        &mut create_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut write_source = std::io::Cursor::new(OwnedBytes::from_static(b"TAIL"));
    let staged_write = (checkout.stage_content(
        &mut write_source,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
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
    let transaction = (checkout.apply_authored_transaction(
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
    )).await?;
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
        (checkout.read_file_range(
            &streamed,
            ByteRange {
                offset: 0,
                length: 12,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes
        .as_ref(),
        b"streamedTAIL"
    );
    assert!(
        (checkout.apply_authored_transaction(
            vec![AuthoredMutation::CreateFileFromContent {
                path: path("duplicate-source-identity")?,
                content: staged_create,
                metadata: FileMetadata::default(),
                file_id: Some(streamed_identity),
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert_eq!(
        (checkout.read_file_range(
            &path("large")?,
            ByteRange {
                offset: 0,
                length: u64::try_from(large.len())?,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes.as_ref(),
        large.as_ref()
    );
    assert_eq!(
        (checkout.read_symbolic_link(
            &path("symbolic")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.bytes
        .as_ref(),
        b"../source"
    );
    assert_eq!(
        (checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .modified_ns,
        MetadataField::Value(42)
    );
    Ok(())
}

#[tokio::test]
async fn path_sdk_exposes_every_sparse_operation_with_one_authenticated_state()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([117; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(volume.id(), VolumeId::from_bytes([117; 16]));
    assert_eq!(volume.config(), config());
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let source = path("source-path")?;
    let destination = path("destination-path")?;
    let source_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: source.clone(), bytes: (bytes::Bytes::from(vec![b's'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: destination.clone(), bytes: (bytes::Bytes::from(vec![b'd'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut metadata = FileMetadata::default();
    metadata.modified_ns = MetadataField::Value(99);
    (checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: source.clone(), metadata: metadata }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(destination.clone(), crate::ContentChange::Resize { logical_bytes: 160 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(source.clone(), crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 64,
            length: 16,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(destination.clone(), crate::ContentChange::Preallocate { range: ByteRange {
            offset: 128,
            length: 32,
        }, keep_size: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.change_content(destination.clone(), ContentChange::CloneFrom { source: source.clone(), source_offset: 0, offset: 32, length: 96 }, ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let alias = path("source-alias")?;
    let renamed = path("source-renamed")?;
    (checkout.mutate(vec![crate::kernel::Mutation::Link { source: source.clone(), destination: alias.clone() }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.mutate(vec![crate::kernel::Mutation::Rename { source: alias, destination: renamed.clone(), replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let lookup = (checkout.lookup_no_follow_with_metadata(
        &renamed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .ok_or("renamed link is absent")?;
    assert_eq!(lookup.record.file_id, source_id);
    assert_eq!(lookup.metadata.modified_ns, MetadataField::Value(99));
    let batch = (checkout.lookup_batch_no_follow(
        &[source.clone(), renamed.clone(), path("absent-path")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
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
    let records = (checkout.list_directory_records(
        &root_path()?,
        std::ops::Bound::Unbounded,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(records.value.entries.len(), 3);
    let plan = (checkout.plan_file_extents(
        &source,
        ByteRange {
            offset: 0,
            length: 128,
        },
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(plan.value.is_some());
    assert_eq!(
        (checkout.seek_file_extent(
            &source,
            64,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(64)
    );
    assert_eq!(
        (checkout.read_metadata(&source, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value,
        metadata
    );
    (checkout.mutate(vec![crate::kernel::Mutation::Remove { path: renamed, expected_file_id: (Some(source_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;

    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    assert!(
        (checkout.stage_content(&mut empty, 0, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.stage_content(
            &mut empty,
            config().limits.maximum_generation_bytes + 1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let body = [b'n'; 256];
        let mut source = std::io::Cursor::new(body.as_slice());
        let staged = (checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).await?;
        assert_eq!(staged.logical_bytes(), body.len() as u64);
        assert_eq!(original.receipt().source_bytes_read, body.len() as u64);
        assert!(original.receipt().backend_write_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&denied, &cancellation);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!((checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancellation,
        )).await.is_err());
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
        assert!((checkout.stage_content_with_accounting(
            &mut source, 64, &mut accounting, &cancellation,
        )).await.is_err());
        assert_eq!(failed.receipt().source_bytes_read, 4);

        let cancelled = CancellationToken::new();
        let stopped = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancelled)
            .map_err(|(cause, _)| cause)?;
        cancelled.cancel();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&stopped, &cancelled);
        let mut source = std::io::Cursor::new(body.as_slice());
        assert!((checkout.stage_content_with_accounting(
            &mut source, body.len() as u64, &mut accounting, &cancelled,
        )).await.is_err());
        assert_eq!(source.position(), 0);
        assert_eq!(stopped.receipt().backend_write_operations, 0);
    }
    Ok(())
}

#[tokio::test]
async fn public_query_and_identity_failures_preserve_the_checkout_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([118; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("regular")?;
    let directory = path("directory")?;
    let missing = path("missing")?;
    let file_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"data")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let directory_id = (checkout.create(crate::AuthoredMutation::CreateDirectory { path: directory.clone(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let empty_directory = (checkout.list_directory_records(
        &directory,
        std::ops::Bound::Unbounded,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(empty_directory.value.entries.is_empty());
    assert!(!empty_directory.value.has_more);
    let unknown_id = FileId::from_bytes([250; 16]);
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.missing".to_vec(),
        128,
    )?;

    let oversized_identity_read = (checkout.read_file_range_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: config().limits.maximum_read_bytes + 1,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("oversized identity read unexpectedly succeeded")?;
    assert!(matches!(
        oversized_identity_read.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*oversized_identity_read.work, WorkCounters::default());
    let zero_span_plan = (checkout.plan_file_extents_by_id(
        file_id,
        ByteRange {
            offset: 0,
            length: 1,
        },
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("zero-span identity plan unexpectedly succeeded")?;
    assert!(matches!(
        zero_span_plan.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*zero_span_plan.work, WorkCounters::default());
    assert_eq!(
        (checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(0)
    );
    assert_eq!(
        (checkout.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
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
            (checkout.seek_file_extent_by_id(
                file_id,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value,
            expected
        );
        assert_eq!(
            (checkout.seek_file_extent(
                &file,
                offset,
                target,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value,
            expected
        );
    }
    let wrong_kind_seek = (checkout.seek_file_extent_by_id(
        directory_id,
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("wrong-kind identity seek unexpectedly succeeded")?;
    assert!(matches!(
        wrong_kind_seek.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert!(
        (checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    assert!(
        (checkout.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .entries
        .is_empty()
    );
    let missing_remove = (checkout.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("attribute-free identity removal unexpectedly succeeded")?;
    assert!(matches!(missing_remove.error, FsError::NotFound));
    let missing_replace = (checkout.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("missing identity attribute replacement unexpectedly succeeded")?;
    assert!(matches!(missing_replace.error, FsError::CreationRejected));
    (checkout.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let duplicate_create = (checkout.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("duplicate identity attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        (checkout.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    (checkout.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let repeated_remove = (checkout.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("repeated identity attribute removal unexpectedly succeeded")?;
    assert!(matches!(repeated_remove.error, FsError::NotFound));

    #[cfg(not(target_arch = "wasm32"))]
    {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut candidate = checkout.private_candidate(&original, &cancellation)?;
        let source = OwnedBytes::from_static(b"actual original-accounted attribute source");
        (candidate.change_named_attribute_by_id(file_id, &attribute, Some((&source, NamedAttributeWriteMode::Create)), &original, &cancellation)).await?;
        let created = (candidate.read_metadata_by_id(
            file_id, &original, &cancellation,
        )).await?;
        let MetadataField::Value(root) = created.named_attributes else {
            return Err("native attribute metadata root absent".into());
        };
        let reader = candidate.pinned_reader_with_native_work(&original, &cancellation)?;
        let (values, allocation) = (reader.read_named_attributes_with_native_work(
            root, &original, &cancellation,
        )).await?;
        assert_eq!(values.len(), 1);
        assert_eq!(&values[0].0, &attribute);
        assert_eq!(values[0].1, source);
        drop(values);
        drop(allocation);
        let duplicate = (candidate.change_named_attribute_by_id(file_id, &attribute, Some((&source, NamedAttributeWriteMode::Create)), &original, &cancellation)).await
            .err().ok_or("native duplicate attribute unexpectedly accepted")?;
        assert!(matches!(duplicate, FsError::CreationRejected));
        let mut updated = created;
        updated.modified_ns = MetadataField::Value(913);
        (candidate.set_attributes_by_id(
            file_id, updated, Some(7), &original, &cancellation,
        )).await?;
        assert_eq!((candidate.read_metadata_by_id(
            file_id, &original, &cancellation,
        )).await?, updated);
        let resized = (candidate.read_file_record_by_id(
            file_id, &original, &cancellation,
        )).await?;
        let resized_bytes = match resized.payload {
            FilePayload::InlineRegular(data) => data.as_bytes().len() as u64,
            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => return Err("native resize changed the regular file kind".into()),
        };
        assert_eq!(resized_bytes, 7);
        (candidate.change_named_attribute_by_id(file_id, &attribute, None, &original, &cancellation)).await?;
        assert!(original.receipt().source_bytes_read >= source.len() as u64);
        assert!(original.receipt().bytes_encoded > 0);
        assert!(original.receipt().allocation_operations > 0);

        let denied = crate::NativeMaterializationWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut denied_candidate = checkout.private_candidate(&denied, &cancellation)?;
        let before = denied_candidate.root.file_table;
        let failure = (denied_candidate.change_named_attribute_by_id(file_id, &attribute, Some((&source, NamedAttributeWriteMode::Upsert)), &denied, &cancellation)).await
            .err().ok_or("native attribute source denial ignored")?;
        assert!(matches!(failure, FsError::Work(_)));
        assert_eq!(denied_candidate.root.file_table, before);
        assert_eq!(denied.receipt().source_bytes_read, 0);

        let denied_metadata = crate::NativeMaterializationWork::new(
            WorkBudget { bytes_encoded: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let mut denied_candidate = checkout.private_candidate(&denied_metadata, &cancellation)?;
        let before = denied_candidate.root.file_table;
        let failure = (denied_candidate.set_attributes_by_id(
            file_id, updated, Some(3), &denied_metadata, &cancellation,
        )).await
            .err().ok_or("native metadata encoding denial ignored")?;
        assert!(matches!(failure, FsError::Work(_)));
        assert_eq!(denied_candidate.root.file_table, before);
        assert_eq!(denied_metadata.receipt().bytes_encoded, 0);
    }

    assert!(
        (checkout.list_directory(&file, None, 8, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.list_directory_records(
            &file,
            std::ops::Bound::Unbounded,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.read_file_range(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.read_file_range(
            &file,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.plan_file_extents(
            &directory,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.seek_file_extent(
            &directory,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.read_symbolic_link(&file, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.read_reparse_point(&file, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.read_metadata(&missing, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.read_named_attribute(
            &missing,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.list_named_attributes(
            &missing,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.change_named_attribute(missing.clone(), &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );

    assert!(
        (checkout.read_file_record_by_id(
            unknown_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.read_file_range_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 5,
                length: 1,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.plan_file_extents_by_id(
            directory_id,
            ByteRange {
                offset: 0,
                length: 1,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.seek_file_extent_by_id(
            directory_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.read_metadata_by_id(unknown_id, WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (checkout.read_named_attribute_by_id(
            unknown_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.list_named_attributes_by_id(
            unknown_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (checkout.change_content(directory_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"x")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    assert!(
        (checkout.change_named_attribute_by_id(unknown_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    assert!(checkout.has_pending_mutations());
    assert_eq!(
        (checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes
        .as_ref(),
        b"data"
    );
    Ok(())
}

#[tokio::test]
async fn public_byte_boundaries_reject_before_allocation_or_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 4;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([189; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("bounded")?;
    let failure = (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"12345")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("oversized create succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*failure.work, WorkCounters::default());

    let file_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"1234")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    for failure in [
        (checkout.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"12345")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await
        .err()
        .ok_or("oversized path write succeeded")?,
        (checkout.change_content(file_id, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"12345")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await
        .err()
        .ok_or("oversized identity write succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    (checkout.change_content(file_id, crate::ContentChange::Write { offset: 4, bytes: (bytes::Bytes::from_static(b"5")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;

    for failure in [
        (checkout.read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("oversized path read succeeded")?,
        (checkout.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
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
        (checkout.plan_file_extents(
            &file,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("oversized path plan succeeded")?,
        (checkout.plan_file_extents_by_id(
            file_id,
            excessive_range,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .err()
        .ok_or("oversized identity plan succeeded")?,
    ] {
        assert!(matches!(
            failure.error,
            FsError::FileRead(FileRangeReadError::InvalidRange)
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let symbolic = (checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (bytes::Bytes::from_static(b"12345")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("oversized symbolic link succeeded")?;
    assert!(matches!(
        symbolic.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*symbolic.work, WorkCounters::default());

    let mut detached =
        (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    let detached_read_failure = (detached.read_range(
        ByteRange {
            offset: 0,
            length: 5,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("oversized detached read succeeded")?;
    assert!(matches!(
        detached_read_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_read_failure.work, WorkCounters::default());
    let detached_failure = (detached.change_content(
        ContentChange::Write { offset: 0, bytes: OwnedBytes::from_static(b"12345") },
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("oversized detached write succeeded")?;
    assert!(matches!(
        detached_failure.error,
        FsError::FileRead(FileRangeReadError::InvalidRange)
    ));
    assert_eq!(*detached_failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn checkout_generation_and_mode_guards_reject_without_candidate_damage()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let first = (fs.create_volume_with_id(
        VolumeId::from_bytes([119; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let second = (fs.create_volume_with_id(
        VolumeId::from_bytes([120; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let first_head = (first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .generation_id();
    let second_head = (second.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .generation_id();
    assert!(
        (first.checkout(
            GenerationSelector::Exact(first_head),
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(matches!(
        (first.checkout(
            GenerationSelector::Exact(second_head), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).await,
        Err(OperationFailure { error: FsError::VolumeMismatch, .. })
    ));
    assert!(
        (first.diff_generations(
            first_head,
            second_head,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );

    let mut read_only = (first.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert!(
        (read_only.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation,)).await
            .is_err()
    );
    assert!(
        (read_only.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
            .is_err()
    );
    assert!(
        (read_only.discard(WorkBudget::UNBOUNDED, &cancellation)).await
            .is_err()
    );
    assert!(
        (read_only.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await
            .is_err()
    );
    assert!(
        (read_only.refresh_live(WorkBudget::UNBOUNDED, &cancellation)).await
            .is_err()
    );
    assert!(
        (read_only.mutate_live(
            Vec::new(),
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (read_only.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );

    let mut writable = (first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let excessive_capacity = Vec::<Mutation>::with_capacity(
        usize::try_from(config().limits.maximum_mutations_per_batch)? + 1,
    );
    let failure =
        (writable.mutate(excessive_capacity, WorkBudget::UNBOUNDED, &cancellation)).await
            .err()
            .ok_or("excessive mutation capacity unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::ExcessiveMutationCapacity));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut clean = (first.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert!(
        (clean.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
            .is_err()
    );
    assert!(
        (clean.resume_live(
            OperationId::new(),
            1,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );

    let mut manual_checkout = (first.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (manual_checkout.create(crate::AuthoredMutation::CreateFile { path: path("dirty")?, bytes: (bytes::Bytes::from_static(b"dirty")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await
            .is_err()
    );
    assert!(manual_checkout.has_pending_mutations());
    (manual_checkout.discard(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(!manual_checkout.has_pending_mutations());
    assert_eq!(
        (manual_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await?
            .value,
        first_head
    );
    Ok(())
}

#[tokio::test]
async fn authored_special_operations_are_profile_exact_and_fail_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([115; 16]),
        posix,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let receipt = (checkout.apply_authored_transaction(
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
    )).await?;
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
        let failure = (checkout.apply_authored_transaction(
            vec![mutation],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await;
        assert!(failure.is_err());
        assert!(
            (checkout.lookup_no_follow(&sentinel, WorkBudget::UNBOUNDED, &cancellation,)).await?
                .value
                .record
                .is_none()
        );
    }

    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let windows_volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([116; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut windows_checkout = (windows_volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let reparse = path("reparse")?;
    let reparse_payload = OwnedBytes::from_static(b"opaque-reparse-data");
    let receipt = (windows_checkout.apply_authored_transaction(
        vec![AuthoredMutation::CreateReparsePoint {
            path: reparse.clone(),
            payload: reparse_payload.clone(),
            metadata: FileMetadata::default(),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(receipt.value.created_file_ids[0].is_some());
    assert_eq!(
        (windows_checkout.read_reparse_point(
            &reparse,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.as_ref(),
        reparse_payload.as_ref()
    );
    Ok(())
}

#[tokio::test]
async fn manifest_transfer_restores_only_after_the_complete_closure_authenticates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Fs::memory();
    let destination = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (source.create_volume_with_id(
        VolumeId::from_bytes([97; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("file")?, bytes: (bytes::Bytes::from(vec![42; 4096])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let first_checkpoint = (checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?;
    let repeated_checkpoint = (checkout.checkpoint(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(first_checkpoint.value, repeated_checkpoint.value);
    let manifest = (checkout.export_manifest(WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    assert_eq!(manifest.generation_id, first_checkpoint.value);
    assert!(manifest.objects.contains(&manifest.generation_root));

    let mut cursor = TransferCursor::START;
    loop {
        let exported = (source.export_generation_batch(
            &manifest,
            cursor,
            2,
            config().limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?;
        let bodies = exported
            .value
            .objects
            .iter()
            .map(|object| object.bytes.clone())
            .collect::<Vec<_>>();
        let imported = (destination.import_generation_batch(
            &manifest,
            cursor,
            &bodies,
            2,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?;
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
        let failure = (destination.restore_volume(
            &invalid,
            OperationId::from_bytes([31; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
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
    let restored = (destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let retried = (destination.restore_volume(
        &manifest,
        restore_operation,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(retried.id(), restored.id());
    let changed_operation = (destination.restore_volume(
        &manifest,
        OperationId::from_bytes([33; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("changed-operation restore unexpectedly succeeded")?;
    assert!(matches!(changed_operation.error, FsError::CreationRejected));
    let mut restored_checkout = (restored.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let bytes = (restored_checkout.read_file_range(
        &path("file")?,
        ByteRange {
            offset: 0,
            length: 4096,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(bytes.value.bytes.as_ref(), vec![42; 4096]);
    Ok(())
}

#[tokio::test]
async fn windows_reparse_payload_round_trips_without_interpretation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut windows = config();
    windows.profile = FilesystemProfile::Windows;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([98; 16]),
        windows,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let payload = bytes::Bytes::from_static(b"opaque-reparse\0payload");
    (checkout.create(crate::AuthoredMutation::CreateReparsePoint { path: path("junction")?, payload: (payload.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let read = (checkout.read_reparse_point(
        &path("junction")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(read.value.as_ref(), payload.as_ref());

    let empty_reparse = path("empty-reparse")?;
    (checkout.create(crate::AuthoredMutation::CreateReparsePoint { path: empty_reparse.clone(), payload: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let empty_reparse_read = (checkout.read_reparse_point(
        &empty_reparse,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(empty_reparse_read.value.is_empty());

    let empty_link = path("empty-link")?;
    (checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: empty_link.clone(), target: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let empty_link_read =
        (checkout.read_symbolic_link(&empty_link, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(empty_link_read.value.bytes.is_empty());
    Ok(())
}

#[tokio::test]
async fn create_retry_is_idempotent_and_exact_selector_matches_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([42; 16]);
    (fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let retry = (fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let opened = fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
    assert_eq!(opened.config(), config());
    let mut head = (opened.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let exact = (retry.value.checkout(
        GenerationSelector::Exact(head.value.generation_id()),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(exact.value.generation_id(), head.value.generation_id());
    let authority = volume_authority_id(volume_id);
    let header = AsyncAuthorityStore::head(&fs.inner.authority, authority, WorkBudget::UNBOUNDED, &cancellation).await?;
    let replay = AsyncAuthorityStore::replay(
        &fs.inner.authority, authority, Sequence::new(header.value.sequence.get() - 1),
        ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    let without_record_decode = exact.work.checked_add(header.work)?.checked_add(replay.work)?;
    let denied = opened.checkout(
        GenerationSelector::Head, pinned(),
        WorkBudget { items_examined: without_record_decode.items_examined, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).await.err().ok_or("head record decoding escaped the checkout item budget")?;
    assert!(matches!(denied.error, FsError::Work(WorkError::BudgetExceeded { counter: "items_examined", .. })));
    assert_eq!(head.value.volume_id(), volume_id);
    assert!(head.value.root().parents.is_empty());
    assert!(head.work.object_bytes_read <= MAXIMUM_GENERATION_ROOT_BYTES);
    let roots = head.value.lookup_batch_no_follow(
        &[root_path()?, root_path()?], WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    assert_eq!(roots.entries.len(), 2);
    assert_eq!(roots.entries[0], roots.entries[1]);
    assert_eq!(roots.entries[0].record.map(|record| record.kind), Some(FileKind::Directory));
    Ok(())
}

#[tokio::test]
async fn unsupported_semantics_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let result = (fs.create_volume_with_id(
        VolumeId::from_bytes([43; 16]),
        durable,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await;
    let Err(failure) = result else {
        return Err("durable memory volume succeeded".into());
    };
    assert!(matches!(failure.error, FsError::UnsupportedDurability));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn conflicting_creation_config_is_rejected_without_changing_opened_config()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([44; 16]);
    (fs.create_volume_with_id(volume_id, config(), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut conflicting = config();
    conflicting.sparse_files = false;
    let retry = (fs.create_volume_with_id(
        volume_id,
        conflicting,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await;
    let Err(failure) = retry else {
        return Err("conflicting creation succeeded".into());
    };
    assert!(matches!(failure.error, FsError::CreationRejected));
    let opened = (fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(opened.value.config(), config());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_facade_operations_perform_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = (fs.create_volume_with_id(
        VolumeId::from_bytes([47; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await;
    let Err(failure) = result else {
        return Err("cancelled create succeeded".into());
    };
    assert!(matches!(failure.error, FsError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work()
-> Result<(), Box<dyn std::error::Error>> {
    macro_rules! cancelled {
        ($future:expr, $label:literal) => {{
            let result = ($future).await;
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
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([137; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &active,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &active,
    )).await?
    .value;
    let file = path("cancelled-file")?;
    let file_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &active)).await?
    .value;
    let mut detached =
        (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &active)).await?
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

#[tokio::test]
async fn private_overlay_commit_retry_and_conflict_are_generation_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let volume = (fs.create_volume_with_id(
        volume_id,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut first = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut retry = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut stale = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        (first.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation)).await?
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
        (checkout.mutate(
            vec![Mutation::Create {
                path: path("a")?,
                record,
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?;
        assert_eq!(
            (checkout.lookup_no_follow(
                &path("a")?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value
            .record
            .map(|value| value.payload),
            Some(record.payload)
        );
    }
    assert!(
        (stale.lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .record
            .is_none()
    );
    (stale.mutate(
        vec![Mutation::Create {
            path: path("b")?,
            record: FileRecord {
                file_id: FileId::from_bytes([8; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;

    let operation_id = OperationId::from_bytes([9; 16]);
    let committed = (first.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    assert!(!first.has_pending_mutations());
    let repeated = (retry.commit(operation_id, None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        repeated.value,
        CheckoutCommitOutcome::AlreadyCommitted { .. }
    ));
    assert!(!retry.has_pending_mutations());
    let conflict = (stale.commit(OperationId::from_bytes([10; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        conflict.value,
        CheckoutCommitOutcome::Conflict { .. }
    ));
    assert!(stale.has_pending_mutations());

    let mut head = (volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(
        (
            head.value
                .lookup_no_follow(&path("a")?, WorkBudget::UNBOUNDED, &cancellation,)
        ).await?
        .value
        .record
        .is_some()
    );
    let lookup = (async { let __authority_args = (volume_authority_id(volume_id), operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = AsyncAuthorityStore::find_operation(&volume.fs.inner.authority, __authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    let mut recovered = (volume.checkout(
        GenerationSelector::Head, writable_pinned(), WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value;
    let denied = (recovered.commit(
        operation_id, None, None, None, crate::PublicationPermit::Unrestricted,
        WorkBudget { items_examined: lookup.work.items_examined, ..WorkBudget::UNBOUNDED },
        &cancellation, |_| async { crate::FsResult::Ok(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) },
    )).await;
    assert!(matches!(denied, Err(OperationFailure {
        error: FsError::Work(WorkError::BudgetExceeded { counter: "items_examined", .. }), ..
    })));

    let clean_failure = (first.commit(OperationId::from_bytes([11; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
    .err()
    .ok_or("clean checkout commit unexpectedly succeeded")?;
    assert!(matches!(clean_failure.error, FsError::NoPendingMutations));
    assert_eq!(*clean_failure.work, WorkCounters::default());

    let mut inverse = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let inverse_record = FileRecord {
        file_id: FileId::from_bytes([15; 16]),
        ..record
    };
    (inverse.mutate(
        vec![Mutation::Create {
            path: path("inverse")?,
            record: inverse_record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(inverse.has_pending_mutations());
    (inverse.mutate(
        vec![Mutation::Remove {
            path: path("inverse")?,
            expected_file_id: MetadataField::Value(inverse_record.file_id),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(!inverse.has_pending_mutations());
    assert!(inverse.dependencies.proof().is_empty());
    let inverse_commit = (inverse.commit(OperationId::from_bytes([16; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await
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

#[tokio::test]
async fn exclusive_writer_checkout_atomically_fences_every_prior_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([0xe1; 16]),
        exclusive_config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut displaced = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut owner = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (displaced.create(crate::AuthoredMutation::CreateFile { path: path("displaced")?, bytes: (bytes::Bytes::from_static(b"stale")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let rejected = (displaced.commit(OperationId::from_bytes([0xe2; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        rejected.value,
        CheckoutCommitOutcome::Fenced { actual_epoch } if actual_epoch.get() == 3
    ));
    (owner.create(crate::AuthoredMutation::CreateFile { path: path("owner")?, bytes: (bytes::Bytes::from_static(b"current")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let committed = (owner.commit(OperationId::from_bytes([0xe3; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        committed.value,
        CheckoutCommitOutcome::Committed { head, .. } if head.epoch.get() == 3
    ));
    Ok(())
}

#[tokio::test]
async fn manual_refresh_is_explicit_bounded_and_never_discards_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([50; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut clean = (volume.checkout(
        GenerationSelector::Head,
        manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut clean_tracking = (volume.checkout(
        GenerationSelector::Head,
        tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut dirty = (volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut conflicted = (volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut pinned_checkout = (volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let root_path = NamespacePath::new(Vec::new(), config().limits)?;
    let metadata =
        (writer.lookup_no_follow(&root_path, WorkBudget::UNBOUNDED, &cancellation)).await?
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
        (conflicted.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_none()
    );
    (conflicted.mutate(
        vec![Mutation::Create {
            path: path("conflicted-local")?,
            record: FileRecord {
                file_id: FileId::from_bytes([16; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (dirty.mutate(
        vec![Mutation::Create {
            path: path("private")?,
            record,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (writer.mutate(
        vec![Mutation::Create {
            path: path("published")?,
            record: FileRecord {
                file_id: FileId::from_bytes([13; 16]),
                ..record
            },
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (writer.commit(OperationId::from_bytes([14; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let conflict = (conflicted.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
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
        (pinned_checkout.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await
            .err()
            .ok_or("pinned refresh unexpectedly succeeded")?;
    assert!(matches!(pinned_failure.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_failure.work, WorkCounters::default());
    let pinned_rebase =
        (pinned_checkout.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await
            .err()
            .ok_or("pinned rebase unexpectedly succeeded")?;
    assert!(matches!(pinned_rebase.error, FsError::RefreshNotAllowed));
    assert_eq!(*pinned_rebase.work, WorkCounters::default());

    let dirty_failure = (dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await
        .err()
        .ok_or("dirty refresh unexpectedly succeeded")?;
    assert!(matches!(
        dirty_failure.error,
        FsError::PendingMutationsRequireRebase
    ));
    assert_eq!(*dirty_failure.work, WorkCounters::default());
    assert!(dirty.has_pending_mutations());
    assert!(
        (
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        ).await?
        .value
        .record
        .is_some()
    );
    let rebased = (dirty.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(rebased.value, RebaseDecision::Safe { .. }));
    assert!(dirty.has_pending_mutations());
    assert!(
        (dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    assert!(
        (
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        ).await?
        .value
        .record
        .is_some()
    );

    let advanced = (clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(advanced.value, writer.generation_id());
    assert!(advanced.work.object_probes > 0);
    let tracking_advanced =
        (clean_tracking.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(tracking_advanced.value, writer.generation_id());
    assert!(
        (clean.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    let equal = (clean.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(equal.value, advanced.value);
    assert_eq!(equal.work.object_probes, 0);
    assert_eq!(equal.work.backend_read_operations, 3);

    assert!(!dirty.dependencies.proof().is_empty());
    (dirty.discard(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(!dirty.has_pending_mutations());
    assert!(dirty.dependencies.proof().is_empty());
    assert!(
        (
            dirty.lookup_no_follow(&path("private")?, WorkBudget::UNBOUNDED, &cancellation,)
        ).await?
        .value
        .record
        .is_none()
    );
    assert!(
        (dirty.lookup_no_follow(
            &path("published")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .record
        .is_some()
    );
    let after_discard = (dirty.refresh_head(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(after_discard.value, writer.generation_id());
    Ok(())
}

#[tokio::test]
async fn private_candidate_rebases_more_changes_than_one_mutation_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_mutations_per_batch = 8;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([241; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut local = (volume.checkout(
        GenerationSelector::Head,
        writable_manual(),
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
    for index in 0..20 {
        (local.create(crate::AuthoredMutation::CreateFile { path: path(&format!("local{index}"))?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    }
    (remote.create(crate::AuthoredMutation::CreateFile { path: path("remote")?, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.commit(OperationId::from_bytes([242; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    assert!(matches!(
        (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value,
        RebaseDecision::Safe { .. }
    ));
    assert!(matches!(
        (local.commit(OperationId::from_bytes([243; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
        .value,
        CheckoutCommitOutcome::Committed { .. }
    ));
    for index in 0..20 {
        assert!(
            (local.lookup_no_follow(
                &path(&format!("local{index}"))?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )).await?
            .value
            .record
            .is_some()
        );
    }
    assert!(
        (local.lookup_no_follow(&path("remote")?, WorkBudget::UNBOUNDED, &cancellation,)).await?
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
        path_index: crate::path_index::MemoryGenerationPathIndex::default(),
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

#[tokio::test]
async fn public_identity_and_posix_special_surfaces_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {

    let mut volume_config = config();
    volume_config.profile = FilesystemProfile::Posix;
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume(volume_config, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    let mut checkout = (volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;

    let file = path("identity.bin")?;
    let clone = path("clone.bin")?;
    let file_id = (checkout.value.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abcdefgh")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let clone_id = (checkout.value.create(crate::AuthoredMutation::CreateFile { path: clone.clone(), bytes: (bytes::Bytes::from_static(b"........")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;

    let record = (checkout.value.read_file_record_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(record.value.file_id, file_id);
    assert_eq!(record.value.kind, FileKind::Regular);
    assert_eq!(
        (checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 2,
                length: 3,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .bytes
        .as_ref(),
        b"cde"
    );
    assert!(
        (checkout.value.plan_file_extents_by_id(
            file_id,
            ByteRange {
                offset: 0,
                length: 8,
            },
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    assert_eq!(
        (checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(0)
    );
    assert_eq!(
        (checkout.value.seek_file_extent_by_id(
            file_id,
            0,
            ExtentSeekTarget::Hole,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        Some(8)
    );
    assert_eq!(
        (checkout.value.seek_file_extent_by_id(
            file_id,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        None
    );
    assert_eq!(
        (checkout.value.seek_file_extent(
            &file,
            9,
            ExtentSeekTarget::Data,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        None
    );

    let mut path_metadata = (checkout.value.read_metadata(
        &file,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    path_metadata.posix_mode = MetadataField::Value(0o100_660);
    (checkout.value.set_attributes(
        file.clone(),
        path_metadata,
        Some(10),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(
        (checkout.value.read_file_range_by_id(
            file_id,
            ByteRange {
                offset: 8,
                length: 2,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
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
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    assert!(
        (checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .entries
        .is_empty()
    );
    (checkout.value.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        (checkout.value.list_named_attributes_by_id(
            file_id,
            None,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .entries
        .len(),
        1
    );
    (checkout.value.change_named_attribute_by_id(file_id, &attribute, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await?;

    let mut metadata = (checkout.value.read_metadata_by_id(
        file_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    metadata.posix_mode = MetadataField::Value(0o100_640);
    (checkout.value.set_attributes_by_id(file_id, metadata, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    metadata.posix_mode = MetadataField::Value(0o100_600);
    (checkout.value.set_attributes_by_id(
        file_id,
        metadata,
        Some(12),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (checkout.value.change_content(file_id, crate::ContentChange::Write { offset: 8, bytes: (bytes::Bytes::from_static(b"tail")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_content(file_id, crate::ContentChange::ZeroRange { range: ByteRange {
            offset: 4,
            length: 2,
        }, allocated: false, extend: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_content(file_id, crate::ContentChange::Preallocate { range: ByteRange {
            offset: 12,
            length: 4,
        }, keep_size: false }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (
        checkout
            .value
            .change_content(clone_id, crate::ContentChange::Resize { logical_bytes: 16 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)
    ).await?;
    (checkout.value.change_content(clone_id, crate::ContentChange::CloneFrom { source: file_id, source_offset: 0, offset: 4, length: 8 }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_named_attribute_by_id(file_id, &attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (checkout.value.read_named_attribute_by_id(
            file_id,
            &attribute,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );

    let target = bytes::Bytes::from_static(b"../identity.bin");
    (checkout.value.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (target.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(
        (checkout.value.read_symbolic_link(
            &path("link")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.bytes.as_ref(),
        target.as_ref()
    );
    (checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("fifo")?, kind: FileKind::Fifo, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("socket")?, kind: FileKind::Socket, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("character")?, kind: FileKind::CharacterDevice, major: 1, minor: 3, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("block")?, kind: FileKind::BlockDevice, major: 8, minor: 0, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (checkout.value.create(crate::AuthoredMutation::CreateEmptySpecial { path: path("invalid")?, kind: FileKind::Regular, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    assert!(
        (checkout.value.create(crate::AuthoredMutation::CreateDevice { path: path("invalid-device")?, kind: FileKind::Regular, major: 0, minor: 0, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );

    (checkout.value.mutate(vec![crate::kernel::Mutation::Link { source: file.clone(), destination: path("hard-link")? }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.mutate(vec![crate::kernel::Mutation::Rename { source: path("hard-link")?, destination: path("renamed-link")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.mutate(vec![crate::kernel::Mutation::Remove { path: path("renamed-link")?, expected_file_id: (Some(file_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let listing = (checkout.value.list_directory_records(
        &root_path()?,
        std::ops::Bound::Unbounded,
        32,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
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

#[tokio::test]
async fn named_attributes_are_sparse_bounded_and_atomic_with_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation,)).await?;
    let mut checkout = (volume.value.checkout(
        GenerationSelector::Head,
        writable_tracking(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let path = path("attributes.bin")?;
    let file_id = (checkout.value.create(crate::AuthoredMutation::CreateFile { path: path.clone(), bytes: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?.value;
    let name = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.acyclic".to_vec(),
        128,
    )?;
    assert!(
        (checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    let empty_listing = (checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(empty_listing.value.entries.is_empty());
    assert!(!empty_listing.value.has_more);
    let absent_remove = (checkout.value.change_named_attribute(path.clone(), &name, None, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("attribute-free removal unexpectedly succeeded")?;
    assert!(matches!(absent_remove.error, FsError::NotFound));
    let absent_replace = (checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"replacement"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("absent attribute replacement unexpectedly succeeded")?;
    assert!(matches!(absent_replace.error, FsError::CreationRejected));
    (checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"first"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let duplicate_create = (checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"duplicate"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("duplicate attribute creation unexpectedly succeeded")?;
    assert!(matches!(duplicate_create.error, FsError::CreationRejected));
    assert_eq!(
        (checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value.map(|chunk| chunk.bytes)
        .as_deref(),
        Some(b"first".as_slice())
    );
    let listing = (checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(listing.value.entries.len(), 1);
    assert_eq!(listing.value.entries[0].name, name);
    assert!(!listing.value.has_more);
    (checkout.value.change_named_attribute(path.clone(), &name, Some((&bytes::Bytes::from_static(b"second"), NamedAttributeWriteMode::Replace)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.value.change_named_attribute(path.clone(), &name, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(
        (checkout.value.read_named_attribute(
            &path,
            &name,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value
        .is_none()
    );
    let removed_listing = (checkout.value.list_named_attributes(
        &path,
        None,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(removed_listing.value.entries.is_empty());
    assert!(!removed_listing.value.has_more);
    (checkout.value.change_named_attribute_by_id(file_id, &name, Some((&OwnedBytes::new(), NamedAttributeWriteMode::Upsert)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!((checkout.value.read_named_attribute_by_id(
        file_id, &name, WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value.ok_or("empty attribute missing")?.bytes.is_empty());
    Ok(())
}

#[tokio::test]
async fn generation_diff_is_semantic_bounded_and_equal_root_constant_work()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([73; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let initial = checkout.generation_id();
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("changed")?, bytes: (bytes::Bytes::from_static(b"one")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let first =
        match (checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected first commit: {outcome:?}").into()),
        };
    let invalid = (volume.diff_generations(
        initial,
        first,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("zero-bound generation diff unexpectedly succeeded")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    (checkout.change_content(path("changed")?, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"two")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let second =
        match (checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected second commit: {outcome:?}").into()),
        };

    let created =
        (volume.diff_generations(initial, first, 16, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    assert!(!created.value.truncated);
    assert!(created.value.files.len() >= 2);
    assert_eq!(created.value.bindings.len(), 1);
    let bounded =
        (volume.diff_generations(initial, first, 1, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    assert!(bounded.value.truncated);
    assert_eq!(bounded.value.files.len(), 1);
    assert!(bounded.value.bindings.is_empty());
    let modified =
        (volume.diff_generations(first, second, 16, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    assert_eq!(modified.value.files.len(), 1);
    assert!(modified.value.bindings.is_empty());
    let equal =
        (volume.diff_generations(second, second, 16, WorkBudget::UNBOUNDED, &cancellation,)).await?;
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());

    let root_metadata =
        (checkout.lookup_no_follow(&root_path()?, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .record
            .ok_or("diff fixture root is absent")?
            .metadata;
    (checkout.mutate(
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
    )).await?;
    let third =
        match (checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected ordered diff commit: {outcome:?}").into()),
        };
    for maximum in [1_u32, 2] {
        let bounded = (volume.diff_generations(
            second,
            third,
            maximum,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?;
        assert!(bounded.value.truncated);
        assert_eq!(
            bounded.value.files.len() + bounded.value.bindings.len(),
            usize::try_from(maximum)?
        );
    }
    let changed_id = (checkout.lookup_no_follow(
        &path("changed")?, WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value.record.ok_or("changed record absent")?.file_id;
    (checkout.mutate(vec![crate::kernel::Mutation::Link { source: path("changed")?, destination: path("alias")? }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.mutate(vec![crate::kernel::Mutation::Rename { source: path("changed")?, destination: path("renamed")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut metadata = (checkout.lookup_no_follow_with_metadata(
        &root_path()?, WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value.ok_or("root metadata absent")?.metadata;
    metadata.modified_ns = MetadataField::Value(123);
    (checkout.apply_authored_transaction(vec![crate::AuthoredMutation::SetMetadata { path: root_path()?, metadata: metadata }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let fourth = match (checkout.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?.value {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected semantic diff commit: {outcome:?}").into()),
    };
    let semantic = (volume.diff_generations(
        third, fourth, 32, WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value;
    let file = semantic.files.iter().find(|change| change.file_id == changed_id)
        .ok_or("hard-link identity change absent")?;
    assert_eq!(file.before.ok_or("original record absent")?.file_id, changed_id);
    assert_eq!(file.after.ok_or("linked record absent")?.link_count, 2);
    let root = (checkout.lookup_no_follow_with_metadata(
        &root_path()?, WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value.ok_or("updated root metadata absent")?;
    assert_eq!(root.metadata, metadata);
    assert!(semantic.files.iter().any(|change|
        change.after.is_some_and(|record| record.kind == FileKind::Directory && record.metadata == root.record.metadata)));
    assert_eq!(semantic.bindings.len(), 3);
    assert!(semantic.bindings.iter().any(|change|
        change.name.as_bytes() == b"changed" && change.before.is_some() && change.after.is_none()));
    for name in [b"alias".as_slice(), b"renamed".as_slice()] {
        assert!(semantic.bindings.iter().any(|change| {
            change.name.as_bytes() == name && change.before.is_none()
                && change.after.as_ref().is_some_and(|entry| entry.file_id == changed_id)
        }));
    }
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    for (before, after) in [(initial, first), (first, second), (second, third), (third, fourth), (fourth, fourth)] {
        let before = (volume.checkout(
            GenerationSelector::Exact(before), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).await?.value;
        let after = (volume.checkout(
            GenerationSelector::Exact(after), pinned(), WorkBudget::UNBOUNDED, &cancellation,
        )).await?.value;
        for maximum in [1, 2, 32] {
            let ordinary = (before.candidate_diff(
                &after, maximum, WorkBudget::UNBOUNDED, &cancellation,
            )).await?.value.0;
            let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, _)| cause)?;
            let baseline = original.live_allocation_bytes();
            let (native, allocation) = (before.candidate_diff(
                &after, maximum, &original, &cancellation,
            )).await?;
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
                let failure = (before.candidate_diff(
                    &after, 32, &denied, &cancellation,
                )).await.err().ok_or("native diff denial succeeded")?;
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

#[tokio::test]
async fn three_way_merge_combines_independent_directory_bindings_and_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([74; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut ours = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut theirs = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (ours.create(crate::AuthoredMutation::CreateFile { path: path("ours")?, bytes: (bytes::Bytes::from_static(b"ours")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (theirs.create(crate::AuthoredMutation::CreateFile { path: path("theirs")?, bytes: (bytes::Bytes::from_static(b"theirs")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let theirs_generation = match (theirs.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected branch commit: {outcome:?}").into()),
    };
    let prepared = (ours.prepare_merge(
        theirs_generation,
        32,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(prepared.value, MergePreparation::Prepared { .. }));
    let mutation_during_merge =
        (ours.mutate(Vec::new(), WorkBudget::UNBOUNDED, &cancellation)).await
            .err()
            .ok_or("prepared merge accepted a mutation")?;
    assert!(matches!(
        mutation_during_merge.error,
        FsError::PreparedMergePending
    ));
    assert!(
        (ours.prepare_merge(
            theirs_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (ours.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await
            .is_err()
    );
    let merged_generation = match (ours.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected merge commit: {outcome:?}").into()),
    };
    let mut merged = (volume.checkout(
        GenerationSelector::Exact(merged_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert!(
        (merged.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .record
            .is_some()
    );
    assert!(
        (merged.lookup_no_follow(&path("theirs")?, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .record
            .is_some()
    );
    assert_eq!(merged.root().parents.len(), 2);

    let mut left = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut right = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (left.mutate(vec![crate::kernel::Mutation::Link { source: path("ours")?, destination: path("left-link")? }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (right.mutate(vec![crate::kernel::Mutation::Link { source: path("ours")?, destination: path("right-link")? }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let right_generation =
        match (right.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?.value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            outcome => return Err(format!("unexpected hard-link commit: {outcome:?}").into()),
        };
    assert!(matches!(
        (left.prepare_merge(
            right_generation,
            32,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value,
        MergePreparation::Prepared { .. }
    ));
    let links_generation = match (left.commit(OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected hard-link merge: {outcome:?}").into()),
    };
    let mut links = (volume.checkout(
        GenerationSelector::Exact(links_generation),
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let source =
        (links.lookup_no_follow(&path("ours")?, WorkBudget::UNBOUNDED, &cancellation,)).await?
            .value
            .record
            .ok_or("merged hard-link source missing")?;
    assert_eq!(source.link_count, 3);
    for linked in ["left-link", "right-link"] {
        let record =
            (links.lookup_no_follow(&path(linked)?, WorkBudget::UNBOUNDED, &cancellation,)).await?
                .value
                .record
                .ok_or("merged hard link missing")?;
        assert_eq!(record.file_id, source.file_id);
    }
    Ok(())
}

#[tokio::test]
async fn three_way_merge_rejects_guards_and_reports_exact_file_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([124; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("conflict.txt")?;
    let file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"base")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let second_file = path("second-conflict.txt")?;
    let second_file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: second_file.clone(), bytes: (bytes::Bytes::from_static(b"base-two")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    let base_generation = match (seed.commit(OperationId::from_bytes([125; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected seed commit: {outcome:?}").into()),
    };
    let mut ours = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut theirs = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut read_only = (volume.checkout(
        GenerationSelector::Head,
        pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert!(
        (read_only.prepare_merge(
            base_generation,
            8,
            8,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .is_err()
    );
    assert!(
        (
            ours.prepare_merge(base_generation, 0, 8, WorkBudget::UNBOUNDED, &cancellation,)
        ).await
        .is_err()
    );
    assert!(
        (
            ours.prepare_merge(base_generation, 8, 0, WorkBudget::UNBOUNDED, &cancellation,)
        ).await
        .is_err()
    );
    (ours.change_content(file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"ours")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (ours.change_content(second_file.clone(), crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"ours-two")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (theirs.change_content(file, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"them")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (theirs.change_content(second_file, crate::ContentChange::Write { offset: 0, bytes: (bytes::Bytes::from_static(b"them-two")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let theirs_generation = match (theirs.commit(OperationId::from_bytes([126; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value
    {
        CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
        outcome => return Err(format!("unexpected competing commit: {outcome:?}").into()),
    };
    assert!(
        (
            ours.prepare_merge(base_generation, 8, 8, WorkBudget::UNBOUNDED, &cancellation,)
        ).await
        .is_err()
    );
    let change_limited = (ours.prepare_merge(
        theirs_generation,
        1,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("truncated merge inputs unexpectedly prepared")?;
    assert!(matches!(change_limited.error, FsError::MergeChangeLimit));
    assert!(change_limited.work.backend_read_operations > 0);
    let bounded_conflicts = (ours.prepare_merge(
        theirs_generation,
        8,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        bounded_conflicts.value,
        MergePreparation::Conflicted {
            ref conflicts,
            truncated: true,
        } if conflicts.len() == 1
            && matches!(conflicts[0], MergeConflict::File(id) if id == file_id || id == second_file_id)
    ));
    let conflicted = (ours.prepare_merge(
        theirs_generation,
        8,
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
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

#[tokio::test]
async fn detached_inline_file_seek_and_attribute_absence_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let file = path("inline-detached")?;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::from_static(b"abc")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut detached =
        (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation)).await?
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
            (detached.seek(offset, target, WorkBudget::UNBOUNDED, &cancellation)).await?;
        assert_eq!(receipt.value, expected);
        assert_eq!(receipt.work, WorkCounters::default());
    }

    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.inline".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    (detached.change_named_attribute(&attribute, Some((&OwnedBytes::from_static(b"value"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    (detached.change_named_attribute(&attribute, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let absent =
        (detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(absent.value.is_none());

    detached.record.kind = FileKind::Fifo;
    detached.record.payload = FilePayload::Empty;
    assert_eq!(detached.logical_bytes(), 0);
    let rejected = (detached.seek(
        0,
        ExtentSeekTarget::Data,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("non-regular detached seek succeeded")?;
    assert!(matches!(
        rejected.error,
        FsError::FileRead(FileRangeReadError::NotRegular)
    ));
    assert_eq!(*rejected.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn remaining_facade_guards_are_fast_typed_and_non_mutating()
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
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([211; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
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

    (checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(
        (checkout.detach_regular_file(
            &path("directory")?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    Ok(())
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

#[tokio::test]
async fn detached_payload_bounds_and_authority_volume_identity_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let file = path("bounded-detached")?;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: file.clone(), bytes: (bytes::Bytes::new()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        config().limits.maximum_component_bytes,
    )?;
    (checkout.change_named_attribute(file.clone(), &attribute, Some((&bytes::Bytes::from_static(b"xx"), NamedAttributeWriteMode::Create)), WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut detached =
        (checkout.detach_regular_file(&file, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    detached.volume.config.limits.maximum_read_bytes = 1;
    assert!(matches!(
        (
            detached.read_named_attribute(&attribute, WorkBudget::UNBOUNDED, &cancellation,)
        ).await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert_authority_volume_identity_fails_closed().await
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

#[tokio::test]
async fn profile_folded_admission_covers_directories_links_and_renames()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;

    (checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("Directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let directory_collision = (checkout.create(crate::AuthoredMutation::CreateDirectory { path: path("directory")?, metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("directory collision unexpectedly succeeded")?;
    assert!(matches!(directory_collision.error, FsError::NameCollision));

    (checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("Link")?, target: (bytes::Bytes::from_static(b"target")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let link_collision = (checkout.create(crate::AuthoredMutation::CreateSymbolicLink { path: path("link")?, target: (bytes::Bytes::from_static(b"other")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("link collision unexpectedly succeeded")?;
    assert!(matches!(link_collision.error, FsError::NameCollision));

    let source = (checkout.create(crate::AuthoredMutation::CreateFile { path: path("Source")?, bytes: (bytes::Bytes::from_static(b"body")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (checkout.mutate(vec![crate::kernel::Mutation::Rename { source: path("Source")?, destination: path("SOURCE")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    let renamed = (checkout.lookup_no_follow(
        &path("source")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value
    .record
    .ok_or("renamed source absent")?;
    assert_eq!(renamed.file_id, source);

    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("Other")?, bytes: (bytes::Bytes::from_static(b"other")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!((checkout.create(crate::AuthoredMutation::CreateFile {
        path: path("other")?, bytes: bytes::Bytes::from_static(b"collision").into(),
        metadata: crate::kernel::FileMetadata::default(),
    }, WorkBudget::UNBOUNDED, &cancellation)).await, Err(OperationFailure { error: FsError::NameCollision, .. })));
    let rename_collision = (checkout.mutate(vec![crate::kernel::Mutation::Rename { source: path("SOURCE")?, destination: path("other")?, replace: false }], WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("rename collision unexpectedly succeeded")?;
    assert!(matches!(rename_collision.error, FsError::NameCollision));

    let link_destination_collision = (checkout.mutate(vec![crate::kernel::Mutation::Link { source: path("SOURCE")?, destination: path("source")? }], WorkBudget::UNBOUNDED, &cancellation)).await
    .err()
    .ok_or("same-fold link unexpectedly succeeded")?;
    assert!(matches!(
        link_destination_collision.error,
        FsError::NameCollision
    ));

    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("Removed")?, bytes: (bytes::Bytes::from_static(b"removed")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.mutate(vec![crate::kernel::Mutation::Remove { path: path("Removed")?, expected_file_id: (None).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("removed")?, bytes: (bytes::Bytes::from_static(b"reused")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    Ok(())
}

#[tokio::test]
async fn profile_folded_authored_batches_reject_intra_batch_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume =
        (fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let failure = (checkout.apply_authored_transaction(
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
    )).await
    .err()
    .ok_or("authored collision unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::NameCollision));
    assert!(!checkout.has_pending_mutations());

    let ordered_aliases = (checkout.apply_authored_transaction(
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
    )).await?;
    let created_id = ordered_aliases.value.created_file_ids[0].ok_or("created id missing")?;
    let renamed = (checkout.lookup_no_follow(
        &path("renamed")?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(
        renamed
            .value
            .record
            .ok_or("renamed record missing")?
            .file_id,
        created_id
    );

    let nested = (checkout.apply_authored_transaction(
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
    )).await?;
    assert_eq!(nested.value.created_file_ids.len(), 2);
    let nested_read = (checkout.read_file_range(
        &path_parts(&["NESTED", "child"])?,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(nested_read.value.bytes.as_ref(), b"nested");
    Ok(())
}

#[tokio::test]
async fn sensitive_volume_still_permits_case_distinct_siblings() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("Foo")?, bytes: (bytes::Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path("foo")?, bytes: (bytes::Bytes::from_static(b"second")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    Ok(())
}

#[tokio::test]
async fn require_nfc_volume_rejects_non_normalized_names() -> Result<(), Box<dyn std::error::Error>> {
    let precomposed = "\u{00e9}";
    let decomposed = "e\u{0301}";
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let rejected = (checkout.create(crate::AuthoredMutation::CreateFile { path: path(decomposed)?, bytes: (bytes::Bytes::from_static(b"body")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await;
    assert!(
        rejected.is_err(),
        "a non-normalized (NFD) name must be rejected under RequireNfc"
    );
    (checkout.create(crate::AuthoredMutation::CreateFile { path: path(precomposed)?, bytes: (bytes::Bytes::from_static(b"body")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    Ok(())
}

#[tokio::test]
async fn stamped_content_change_equals_the_change_then_its_stamp_in_one_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([131; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let timed = path("timed")?;
    let untimed = path("untimed")?;
    let mut seed = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file_id = (seed.create(crate::AuthoredMutation::CreateFile { path: timed.clone(), bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
    .value;
    (seed.create(crate::AuthoredMutation::CreateFile { path: untimed.clone(), bytes: (bytes::Bytes::from(vec![b'b'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let times = FileMetadata {
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(2),
        changed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    (seed.set_attributes_by_id(file_id, times, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (seed.commit(OperationId::from_bytes([132; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut stamped = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut separate = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    (stamped.change_content(file_id, ContentChange::Write { offset: 4, bytes: OwnedBytes::from_static(b"STAMP") }, ContentTimes::Stamp(99), WorkBudget::UNBOUNDED, &cancellation)).await?;
    (separate.change_content(file_id, crate::ContentChange::Write { offset: 4, bytes: (bytes::Bytes::from_static(b"STAMP")).into() }, crate::ContentTimes::Preserve, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let mut expected =
        (separate.read_metadata_by_id(file_id, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    assert_eq!(expected, times, "a preserving write leaves metadata intact");
    assert!(expected.stamp_content_change(99));
    (separate.set_attributes_by_id(file_id, expected, None, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(stamped.root(), separate.root());
    let metadata =
        (stamped.read_metadata_by_id(file_id, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    assert_eq!(metadata.modified_ns, MetadataField::Value(99));
    assert_eq!(metadata.changed_ns, MetadataField::Value(99));
    assert_eq!(metadata.accessed_ns, MetadataField::Value(2));

    // The change and its stamp are one mutation: a change that fails stamps nothing.
    let before = stamped.root().clone();
    assert!(
        (stamped.change_content(file_id, ContentChange::CloneFrom {
                source: FileId::from_bytes([200; 16]),
                source_offset: 0,
                offset: 0,
                length: 1,
            }, ContentTimes::Stamp(100), WorkBudget::UNBOUNDED, &cancellation)).await
        .is_err()
    );
    assert_eq!(stamped.root(), &before);

    // A profile that represents no content times keeps its metadata object.
    let untimed_before =
        (stamped.read_metadata(&untimed, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    (stamped.change_content(
        untimed.clone(),
        ContentChange::Resize { logical_bytes: 7 },
        ContentTimes::Stamp(101),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    let untimed_after =
        (stamped.read_metadata(&untimed, WorkBudget::UNBOUNDED, &cancellation)).await?
            .value;
    assert_eq!(untimed_after, untimed_before);
    Ok(())
}

#[tokio::test]
async fn grouped_changes_land_together_and_each_keeps_its_own_result()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([141; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let existing = (checkout.create(crate::AuthoredMutation::CreateFile { path: path("existing")?, bytes: (bytes::Bytes::from(vec![b'a'; 128])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
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
    let applied = (checkout.apply_group(
        vec![create("first")?, content(b"ONE"), create("second")?],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
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
    let mixed = (checkout.apply_group(
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
    )).await?
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
            (checkout.lookup_no_follow(
                &path(name)?,
                WorkBudget::UNBOUNDED,
                &cancellation
            )).await?
            .value
            .record
            .is_some(),
            "{name} is bound"
        );
    }
    let read = (checkout.read_file_range_by_id(
        existing,
        ByteRange {
            offset: 0,
            length: 4,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(read.bytes.as_ref(), b"TWOa");
    let streamed_id = FileId::new();
    let streamed = (checkout.apply_group(
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
    )).await?.value;
    assert!(matches!(streamed[0], Ok(GroupedOutcome::Created(id)) if id == streamed_id));
    assert!(streamed[1].is_err());
    assert!(matches!(streamed[2], Ok(GroupedOutcome::Changed)));
    assert!(matches!(streamed[3], Ok(GroupedOutcome::Changed)));
    let streamed_read = (checkout.read_file_range_by_id(
        streamed_id, ByteRange { offset: 0, length: 4096 },
        WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value;
    assert_eq!(streamed_read.bytes.as_ref(), vec![b's'; 4096].as_slice());
    let ordered_read = (checkout.read_file_range_by_id(
        existing, ByteRange { offset: 0, length: 4 },
        WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value;
    assert_eq!(ordered_read.bytes.as_ref(), b"LAST");

    let denied = (checkout.apply_group(
        vec![create("denied-group")?],
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
        &cancellation,
    )).await;
    assert!(denied.is_err(), "output admission must precede effects");
    assert!((checkout.lookup_no_follow(
        &path("denied-group")?, WorkBudget::UNBOUNDED, &cancellation,
    )).await?.value.record.is_none());
    Ok(())
}

#[tokio::test]
async fn repeated_metadata_lookups_preserve_binding_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([151; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
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
    (local.create(crate::AuthoredMutation::CreateFile { path: path("authored")?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;

    let observed = path("observed")?;
    let first = (local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(first.value.is_none());
    let second = (local.lookup_no_follow_with_metadata(
        &observed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(second.value.is_none());

    (remote.create(crate::AuthoredMutation::CreateFile { path: observed, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
    (remote.commit(OperationId::from_bytes([152; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let rebase = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    Ok(())
}

#[tokio::test]
async fn batch_lookups_preserve_each_binding_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    for (seed, authored) in [(171_u8, false), (181, true)] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = (fs.create_volume_with_id(
            VolumeId::from_bytes([seed; 16]),
            config(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await?
        .value;
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
        if authored {
            (local.create(crate::AuthoredMutation::CreateFile { path: path("authored")?, bytes: (bytes::Bytes::from_static(b"local")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
        }
        let paths = [path("first")?, path("second")?];
        let batch =
        (local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation)).await?;
        assert!(
            batch
                .value
                .entries
                .iter()
                .all(|entry| entry.record.is_none())
        );
        (local.lookup_batch_no_follow(&paths, WorkBudget::UNBOUNDED, &cancellation)).await?;

        (remote.create(crate::AuthoredMutation::CreateFile { path: path("second")?, bytes: (bytes::Bytes::from_static(b"remote")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
        (remote.commit(OperationId::from_bytes([seed.wrapping_add(1); 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
        let rebase = (local.rebase_head(8, WorkBudget::UNBOUNDED, &cancellation)).await?;
        assert!(matches!(rebase.value, RebaseDecision::Conflicted { .. }));
    }
    Ok(())
}

#[tokio::test]
async fn one_reader_answers_a_batch_of_identities() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([191; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut writer = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut identities = Vec::new();
    for name in ["a", "b"] {
        (writer.create(crate::AuthoredMutation::CreateFile { path: path(name)?, bytes: (bytes::Bytes::from_static(b"x")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?;
        let record =
            (writer.lookup_no_follow(&path(name)?, WorkBudget::UNBOUNDED, &cancellation)).await?
                .value
                .record
                .ok_or("created file missing")?;
        identities.push(record);
    }
    let absent = FileId::from_bytes([192; 16]);
    let reader = writer.pinned_reader()?;
    assert_eq!(reader.generation_id(), writer.generation_id());
    let records = (reader.file_records_by_id(
        &[identities[1].file_id, absent, identities[0].file_id],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(
        records.value,
        vec![Some(identities[1]), None, Some(identities[0])]
    );
    Ok(())
}

#[tokio::test]
async fn each_grouped_change_has_its_own_budget() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([161; 16]),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let mut files = Vec::new();
    for (index, name) in ["one", "two", "three", "four"].into_iter().enumerate() {
        let file_id = (checkout.create(crate::AuthoredMutation::CreateFile { path: path(name)?, bytes: (bytes::Bytes::from(vec![u8::try_from(index)?; 16])).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)).await?
        .value;
        files.push(file_id);
    }
    let write = |file_id: FileId, fill: u8| GroupedChange::Content {
        file_id,
        change: ContentChange::Write { offset: 0, bytes: OwnedBytes::from(vec![fill; 4_096]) },
        times: ContentTimes::Preserve,
    };
    let single = (checkout.apply_group(
        vec![write(files[0], 1)],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(single.value.iter().all(Result::is_ok));
    // Enough for any one change, far from enough for three sharing it.
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_hashed = single.work.bytes_hashed * 3 / 2;
    let group = (checkout.apply_group(
        vec![write(files[1], 2), write(files[2], 3), write(files[3], 4)],
        budget,
        &cancellation,
    )).await?;
    assert!(
        group.value.iter().all(Result::is_ok),
        "a change failed on its neighbours' work: {:?}",
        group.value
    );
    assert!(group.work.bytes_hashed > budget.bytes_hashed);
    Ok(())
}
#[tokio::test]
async fn a_whole_file_rewrite_larger_than_the_read_bound_is_admitted()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([231; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("large")?;
    let stage = async |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        (checkout.stage_content(&mut source, 64, WorkBudget::UNBOUNDED, &cancellation)).await
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };

    let mut first = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let original = stage(&first, b"0123456789abcdef0123456789abcdef").await?;
    (first.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (first.commit(OperationId::from_bytes([231; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut second = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let replacement = stage(&second, b"ZYXWVUTSRQPONMLKzyxwvutsrqponmlk").await?;
    (second.apply_authored_transaction(
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
    )).await
    .map_err(|failure| format!("whole-file rewrite refused: {:?}", failure.error))?;
    (second.commit(OperationId::from_bytes([232; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;
    let head = (second.read_file_range(
        &file,
        ByteRange {
            offset: 24,
            length: 8,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    assert_eq!(head.bytes.as_ref(), b"rqponmlk");
    Ok(())
}
#[tokio::test]
async fn a_stale_whole_file_rewrite_still_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 8;
    let volume = (fs.create_volume_with_id(
        VolumeId::from_bytes([233; 16]),
        limited,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?
    .value;
    let file = path("large")?;
    let checkout = async || {
        (volume.checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )).await
        .map(|checkout| checkout.value)
        .map_err(|failure| format!("checkout failed: {:?}", failure.error))
    };
    let stage = async |checkout: &Checkout<_, _>, body: &'static [u8]| {
        let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
        (checkout.stage_content(&mut source, 64, WorkBudget::UNBOUNDED, &cancellation)).await
            .map(|staged| staged.value)
            .map_err(|failure| format!("staging failed: {:?}", failure.error))
    };
    let rewrite = async |checkout: &mut Checkout<_, _>, body: &'static [u8]| {
        let content = stage(checkout, body).await?;
        (checkout.apply_authored_transaction(
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
        )).await
        .map(|_| ())
        .map_err(|failure| format!("rewrite refused: {:?}", failure.error))
    };

    let mut seed = checkout().await?;
    let original = stage(&seed, b"0123456789abcdef0123456789abcdef").await?;
    (seed.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: file.clone(),
            content: original,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    (seed.commit(OperationId::from_bytes([233; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?;

    let mut first = checkout().await?;
    let mut stale = checkout().await?;
    rewrite(&mut first, b"FIRSTFIRSTFIRSTFIRSTFIRSTFIRST12").await?;
    let committed = (first.commit(OperationId::from_bytes([234; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
    .value;
    assert!(
        matches!(committed, CheckoutCommitOutcome::Committed { .. }),
        "{committed:?}"
    );
    rewrite(&mut stale, b"STALESTALESTALESTALESTALESTALE12").await?;
    let refused = (stale.commit(OperationId::from_bytes([235; 16]), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })).await?
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
    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| {
                std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))
            },
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| {
                std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))
            },
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let all = all.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(all.matches_fork_selection(&source, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert!(all.matches_fork_selection(&source, Some(vec!["/".to_owned()]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    assert!(!initial.matches_fork_selection(&source, Some(vec!["/".to_owned()]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
    Ok(())
}

#[tokio::test]
async fn selected_fork_verification_zero_allocation_and_cancellation_keep_failed_prefix()
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
    let workspace = (fs.create_workspace("selected-fork-verification-gates", WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    (workspace.write_text("/file", "body")).await?;
    let source = (async { workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }).await?;
    let child = (workspace.fork(
        "selected-fork-verification-child",
        ForkOptions::from_generation(source.clone(), crate::IdempotencyKey::new())
            .inherit_paths(vec!["/file".to_owned()]),
    crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| {
            std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))
        },
    )).await?.value;
    let initial = (async { child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }).await?;
    *tracked.lock().map_err(|_| "tracking lock poisoned")? = Some(ObjectId {
        kind: ObjectKind::GenerationRoot, digest: source.id().digest(),
    });
    reads.store(0, Ordering::Relaxed);
    let cancellation = CancellationToken::new();
    for budget in [
        WorkBudget::default(),
        WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED },
    ] {
        let failed = (initial.matches_fork_selection(
            &source, Some(vec!["/file".to_owned()]), budget, &cancellation,
        )).await.err().ok_or("verification ignored its allocation gate")?;
        assert!(matches!(failed.error, WorkspaceError::Work(_)));
        assert_eq!(failed.work.allocation_operations, 0);
        assert_eq!(failed.work.backend_read_operations, 0);
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed = (initial.matches_fork_selection(
        &source, Some(vec!["/file".to_owned()]), WorkBudget::UNBOUNDED, &cancelled,
    )).await.err().ok_or("verification ignored cancellation")?;
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
