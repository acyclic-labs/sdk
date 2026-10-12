//! Production native-root admission and source-bound attachment failure receipts.
#![cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]

use acyclic_fs::demand::{
    DemandDirectoryObserver, DemandError, DemandResult, DemandSource, FilteredDemandSource,
    SourceReference, native::NativeDemandSource,
};
use acyclic_fs::kernel::{LogicalName, NameEncoding, NamespacePath};
use acyclic_fs::model::{FilesystemProfile, Lifecycle, VolumeConfig, VolumeLimits};
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LazyOverlay, LazyOverlayId, LazyShadow, LazyShadowId,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceState, LazyWorkspaceStore,
    MemoryLazyWorkspaceStore, OperationFailure, OperationId, OperationReceipt,
    WorkBudget, WorkCounters, WorkspaceId,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn finite_budget() -> Result<WorkBudget, serde_json::Error> {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    if let serde_json::Value::Object(fields) = &mut value {
        for allowance in fields.values_mut() {
            *allowance = serde_json::json!(100_000_000_u64);
        }
    }
    serde_json::from_value(value)
}

#[derive(Default)]
struct DirectoryObserver(AtomicUsize);

impl DemandDirectoryObserver for DirectoryObserver {
    fn observe_directory(
        &self, _directory: &NamespacePath, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        cancellation.check().map_err(|_| OperationFailure::before_work(DemandError::Cancelled))?;
        let work = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        work.verify(budget).map_err(|error| OperationFailure::before_work(DemandError::Work(error)))?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(OperationReceipt { value: (), work })
    }
}

#[tokio::test]
async fn native_root_admission_denies_owned_memory_before_native_io() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let reference = SourceReference { identity: [32; 16], epoch: 1 };
    let cancellation = CancellationToken::new();
    for memory_denial in [false, true] {
        let path = directory.path().to_path_buf();
        let input_bytes = path.capacity() as u64;
        let mut budget = finite_budget()?;
        if memory_denial { budget.peak_allocation_bytes = 0; }
        else { budget.allocation_operations = 0; }
        let failure = NativeDemandSource::open_with_reference_measured(
            path, FilesystemProfile::Portable, VolumeLimits::default(), reference,
            None, budget, &cancellation,
        ).await.err().ok_or("native root ignored its owned allocation bound")?;
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.peak_allocation_bytes, if memory_denial { 0 } else { input_bytes });
        assert_eq!(failure.work.source_entries_visited, 0);
        assert_eq!(failure.work.source_bytes_read, 0);
        failure.work.verify(budget)?;
    }
    let opened = NativeDemandSource::open_with_reference_measured(
        directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
        reference, None, finite_budget()?, &cancellation,
    ).await?;
    assert_eq!(opened.value.reference(), reference);
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn original_native_root_custody_and_current_reference_are_provider_owned() -> TestResult {
    use acyclic_fs::NativeMaterializationWork;
    acyclic_native_runtime::initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let budget = finite_budget()?;
    let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).map_err(|(error, _)| error)?;
    let before = work.live_allocation_bytes();
    let (caller_path, caller_guard) = work.path(&root, None)?;
    let reference = SourceReference { identity: [92; 16], epoch: 18 };
    let source = NativeDemandSource::open_with_reference_and_native_work(
        &caller_path, FilesystemProfile::Portable, VolumeLimits::default(), reference, None, &work,
    ).await?;
    drop(caller_path);
    drop(caller_guard);
    assert!(work.live_allocation_bytes() > before, "provider retains its own root/control custody");
    let identity = source.root_identity();
    assert_eq!(source.validate_root_with_native_work(&root, identity, &work)?, reference);
    source.validate_current_with_native_work(reference, &work)?;
    let advanced = source.invalidate_with_native_work(&work)?;
    assert_eq!(advanced.identity, reference.identity);
    assert_eq!(advanced.epoch, reference.epoch + 1);
    assert!(matches!(source.validate_current_with_native_work(reference, &work), Err(DemandError::StaleSource)));
    assert_eq!(source.validate_root_with_native_work(&root, identity, &work)?, advanced);
    let ordinary = NativeDemandSource::open(
        &root, FilesystemProfile::Portable, VolumeLimits::default(), Some(reference), None,
    ).await?;
    let prefix = work.receipt();
    assert!(matches!(ordinary.validate_root_with_native_work(&root, identity, &work), Err(DemandError::NativeWorkUnavailable)));
    assert!(matches!(ordinary.invalidate_with_native_work(&work), Err(DemandError::NativeWorkUnavailable)));
    assert_eq!(work.receipt(), prefix);
    std::fs::rename(&root, temporary.path().join("held-original"))?;
    std::fs::create_dir(&root)?;
    assert!(matches!(source.validate_root_with_native_work(&root, identity, &work), Err(DemandError::SourceUnavailable)));
    let retained = work.live_allocation_bytes();
    drop(source);
    assert!(work.live_allocation_bytes() < retained, "the physical root spelling retires only with its provider");
    let detached_source = NativeDemandSource::open_with_reference_and_native_work(
        &root, FilesystemProfile::Portable, VolumeLimits::default(), advanced, None, &work,
    ).await?;
    let original = work.runtime_meter_owner();
    let original_weak = Arc::downgrade(&original);
    drop(original);
    work.receipt().verify(budget)?;
    drop(work);
    assert!(original_weak.upgrade().is_some(), "public provider outlives every caller accounting guard");
    drop(detached_source);
    assert!(original_weak.upgrade().is_none(), "provider Drop retires its intrinsic original keeper");
    Ok(())
}

// Faults occur only after the real companion operation ran. In particular,
// Binding is a lost acknowledgement of the real durable-in-memory CAS.
#[derive(Clone)]
struct FaultingCompanion {
    inner: MemoryLazyWorkspaceStore,
    fault: Arc<AtomicUsize>,
}

impl FaultingCompanion {
    fn new(fault: usize) -> Self {
        Self {
            inner: MemoryLazyWorkspaceStore::default(),
            fault: Arc::new(AtomicUsize::new(fault)),
        }
    }

    fn finish<T>(&self, stage: usize, receipt: OperationReceipt<T>)
        -> Result<OperationReceipt<T>, LazyWorkspaceError>
    {
        if stage != 0 && self.fault.compare_exchange(stage, 0, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            return Err(LazyWorkspaceError::SpentWork(Box::new(OperationFailure::new(
                LazyWorkspaceError::Store("lost real companion acknowledgement".to_owned()),
                receipt.work,
            ))));
        }
        Ok(receipt)
    }
}

impl LazyWorkspaceStore for FaultingCompanion {
    type Error = LazyWorkspaceError;

    async fn load_lazy_workspace(&self, id: WorkspaceId)
        -> Result<Option<LazyWorkspaceState>, Self::Error>
    {
        Ok(self.load_lazy_workspace_measured(id, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value)
    }

    async fn load_lazy_workspace_measured(
        &self, id: WorkspaceId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyWorkspaceState>>, LazyWorkspaceError> {
        let receipt = self.inner.load_lazy_workspace_measured(id, budget, cancellation).await?;
        self.finish(1, receipt)
    }

    async fn compare_and_swap_lazy_workspace(
        &self, id: WorkspaceId, revision: u64, replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        Ok(self.compare_and_swap_lazy_workspace_measured(
            id, revision, replacement, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await?.value)
    }

    async fn compare_and_swap_lazy_workspace_measured(
        &self, id: WorkspaceId, revision: u64, replacement: LazyWorkspaceState,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        let receipt = self.inner.compare_and_swap_lazy_workspace_measured(
            id, revision, replacement, budget, cancellation,
        ).await?;
        self.finish(4, receipt)
    }

    async fn load_lazy_overlay(&self, id: LazyOverlayId)
        -> Result<Option<LazyOverlay>, Self::Error>
    {
        Ok(self.load_lazy_overlay_measured(id, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value)
    }

    async fn load_lazy_overlay_measured(
        &self, id: LazyOverlayId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlay>>, LazyWorkspaceError> {
        let receipt = self.inner.load_lazy_overlay_measured(id, budget, cancellation).await?;
        self.finish(0, receipt)
    }

    async fn put_lazy_overlay(&self, id: LazyOverlayId, value: LazyOverlay)
        -> Result<(), Self::Error>
    {
        self.put_lazy_overlay_measured(id, value, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
        Ok(())
    }

    async fn put_lazy_overlay_measured(
        &self, id: LazyOverlayId, value: LazyOverlay, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        let receipt = self.inner.put_lazy_overlay_measured(id, value, budget, cancellation).await?;
        self.finish(2, receipt)
    }

    async fn load_lazy_shadow(&self, id: LazyShadowId)
        -> Result<Option<LazyShadow>, Self::Error>
    {
        Ok(self.load_lazy_shadow_measured(id, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?.value)
    }

    async fn load_lazy_shadow_measured(
        &self, id: LazyShadowId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyShadow>>, LazyWorkspaceError> {
        let receipt = self.inner.load_lazy_shadow_measured(id, budget, cancellation).await?;
        self.finish(0, receipt)
    }

    async fn put_lazy_shadow(&self, id: LazyShadowId, value: LazyShadow)
        -> Result<(), Self::Error>
    {
        self.put_lazy_shadow_measured(id, value, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
        Ok(())
    }

    async fn put_lazy_shadow_measured(
        &self, id: LazyShadowId, value: LazyShadow, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        let receipt = self.inner.put_lazy_shadow_measured(id, value, budget, cancellation).await?;
        self.finish(3, receipt)
    }
}

#[tokio::test]
async fn companion_faults_preserve_creation_plus_each_real_request() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    std::fs::create_dir(directory.path().join("unresolved"))?;
    std::fs::write(directory.path().join("unresolved/file"), b"not enumerated")?;
    let reference = SourceReference { identity: [41; 16], epoch: 3 };
    let observer = Arc::new(DirectoryObserver::default());
    let budget = finite_budget()?;
    let source = Arc::new(NativeDemandSource::open_with_reference_measured(
        directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
        reference, Some(observer.clone()), budget, &CancellationToken::new(),
    ).await?.value);
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    for stage in 1..=4 {
        let operation = OperationId::new();
        let prior = Fs::memory().create_workspace_with_operation(
            "companion-fault", config, operation, budget, &CancellationToken::new(),
        ).await?;
        let fs = Fs::memory();
        let store = FaultingCompanion::new(stage);
        let failure = LazyWorkspace::attach_with_operation_measured(
            &fs, "companion-fault", source.clone(), store.clone(), config, operation,
            budget, &CancellationToken::new(),
        ).await.err().ok_or("real companion fault did not fail attachment")?;
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Store(_)));
        failure.work.verify(budget)?;
        let workspace = fs.open_workspace("companion-fault", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let initial = workspace.operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.ok_or("companion failure lost the real committed creation")?;
        assert_eq!(initial.id(), prior.value.id());
        assert_eq!(initial.workspace_id(), prior.value.workspace_id());
        assert_eq!(store.inner.load_lazy_workspace(workspace.id()).await?.is_some(), stage == 4);
        workspace.write_text("/later.txt", "later authored head").await?;
        let head = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
        assert_ne!(head, initial.id());
        let recovered = LazyWorkspace::attach_with_operation_measured(
            &fs, "companion-fault", source.clone(), store.clone(), config, operation,
            budget, &CancellationToken::new(),
        ).await?;
        recovered.work.verify(budget)?;
        let (lazy, original) = recovered.value;
        assert_eq!(original.id(), initial.id());
        assert_eq!(original.workspace_id(), initial.workspace_id());
        assert_eq!(lazy.workspace().head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), head);
        let state = store.inner.load_lazy_workspace(workspace.id()).await?
            .ok_or("retry did not publish the companion binding")?;
        assert_eq!(state.source, reference);
        let snapshot = lazy.snapshot(&acyclic_fs::lazy_workspace::LazyTreapWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        )).await?;
        assert_eq!(snapshot.workspace_id, original.workspace_id());
        assert_eq!(snapshot.authored_generation, head);
        assert_eq!(snapshot.source, reference);
        assert_eq!(observer.0.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn source_epoch_rebind_and_reopen_never_rewrite_original_creation() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    std::fs::create_dir_all(directory.path().join("unobserved/deeper"))?;
    std::fs::write(directory.path().join("unobserved/deeper/file"), b"source bytes")?;
    let observer = Arc::new(DirectoryObserver::default());
    let reference = SourceReference { identity: [51; 16], epoch: 8 };
    let budget = finite_budget()?;
    let cancellation = CancellationToken::new();
    let source = Arc::new(NativeDemandSource::open_with_reference_measured(
        directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
        reference, Some(observer.clone()), budget, &cancellation,
    ).await?.value);
    let fs = Fs::memory();
    let store = FaultingCompanion::new(0);
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = OperationId::new();
    let created = LazyWorkspace::attach_with_operation_measured(
        &fs, "epoch-attachment", source.clone(), store.clone(), config, operation,
        budget, &cancellation,
    ).await?;
    created.work.verify(budget)?;
    let (lazy, original) = created.value;
    let initial = lazy.snapshot(&acyclic_fs::lazy_workspace::LazyTreapWork::new(
        WorkBudget::UNBOUNDED, &cancellation,
    )).await?;
    assert_eq!(initial.authored_generation, original.id());
    assert_eq!(initial.workspace_id, original.workspace_id());
    assert_eq!(initial.source, reference);
    let original_id = original.id();
    let workspace_id = original.workspace_id();
    lazy.workspace().write_text("/later.txt", "mutable authored head").await?;
    let head = lazy.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    assert_ne!(head, original_id);
    let advanced = source.invalidate(&mut acyclic_fs::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation))?;
    let rebind_work = acyclic_fs::lazy_workspace::LazyTreapWork::new(budget, &cancellation);
    let result = lazy.rebind_source(&rebind_work).await;
    let rebound = rebind_work.measured(result, |error, work| error.with_prior_work(work))?;
    rebound.work.verify(budget)?;
    assert_eq!(rebound.value.as_state().source, advanced);
    assert_eq!(advanced.identity, reference.identity);
    assert_eq!(advanced.epoch, reference.epoch + 1);
    assert_eq!(rebound.value.as_state().overlay, initial.overlay);
    assert_eq!(rebound.value.as_state().shadows, initial.shadows);
    drop(original);
    drop(lazy);
    drop(source);
    let reopened_source = Arc::new(NativeDemandSource::open_with_reference_measured(
        directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
        advanced, Some(observer.clone()), budget, &cancellation,
    ).await?.value);
    let recovered = LazyWorkspace::attach_with_operation_measured(
        &fs, "epoch-attachment", reopened_source, store.clone(), config, operation,
        budget, &cancellation,
    ).await?;
    recovered.work.verify(budget)?;
    assert_eq!(recovered.value.1.id(), original_id);
    assert_eq!(recovered.value.1.workspace_id(), workspace_id);
    assert_eq!(recovered.value.0.workspace().head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), head);
    assert_eq!(recovered.value.0.snapshot().await?.source, advanced);
    assert_eq!(store.load_lazy_workspace(workspace_id).await?.ok_or("lost binding")?.source, advanced);

    let foreign_source = Arc::new(NativeDemandSource::open_with_reference_measured(
        directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
        SourceReference { identity: [52; 16], epoch: advanced.epoch }, None, budget, &cancellation,
    ).await?.value);
    let rejected = LazyWorkspace::attach_with_operation_measured(
        &fs, "epoch-attachment", foreign_source, store.clone(), config, operation,
        budget, &cancellation,
    ).await.err().ok_or("foreign source adopted an original binding")?;
    assert!(matches!(rejected.error.semantic_error(), LazyWorkspaceError::StaleSource));
    rejected.work.verify(budget)?;
    assert_eq!(store.load_lazy_workspace(workspace_id).await?, Some(rebound.value));
    assert_eq!(fs.open_workspace("epoch-attachment", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), head);
    assert_eq!(observer.0.load(Ordering::SeqCst), 0);
    Ok(())
}

fn requested(name: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let components = if name.is_empty() {
        Vec::new()
    } else {
        vec![LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), limits.maximum_component_bytes)?]
    };
    Ok(NamespacePath::new(components, limits)?)
}

#[tokio::test]
async fn native_request_denials_precede_real_io_and_buffer_allocation() -> TestResult {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"actual source bytes")?;
    let source = NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default(), None, None).await?;
    let path = requested("file")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source.lookup(source.reference(), &path, budget, &token).await?.value.ok_or("file absent")?;

    let mut no_io = budget;
    no_io.backend_read_operations = 0;
    let failure = source.lookup(source.reference(), &path, no_io, &token).await.err().ok_or("zero IO was not enforced")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    failure.work.verify(no_io)?;

    let mut no_memory = budget;
    no_memory.allocation_operations = 0;
    let failure = source.lookup(source.reference(), &path, no_memory, &token).await.err().ok_or("path clone exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    failure.work.verify(no_memory)?;

    let mut no_output = budget;
    no_output.output_bytes = 0;
    let failure = source.read_range(source.reference(), &path, node.version, 0, 6, no_output, &token)
        .await.err().ok_or("range output allowance was ignored")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 0);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(no_output)?;

    token.cancel();
    let lookup = source.lookup(source.reference(), &path, budget, &token).await.err().ok_or("cancelled lookup succeeded")?;
    let read = source.read_range(source.reference(), &path, node.version, 0, 6, budget, &token)
        .await.err().ok_or("cancelled range succeeded")?;
    let opened = source.open_file(source.reference(), &path, node.version, budget, &token)
        .await.err().ok_or("cancelled open succeeded")?;
    let page = source.list_page(source.reference(), &requested("")?, None, 1, budget, &token)
        .await.err().ok_or("cancelled page succeeded")?;
    for failure in [lookup, read, opened, page] {
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    Ok(())
}

#[tokio::test]
async fn native_read_failure_retains_bytes_spent_before_current_fence_denial() -> TestResult {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"native-read")?;
    let source = NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default(), None, None).await?;
    let path = requested("file")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source.lookup(source.reference(), &path, budget, &token).await?.value.ok_or("file absent")?;
    let baseline = source.read_range(source.reference(), &path, node.version, 0, 6, budget, &token).await?;
    assert_eq!(baseline.value.bytes.as_ref(), b"native");
    baseline.work.verify(budget)?;
    let mut limited = budget;
    limited.backend_read_operations = baseline.work.backend_read_operations.checked_sub(1).ok_or("no native IO")?;
    let failure = source.read_range(source.reference(), &path, node.version, 0, 6, limited, &token)
        .await.err().ok_or("current-proof IO exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 6);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(limited)?;

    let file = source.open_file(source.reference(), &path, node.version, budget, &token).await?.value;
    let baseline = file.read_range(0, 6, budget, &token)?;
    limited.backend_read_operations = baseline.work.backend_read_operations.checked_sub(1).ok_or("no held native IO")?;
    let failure = file.read_range(0, 6, limited, &token).err().ok_or("held file ignored native IO limit")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 6);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(limited)?;
    Ok(())
}

#[tokio::test]
async fn native_continuation_replays_consumed_entries_after_budget_denial() -> TestResult {
    let directory = tempfile::tempdir()?;
    for index in 0..8 { std::fs::write(directory.path().join(format!("entry-{index}")), b"body")?; }
    let source = NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default(), None, None).await?;
    let path = requested("")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let mut first = source.list_page(source.reference(), &path, None, 1, budget, &token).await?;
    acyclic_fs::demand::accept_publication(&mut first.value.publication, first.work.remaining(budget)?, &token).await?;
    let cursor = first.value.next.ok_or("expected continuation")?;
    let mut limited = budget;
    limited.source_entries_visited = 0;
    let failure = source.list_page(source.reference(), &path, Some(cursor.clone()), 2, limited, &token)
        .await.err().ok_or("cursor advanced beyond its source-item allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_entries_visited, 0);
    failure.work.verify(limited)?;
    let mut names = first.value.entries.iter().map(|entry| entry.name().as_bytes().to_vec()).collect::<Vec<_>>();
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let mut page = source.list_page(source.reference(), &path, Some(cursor), 2, budget, &token).await?;
        page.work.verify(budget)?;
        acyclic_fs::demand::accept_publication(&mut page.value.publication, page.work.remaining(budget)?, &token).await?;
        names.extend(page.value.entries.iter().map(|entry| entry.name().as_bytes().to_vec()));
        next = page.value.next;
    }
    names.sort();
    let expected = (0..8).map(|index| format!("entry-{index}").into_bytes()).collect::<Vec<_>>();
    assert_eq!(names, expected);
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn finite_native_utf16_paths_preserve_unpaired_names_and_restart() -> TestResult {
    use std::os::windows::ffi::OsStringExt as _;
    let directory = tempfile::tempdir()?;
    let units = [u16::from(b'o'), 0xd800, u16::from(b'p')];
    let native_name = std::ffi::OsString::from_wide(&units);
    std::fs::write(directory.path().join(native_name), b"opaque")?;
    let limits = VolumeLimits::default();
    let bytes = units.into_iter().flat_map(u16::to_le_bytes).collect::<Vec<_>>();
    let name = LogicalName::new(NameEncoding::WindowsUtf16Le, bytes, limits.maximum_component_bytes)?;
    let path = NamespacePath::new(vec![name.clone()], limits)?;
    let source = NativeDemandSource::open(directory.path(), FilesystemProfile::Windows, limits, None, None).await?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source.lookup(source.reference(), &path, budget, &token).await?.value.ok_or("opaque file absent")?;
    let mut page = source.list_page(source.reference(), &requested("")?, None, 8, budget, &token).await?;
    acyclic_fs::demand::accept_publication(&mut page.value.publication, page.work.remaining(budget)?, &token).await?;
    assert_eq!(page.value.entries.len(), 1);
    assert_eq!(page.value.entries[0].name(), &name);
    page.work.verify(budget)?;
    let read = source.read_range(source.reference(), &path, node.version, 0, 6, budget, &token).await?;
    assert_eq!(read.value.bytes.as_ref(), b"opaque");
    read.work.verify(budget)?;
    let mut limited = budget;
    limited.peak_allocation_bytes = read.work.peak_allocation_bytes.checked_sub(1).ok_or("no native live buffer")?;
    let failure = source.read_range(source.reference(), &path, node.version, 0, 6, limited, &token)
        .await.err().ok_or("native UTF16 path/read allocation exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    failure.work.verify(limited)?;
    let reopened = NativeDemandSource::open(
        directory.path(), FilesystemProfile::Windows, limits, Some(source.reference()), None,
    ).await?;
    let read = reopened.read_range(reopened.reference(), &path, node.version, 0, 6, budget, &token).await?;
    assert_eq!(read.value.bytes.as_ref(), b"opaque");
    read.work.verify(budget)?;
    Ok(())
}

#[tokio::test]
async fn filtered_page_reserves_comparisons_before_committing_native_cursor() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c"] { std::fs::write(directory.path().join(name), b"actual")?; }
    let native = NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default(), None, None).await?;
    let source = FilteredDemandSource::new(native, vec![requested("private")?, requested("excluded")?]);
    let root = requested("")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let mut first = source.list_page(source.reference(), &root, None, 1, budget, &token).await?;
    acyclic_fs::demand::accept_publication(&mut first.value.publication, first.work.remaining(budget)?, &token).await?;
    let cursor = first.value.next.ok_or("native continuation absent")?;
    let mut denied = budget;
    denied.items_examined = 2;
    let failure = source.list_page(source.reference(), &root, Some(cursor.clone()), 8, denied, &token)
        .await.err().ok_or("filter advanced native cursor without projection allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.source_entries_visited, 0);
    assert_eq!(failure.work.allocation_operations, 0);
    failure.work.verify(denied)?;
    let mut next = source.list_page(source.reference(), &root, Some(cursor), 8, budget, &token).await?;
    acyclic_fs::demand::accept_publication(&mut next.value.publication, next.work.remaining(budget)?, &token).await?;
    next.work.verify(budget)?;
    assert!(next.value.next.is_none());
    let mut names = first.value.entries.iter().chain(next.value.entries.iter())
        .map(|entry| entry.name().as_bytes().to_vec()).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[tokio::test]
async fn native_unpublished_pages_restore_initial_and_continuation_positions() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c", "d"] { std::fs::write(directory.path().join(name), b"body")?; }
    let source = NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default(), None, None).await?;
    let root = requested("")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let mut position = None;
    for _ in 0..2 {
        let page = source.list_page(source.reference(), &root, position.clone(), 1, budget, &token).await?;
        page.work.verify(budget)?;
        assert_eq!(page.work.source_bytes_read, 0);
        assert!(page.value.retained_bytes() > 0);
        let expected = page.value.entries.iter().map(|entry| entry.name().clone()).collect::<Vec<_>>();
        drop(page);
        let mut retry = source.list_page(source.reference(), &root, position.clone(), 1, budget, &token).await?;
        assert_eq!(retry.value.entries.iter().map(|entry| entry.name().clone()).collect::<Vec<_>>(), expected);
        let aborted = acyclic_fs::demand::abort_publication(&mut retry.value.publication, retry.work.remaining(budget)?)?;
        aborted.work.verify(retry.work.remaining(budget)?)?;
        retry.work.checked_add(aborted.work)?.verify(budget)?;
        let mut accepted = source.list_page(source.reference(), &root, position.clone(), 1, budget, &token).await?;
        assert_eq!(accepted.value.entries.iter().map(|entry| entry.name().clone()).collect::<Vec<_>>(), expected);
        let committed = acyclic_fs::demand::accept_publication(&mut accepted.value.publication, accepted.work.remaining(budget)?, &token).await?;
        accepted.work.checked_add(committed.work)?.verify(budget)?;
        position = accepted.value.next;
        assert!(position.is_some());
    }
    Ok(())
}

#[tokio::test]
async fn native_late_publication_cancellation_and_cleanup_denial_preserve_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c"] { std::fs::write(directory.path().join(name), b"actual")?; }
    let source = NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default(), None, None).await?;
    let root = requested("")?;
    let live = CancellationToken::new();
    let budget = finite_budget()?;
    for cancelled in [false, true] {
        let mut page = source.list_page(source.reference(), &root, None, 2, budget, &live).await?;
        assert!(page.work.backend_read_operations > 0);
        assert!(page.work.items_examined > 0);
        assert!(page.work.allocation_operations > 0);
        page.work.verify(budget)?;
        let expected = page.value.entries.iter().map(|entry| entry.name().clone()).collect::<Vec<_>>();
        let projected = CancellationToken::new();
        let mut remaining = page.work.remaining(budget)?;
        if cancelled { projected.cancel(); } else { remaining.items_examined = 0; }
        let failure = acyclic_fs::demand::accept_publication(&mut page.value.publication, remaining, &projected).await
            .err().ok_or("unadmitted publication committed native cursor")?;
        if cancelled { assert!(matches!(failure.error, DemandError::Cancelled)); }
        else { assert!(matches!(failure.error, DemandError::Work(_))); }
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        failure.work.verify(remaining)?;
        // The original source receipt remains observable after late projection failure.
        assert!(page.work.backend_read_operations > 0);
        let mut cleanup_budget = page.work.remaining(budget)?;
        cleanup_budget.items_examined = 1;
        let aborted = acyclic_fs::demand::abort_publication(&mut page.value.publication, cleanup_budget)?;
        aborted.work.verify(cleanup_budget)?;
        let retained = page.work.checked_add(*failure.work)?.checked_add(aborted.work)?;
        retained.verify(budget)?;
        let mut retry = source.list_page(source.reference(), &root, None, 2, budget, &live).await?;
        assert_eq!(retry.value.entries.iter().map(|entry| entry.name().clone()).collect::<Vec<_>>(), expected);
        acyclic_fs::demand::abort_publication(&mut retry.value.publication, retry.work.remaining(budget)?)?;
    }
    let mut committed = source.list_page(source.reference(), &root, None, 2, budget, &live).await?;
    let mut cleanup_budget = committed.work.remaining(budget)?;
    cleanup_budget.items_examined = 1;
    let accepted = acyclic_fs::demand::accept_publication(&mut committed.value.publication, cleanup_budget, &live).await?;
    accepted.work.verify(cleanup_budget)?;
    committed.work.checked_add(accepted.work)?.verify(budget)?;
    Ok(())
}
