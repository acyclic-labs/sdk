//! Production native-root admission and source-bound attachment failure receipts.
#![cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]

use acyclic_fs::demand::{
    DemandDirectoryObserver, DemandError, DemandResult, DemandSource, SourceReference,
    native::NativeDemandSource,
};
use acyclic_fs::facade::FsWork;
use acyclic_fs::kernel::{
    KernelAccounting, KernelOwnedAllocation, LogicalName, NameEncoding, NamespacePath,
};
use acyclic_fs::model::{FilesystemProfile, Lifecycle, VolumeConfig, VolumeLimits};
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LazyOverlay, LazyOverlayId, LazyShadow, LazyShadowId,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceState, LazyWorkspaceStore,
    MemoryLazyWorkspaceStore, OperationFailure, OperationId, OperationReceipt, WorkBudget,
    WorkCounters, WorkspaceId,
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
        &self,
        _directory: &NamespacePath,
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        accounting
            .charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(DemandError::from)?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn native_root_admission_denies_owned_memory_before_native_io() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let reference = SourceReference {
        identity: [32; 16],
        epoch: 1,
    };
    let cancellation = CancellationToken::new();
    for memory_denial in [false, true] {
        let path = directory.path().to_path_buf();
        let input_bytes = path.capacity() as u64;
        let mut budget = finite_budget()?;
        if memory_denial {
            budget.peak_allocation_bytes = 0;
        } else {
            budget.allocation_operations = 0;
        }
        let failure = NativeDemandSource::open(
            std::borrow::Cow::Owned(path),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(reference),
            None,
            budget,
            &cancellation,
        )
        .await
        .err()
        .ok_or("native root ignored its owned allocation bound")?;
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(
            failure.work.peak_allocation_bytes,
            if memory_denial { 0 } else { input_bytes }
        );
        assert_eq!(failure.work.source_entries_visited, 0);
        assert_eq!(failure.work.source_bytes_read, 0);
        failure.work.verify(budget)?;
    }
    let opened = NativeDemandSource::open(
        std::borrow::Cow::Owned(directory.path().to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        Some(reference),
        None,
        finite_budget()?,
        &cancellation,
    )
    .await?;
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
    let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let before = work.live_allocation_bytes();
    let (caller_path, caller_guard) = work.path(&root, None)?;
    let reference = SourceReference {
        identity: [92; 16],
        epoch: 18,
    };
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(std::path::Path::new(&caller_path)),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        Some(reference),
        None,
        &work,
        (&work).cancellation(),
    )
    .await?;
    drop(caller_path);
    drop(caller_guard);
    assert!(
        work.live_allocation_bytes() > before,
        "provider retains its own root/control custody"
    );
    let namespace = requested("")?;
    assert!(
        source
            .lookup(reference, &namespace, &work, work.cancellation())
            .await?
            .is_some()
    );
    let advanced = source.invalidate();
    assert_eq!(advanced.identity, reference.identity);
    assert_eq!(advanced.epoch, reference.epoch + 1);
    assert!(matches!(
        source
            .lookup(reference, &namespace, &work, work.cancellation())
            .await,
        Err(DemandError::StaleSource)
    ));
    assert!(
        source
            .lookup(advanced, &namespace, &work, work.cancellation())
            .await?
            .is_some()
    );
    std::fs::rename(&root, temporary.path().join("held-original"))?;
    std::fs::create_dir(&root)?;
    assert!(matches!(
        source
            .lookup(advanced, &namespace, &work, work.cancellation())
            .await,
        Err(DemandError::SourceUnavailable)
    ));
    let retained = work.live_allocation_bytes();
    drop(source);
    assert!(
        work.live_allocation_bytes() < retained,
        "the physical root spelling retires only with its provider"
    );
    let detached_source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(std::path::Path::new(&root)),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        Some(advanced),
        None,
        &work,
        (&work).cancellation(),
    )
    .await?;
    let original = work.runtime_meter();
    let original_weak = Arc::downgrade(&original);
    drop(original);
    work.receipt().verify(budget)?;
    drop(work);
    assert!(
        original_weak.upgrade().is_some(),
        "public provider outlives every caller accounting guard"
    );
    drop(detached_source);
    assert!(
        original_weak.upgrade().is_none(),
        "provider Drop retires its intrinsic original keeper"
    );
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

    fn finish<T>(&self, stage: usize, value: T) -> Result<T, LazyWorkspaceError> {
        if stage != 0
            && self
                .fault
                .compare_exchange(stage, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(LazyWorkspaceError::Store(
                "lost real companion acknowledgement".to_owned(),
            ));
        }
        Ok(value)
    }
}

impl LazyWorkspaceStore for FaultingCompanion {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        self.finish(1, self.inner.load_lazy_workspace(id, accounting).await?)
    }
    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: LazyWorkspaceState,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        self.finish(
            4,
            self.inner
                .compare_and_swap_lazy_workspace(id, revision, replacement, accounting)
                .await?,
        )
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.inner.load_lazy_overlay(id, accounting).await
    }
    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.finish(2, self.inner.put_lazy_overlay(id, value, accounting).await?)
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.inner.load_lazy_shadow(id, accounting).await
    }
    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.finish(3, self.inner.put_lazy_shadow(id, value, accounting).await?)
    }
}

#[tokio::test]
async fn companion_faults_preserve_creation_plus_each_real_request() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    std::fs::create_dir(directory.path().join("unresolved"))?;
    std::fs::write(directory.path().join("unresolved/file"), b"not enumerated")?;
    let reference = SourceReference {
        identity: [41; 16],
        epoch: 3,
    };
    let observer = Arc::new(DirectoryObserver::default());
    let budget = finite_budget()?;
    let source = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(reference),
            Some(observer.clone()),
            budget,
            &CancellationToken::new(),
        )
        .await?
        .value,
    );
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    for stage in 1..=4 {
        let operation = OperationId::new();
        let prior = Fs::memory()
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                "companion-fault",
                config,
                operation,
                budget,
                &CancellationToken::new(),
                None,
            )
            .await?;
        let fs = Fs::memory();
        let store = FaultingCompanion::new(stage);
        let failure = async {
            let cancellation = &CancellationToken::new();
            let budget = budget;
            let mut accounting = budget.accounting(cancellation);
            let result = LazyWorkspace::attach(
                &fs,
                "companion-fault",
                source.clone(),
                store.clone(),
                config,
                operation,
                &mut accounting,
            )
            .await;
            budget.finish(result, &accounting)
        }
        .await
        .err()
        .ok_or("real companion fault did not fail attachment")?;
        assert!(matches!(
            failure.error.semantic_error(),
            LazyWorkspaceError::Store(_)
        ));
        failure.work.verify(budget)?;
        let workspace = fs
            .open_workspace(
                "companion-fault",
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let initial = workspace
            .operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?
            .ok_or("companion failure lost the real committed creation")?;
        assert_eq!(initial.id(), prior.value.id());
        assert_eq!(initial.workspace_id(), prior.value.workspace_id());
        assert_eq!(
            store
                .inner
                .load_lazy_workspace(
                    workspace.id(),
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new())
                )
                .await?
                .is_some(),
            stage == 4
        );
        workspace
            .write_text("/later.txt", "later authored head")
            .await?;
        let head = workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?
            .id();
        assert_ne!(head, initial.id());
        let recovered = async {
            let cancellation = &CancellationToken::new();
            let budget = budget;
            let mut accounting = budget.accounting(cancellation);
            let result = LazyWorkspace::attach(
                &fs,
                "companion-fault",
                source.clone(),
                store.clone(),
                config,
                operation,
                &mut accounting,
            )
            .await;
            budget.finish(result, &accounting)
        }
        .await?;
        recovered.work.verify(budget)?;
        let (lazy, original) = recovered.value;
        assert_eq!(original.id(), initial.id());
        assert_eq!(original.workspace_id(), initial.workspace_id());
        assert_eq!(
            lazy.workspace()
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new()
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?
                .id(),
            head
        );
        let state = store
            .inner
            .load_lazy_workspace(
                workspace.id(),
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .await?
            .ok_or("retry did not publish the companion binding")?;
        assert_eq!(state.source, reference);
        let snapshot = lazy
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
            .await?;
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
    std::fs::write(
        directory.path().join("unobserved/deeper/file"),
        b"source bytes",
    )?;
    let observer = Arc::new(DirectoryObserver::default());
    let reference = SourceReference {
        identity: [51; 16],
        epoch: 8,
    };
    let budget = finite_budget()?;
    let cancellation = CancellationToken::new();
    let source = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(reference),
            Some(observer.clone()),
            budget,
            &cancellation,
        )
        .await?
        .value,
    );
    let fs = Fs::memory();
    let store = FaultingCompanion::new(0);
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = OperationId::new();
    let created = async {
        let cancellation = &cancellation;
        let budget = budget;
        let mut accounting = budget.accounting(cancellation);
        let result = LazyWorkspace::attach(
            &fs,
            "epoch-attachment",
            source.clone(),
            store.clone(),
            config,
            operation,
            &mut accounting,
        )
        .await;
        budget.finish(result, &accounting)
    }
    .await?;
    created.work.verify(budget)?;
    let (lazy, original) = created.value;
    let initial = lazy
        .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&cancellation))
        .await?;
    assert_eq!(initial.authored_generation, original.id());
    assert_eq!(initial.workspace_id, original.workspace_id());
    assert_eq!(initial.source, reference);
    let original_id = original.id();
    let workspace_id = original.workspace_id();
    lazy.workspace()
        .write_text("/later.txt", "mutable authored head")
        .await?;
    let head = lazy
        .workspace()
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id();
    assert_ne!(head, original_id);
    let advanced = source.invalidate();
    let mut accounting = budget.accounting(&cancellation);
    let result = lazy.state(&mut accounting).await;
    let rebound = budget.finish(result, &accounting)?;
    rebound.work.verify(budget)?;
    assert_eq!(rebound.value.source, advanced);
    assert_eq!(advanced.identity, reference.identity);
    assert_eq!(advanced.epoch, reference.epoch + 1);
    assert_eq!(rebound.value.overlay, initial.overlay);
    assert_eq!(rebound.value.shadows, initial.shadows);
    drop(original);
    drop(lazy);
    drop(source);
    let reopened_source = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(advanced),
            Some(observer.clone()),
            budget,
            &cancellation,
        )
        .await?
        .value,
    );
    let recovered = async {
        let cancellation = &cancellation;
        let budget = budget;
        let mut accounting = budget.accounting(cancellation);
        let result = LazyWorkspace::attach(
            &fs,
            "epoch-attachment",
            reopened_source,
            store.clone(),
            config,
            operation,
            &mut accounting,
        )
        .await;
        budget.finish(result, &accounting)
    }
    .await?;
    recovered.work.verify(budget)?;
    assert_eq!(recovered.value.1.id(), original_id);
    assert_eq!(recovered.value.1.workspace_id(), workspace_id);
    assert_eq!(
        recovered
            .value
            .0
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?
            .id(),
        head
    );
    assert_eq!(
        recovered
            .value
            .0
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&cancellation))
            .await?
            .source,
        advanced
    );
    assert_eq!(
        store
            .load_lazy_workspace(
                workspace_id,
                &mut WorkBudget::UNBOUNDED.accounting(&cancellation)
            )
            .await?
            .ok_or("lost binding")?
            .source,
        advanced
    );

    let foreign_source = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            Some(SourceReference {
                identity: [52; 16],
                epoch: advanced.epoch,
            }),
            None,
            budget,
            &cancellation,
        )
        .await?
        .value,
    );
    let rejected = async {
        let cancellation = &cancellation;
        let budget = budget;
        let mut accounting = budget.accounting(cancellation);
        let result = LazyWorkspace::attach(
            &fs,
            "epoch-attachment",
            foreign_source,
            store.clone(),
            config,
            operation,
            &mut accounting,
        )
        .await;
        budget.finish(result, &accounting)
    }
    .await
    .err()
    .ok_or("foreign source adopted an original binding")?;
    assert!(matches!(
        rejected.error.semantic_error(),
        LazyWorkspaceError::StaleSource
    ));
    rejected.work.verify(budget)?;
    assert_eq!(
        store
            .load_lazy_workspace(
                workspace_id,
                &mut WorkBudget::UNBOUNDED.accounting(&cancellation)
            )
            .await?
            .as_ref(),
        Some(&rebound.value)
    );
    assert_eq!(
        fs.open_workspace(
            "epoch-attachment",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
        .id(),
        head
    );
    assert_eq!(observer.0.load(Ordering::SeqCst), 0);
    Ok(())
}

fn requested(name: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let components = if name.is_empty() {
        Vec::new()
    } else {
        vec![LogicalName::new(
            NameEncoding::Utf8,
            name.as_bytes().to_vec(),
            limits.maximum_component_bytes,
        )?]
    };
    Ok(NamespacePath::new(components, limits)?)
}

#[tokio::test]
async fn native_request_denials_precede_real_io_and_buffer_allocation() -> TestResult {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"actual source bytes")?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let path = requested("file")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source
        .lookup(source.reference(), &path, budget, &token)
        .await?
        .value
        .ok_or("file absent")?;

    let mut no_io = budget;
    no_io.backend_read_operations = 0;
    let failure = source
        .lookup(source.reference(), &path, no_io, &token)
        .await
        .err()
        .ok_or("zero IO was not enforced")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    failure.work.verify(no_io)?;

    let mut no_memory = budget;
    no_memory.allocation_operations = 0;
    let failure = source
        .lookup(source.reference(), &path, no_memory, &token)
        .await
        .err()
        .ok_or("path clone exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    failure.work.verify(no_memory)?;

    let mut no_output = budget;
    no_output.output_bytes = 0;
    let failure = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            no_output,
            &token,
        )
        .await
        .err()
        .ok_or("range output allowance was ignored")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 0);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(no_output)?;

    token.cancel();
    let lookup = source
        .lookup(source.reference(), &path, budget, &token)
        .await
        .err()
        .ok_or("cancelled lookup succeeded")?;
    let read = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await
        .err()
        .ok_or("cancelled range succeeded")?;
    let opened = source
        .open_file(source.reference(), &path, node.version, budget, &token)
        .await
        .err()
        .ok_or("cancelled open succeeded")?;
    let page = source
        .list_page(source.reference(), &requested("")?, None, 1, budget, &token)
        .await
        .err()
        .ok_or("cancelled page succeeded")?;
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
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let path = requested("file")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source
        .lookup(source.reference(), &path, budget, &token)
        .await?
        .value
        .ok_or("file absent")?;
    let baseline = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await?;
    assert_eq!(baseline.value.bytes.as_ref(), b"native");
    baseline.work.verify(budget)?;
    let mut limited = budget;
    limited.backend_read_operations = baseline
        .work
        .backend_read_operations
        .checked_sub(1)
        .ok_or("no native IO")?;
    let failure = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            limited,
            &token,
        )
        .await
        .err()
        .ok_or("current-proof IO exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 6);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(limited)?;

    let file = source
        .open_file(source.reference(), &path, node.version, budget, &token)
        .await?
        .value;
    let baseline = {
        let allowance = budget;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = (&*file).read_range(0, 6, &mut accounting);
        allowance.finish(result, &accounting)
    }?;
    limited.backend_read_operations = baseline
        .work
        .backend_read_operations
        .checked_sub(1)
        .ok_or("no held native IO")?;
    let failure = {
        let allowance = limited;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = (&*file).read_range(0, 6, &mut accounting);
        allowance.finish(result, &accounting)
    }
    .err()
    .ok_or("held file ignored native IO limit")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 6);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(limited)?;
    Ok(())
}

#[tokio::test]
async fn native_continuation_replays_consumed_entries_after_budget_denial() -> TestResult {
    let directory = tempfile::tempdir()?;
    for index in 0..8 {
        std::fs::write(directory.path().join(format!("entry-{index}")), b"body")?;
    }
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let path = requested("")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let mut first = source
        .list_page(source.reference(), &path, None, 1, budget, &token)
        .await?;
    {
        let allowance = first.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match first.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    let cursor = first.value.next.ok_or("expected continuation")?;
    let mut limited = budget;
    limited.source_entries_visited = 0;
    let failure = source
        .list_page(
            source.reference(),
            &path,
            Some(cursor.clone()),
            2,
            limited,
            &token,
        )
        .await
        .err()
        .ok_or("cursor advanced beyond its source-item allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_entries_visited, 0);
    failure.work.verify(limited)?;
    let mut names = first
        .value
        .entries
        .iter()
        .map(|entry| entry.name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let mut page = source
            .list_page(source.reference(), &path, Some(cursor), 2, budget, &token)
            .await?;
        page.work.verify(budget)?;
        {
            let allowance = page.work.remaining(budget)?;
            let cancellation = &token;
            let mut accounting = allowance.accounting(cancellation);
            let result = match page.value.publication.as_deref_mut() {
                Some(publication) => publication.accept(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        names.extend(
            page.value
                .entries
                .iter()
                .map(|entry| entry.name.as_bytes().to_vec()),
        );
        next = page.value.next;
    }
    names.sort();
    let expected = (0..8)
        .map(|index| format!("entry-{index}").into_bytes())
        .collect::<Vec<_>>();
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
    let bytes = units
        .into_iter()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let name = LogicalName::new(
        NameEncoding::WindowsUtf16Le,
        bytes,
        limits.maximum_component_bytes,
    )?;
    let path = NamespacePath::new(vec![name.clone()], limits)?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Windows,
        limits,
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let node = source
        .lookup(source.reference(), &path, budget, &token)
        .await?
        .value
        .ok_or("opaque file absent")?;
    let mut page = source
        .list_page(source.reference(), &requested("")?, None, 8, budget, &token)
        .await?;
    {
        let allowance = page.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match page.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    assert_eq!(page.value.entries.len(), 1);
    assert_eq!(page.value.entries[0].name, name);
    page.work.verify(budget)?;
    let read = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"opaque");
    read.work.verify(budget)?;
    let mut limited = budget;
    limited.peak_allocation_bytes = read
        .work
        .peak_allocation_bytes
        .checked_sub(1)
        .ok_or("no native live buffer")?;
    let failure = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            limited,
            &token,
        )
        .await
        .err()
        .ok_or("native UTF16 path/read allocation exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    failure.work.verify(limited)?;
    let reopened = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Windows,
        limits,
        Some(source.reference()),
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let read = reopened
        .read_range(
            reopened.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"opaque");
    read.work.verify(budget)?;
    Ok(())
}

#[tokio::test]
async fn filtered_page_reserves_comparisons_before_committing_native_cursor() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c"] {
        std::fs::write(directory.path().join(name), b"actual")?;
    }
    let native = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let source = native
        .exclude(
            vec![requested("private")?, requested("excluded")?],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )?
        .value;
    let root = requested("")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let mut first = source
        .list_page(source.reference(), &root, None, 1, budget, &token)
        .await?;
    {
        let allowance = first.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match first.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    let cursor = first.value.next.ok_or("native continuation absent")?;
    let mut denied = budget;
    denied.items_examined = 2;
    let failure = source
        .list_page(
            source.reference(),
            &root,
            Some(cursor.clone()),
            8,
            denied,
            &token,
        )
        .await
        .err()
        .ok_or("filter advanced native cursor without projection allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.source_entries_visited, 0);
    assert_eq!(failure.work.allocation_operations, 0);
    failure.work.verify(denied)?;
    let mut next = source
        .list_page(source.reference(), &root, Some(cursor), 8, budget, &token)
        .await?;
    {
        let allowance = next.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match next.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    next.work.verify(budget)?;
    assert!(next.value.next.is_none());
    let mut names = first
        .value
        .entries
        .iter()
        .chain(next.value.entries.iter())
        .map(|entry| entry.name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[tokio::test]
async fn native_unpublished_pages_restore_initial_and_continuation_positions() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c", "d"] {
        std::fs::write(directory.path().join(name), b"body")?;
    }
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let root = requested("")?;
    let token = CancellationToken::new();
    let budget = finite_budget()?;
    let mut position = None;
    for _ in 0..2 {
        let page = source
            .list_page(
                source.reference(),
                &root,
                position.clone(),
                1,
                budget,
                &token,
            )
            .await?;
        page.work.verify(budget)?;
        assert_eq!(page.work.source_bytes_read, 0);
        assert!(page.value.retained_bytes() > 0);
        let expected = page
            .value
            .entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        drop(page);
        let mut retry = source
            .list_page(
                source.reference(),
                &root,
                position.clone(),
                1,
                budget,
                &token,
            )
            .await?;
        assert_eq!(
            retry
                .value
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let aborted = {
            let allowance = retry.work.remaining(budget)?;
            let cancellation = &CancellationToken::new();
            let mut accounting = allowance.accounting(cancellation);
            let result = match retry.value.publication.as_deref_mut() {
                Some(publication) => publication.abort(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        aborted.work.verify(retry.work.remaining(budget)?)?;
        retry.work.checked_add(aborted.work)?.verify(budget)?;
        let mut accepted = source
            .list_page(
                source.reference(),
                &root,
                position.clone(),
                1,
                budget,
                &token,
            )
            .await?;
        assert_eq!(
            accepted
                .value
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let committed = {
            let allowance = accepted.work.remaining(budget)?;
            let cancellation = &token;
            let mut accounting = allowance.accounting(cancellation);
            let result = match accepted.value.publication.as_deref_mut() {
                Some(publication) => publication.accept(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        accepted.work.checked_add(committed.work)?.verify(budget)?;
        position = accepted.value.next;
        assert!(position.is_some());
    }
    Ok(())
}

#[tokio::test]
async fn native_late_publication_cancellation_and_cleanup_denial_preserve_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c"] {
        std::fs::write(directory.path().join(name), b"actual")?;
    }
    let source = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let root = requested("")?;
    let live = CancellationToken::new();
    let budget = finite_budget()?;
    for cancelled in [false, true] {
        let mut page = source
            .list_page(source.reference(), &root, None, 2, budget, &live)
            .await?;
        assert!(page.work.backend_read_operations > 0);
        assert!(page.work.items_examined > 0);
        assert!(page.work.allocation_operations > 0);
        page.work.verify(budget)?;
        let expected = page
            .value
            .entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        let projected = CancellationToken::new();
        let mut remaining = page.work.remaining(budget)?;
        if cancelled {
            projected.cancel();
        } else {
            remaining.items_examined = 0;
        }
        let failure = {
            let allowance = remaining;
            let cancellation = &projected;
            let mut accounting = allowance.accounting(cancellation);
            let result = match page.value.publication.as_deref_mut() {
                Some(publication) => publication.accept(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }
        .err()
        .ok_or("unadmitted publication committed native cursor")?;
        if cancelled {
            assert!(matches!(failure.error, DemandError::Cancelled));
        } else {
            assert!(matches!(failure.error, DemandError::Work(_)));
        }
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        failure.work.verify(remaining)?;
        // The original source receipt remains observable after late projection failure.
        assert!(page.work.backend_read_operations > 0);
        let mut cleanup_budget = page.work.remaining(budget)?;
        cleanup_budget.items_examined = 1;
        let aborted = {
            let allowance = cleanup_budget;
            let cancellation = &CancellationToken::new();
            let mut accounting = allowance.accounting(cancellation);
            let result = match page.value.publication.as_deref_mut() {
                Some(publication) => publication.abort(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        aborted.work.verify(cleanup_budget)?;
        let retained = page
            .work
            .checked_add(*failure.work)?
            .checked_add(aborted.work)?;
        retained.verify(budget)?;
        let mut retry = source
            .list_page(source.reference(), &root, None, 2, budget, &live)
            .await?;
        assert_eq!(
            retry
                .value
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        {
            let allowance = retry.work.remaining(budget)?;
            let cancellation = &CancellationToken::new();
            let mut accounting = allowance.accounting(cancellation);
            let result = match retry.value.publication.as_deref_mut() {
                Some(publication) => publication.abort(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
    }
    let mut committed = source
        .list_page(source.reference(), &root, None, 2, budget, &live)
        .await?;
    let mut cleanup_budget = committed.work.remaining(budget)?;
    cleanup_budget.items_examined = 1;
    let accepted = {
        let allowance = cleanup_budget;
        let cancellation = &live;
        let mut accounting = allowance.accounting(cancellation);
        let result = match committed.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    accepted.work.verify(cleanup_budget)?;
    committed.work.checked_add(accepted.work)?.verify(budget)?;
    Ok(())
}
