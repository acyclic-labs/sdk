use super::*;

#[derive(Clone)]
pub(super) struct FaultingCompanion {
    inner: MemoryLazyWorkspaceStore,
    fault: Arc<AtomicUsize>,
}

impl FaultingCompanion {
    pub(super) fn new(fault: usize) -> Self {
        Self {
            inner: MemoryLazyWorkspaceStore::default(),
            fault: Arc::new(AtomicUsize::new(fault)),
        }
    }

    fn finish<T>(&self, stage: usize, value: T) -> Result<T, LazyWorkspaceError> {
        if self
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
    let budget = FINITE_BUDGET;
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
                acyclic_fs::VolumeInitialization {
                    config,
                    access_policy: None,
                },
                operation,
                budget,
                &CancellationToken::new(),
                None,
            )
            .await?;
        let fs = Fs::memory();
        let store = FaultingCompanion::new(stage);
        let failure = {
            let cancellation = &CancellationToken::new();
            let mut accounting = budget.accounting(cancellation);
            let result = LazyWorkspace::attach(
                &fs,
                "companion-fault",
                source.clone(),
                store.clone(),
                acyclic_fs::VolumeInitialization {
                    config: config,
                    access_policy: None,
                },
                operation,
                &mut accounting,
            )
            .await;
            budget.finish(result, &accounting)
        }
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
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
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
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?
            .id();
        assert_ne!(head, initial.id());
        let recovered = {
            let cancellation = &CancellationToken::new();
            let mut accounting = budget.accounting(cancellation);
            let result = LazyWorkspace::attach(
                &fs,
                "companion-fault",
                source.clone(),
                store.clone(),
                acyclic_fs::VolumeInitialization {
                    config: config,
                    access_policy: None,
                },
                operation,
                &mut accounting,
            )
            .await;
            budget.finish(result, &accounting)
        }?;
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
