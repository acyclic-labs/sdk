use super::*;

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
    let budget = FINITE_BUDGET;
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
    let store = MemoryLazyWorkspaceStore::default();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = OperationId::new();
    let created = {
        let mut accounting = budget.accounting(&cancellation);
        let result = LazyWorkspace::attach(
            &fs,
            "epoch-attachment",
            source.clone(),
            store.clone(),
            acyclic_fs::VolumeInitialization::new(config, None),
            operation,
            &mut accounting,
        )
        .await;
        budget.finish(result, &accounting)
    }?;
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
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
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
    let recovered = {
        let mut accounting = budget.accounting(&cancellation);
        let result = LazyWorkspace::attach(
            &fs,
            "epoch-attachment",
            reopened_source,
            store.clone(),
            acyclic_fs::VolumeInitialization::new(config, None),
            operation,
            &mut accounting,
        )
        .await;
        budget.finish(result, &accounting)
    }?;
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
    let rejected = {
        let mut accounting = budget.accounting(&cancellation);
        let result = LazyWorkspace::attach(
            &fs,
            "epoch-attachment",
            foreign_source,
            store.clone(),
            acyclic_fs::VolumeInitialization::new(config, None),
            operation,
            &mut accounting,
        )
        .await;
        budget.finish(result, &accounting)
    }
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
