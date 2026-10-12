use super::*;
use std::path::Path;

#[tokio::test]
async fn issued_mixed_working_view_retains_actual_lazy_project_and_materialized_scratch()
-> Result<()> {
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "mixed-issued-view",
        true,
        Some(&work),
    )
    .await?;
    let manifest = fixture.view.manifest();
    assert!(matches!(
        manifest.volumes[0].namespace,
        NativeVolumeNamespace::LazyMount { .. }
    ));
    assert_eq!(manifest.volumes[0].volume.class(), VolumeClass::Project);
    assert!(matches!(
        manifest.volumes[1].namespace,
        NativeVolumeNamespace::MaterializedDirectory
    ));
    let issued = fixture
        .view
        .task_working_view_with_native_work(&fixture.owner, &work)?
        .await?;
    let project = issued
        .read_builtin_file(Path::new("src/cold.txt"), 64, &work)?
        .await?;
    assert_eq!(project.as_slice(), b"genuine cold project");
    assert!(project.matches_meter(&fixture.lazy_work.as_ref().unwrap().runtime_meter()));
    drop(project);
    issued
        .write_builtin_file(Path::new("dst/issued.txt"), "scratch", false, None, &work)?
        .await?;
    assert_eq!(
        issued
            .read_builtin_file(Path::new("dst/issued.txt"), 16, &work)?
            .await?
            .as_slice(),
        b"scratch"
    );
    let writes = work.receipt().backend_write_operations;
    for path in ["src/denied.txt", "results/denied.txt"] {
        assert!(
            issued
                .write_builtin_file(Path::new(path), "denied", true, None, &work)?
                .await
                .is_err()
        );
        assert!(!manifest.options.root.join(path).exists());
    }
    assert_eq!(work.receipt().backend_write_operations, writes);
    let foreign = test_work(
        acyclic_fs::performance::work_budget(100_000_000, 100_000_000),
        &cancellation,
    )?;
    assert!(
        issued
            .read_builtin_file(Path::new("src/cold.txt"), 64, &foreign)
            .is_err()
    );
    assert_eq!(foreign.receipt().source_bytes_read, 0);
    fixture
        .lazy_mount
        .as_ref()
        .unwrap()
        .retire(fixture.lazy_work.as_ref().unwrap().cancellation())?;
    Ok(())
}

#[tokio::test]
async fn genuine_view_retirement_preserves_cached_proof_on_admission_and_wait_denial() -> Result<()>
{
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000);
    let original = test_work(budget, &cancellation)?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&original),
    )
    .await?;
    let process = fixture.provider(&original).await?;
    let result = fixture
        .effects(process.clone())?
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, &original)
        .await?;
    assert!(matches!(result.0, EffectStatus::Succeeded { .. }));
    let wait = acyclic_fs::CancellationToken::new();
    let cleanup = test_work(budget, &wait)?;
    let mut cached = Some(
        process
            .drain_with_native_work(process.original_attempt(), &fixture.view, &cleanup, &wait)?
            .await?,
    );
    let mut partial = None;
    let mut denied = budget;
    denied.allocation_operations = 1;
    let denied = test_work(denied, &wait)?;
    assert!(
        fixture
            .view
            .cleanup_mounts_with_native_work(
                &fixture.owner,
                &mut cached,
                &mut partial,
                &original,
                &denied,
                &wait
            )
            .is_err()
    );
    assert!(cached.is_some() && partial.is_none());
    let refused = acyclic_fs::CancellationToken::new();
    refused.cancel();
    assert!(
        fixture
            .view
            .cleanup_mounts_with_native_work(
                &fixture.owner,
                &mut cached,
                &mut partial,
                &original,
                &cleanup,
                &refused
            )?
            .await
            .is_err()
    );
    assert!(cached.is_some() && partial.is_none());
    cancellation.cancel();
    let receipt = fixture
        .view
        .cleanup_mounts_with_native_work(
            &fixture.owner,
            &mut cached,
            &mut partial,
            &original,
            &cleanup,
            &wait,
        )?
        .await?;
    assert!(cached.is_none() && partial.is_none());
    receipt
        .process
        .require_view_binding(&fixture.view, &fixture.owner, &original)?;
    Ok(())
}

#[tokio::test]
async fn issued_working_view_denies_legacy_foreign_work_full_fence_and_stale_owner() -> Result<()> {
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(100_000_000, 100_000_000);
    let work = test_work(budget, &cancellation)?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&work),
    )
    .await?;
    let issued = fixture
        .view
        .task_working_view_with_native_work(&fixture.owner, &work)?
        .await?;
    let foreign_work = test_work(budget, &cancellation)?;
    assert!(issued.verify_with_native_work(&foreign_work).is_err());
    let foreign = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&work),
    )
    .await?;
    assert_ne!(
        fixture.owner.task_binding().1,
        foreign.owner.task_binding().1
    );
    assert!(
        fixture
            .view
            .task_working_view_with_native_work(&foreign.owner, &work)?
            .await
            .is_err()
    );
    let context = fixture
        .task
        .task_context_with_native_work(
            fixture.owner.task_binding().0,
            fixture.owner.task_binding().1,
            &work,
        )
        .await?;
    let guard = context
        .volume_callback_guard_with_native_work(&fixture.owner, &work)
        .await?;
    let callback = guard
        .for_volume(&fixture.owner, &fixture.source, false, &work)?
        .0;
    let writable = guard
        .for_volume(&fixture.owner, &fixture.destination, true, &work)?
        .0;
    for callback in [&callback, &writable] {
        assert!(callback.check(&work).unwrap().await.is_ok());
    }
    assert!(callback.check(&foreign_work).unwrap().await.is_err());
    let ungranted = VolumeRef::new(
        fixture.source.provider().clone(),
        "ungranted".into(),
        VolumeClass::AgentPrivate,
        fixture.source.owner().clone(),
        &mut None,
    )?;
    for (owner, volume, writable, ledger) in [
        (&fixture.owner, &fixture.source, true, &work),
        (&foreign.owner, &fixture.source, false, &work),
        (&fixture.owner, &fixture.source, false, &foreign_work),
        (&fixture.owner, &ungranted, false, &work),
    ] {
        assert!(guard.for_volume(owner, volume, writable, ledger).is_err());
    }
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0, None)
        .await?;
    assert!(issued.verify_with_native_work(&work)?.await.is_err());
    assert!(
        issued
            .write_builtin_file(Path::new("dst/stale.txt"), "denied", true, None, &work)?
            .await
            .is_err()
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/stale.txt")
            .exists()
    );
    let pending = callback.check(&work).unwrap();
    cancellation.cancel();
    let error = pending.await.unwrap_err();
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(source) = cause {
        if matches!(
            source.downcast_ref::<Error>(),
            Some(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ) {
            return Ok(());
        }
        cause = source.source();
    }
    panic!("original callback cancellation");
}

#[tokio::test]
async fn publication_races_preserve_output_and_do_not_claim_joint_volume_success() -> Result<()> {
    for mode in ["pause-write", "partial-publication"] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let process = fixture.provider(fixture.view.work()).await?;
        let effects = Arc::new(fixture.effects(process.clone())?);
        let running = {
            let effects = effects.clone();
            let owner = fixture.owner.clone();
            let command = fixture.command;
            let plan = fixture.plan.clone();
            tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await })
        };
        let root = &fixture.view.manifest().options.root;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join("publication-ready").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        let changed = if mode == "pause-write" {
            &fixture.destination
        } else {
            &fixture.second_destination
        };
        let verifier = fixture.issuer.verifier(&mut None)?;
        let grant = ContentGrant::verify(
            &verifier,
            &fixture.scope,
            changed,
            VolumeOperation::Write,
            128,
            &mut None,
        )?;
        fixture
            .files
            .put_content(
                changed,
                &grant,
                "concurrent.txt",
                b"concurrent writer",
                "text/plain",
                "concurrent.txt",
                128,
                &IdempotencyKey::new("concurrent-destination")?,
                None,
                None,
            )
            .await?;
        std::fs::write(root.join("publication-release"), b"release")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let error = running
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .unwrap_err();
        let Error::NativePublication { failure, .. } = &error else {
            panic!("typed publication failure")
        };
        assert!(matches!(
            &failure.cause,
            Error::NativeView {
                cause: acyclic_fs::MountLifecycleError::Source(acyclic_fs::MountSourceError::Stale),
                ..
            }
        ));
        let diagnostic = failure.diagnostic.as_ref().unwrap();
        let output: NativeProcessResult = crate::contract::json_from_slice(
            &fixture
                .content
                .read(diagnostic, diagnostic.descriptor().byte_length(), None)?
                .await?,
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
        assert!(output.success && output.stop.is_none());
        assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
        for (volume, path, readable) in [
            (
                &fixture.destination,
                "/output.txt",
                mode == "partial-publication",
            ),
            (changed, "/output.txt", false),
        ] {
            let workspace =
                workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
            assert_eq!(
                fixture.files.read(&workspace, None, path, 64).await.is_ok(),
                readable
            );
        }
        let workspace = workspace_ref(
            changed.provider().clone(),
            &changed.storage_name(&mut None)?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&workspace, None, "/concurrent.txt", 64)
                .await?
                .as_ref(),
            b"concurrent writer"
        );
        let attempt = process.original_attempt();
        let receipt = process
            .receipt(attempt, fixture.view.work(), None)
            .await?
            .unwrap();
        assert!(!receipt.observed && receipt.published.is_empty());
        std::fs::remove_file(root.join("dst/output.txt"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            EffectStatus::Indeterminate
        );
        assert!(!root.join("dst/output.txt").exists());
        drop(effects);
        drop(process);
        let recovered = Arc::new(
            Process::recover(
                fixture.owner.clone(),
                fixture.command,
                &fixture.plan,
                fixture.approval(),
                fixture.authority(),
                fixture.view.work(),
            )
            .await?,
        );
        assert_eq!(
            recovered
                .receipt(attempt, fixture.view.work(), None)
                .await?
                .as_ref(),
            Some(&receipt)
        );
        assert_eq!(
            fixture
                .effects(recovered)?
                .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
                .await?,
            EffectStatus::Indeterminate
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(
            fixture
                .task
                .task_host()
                .outcome(fixture.owner.task_binding().0)
                .await?,
            None
        );
        fixture.owner.verify(false, &mut None).await?;
    }
    Ok(())
}
