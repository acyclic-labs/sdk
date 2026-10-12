use super::*;

#[tokio::test]
async fn cold_native_snapshot_and_drain_never_relaunch() -> Result<()> {
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000);
    let original = test_work(budget, &cancellation)?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "mixed-issued-view",
        true,
        Some(&original),
    )
    .await?;
    let process = fixture.provider(&original).await?;
    let attempt = process.original_attempt();
    let query_cancel = acyclic_fs::CancellationToken::new();
    let query = test_work(budget, &query_cancel)?;
    let wrong = crate::EffectAttemptId::from_bytes([255; 16]);
    assert!(matches!(
        process.drain_with_native_work(wrong, &fixture.view, &query, &query_cancel),
        Err(Error::NativeBoundary {
            code: ErrorCode::Conflict,
            ..
        })
    ));
    assert!(matches!(
        process.published_project_candidate_with_native_work(wrong, &fixture.source, &query),
        Err(Error::NativeBoundary {
            code: ErrorCode::Conflict,
            ..
        })
    ));
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        &query,
    )
    .await?;
    assert!(matches!(
        recovered
            .drain_with_native_work(attempt, &fixture.view, &query, &query_cancel)?
            .await,
        Err(Error::Indeterminate(_))
    ));
    let outcome = fixture
        .effects(process.clone())?
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, &original)
        .await?;
    assert!(matches!(outcome.0, EffectStatus::Succeeded { .. }));
    let receipt = process.receipt(attempt, &original, None).await?.unwrap();
    let index = fixture
        .view
        .manifest()
        .volumes
        .iter()
        .position(|mapping| mapping.volume == fixture.source)
        .unwrap();
    assert!(!fixture.view.manifest().volumes[index].writable);
    let accepted = receipt
        .published
        .iter()
        .find(|published| published.index == index)
        .unwrap();
    assert!(accepted.snapshot.operation().is_none());
    let expected = accepted
        .snapshot
        .snapshot()
        .lazy_snapshot_with_native_work(&original, &cancellation)?
        .unwrap();
    let output = fixture.view.manifest().options.root.join("dst/output.txt");
    std::fs::remove_file(&output).map_err(|e| Error::Storage(e.to_string()))?;
    drop(process);
    cancellation.cancel();
    let original_prefix = original.receipt();
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        &query,
    )
    .await?;
    let proof = recovered
        .drain_with_native_work(attempt, &fixture.view, &query, &query_cancel)?
        .await?;
    proof.require_view_binding(&fixture.view, &fixture.owner, &original)?;
    proof.require_command_binding(fixture.command)?;
    assert!(
        proof
            .require_command_binding(OperationId::from_bytes([63; 16]))
            .is_err()
    );
    let live = query.live_allocation_bytes();
    drop(proof);
    assert!(query.live_allocation_bytes() < live);
    let (candidate, custody) = recovered
        .published_project_candidate_with_native_work(attempt, &fixture.source, &query)?
        .await?;
    assert_eq!(candidate, expected);
    assert!(custody.work().shares_ledger(&query));
    let inspection = fixture
        .inspect_project_candidate_with_native_work(
            candidate,
            OperationId::from_bytes([219; 16]),
            &query,
        )
        .await?;
    let selected = recovered
        .inspect_published_project_snapshot_with_native_work(
            attempt,
            &inspection,
            &fixture.source,
            candidate,
            &query,
        )?
        .await?;
    assert_eq!(selected.0, expected);
    assert!(selected.1.work().shares_ledger(&query));
    assert_eq!(
        recovered
            .receipt(attempt, &query, None)
            .await?
            .unwrap()
            .published,
        receipt.published
    );
    assert_eq!(original.receipt(), original_prefix);
    assert_eq!(query.receipt().backend_write_operations, 0);
    assert_eq!(query.receipt().materializations, 0);
    assert!(!output.exists());
    Ok(())
}

#[tokio::test]
async fn original_supervisor_stop_and_cleanup_preserve_the_attempt() -> Result<()> {
    for stop in ["drain", "signal", "cancelled-task"] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000);
        let original = test_work(budget, &cancellation)?;
        let fixture = process_fixture(
            Arc::new(MemoryStream::default()),
            "wait",
            true,
            Some(&original),
        )
        .await?;
        let process = fixture.provider(&original).await?;
        let effects = fixture.effects(process.clone())?;
        let owner = fixture.owner.clone();
        let command = fixture.command;
        let plan = fixture.plan.clone();
        let work = original.clone();
        let dispatch = tokio::spawn(async move {
            effects
                .run_task_effect_with_native_work(&owner, command, &plan, &work)
                .await
        });
        let root = &fixture.view.manifest().options.root;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join("ready").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        let wait = acyclic_fs::CancellationToken::new();
        let cleanup = test_work(budget, &wait)?;
        let attempt = process.original_attempt();
        let recovered = Process::recover(
            fixture.owner.clone(),
            fixture.command,
            &fixture.plan,
            fixture.approval(),
            fixture.authority(),
            &cleanup,
        )
        .await?;
        assert!(
            recovered
                .signal_stop(attempt, &cleanup, &wait)
                .await
                .is_err()
        );
        assert!(
            process
                .signal_stop(
                    crate::EffectAttemptId::from_bytes([255; 16]),
                    &cleanup,
                    &wait
                )
                .await
                .is_err()
        );
        let mut denied = budget;
        denied.items_examined = 0;
        let denied = test_work(denied, &wait)?;
        assert!(matches!(
            process.signal_stop(attempt, &denied, &wait).await,
            Err(Error::NativeWork { .. })
        ));
        assert!(!dispatch.is_finished());
        if stop == "drain" {
            assert!(matches!(
                recovered
                    .drain_with_native_work(attempt, &fixture.view, &cleanup, &wait)?
                    .await,
                Err(Error::Indeterminate(_))
            ));
            cancellation.cancel();
            let proof = process
                .drain_with_native_work(attempt, &fixture.view, &cleanup, &wait)?
                .await?;
            proof.require_view_binding(&fixture.view, &fixture.owner, &original)?;
            assert!(fixture.view.work().cancellation().is_cancelled());
            assert_eq!(cleanup.receipt().materializations, 0);
        } else {
            if stop == "cancelled-task" {
                fixture
                    .task
                    .task_host()
                    .cancel(fixture.owner.task_binding().0, None)
                    .await?;
            }
            process.signal_stop(attempt, &cleanup, &wait).await?;
            let receipt = process.request_stop(attempt, &cleanup, &wait).await?;
            assert!(receipt.observed);
            let (captured, custody) = process.output(attempt, &cleanup).await?.unwrap();
            let bytes = fixture
                .content
                .read(&captured, captured.descriptor().byte_length(), None)?
                .await?;
            let output: NativeProcessResult = crate::contract::json_from_slice(&bytes)
                .map_err(|e| Error::Invalid(e.to_string()))?;
            assert!(!output.success);
            assert!(
                matches!(output.stop.as_ref(), Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed)
            );
            drop(custody);
            assert_eq!(
                recovered.receipt(attempt, &cleanup, None).await?.as_ref(),
                Some(&receipt)
            );
            assert!(!fixture.view.work().cancellation().is_cancelled());
            assert_eq!(
                fixture.owner.verify(false, &mut None).await.is_ok(),
                stop == "signal"
            );
            fixture.owner.verify(true, &mut None).await?;
            if stop == "signal" {
                fixture
                    .view
                    .validate_for_dispatch(fixture.view.manifest().validation_budget()?)
                    .await
                    .map_err(|(cause, _)| cause)?;
            }
            let workspace = workspace_ref(
                fixture.destination.provider().clone(),
                &fixture.destination.storage_name(&mut None)?,
            )?;
            assert!(
                fixture
                    .files
                    .read(&workspace, None, "/output.txt", 64)
                    .await
                    .is_err()
            );
        }
        let _completion = dispatch.await.map_err(|e| Error::Storage(e.to_string()))?;
        tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
        assert!(!root.join("escaped").exists());
    }
    Ok(())
}

#[tokio::test]
async fn original_native_cancel_at_observed_commit_keeps_launch_uncertain_without_new_allowance()
-> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000);
    let work = test_work(budget, &cancellation)?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = process_fixture(stream.clone(), "write", true, Some(&work)).await?;
    let process = fixture.provider(&work).await?;
    *stream.cancel_observed.lock().await = Some(cancellation.clone());
    assert!(
        fixture
            .effects(process.clone())?
            .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, &work)
            .await
            .is_err()
    );
    assert!(fixture.view.work().cancellation().is_cancelled());
    let attempt = process.original_attempt();
    let before = work.receipt();
    assert!(process.reconcile_with_native_work(attempt, &work).is_err());
    assert_eq!(work.receipt(), before);
    let wait = acyclic_fs::CancellationToken::new();
    let query = test_work(budget, &wait)?;
    let receipt = process.receipt(attempt, &query, None).await?.unwrap();
    assert!(!receipt.observed);
    assert_eq!(receipt.effect.status, EffectStatus::Indeterminate);
    assert_eq!(query.receipt().backend_write_operations, 0);
    assert_eq!(query.receipt().materializations, 0);
    let output = fixture.view.manifest().options.root.join("dst/output.txt");
    assert!(output.exists());
    std::fs::remove_file(&output).map_err(|e| Error::Storage(e.to_string()))?;
    assert_eq!(
        process
            .receipt(attempt, &query, None)
            .await?
            .unwrap()
            .effect,
        receipt.effect
    );
    assert!(!output.exists());
    Ok(())
}

#[tokio::test]
async fn receipt_commit_faults_never_replay_an_unknown_native_attempt() -> Result<()> {
    for (kind, lost_reply) in [
        ("launch", false),
        ("launch", true),
        ("observed", false),
        ("observed", true),
    ] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = test_work(
            acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
            &cancellation,
        )?;
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = process_fixture(stream.clone(), "write", true, Some(&work)).await?;
        let process = fixture.provider(&work).await?;
        let effects = fixture.effects(process.clone())?;
        *stream.fault.lock().await = Some((kind, lost_reply));
        assert!(
            effects
                .run_task_effect_with_native_work(
                    &fixture.owner,
                    fixture.command,
                    &fixture.plan,
                    &work
                )
                .await
                .is_err()
        );
        assert!(stream.fault.lock().await.is_none());
        let attempt = process.original_attempt();
        let receipt = process.receipt(attempt, &work, None).await?;
        assert_eq!(receipt.is_none(), kind == "launch" && !lost_reply);
        drop(effects);
        drop(process);
        let recovered = Arc::new(
            Process::recover(
                fixture.owner.clone(),
                fixture.command,
                &fixture.plan,
                fixture.approval(),
                fixture.authority(),
                &work,
            )
            .await?,
        );
        let effects = fixture.effects(recovered.clone())?;
        let status = effects
            .reconcile_task_effect_with_native_work(
                &fixture.owner,
                fixture.command,
                &fixture.plan,
                &work,
            )
            .await?;
        assert_eq!(
            matches!(status.0, EffectStatus::Succeeded { .. }),
            kind == "observed" && lost_reply
        );
        if !(kind == "observed" && lost_reply) {
            assert_eq!(status.0, EffectStatus::Indeterminate);
        }
        assert_eq!(recovered.receipt(attempt, &work, None).await?, receipt);
        let root = &fixture.view.manifest().options.root;
        assert_eq!(root.join("dst/output.txt").exists(), kind == "observed");
        if kind == "observed" {
            std::fs::remove_file(root.join("dst/output.txt"))
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        assert_eq!(
            effects
                .run_task_effect_with_native_work(
                    &fixture.owner,
                    fixture.command,
                    &fixture.plan,
                    &work
                )
                .await?
                .0,
            status.0
        );
        assert!(!root.join("dst/output.txt").exists());
        let workspace = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name(&mut None)?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&workspace, None, "/output.txt", 64)
                .await
                .is_ok(),
            kind == "observed"
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_precapture_identity_failure_retains_work_and_never_replays() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let work = fixture.view.work();
    let process = fixture.provider(work).await?;
    let root = &fixture.view.manifest().options.root;
    let original = root.with_file_name("original-native-root");
    std::fs::rename(root, &original).map_err(|e| Error::Storage(e.to_string()))?;
    std::fs::create_dir(root).map_err(|e| Error::Storage(e.to_string()))?;
    std::fs::write(root.join("user.txt"), b"external replacement")
        .map_err(|e| Error::Storage(e.to_string()))?;
    let status = fixture
        .effects(process.clone())?
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
        .await?;
    assert_eq!(status.0, EffectStatus::Indeterminate);
    let attempt = process.original_attempt();
    let receipt = process.receipt(attempt, work, None).await?.unwrap();
    assert!(receipt.work.backend_read_operations > 0);
    assert!(
        receipt
            .invocation_failure
            .as_ref()
            .is_some_and(|reason| !reason.is_empty())
    );
    assert!(receipt.diagnostic.is_none() && receipt.publication_failure.is_none());
    assert!(!original.join("dst/output.txt").exists());
    assert_eq!(
        std::fs::read(root.join("user.txt")).map_err(|e| Error::Storage(e.to_string()))?,
        b"external replacement"
    );
    drop(process);
    std::fs::remove_file(root.join("user.txt")).map_err(|e| Error::Storage(e.to_string()))?;
    std::fs::remove_dir(root).map_err(|e| Error::Storage(e.to_string()))?;
    std::fs::rename(&original, root).map_err(|e| Error::Storage(e.to_string()))?;
    let recovered = Arc::new(
        Process::recover(
            fixture.owner.clone(),
            fixture.command,
            &fixture.plan,
            fixture.approval(),
            fixture.authority(),
            work,
        )
        .await?,
    );
    assert_eq!(recovered.receipt(attempt, work, None).await?, Some(receipt));
    assert_eq!(
        fixture
            .effects(recovered)?
            .reconcile_task_effect_with_native_work(
                &fixture.owner,
                fixture.command,
                &fixture.plan,
                work
            )
            .await?
            .0,
        EffectStatus::Indeterminate
    );
    assert!(!root.join("dst/output.txt").exists());
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn receipt_reconciliation_reopens_all_local_stores_after_provider_objects_drop() -> Result<()>
{
    let storage = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("filesystem"),
    ))
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
    let auxiliary = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("auxiliary"),
    ))
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
    let local = Arc::new(
        acyclic_stream::LocalStream::open(storage.path().join("coordinator"), Default::default())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?,
    );
    let weak_stream = Arc::downgrade(&local);
    let fixture = process_fixture_with_filesystems(
        local,
        tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?,
        filesystem,
        auxiliary,
        "write",
        true,
        None,
    )
    .await?;
    let seed = NativeRestart::from_fixture(&fixture);
    let auxiliary = fixture.view.manifest().volumes[2].clone();
    let weak_files = Arc::downgrade(&fixture.files);
    let process = fixture.provider(fixture.view.work()).await?;
    let weak_process = Arc::downgrade(&process);
    let status = fixture
        .effects(process.clone())?
        .run_task_effect_with_native_work(
            &fixture.owner,
            seed.command,
            &seed.plan,
            fixture.view.work(),
        )
        .await?;
    assert!(matches!(status.0, EffectStatus::Succeeded { .. }));
    drop(process);
    assert!(weak_process.upgrade().is_none());
    let ProcessFixture {
        directory,
        task,
        owner,
        files,
        content,
        results,
        approvals,
        view,
        ..
    } = fixture;
    drop((task, owner, approvals, content, results, view, files));
    assert!(weak_stream.upgrade().is_none() && weak_files.upgrade().is_none());
    let wait = acyclic_fs::CancellationToken::new();
    let query = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &wait,
    )?;
    let LocalNativeRecovery {
        files,
        effects,
        owner,
        process,
        ..
    } = reopen_native(storage.path(), &seed, &query).await?;
    assert_eq!(
        effects
            .reconcile_task_effect_with_native_work(&owner, seed.command, &seed.plan, &query)
            .await?
            .0,
        status.0
    );
    let output = seed.root.join("dst/output.txt");
    std::fs::remove_file(&output).map_err(|e| Error::Storage(e.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect_with_native_work(&owner, seed.command, &seed.plan, &query)
            .await?
            .0,
        status.0
    );
    assert!(!output.exists());
    let destination = workspace_ref(
        seed.destination.provider().clone(),
        &seed.destination.storage_name(&mut None)?,
    )?;
    assert_eq!(
        files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let auxiliary_files = FilesystemHost::new(
        Fs::local(acyclic_fs::LocalOptions::new(
            storage.path().join("auxiliary"),
        ))
        .await
        .map_err(|e| Error::Storage(e.to_string()))?,
        auxiliary.volume.provider(),
        None,
    )?;
    let workspace = workspace_ref(
        auxiliary.volume.provider().clone(),
        &auxiliary.volume.storage_name(&mut None)?,
    )?;
    assert_eq!(
        auxiliary_files
            .read(&workspace, Some(&auxiliary.generation), "/input.txt", 64)
            .await?
            .as_ref(),
        b"auxiliary input"
    );
    assert!(directory.path().exists());
    Ok(())
}
