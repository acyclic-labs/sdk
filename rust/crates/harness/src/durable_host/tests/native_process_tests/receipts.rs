use super::*;

#[tokio::test]
async fn cold_unchanged_project_keeps_the_actual_sdk_snapshot_without_relaunch() -> Result<()> {
    initialize_issued_view_fixture_services()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "mixed-issued-view",
        true,
        Some(&original),
    )
    .await?;
    let process = fixture.provider(&original).await?;
    let effects = fixture.effects(process.clone())?;
    let (status, status_custody) = effects
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, &original)
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let attempt = process.original_attempt();
    let receipt = process
        .receipt(attempt, &original, None)
        .await
        .unwrap()
        .unwrap();
    let index = fixture
        .view
        .manifest()
        .volumes
        .iter()
        .position(|mapping| mapping.volume == fixture.source)
        .expect("actual original Project mapping");
    assert_eq!(fixture.source.class(), VolumeClass::Project);
    assert!(
        !fixture.view.manifest().volumes[index].writable,
        "readonly Project qualification never requires a writable fixture"
    );
    let accepted = receipt
        .published
        .iter()
        .find(|published| published.index == index)
        .expect("unchanged Project retains its actual SDK publication snapshot");
    assert!(
        accepted.snapshot.operation().is_none(),
        "no Project write operation"
    );
    let expected = accepted
        .snapshot
        .snapshot()
        .lazy_snapshot_with_native_work(&original, &cancellation)?
        .expect("actual SDK-qualified lazy Project snapshot");
    let output = fixture.view.manifest().options.root.join("dst/output.txt");
    std::fs::remove_file(&output).map_err(|error| Error::Storage(error.to_string()))?;
    drop(effects);
    drop(process);
    cancellation.cancel();
    let original_prefix = original.receipt();
    let query_cancellation = acyclic_fs::CancellationToken::new();
    let query = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &query_cancellation,
    )?;
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        &query,
    )
    .await?;
    let before = query.receipt();
    let (candidate, candidate_custody) = recovered
        .published_project_candidate_with_native_work(attempt, &fixture.source, &query)?
        .await?;
    assert_eq!(candidate, expected);
    assert!(candidate_custody.work().shares_ledger(&query));
    let repeated = recovered
        .receipt(attempt, &query, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.published, repeated.published);
    assert_eq!(
        query.receipt().backend_write_operations,
        before.backend_write_operations
    );
    assert_eq!(query.receipt().materializations, before.materializations);
    let inspection = fixture
        .inspect_project_candidate_with_native_work(
            candidate,
            OperationId::from_bytes([219; 16]),
            &query,
        )
        .await?;
    let selection_prefix = query.receipt();
    let (selected, selected_custody) = recovered
        .inspect_published_project_snapshot_with_native_work(
            attempt,
            &inspection,
            &fixture.source,
            candidate,
            &query,
        )?
        .await?;
    assert_eq!(selected, expected);
    assert!(selected_custody.work().shares_ledger(&query));
    assert_eq!(
        query.receipt().backend_write_operations,
        selection_prefix.backend_write_operations
    );
    assert_eq!(
        query.receipt().materializations,
        selection_prefix.materializations
    );
    assert_eq!(original.receipt(), original_prefix);
    assert!(
        !output.exists(),
        "cold snapshot query never reruns the accepted process"
    );
    drop(repeated);
    drop(selected_custody);
    drop(inspection);
    drop(candidate_custody);
    drop(receipt);
    drop(status);
    drop(status_custody);
    Ok(())
}

#[tokio::test]
async fn native_process_drain_factory_rejects_other_attempt_before_admission() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&original),
    )
    .await?;
    let process = fixture.provider(&original).await?;
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let cleanup = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cleanup_cancellation,
    )?;
    let before = cleanup.receipt();
    assert!(matches!(
        process.drain_with_native_work(
            crate::EffectAttemptId::from_bytes([255; 16]),
            &fixture.view,
            &cleanup,
            &cleanup_cancellation,
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            ..
        })
    ));
    assert!(matches!(
        process.published_project_candidate_with_native_work(
            crate::EffectAttemptId::from_bytes([255; 16]),
            &fixture.source,
            &cleanup,
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            ..
        })
    ));
    assert_eq!(cleanup.receipt(), before);
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        &cleanup,
    )
    .await?;
    assert!(matches!(
        recovered
            .drain_with_native_work(
                process.original_attempt(),
                &fixture.view,
                &cleanup,
                &cleanup_cancellation,
            )?
            .await,
        Err(Error::Indeterminate(_))
    ));
    assert_eq!(cleanup.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn cold_native_drain_proof_authenticates_terminal_result_without_relaunch() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&original),
    )
    .await?;
    let process = fixture.provider(&original).await?;
    let effects = fixture.effects(process.clone())?;
    let (status, status_custody) = effects
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, &original)
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let attempt = process.original_attempt();
    cancellation.cancel();
    let original_prefix = original.receipt();
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let cleanup = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cleanup_cancellation,
    )?;
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        &cleanup,
    )
    .await?;
    let proof = recovered
        .drain_with_native_work(attempt, &fixture.view, &cleanup, &cleanup_cancellation)?
        .await?;
    proof.require_view_binding(fixture.view.as_ref(), fixture.owner.as_ref(), &original)?;
    let command_check_prefix = cleanup.receipt();
    proof.require_command_binding(fixture.command)?;
    assert!(matches!(
        proof.require_command_binding(OperationId::from_bytes([63; 16])),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            ..
        })
    ));
    assert_eq!(cleanup.receipt(), command_check_prefix);
    assert!(fixture.view.work().cancellation().is_cancelled());
    assert_eq!(original.receipt(), original_prefix);
    assert_eq!(cleanup.receipt().backend_write_operations, 0);
    assert_eq!(cleanup.receipt().materializations, 0);
    let live = cleanup.live_allocation_bytes();
    drop(proof);
    assert!(cleanup.live_allocation_bytes() < live);
    drop(status);
    drop(status_custody);
    Ok(())
}

#[tokio::test]
async fn warm_native_drain_keeps_original_tree_and_accepts_authorized_cleanup_span() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
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
    let dispatch_work = original.clone();
    let dispatch = tokio::spawn(async move {
        effects
            .run_task_effect_with_native_work(&owner, command, &plan, &dispatch_work)
            .await
    });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let cleanup = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cleanup_cancellation,
    )?;
    assert!(!cleanup.shares_ledger(&original));
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        &cleanup,
    )
    .await?;
    assert!(matches!(
        recovered
            .drain_with_native_work(
                process.original_attempt(),
                &fixture.view,
                &cleanup,
                &cleanup_cancellation,
            )?
            .await,
        Err(Error::Indeterminate(_))
    ));
    assert!(!dispatch.is_finished());
    cancellation.cancel();
    let proof = process
        .drain_with_native_work(
            process.original_attempt(),
            &fixture.view,
            &cleanup,
            &cleanup_cancellation,
        )?
        .await?;
    proof.require_view_binding(fixture.view.as_ref(), fixture.owner.as_ref(), &original)?;
    assert!(fixture.view.work().cancellation().is_cancelled());
    let _actual_completion = dispatch
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    assert_eq!(cleanup.receipt().materializations, 0);
    drop(proof);
    Ok(())
}

#[tokio::test]
async fn original_native_cancel_at_observed_commit_keeps_launch_uncertain_without_new_allowance()
-> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let stream = Arc::new(ReceiptFaultStream::default());
    let fixture = process_fixture(stream.clone(), "write", true, Some(&work)).await?;
    let process = fixture.provider(&work).await?;
    *stream.cancel_observed.lock().await = Some(cancellation.clone());
    assert!(fixture.effects(process.clone())?.run_task_effect_with_native_work(
        &fixture.owner, fixture.command, &fixture.plan, &work,
    ).await.is_err());
    assert!(fixture.view.work().cancellation().is_cancelled());
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command, &mut None)?.into_bytes(),
    );
    let prefix = work.receipt();
    assert!(process.reconcile_with_native_work(attempt, &work).is_err());
    assert_eq!(work.receipt(), prefix);
    let query_cancellation = acyclic_fs::CancellationToken::new();
    let query = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &query_cancellation,
    )?;
    let receipt = process
        .receipt(attempt, &query, None)
        .await
        .unwrap()
        .unwrap();
    assert!(!receipt.observed);
    assert_eq!(receipt.effect.status, EffectStatus::Indeterminate);
    assert_eq!(query.receipt().backend_write_operations, 0);
    assert_eq!(query.receipt().materializations, 0);
    let (effect, custody) = receipt.into_effect_with_native_custody().unwrap();
    assert!(custody.work().shares_ledger(&query));
    assert!(!custody.work().shares_ledger(&work));
    let output = fixture.view.manifest().options.root.join("dst/output.txt");
    assert!(output.exists());
    std::fs::remove_file(&output).map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
    let again = process
        .receipt(attempt, &query, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.effect, effect);
    assert!(!output.exists());
    assert!(fixture.view.work().cancellation().is_cancelled());
    drop(effect);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_precapture_identity_failure_retains_work_and_never_replays() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let process = fixture.provider(fixture.view.work()).await?;
    let root = fixture.view.manifest().options.root.clone();
    let original = root.with_file_name("original-native-root");
    std::fs::rename(&root, &original).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::write(root.join("user.txt"), b"external replacement")
        .map_err(|error| Error::Storage(error.to_string()))?;
    let effects = fixture.effects(process.clone())?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert_eq!(status, EffectStatus::Indeterminate);
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command, &mut None)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::NotFound("native pre-capture observation".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &status,
        true,
    )?;
    assert!(receipt.work.backend_read_operations > 0);
    assert!(
        receipt
            .invocation_failure
            .as_ref()
            .is_some_and(|message| !message.is_empty())
    );
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert!(!original.join("dst/output.txt").exists());
    assert_eq!(
        std::fs::read(root.join("user.txt")).map_err(|error| Error::Storage(error.to_string()))?,
        b"external replacement",
    );
    drop((effects, process));
    std::fs::remove_file(root.join("user.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::remove_dir(&root).map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::rename(&original, &root).map_err(|error| Error::Storage(error.to_string()))?;
    let recovered = Arc::new(
        Process::recover(
            fixture.owner.clone(),
            fixture.command,
            fixture.plan.clone(),
            fixture.approval(),
            fixture.authority(),
        )
        .await?,
    );
    assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
    assert_eq!(
        fixture
            .effects(recovered)?
            .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan,)
            .await?,
        EffectStatus::Indeterminate,
    );
    assert!(!root.join("dst/output.txt").exists());
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
        let stream = Arc::new(ReceiptFaultStream::default());
        let fixture = process_fixture(stream.clone(), "write", true, None).await?;
        *stream.fault.lock().await = Some((kind, lost_reply));
        let process = fixture.provider(fixture.view.work()).await?;
        let effects = fixture.effects(process.clone())?;
        assert!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await
                .is_err()
        );
        assert!(
            stream.fault.lock().await.is_none(),
            "fault was not exercised"
        );
        let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command, &mut None)?;
        let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
        let receipt = process.receipt(attempt).await?;
        if let Some(receipt) = &receipt {
            let retained_status = if kind == "observed" && lost_reply {
                receipt.effect.status.clone()
            } else {
                EffectStatus::Indeterminate
            };
            assert_native_receipt_binding(
                &fixture.owner,
                fixture.command,
                &fixture.plan,
                receipt,
                &retained_status,
                kind == "observed" && lost_reply,
            )?;
            assert_eq!(receipt.diagnostic, None);
            assert_eq!(receipt.publication_failure, None);
            assert_eq!(receipt.invocation_failure, None);
            if receipt.observed {
                assert!(receipt.work.backend_read_operations > 0);
                assert!(receipt.work.backend_write_operations > 0);
            } else {
                assert_eq!(receipt.work, acyclic_fs::WorkCounters::default());
            }
        } else {
            assert_eq!((kind, lost_reply), ("launch", false));
        }
        drop((effects, process));
        let recovered = Arc::new(
            Process::<ReceiptFaultStream>::recover(
                fixture.owner.clone(),
                fixture.command,
                fixture.plan.clone(),
                fixture.approval(),
                fixture.authority(),
            )
            .await?,
        );
        let effects = fixture.effects(recovered.clone())?;
        let status = effects
            .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
            .await?;
        if kind == "observed" && lost_reply {
            assert!(matches!(status, EffectStatus::Succeeded { .. }));
        } else {
            assert_eq!(status, EffectStatus::Indeterminate);
        }
        assert_eq!(recovered.receipt(attempt).await?, receipt);
        assert_eq!(
            crate::contract::canonical_json_bytes(&recovered.receipt(attempt).await?)?,
            crate::contract::canonical_json_bytes(&receipt)?,
        );
        assert_eq!(
            recovered.reconcile(attempt).await?,
            receipt.as_ref().map(|receipt| receipt.effect.clone()),
        );
        let root = &fixture.view.manifest().options.root;
        assert_eq!(root.join("dst/output.txt").exists(), kind == "observed");
        if kind == "observed" {
            std::fs::remove_file(root.join("dst/output.txt"))
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status
        );
        assert!(!root.join("dst/output.txt").exists());
        assert_eq!(recovered.receipt(attempt).await?, receipt);
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name(&mut None)?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await
                .is_ok(),
            kind == "observed"
        );
    }
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn receipt_reconciliation_reopens_all_local_stores_after_provider_objects_drop() -> Result<()>
{
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let filesystem = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("filesystem"),
    ))
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let auxiliary = Fs::local(acyclic_fs::LocalOptions::new(
        storage.path().join("auxiliary"),
    ))
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let local = Arc::new(
        acyclic_stream::LocalStream::open(storage.path().join("coordinator"), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    let weak_stream = Arc::downgrade(&local);
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = process_fixture_with_filesystems(
        local, directory, filesystem, auxiliary, "write", true, None,
    )
    .await?;
    let seed = NativeRestart::from_fixture(&fixture);
    let auxiliary = fixture.view.manifest().volumes[2].clone();
    let weak_files = Arc::downgrade(&fixture.files);
    let process = fixture.provider(fixture.view.work()).await?;
    let weak_process = Arc::downgrade(&process);
    let status = fixture
        .effects(process.clone())?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(seed.task, seed.command, &mut None)?.into_bytes(),
    );
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing original local native receipt".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        seed.command,
        &seed.plan,
        &receipt,
        &status,
        true,
    )?;
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    assert!(receipt.work.backend_read_operations > 0);
    assert!(receipt.work.backend_write_operations > 0);
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
    let native_output = seed.root.join("dst/output.txt");
    drop((task, owner, approvals, content, results, view, files));
    assert!(weak_stream.upgrade().is_none());
    assert!(weak_files.upgrade().is_none());
    let LocalNativeRecovery {
        files,
        effects,
        owner,
        process,
        ..
    } = reopen_native(storage.path(), &seed).await?;
    assert_eq!(
        effects
            .reconcile_task_effect(&owner, seed.command, &seed.plan)
            .await?,
        status
    );
    assert_eq!(process.receipt(attempt).await?.as_ref(), Some(&receipt));
    assert_eq!(
        crate::contract::canonical_json_bytes(&process.receipt(attempt).await?)?,
        crate::contract::canonical_json_bytes(&Some(&receipt))?,
    );
    std::fs::remove_file(&native_output).map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&owner, seed.command, seed.plan.clone())
            .await?,
        status
    );
    assert!(!native_output.exists());
    assert_eq!(process.receipt(attempt).await?, Some(receipt));
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
        .map_err(|error| Error::Storage(error.to_string()))?,
        auxiliary.volume.provider(),
        None,
    )?;
    let auxiliary_ref = workspace_ref(
        auxiliary.volume.provider().clone(),
        &auxiliary.volume.storage_name(&mut None)?,
    )?;
    assert_eq!(
        auxiliary_files
            .read(
                &auxiliary_ref,
                Some(&auxiliary.generation),
                "/input.txt",
                64
            )
            .await?
            .as_ref(),
        b"auxiliary input"
    );
    assert!(directory.path().exists());
    Ok(())
}

#[tokio::test]
async fn original_supervisor_stop_is_attempt_fenced_and_does_not_cancel_the_native_view()
-> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "wait", true, None).await?;
    let process = fixture.provider(fixture.view.work()).await?;
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let plan = fixture.plan.clone();
    let dispatch =
        tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command, &mut None)?.into_bytes(),
    );
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let budget = fixture.view.manifest().validation_budget()?;
    let work = test_work(budget, &cleanup_cancellation)?;
    assert!(matches!(
        process
            .signal_stop(
                crate::EffectAttemptId::from_bytes([255; 16]),
                &work,
                &cleanup_cancellation
            )
            .await,
        Err(Error::NativeBoundary { .. }),
    ));
    let reopened = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        fixture.plan.clone(),
        fixture.approval(),
        fixture.authority(),
    )
    .await?;
    assert!(matches!(
        reopened
            .signal_stop(attempt, &work, &cleanup_cancellation)
            .await,
        Err(Error::NativeBoundary { .. }),
    ));
    let mut denied_budget = budget;
    denied_budget.items_examined = 0;
    let denied = test_work(denied_budget, &cleanup_cancellation)?;
    assert!(matches!(
        process
            .signal_stop(attempt, &denied, &cleanup_cancellation)
            .await,
        Err(Error::NativeWork { .. }),
    ));
    assert!(!dispatch.is_finished());
    process
        .signal_stop(attempt, &work, &cleanup_cancellation)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let receipt = process
        .request_stop(attempt, &work, &cleanup_cancellation)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(receipt.observed);
    let status = dispatch
        .await
        .map_err(|error| Error::Storage(error.to_string()))??;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "owned stop lost its actual captured result: {status:?}"
        )));
    };
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &status,
        true,
    )?;
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(!output.success);
    assert!(matches!(
        output.stop.as_ref(),
        Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed,
    ));
    fixture.owner.verify(false, &mut None).await?;
    fixture
        .view
        .validate_for_dispatch(budget)
        .await
        .map_err(|(cause, _)| cause)?;
    assert!(
        fixture
            .files
            .read(
                &workspace_ref(
                    fixture.destination.provider().clone(),
                    &fixture.destination.storage_name(&mut None)?,
                )?,
                None,
                "/output.txt",
                64
            )
            .await
            .is_err()
    );
    assert_eq!(reopened.receipt(attempt).await?, Some(receipt));
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    Ok(())
}

#[tokio::test]
async fn original_warm_supervisor_stop_settles_after_real_task_cancellation() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "wait", true, None).await?;
    let process = fixture.provider(fixture.view.work()).await?;
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let plan = fixture.plan.clone();
    let dispatch =
        tokio::spawn(async move { effects.run_task_effect(&owner, command, plan).await });
    let root = fixture.view.manifest().options.root.clone();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command, &mut None)?.into_bytes(),
    );
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0, None)
        .await?;
    let cleanup_cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        fixture.view.manifest().validation_budget()?,
        &cleanup_cancellation,
    )?;
    process
        .signal_stop(attempt, &work, &cleanup_cancellation)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let receipt = process
        .request_stop(attempt, &work, &cleanup_cancellation)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(receipt.observed);
    let captured = process.output(attempt).await?.ok_or_else(|| {
        Error::Storage("cancelled original supervisor lost captured output".into())
    })?;
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&captured).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        output.stop.as_ref(),
        Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed,
    ));
    let _ = dispatch
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(fixture.owner.verify(false, &mut None).await.is_err());
    assert!(fixture.owner.verify(true, &mut None).await.is_ok());
    Ok(())
}
