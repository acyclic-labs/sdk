use super::*;

async fn file(
    fixture: &ProcessFixture,
    volume: &VolumeRef,
    path: &str,
    maximum: u64,
) -> Result<OwnedBytes> {
    let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
    fixture.files.read(&workspace, None, path, maximum).await
}

async fn output(fixture: &ProcessFixture, status: &EffectStatus) -> Result<NativeProcessResult> {
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!(
            "unobserved native process: {status:?}"
        )));
    };
    crate::contract::json_from_slice(&fixture.content.read(result).await?)
        .map_err(|error| Error::Invalid(error.to_string()))
}

#[tokio::test]
async fn approved_commands_preserve_arguments_environment_stdin_and_selected_volumes() -> Result<()>
{
    for mode in ["bash", "many-bash", "nonzero", "stdin-empty", "stdin-bytes"] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let work = fixture.view.work();
        let process = fixture.provider(work).await?;
        let effects = fixture.effects(process)?;
        let outcome = effects
            .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
            .await?;
        let output = output(&fixture, &outcome.0).await?;
        match mode {
            "bash" => {
                assert_eq!(fixture.view.manifest().volumes.len(), 3);
                assert_ne!(
                    fixture.view.manifest().volumes[0].volume.provider(),
                    fixture.view.manifest().volumes[2].volume.provider()
                );
                assert!(output.success);
                assert_eq!(output.stdout, b"approved-bash|first arg|second arg");
                assert_eq!(output.stderr, b"approved-env");
                for (path, expected) in [
                    ("/output.txt", b"pinned input".as_slice()),
                    ("/auxiliary.txt", b"auxiliary input".as_slice()),
                ] {
                    assert_eq!(
                        file(&fixture, &fixture.destination, path, 64)
                            .await?
                            .as_ref(),
                        expected
                    );
                }
            }
            "many-bash" => {
                assert_eq!(fixture.view.manifest().volumes.len(), 19);
                assert!(output.success);
                assert_eq!(
                    file(&fixture, &fixture.destination, "/many.txt", 512)
                        .await?
                        .as_ref(),
                    "extra input|".repeat(16).as_bytes()
                );
            }
            "nonzero" => {
                assert!(!output.success);
                assert_eq!(output.exit_code, Some(22));
                assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
                assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
                let path = fixture.view.manifest().options.root.join("dst/output.txt");
                std::fs::remove_file(&path).map_err(|error| Error::Storage(error.to_string()))?;
                let replay = effects
                    .run_task_effect_with_native_work(
                        &fixture.owner,
                        fixture.command,
                        &fixture.plan,
                        work,
                    )
                    .await?;
                assert_eq!(replay.0, outcome.0);
                assert!(!path.exists());
            }
            _ => {
                assert!(output.success && output.stop.is_none());
                assert_eq!(output.exit_code, Some(0));
                assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
                let expected = if mode == "stdin-empty" {
                    [].as_slice()
                } else {
                    [0, 255, b'\n', b'x'].as_slice()
                };
                assert_eq!(
                    file(&fixture, &fixture.destination, "/output.txt", 64)
                        .await?
                        .as_ref(),
                    expected
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn timeout_and_output_overflow_retain_cleanup_failure_without_publication() -> Result<()> {
    for (mode, kind) in [
        ("timeout", NativeProcessStopKind::Timeout),
        ("flood", NativeProcessStopKind::OutputLimit),
    ] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let work = fixture.view.work();
        let process = fixture.provider(work).await?;
        let effects = fixture.effects(process)?;
        let outcome = effects
            .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
            .await?;
        let output = output(&fixture, &outcome.0).await?;
        assert!(!output.success);
        let stop = output.stop.unwrap();
        assert!(stop.cleanup_completed);
        assert_eq!(stop.kind, kind);
        assert!(output.stdout.len() + output.stderr.len() <= 8192);
        if mode == "flood" {
            assert_eq!(output.stdout.len() + output.stderr.len(), 8192);
        }
        assert!(
            file(&fixture, &fixture.destination, "/output.txt", 64)
                .await
                .is_err()
        );
        let replay = effects
            .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
            .await?;
        assert_eq!(replay.0, outcome.0);
        if mode == "timeout" {
            tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
            assert!(
                !fixture
                    .view
                    .manifest()
                    .options
                    .root
                    .join("escaped")
                    .exists()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn bash_source_mutation_preserves_sdk_sources_and_rejects_all_publication() -> Result<()> {
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "mutate-source-bash",
        true,
        None,
    )
    .await?;
    let work = fixture.view.work();
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name(&mut None)?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let process = fixture.provider(work).await?;
    let effects = fixture.effects(process.clone())?;
    let error = effects
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
        .await
        .expect_err("source mutation denies publication");
    let Error::NativePublication { failure, .. } = error else {
        panic!("missing publication failure")
    };
    assert!(failure.completed.is_empty() && failure.published.is_empty());
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    assert_eq!(
        file(&fixture, &fixture.source, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    assert!(
        file(&fixture, &fixture.destination, "/output.txt", 64)
            .await
            .is_err()
    );
    let attempt = process.original_attempt();
    assert!(process.output(attempt, work).await?.is_none());
    let receipt = process.receipt(attempt, work, None).await?.unwrap();
    assert!(!receipt.observed);
    assert_eq!(receipt.effect.status, EffectStatus::Indeterminate);
    let diagnostic = failure.diagnostic.as_ref().unwrap();
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(diagnostic).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    let path = fixture.view.manifest().options.root.join("dst/output.txt");
    std::fs::remove_file(&path).map_err(|error| Error::Storage(error.to_string()))?;
    let replay = effects
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
        .await?;
    assert_eq!(replay.0, EffectStatus::Indeterminate);
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn real_process_requires_approval_and_durable_dispatch_then_reconciles_receipt() -> Result<()>
{
    let denied = process_fixture(Arc::new(MemoryStream::default()), "write", false, None).await?;
    assert!(matches!(
        denied.provider(denied.view.work()).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(
        !denied
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let fixture =
        process_fixture(Arc::new(MemoryStream::default()), "review-test", true, None).await?;
    let work = fixture.view.work();
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name(&mut None)?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    assert!(
        fixture
            .owner
            .require_volume_grant(&fixture.source, VolumeOperation::Write, &mut None)
            .is_err()
    );
    let task = fixture.owner.task_binding().0;
    let inspection = fixture.task.inspect_source(task, task).await?;
    let page = inspection
        .list(&fixture.source, "", "", &before, None, 1)
        .await?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "input.txt");
    assert_eq!(
        inspection
            .read_path(&fixture.source, "", "input.txt", &before)
            .await?
            .1
            .as_ref(),
        b"pinned input"
    );
    let process = fixture.provider(work).await?;
    let attempt = process.original_attempt();
    assert!(process.receipt(attempt, work, None).await?.is_none());
    let dispatch = EffectDispatch {
        provider: fixture.plan.provider.clone(),
        effect_id: task_effect_id(task, fixture.command, &mut None)?,
        attempt_id: attempt,
        effect_kind: fixture.plan.effect_kind.clone(),
        request: fixture.plan.request.clone(),
        guarantee: fixture.plan.guarantee,
        request_digest: [0; 32],
    };
    assert!(matches!(
        process.dispatch_with_native_work(dispatch, work)?.await,
        Err(Error::Unauthorized(_))
    ));
    assert!(process.receipt(attempt, work, None).await?.is_none());
    let effects = fixture.effects(process.clone())?;
    let outcome = effects
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
        .await?;
    let output = output(&fixture, &outcome.0).await?;
    assert!(output.success);
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    assert_eq!(
        file(&fixture, &fixture.destination, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    assert_eq!(
        file(&fixture, &fixture.source, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let receipt = process.receipt(attempt, work, None).await?.unwrap();
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &outcome.0,
        true,
    )?;
    let mappings = &fixture.view.manifest().volumes;
    for (position, published) in receipt.published.iter().enumerate() {
        assert!(published.index < mappings.len());
        assert!(
            !receipt.published[..position]
                .iter()
                .any(|prior| prior.index == published.index)
        );
    }
    assert!(
        receipt
            .published
            .iter()
            .any(|published| mappings[published.index].writable)
    );
    let weak = Arc::downgrade(&process);
    drop(effects);
    drop(process);
    assert!(weak.upgrade().is_none());
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        work,
    )
    .await?;
    let known = recovered
        .reconcile_with_native_work(attempt, work)?
        .await?
        .unwrap();
    assert_eq!(known.0.status, outcome.0);
    let reopened = recovered.receipt(attempt, work, None).await?.unwrap();
    assert_eq!(
        crate::contract::canonical_json_bytes(&reopened)?,
        crate::contract::canonical_json_bytes(&receipt)?
    );
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    assert!(
        file(&fixture, &fixture.source, "/tests/generated.txt", 128)
            .await
            .is_err()
    );
    assert_eq!(
        file(&fixture, &fixture.destination, "/tests/generated.txt", 128)
            .await?
            .as_ref(),
        b"isolated test"
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name(&mut None)?,
    )?;
    let verifier = fixture.issuer.verifier(&mut None).expect("verifier copy");
    let write = ContentGrant::verify(
        &verifier,
        &fixture.scope,
        &fixture.second_destination,
        VolumeOperation::Write,
    )?;
    fixture
        .files
        .put_content(
            &fixture.second_destination,
            &write,
            "concurrent.txt",
            b"user edit",
            "text/plain",
            "concurrent.txt",
            128,
            &IdempotencyKey::new("review-concurrent")?,
            None,
            None,
        )
        .await?;
    let review_head = fixture.files.resolve(&destination).await?.generation;
    let read = ContentGrant::verify(
        &verifier,
        &fixture.scope,
        &fixture.destination,
        VolumeOperation::Read,
    )?;
    let import = fixture
        .files
        .prepare_volume_import(
            &fixture.destination,
            &read,
            &review_head,
            &fixture.second_destination,
            &write,
        )
        .await?;
    let key = IdempotencyKey::new("review-import")?;
    assert!(matches!(
        import.apply(&key).await?,
        acyclic_fs::JoinOutcome::Applied(_)
    ));
    assert!(matches!(
        import.apply(&key).await?,
        acyclic_fs::JoinOutcome::AlreadyApplied(_)
    ));
    for (path, expected) in [
        ("/tests/generated.txt", b"isolated test".as_slice()),
        ("/concurrent.txt", b"user edit".as_slice()),
    ] {
        assert_eq!(
            file(&fixture, &fixture.second_destination, path, 128)
                .await?
                .as_ref(),
            expected
        );
    }
    assert_eq!(
        fixture.files.resolve(&destination).await?.generation,
        review_head
    );
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    assert!(
        file(&fixture, &fixture.source, "/tests/generated.txt", 128)
            .await
            .is_err()
    );
    let path = fixture
        .view
        .manifest()
        .options
        .root
        .join("dst/tests/generated.txt");
    std::fs::remove_file(&path).map_err(|error| Error::Storage(error.to_string()))?;
    let replay = fixture
        .effects(fixture.provider(work).await?)?
        .run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan, work)
        .await?;
    assert_eq!(replay.0, outcome.0);
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_after_actual_provider_drop_keeps_receipt_and_cleans_descendants() -> Result<()>
{
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "wait", true, None).await?;
    let work = fixture.view.work();
    let process = fixture.provider(work).await?;
    let weak = Arc::downgrade(&process);
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let plan = fixture.plan.clone();
    let command = fixture.command;
    let original = work.clone();
    let dispatch = tokio::spawn(async move {
        effects
            .run_task_effect_with_native_work(&owner, command, &plan, &original)
            .await
    });
    let root = &fixture.view.manifest().options.root;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?;
    dispatch.abort();
    let _ = dispatch.await;
    drop(process);
    assert!(weak.upgrade().is_none());
    fixture
        .task
        .task_host()
        .cancel(fixture.owner.task_binding().0, None)
        .await?;
    let recovered = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        &fixture.plan,
        fixture.approval(),
        fixture.authority(),
        work,
    )
    .await?;
    let attempt = recovered.original_attempt();
    let captured = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(output) = recovered.output(attempt, work).await? {
                break Result::Ok(output);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))??;
    let output = output(
        &fixture,
        &EffectStatus::Succeeded {
            result: captured.0.clone(),
        },
    )
    .await?;
    assert!(!output.success);
    assert!(
        matches!(output.stop, Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed)
    );
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    assert!(fixture.owner.verify(false, &mut None).await.is_err());
    assert!(fixture.owner.verify(true, &mut None).await.is_ok());
    let receipt = recovered.receipt(attempt, work, None).await?.unwrap();
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &EffectStatus::Succeeded {
            result: captured.0.clone(),
        },
        true,
    )?;
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
    let effects = fixture.effects(recovered.clone())?;
    let known = effects
        .reconcile_task_effect_with_native_work(
            &fixture.owner,
            fixture.command,
            &fixture.plan,
            work,
        )
        .await?;
    assert_eq!(known.0, EffectStatus::Succeeded { result: captured.0 });
    let reopened = recovered.receipt(attempt, work, None).await?.unwrap();
    assert_eq!(
        crate::contract::canonical_json_bytes(&reopened)?,
        crate::contract::canonical_json_bytes(&receipt)?
    );
    let (task, fence) = fixture.owner.task_binding();
    fixture
        .task
        .task_host()
        .settle_task(task, fence, Outcome::Cancelled, &mut None)
        .await?;
    assert_eq!(
        fixture.task.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    );
    assert!(fixture.owner.verify(true, &mut None).await.is_err());
    assert!(recovered.receipt(attempt, work, None).await.is_err());
    assert!(fixture.directory.path().exists());
    Ok(())
}

#[tokio::test]
async fn exactly_approved_mcp_requests_cannot_also_supply_literal_stdin() -> Result<()> {
    for mode in ["mcp-stdin-empty", "mcp-stdin-bytes"] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let request = fixture.request().await?;
        assert!(request.mcp_stdio.is_some() && request.standard_input.is_some());
        assert!(fixture.provider(fixture.view.work()).await.is_err());
        assert!(
            !fixture
                .view
                .manifest()
                .options
                .root
                .join("dst/output.txt")
                .exists()
        );
    }
    Ok(())
}

#[tokio::test]
async fn stock_task_commands_select_existing_effect_registry_explicitly() -> Result<()> {
    use crate::filesystem::{EFFECT_TASK_COMMAND_KIND, TaskCommandHost as _, TaskCommandProgress};
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let (task, fence) = fixture.owner.task_binding();
    let context = fixture
        .task
        .harness()
        .durable_context(task, OperationId::from_bytes(task.into_bytes()))
        .await?;
    let payload = crate::contract::canonical_json_bytes(&fixture.plan)?;
    let payload_ref = fixture
        .results
        .stage(
            fixture.command,
            "process/effect-command.json",
            &payload,
            "application/json",
            "effect-command.json",
        )
        .await?;
    let command = WorkflowCommand {
        operation_id: fixture.command,
        kind: EFFECT_TASK_COMMAND_KIND.into(),
        payload: payload_ref,
    };
    let value =
        serde_json::to_value(&fixture.plan).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        fixture
            .task
            .commands()
            .execute(
                context.clone(),
                fence.clone(),
                command.clone(),
                value.clone()
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let commands = fixture.task.commands().with_effects(Arc::new(
        fixture.effects(fixture.provider(fixture.view.work()).await?)?,
    ));
    let work = fixture.view.work();
    let context = fixture
        .task
        .task_context_with_native_work(task, &fence, work)
        .await?;
    let TaskCommandProgress::Ready(result) = commands
        .execute_with_native_work(
            context.clone(),
            fence.clone(),
            command.clone(),
            value.clone(),
            work,
        )?
        .await?
    else {
        return Err(Error::Storage("native command was not ready".into()));
    };
    let Outcome::Succeeded(file) = serde_json::from_value::<Outcome<FileRef>>(result.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?
    else {
        return Err(Error::Storage("native command was not successful".into()));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&file).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    let TaskCommandProgress::Ready(replayed) = commands
        .execute_with_native_work(context, fence.clone(), command.clone(), value, work)?
        .await?
    else {
        return Err(Error::Storage("native command replay was not ready".into()));
    };
    assert_eq!(result, replayed);
    let mut session = fixture.task.open_task(task, fence.clone()).await?;
    session
        .step(
            OperationId::from_bytes([66; 16]),
            IdempotencyKey::new("native-workflow-command")?,
            serde_json::to_value(command).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .await?;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    assert!(matches!(
        fixture.task.resume_task(fixture.lease.clone(), &commands, 2, Some(&io)).await,
        crate::filesystem::TaskWorkerAttempt::Progress(crate::filesystem::TaskWorkerOutcome::Completed { task: completed }) if completed == task
    ));
    assert!(
        matches!(fixture.task.task_host().outcome(task).await?, Some(Outcome::Succeeded(value)) if value["commands"][0]["value"] == result)
    );
    Ok(())
}
