use super::*;

#[tokio::test]
async fn timeout_and_output_overflow_retain_cleanup_failure_without_publication() -> Result<()> {
    for mode in ["timeout", "flood"] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let process = fixture.provider(fixture.view.work()).await?;
        let effects = fixture.effects(process.clone())?;
        let status = effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?;
        let EffectStatus::Succeeded { result } = &status else {
            return Err(Error::Storage(format!(
                "stopped capture unobserved: {status:?}"
            )));
        };
        let attempt = crate::EffectAttemptId::from_bytes(
            task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
        );
        let receipt = process
            .receipt(attempt)
            .await?
            .ok_or_else(|| Error::Storage("missing stopped-capture receipt".into()))?;
        assert_native_receipt_binding(
            &fixture.owner,
            fixture.command,
            &fixture.plan,
            &receipt,
            &status,
            true,
        )?;
        assert_eq!(receipt.diagnostic, None);
        assert_eq!(receipt.publication_failure, None);
        assert_eq!(receipt.invocation_failure, None);
        assert_eq!(receipt.work, acyclic_fs::WorkCounters::default());
        assert_eq!(process.output(attempt).await?.as_ref(), Some(result));
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(result).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(!output.success);
        let stop = output
            .stop
            .ok_or_else(|| Error::Storage("missing stopped-capture reason".into()))?;
        assert!(stop.cleanup_completed);
        assert_eq!(
            stop.kind,
            if mode == "timeout" {
                NativeProcessStopKind::Timeout
            } else {
                NativeProcessStopKind::OutputLimit
            }
        );
        assert!(!stop.message.is_empty());
        assert!(output.stdout.len() + output.stderr.len() <= 8192);
        if mode == "flood" {
            assert_eq!(output.stdout.len() + output.stderr.len(), 8192);
        }
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await
                .is_err()
        );
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status,
        );
        assert_eq!(process.receipt(attempt).await?.as_ref(), Some(&receipt));
        drop((effects, process));
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
        assert_eq!(recovered.receipt(attempt).await?.as_ref(), Some(&receipt));
        let retained = recovered
            .output(attempt)
            .await?
            .ok_or_else(|| Error::Storage("reopen lost stopped-capture output".into()))?;
        assert_eq!(&retained, result);
        let reopened_output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(&retained).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(reopened_output.stop.as_ref(), Some(&stop));
        let effects = fixture.effects(recovered.clone())?;
        assert_eq!(
            effects
                .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
                .await?,
            status
        );
        assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
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
async fn approved_bash_preserves_ordered_arguments_and_selected_environment() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "bash", true, None).await?;
    assert_eq!(fixture.view.manifest().volumes.len(), 3);
    assert_ne!(
        fixture.view.manifest().volumes[0].volume.provider(),
        fixture.view.manifest().volumes[2].volume.provider()
    );
    let effects = fixture.effects(fixture.provider(fixture.view.work()).await?)?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!("Bash did not complete: {status:?}")));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&result).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        output.success,
        "Bash exit {:?}: {}",
        output.exit_code,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"approved-bash|first arg|second arg");
    assert_eq!(output.stderr, b"approved-env");
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/auxiliary.txt", 64)
            .await?
            .as_ref(),
        b"auxiliary input"
    );
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
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let process = fixture.provider(fixture.view.work()).await?;
    let effects = fixture.effects(process.clone())?;
    let failure_error = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await
        .expect_err("source mutation rejects the original publication");
    let Error::NativePublication { failure, .. } = &failure_error else {
        panic!("source mutation retains its actual publication failure");
    };
    assert!(failure.completed.is_empty());
    assert!(failure.published.is_empty());
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    assert_eq!(
        fixture
            .files
            .read(&source, None, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await
            .is_err()
    );
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(fixture.owner.task_binding().0, fixture.command)?.into_bytes(),
    );
    assert!(process.output(attempt).await?.is_none());
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("source mutation lost original launch".into()))?;
    assert!(!receipt.observed);
    assert_eq!(receipt.effect.status, EffectStatus::Indeterminate);
    let output = failure
        .diagnostic
        .as_ref()
        .ok_or_else(|| Error::Storage("source mutation lost staged output".into()))?;
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(output).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(output.success);
    std::fs::remove_file(fixture.view.manifest().options.root.join("dst/output.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        EffectStatus::Indeterminate
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn approved_bash_uses_nineteen_independently_selected_volumes() -> Result<()> {
    let fixture =
        process_fixture(Arc::new(MemoryStream::default()), "many-bash", true, None).await?;
    assert_eq!(fixture.view.manifest().volumes.len(), 19);
    let status = fixture
        .effects(fixture.provider(fixture.view.work()).await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    assert!(matches!(status, EffectStatus::Succeeded { .. }));
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/many.txt", 512)
            .await?
            .as_ref(),
        "extra input|".repeat(16).as_bytes()
    );
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
    let TaskCommandProgress::Ready(result) = commands
        .execute(
            context.clone(),
            fence.clone(),
            command.clone(),
            value.clone(),
        )
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
        .execute(context, fence.clone(), command.clone(), value)
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
    assert!(
        matches!(fixture.task.run_task(fixture.lease.clone(), &commands, 2).await?, crate::filesystem::TaskWorkerOutcome::Completed { task: completed } if completed == task)
    );
    assert!(
        matches!(fixture.task.task_host().outcome(task).await?, Some(Outcome::Succeeded(value)) if value["commands"][0]["value"] == result)
    );
    Ok(())
}

#[tokio::test]
async fn nonzero_exit_preserves_output_and_is_not_implicitly_replayed() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "nonzero", true, None).await?;
    let process = fixture.provider(fixture.view.work()).await?;
    let effects = fixture.effects(process)?;
    let status = effects
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "nonzero invocation unobserved: {status:?}"
        )));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(!output.success);
    assert_eq!(output.exit_code, Some(22));
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    std::fs::remove_file(fixture.view.manifest().options.root.join("dst/output.txt"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        status
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
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
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name()?,
    )?;
    let source_head = fixture.files.resolve(&source).await?.generation;
    assert!(
        fixture
            .owner
            .require_volume_grant(&fixture.source, VolumeOperation::Write, &mut None)
            .is_err()
    );
    let task = fixture.owner.task_binding().0;
    let inspection = fixture.task.inspect_source(task, task).await?;
    let page = inspection
        .list(&fixture.source, "", "", &source_head, None, 1)
        .await?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "input.txt");
    assert_eq!(
        inspection
            .read_path(&fixture.source, "", "input.txt", &source_head)
            .await?
            .1
            .as_ref(),
        b"pinned input"
    );
    let process = fixture.provider(fixture.view.work()).await?;
    let effect_id = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect_id.into_bytes());
    assert_eq!(process.receipt(attempt).await?, None);
    let unadmitted = EffectDispatch {
        provider: fixture.plan.provider.clone(),
        effect_id,
        attempt_id: attempt,
        effect_kind: fixture.plan.effect_kind.clone(),
        request: fixture.plan.request.clone(),
        guarantee: fixture.plan.guarantee,
        request_digest: [0; 32],
    };
    assert!(matches!(
        process.dispatch(unadmitted).await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(process.receipt(attempt).await?, None);
    let status = fixture
        .effects(process.clone())?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(Error::Storage(format!(
            "native process did not complete: {status:?}"
        )));
    };
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
    assert!(output.success);
    assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("approved stderr"));
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name()?,
    )?;
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/output.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    assert_eq!(
        fixture
            .files
            .read(&source, None, "/input.txt", 64)
            .await?
            .as_ref(),
        b"pinned input"
    );
    let receipt = process
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing successful native receipt".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &status,
        true,
    )?;
    assert!(!receipt.published.is_empty());
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
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    assert!(receipt.work.backend_read_operations > 0);
    assert!(receipt.work.backend_write_operations > 0);
    let weak = Arc::downgrade(&process);
    drop(process);
    assert!(weak.upgrade().is_none());
    let recovered: Process = Process::recover(
        fixture.owner.clone(),
        fixture.command,
        fixture.plan.clone(),
        fixture.approval(),
        fixture.authority(),
    )
    .await?;
    assert_eq!(
        recovered
            .reconcile(attempt)
            .await?
            .map(|receipt| receipt.status),
        Some(status.clone())
    );
    assert_eq!(
        crate::contract::canonical_json_bytes(&recovered.receipt(attempt).await?)?,
        crate::contract::canonical_json_bytes(&Some(&receipt))?,
    );
    assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
    assert_eq!(
        fixture.files.resolve(&source).await?.generation,
        source_head
    );
    assert!(
        fixture
            .files
            .read(&source, None, "/tests/generated.txt", 128)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .files
            .read(&destination, None, "/tests/generated.txt", 128)
            .await?
            .as_ref(),
        b"isolated test"
    );
    let caller = workspace_ref(
        fixture.second_destination.provider().clone(),
        &fixture.second_destination.storage_name()?,
    )?;
    let caller_write = ContentGrant::verify(
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.scope,
        &fixture.second_destination,
        VolumeOperation::Write,
    )?;
    fixture
        .files
        .put_content(
            &fixture.second_destination,
            &caller_write,
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
    let review_read = ContentGrant::verify(
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.scope,
        &fixture.destination,
        VolumeOperation::Read,
    )?;
    let import = fixture
        .files
        .prepare_volume_import(
            &fixture.destination,
            &review_read,
            &review_head,
            &fixture.second_destination,
            &caller_write,
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
    assert_eq!(
        fixture
            .files
            .read(&caller, None, "/tests/generated.txt", 128)
            .await?
            .as_ref(),
        b"isolated test"
    );
    assert_eq!(
        fixture
            .files
            .read(&caller, None, "/concurrent.txt", 128)
            .await?
            .as_ref(),
        b"user edit"
    );
    assert_eq!(
        fixture.files.resolve(&destination).await?.generation,
        review_head
    );
    assert_eq!(
        fixture.files.resolve(&source).await?.generation,
        source_head
    );
    assert!(
        fixture
            .files
            .read(&source, None, "/tests/generated.txt", 128)
            .await
            .is_err()
    );
    std::fs::remove_file(
        fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/tests/generated.txt"),
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    assert_eq!(
        fixture
            .effects(fixture.provider(fixture.view.work()).await?)?
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?,
        status
    );
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/tests/generated.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_after_actual_provider_drop_keeps_receipt_and_cleans_descendants() -> Result<()>
{
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "wait", true, None).await?;
    let process = fixture.provider(fixture.view.work()).await?;
    let weak = Arc::downgrade(&process);
    let effects = fixture.effects(process.clone())?;
    let owner = fixture.owner.clone();
    let plan = fixture.plan.clone();
    let command = fixture.command;
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
        fixture.plan.clone(),
        fixture.approval(),
        fixture.authority(),
    )
    .await?;
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let path = format!("harness/v2/native-process/{attempt}");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while fixture
            .owner
            .stream()
            .bounds(acyclic_stream::StreamPath::new(&path)?)
            .await?
            .tail
            != 2
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Result::Ok(())
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))??;
    let captured = recovered
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing cancellation output".into()))?;
    let receipt = recovered
        .receipt(attempt)
        .await?
        .ok_or_else(|| Error::Storage("missing cancelled native receipt".into()))?;
    assert_native_receipt_binding(
        &fixture.owner,
        fixture.command,
        &fixture.plan,
        &receipt,
        &EffectStatus::Succeeded {
            result: captured.clone(),
        },
        true,
    )?;
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    let output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&captured).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(!output.success);
    assert!(
        matches!(output.stop.as_ref(), Some(stop) if stop.kind == NativeProcessStopKind::ControlStopped && stop.cleanup_completed && !stop.message.is_empty())
    );
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(!root.join("escaped").exists());
    assert!(fixture.owner.verify(false, &mut None).await.is_err());
    assert!(fixture.owner.verify(true, &mut None).await.is_ok());
    drop(recovered);
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
    assert_eq!(recovered.receipt(attempt).await?.as_ref(), Some(&receipt));
    let retained = recovered
        .output(attempt)
        .await?
        .ok_or_else(|| Error::Storage("reopen lost cancellation cleanup output".into()))?;
    assert_eq!(retained, captured);
    let reopened_output: NativeProcessResult =
        crate::contract::json_from_slice(&fixture.content.read(&retained).await?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened_output, output);
    let known = fixture
        .effects(recovered.clone())?
        .reconcile_task_effect(&fixture.owner, fixture.command, &fixture.plan)
        .await?;
    assert_eq!(known, EffectStatus::Succeeded { result: captured });
    assert_eq!(recovered.receipt(attempt).await?, Some(receipt));
    let (task, fence) = fixture.owner.task_binding();
    fixture
        .task
        .task_host()
        .settle_task(task, &fence, Outcome::Cancelled, &mut None)
        .await?;
    assert_eq!(
        fixture.task.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    );
    assert!(fixture.owner.verify(true, &mut None).await.is_err());
    assert!(recovered.receipt(attempt).await.is_err());
    assert!(fixture.directory.path().exists());
    Ok(())
}

#[tokio::test]
async fn approved_literal_stdin_reaches_eof_without_terminating_the_process() -> Result<()> {
    for (mode, expected) in [
        ("stdin-empty", [].as_slice()),
        ("stdin-bytes", [0, 255, b'\n', b'x'].as_slice()),
    ] {
        let fixture = process_fixture(Arc::new(MemoryStream::default()), mode, true, None).await?;
        let effects = fixture.effects(fixture.provider(fixture.view.work()).await?)?;
        let status = effects
            .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
            .await?;
        let EffectStatus::Succeeded { result } = status else {
            return Err(Error::Storage(format!(
                "literal stdin did not finish normally: {status:?}"
            )));
        };
        let output: NativeProcessResult =
            crate::contract::json_from_slice(&fixture.content.read(&result).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(output.success && output.stop.is_none());
        assert_eq!(output.exit_code, Some(0));
        assert!(String::from_utf8_lossy(&output.stdout).contains("approved stdout"));
        let destination = workspace_ref(
            fixture.destination.provider().clone(),
            &fixture.destination.storage_name()?,
        )?;
        assert_eq!(
            fixture
                .files
                .read(&destination, None, "/output.txt", 64)
                .await?
                .as_ref(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn even_exactly_approved_mcp_requests_cannot_also_supply_literal_stdin() -> Result<()> {
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
