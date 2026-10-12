use super::*;

#[tokio::test]
async fn ordinary_task_io_uses_verified_created_volume_without_root_grant_widening() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let read = created
        .private
        .capability(VolumeOperation::Read, &mut None)?;
    let write = created
        .private
        .capability(VolumeOperation::Write, &mut None)?;
    for grant in [&read, &write] {
        assert!(!fixture.root_scope.grants().contains(grant));
    }
    assert!(matches!(
        fixture.admit_child(created.request.child.clone()).await?,
        Admission::Accepted(_)
    ));
    let task = TaskId::from_bytes([84; 16]);
    let context = fixture
        .harness
        .durable_context(task, created.request.child.operation_id)
        .await?;
    assert_eq!(
        context.scope().current_created_private_volume(&mut None)?,
        Some(&created.private)
    );
    let file = context
        .stage_file(
            OperationId::from_bytes([93; 16]),
            "ordinary/result.txt",
            b"created private bytes",
            "text/plain",
            "result.txt",
        )
        .await?;
    let artifact = context
        .stage_artifact(
            OperationId::from_bytes([100; 16]),
            "ordinary/artifact.txt",
            b"created artifact bytes",
            "text/plain",
            "artifact.txt",
        )
        .await?;
    for (reference, bytes) in [
        (&file, b"created private bytes".as_slice()),
        (&artifact, b"created artifact bytes".as_slice()),
    ] {
        assert_eq!(reference.volume(), &created.private);
        assert_ne!(reference.volume(), &created.project);
        assert_eq!(context.read_file(reference).await?.as_ref(), bytes);
    }
    assert_eq!(
        context.read_artifact(&artifact).await?.as_ref(),
        b"created artifact bytes"
    );
    let reopened = fixture
        .harness
        .durable_context(task, created.request.child.operation_id)
        .await?;
    let readonly = reopened.scoped(Capabilities::new([read.clone()]), reopened.scope().limits())?;
    for reader in [&context, &reopened, &readonly] {
        assert_eq!(
            reader.scope().current_created_agent(&mut None)?,
            Some(created.seed.child_agent)
        );
        assert_eq!(
            reader.scope().current_created_authority(&mut None)?,
            Some(&created.seed.child)
        );
        assert_eq!(
            reader.read_file(&file).await?.as_ref(),
            b"created private bytes"
        );
    }
    assert!(
        readonly
            .stage_file(
                OperationId::from_bytes([94; 16]),
                "ordinary/denied.txt",
                b"denied",
                "text/plain",
                "denied.txt"
            )
            .await
            .is_err()
    );
    assert!(
        readonly
            .stage_artifact(
                OperationId::from_bytes([101; 16]),
                "ordinary/denied-artifact.txt",
                b"denied",
                "text/plain",
                "denied-artifact.txt"
            )
            .await
            .is_err()
    );
    let no_access = reopened.scoped(Capabilities::default(), reopened.scope().limits())?;
    assert!(no_access.read_file(&file).await.is_err());
    assert!(
        no_access
            .scoped(
                Capabilities::new([write.clone()]),
                no_access.scope().limits()
            )
            .is_err()
    );
    for task in [fixture.root, TaskId::from_bytes([83; 16])] {
        let foreign = fixture
            .harness
            .durable_context(task, OperationId::from_bytes(task.into_bytes()))
            .await?;
        assert!(foreign.read_file(&file).await.is_err());
    }
    let mut descendant = created.request.child.clone();
    descendant.operation_id = OperationId::from_bytes([97; 16]);
    descendant.parent = Some(task);
    descendant.grants = Capabilities::new([read.clone()]);
    descendant.limits.file_bytes = 1_024;
    let crate::distributed::WorkPull::Claimed(lease) =
        fixture.host.recover_work(task, &mut None).await?
    else {
        panic!("parent lease")
    };
    assert!(matches!(
        fixture
            .host
            .admit_owned(descendant.clone(), (&lease.reservation).into())
            .await?,
        Admission::Accepted(_)
    ));
    let child = TaskId::from_bytes([97; 16]);
    fixture.start_next(child).await?;
    let inherited = fixture
        .harness
        .durable_context(child, descendant.operation_id)
        .await?;
    assert_eq!(
        inherited.read_file(&file).await?.as_ref(),
        b"created private bytes"
    );
    assert!(!inherited.scope().grants().contains(&write));
    assert!(
        inherited
            .stage_file(
                OperationId::from_bytes([98; 16]),
                "inherited-denied.txt",
                b"denied",
                "text/plain",
                "inherited-denied.txt"
            )
            .await
            .is_err()
    );
    assert!(!fixture.root_scope.grants().contains(&read));
    Ok(())
}

async fn corrupt_claim(
    fixture: &CreationFixture,
    volume: &VolumeRef,
    field: &[&str],
    value: Value,
    key: &str,
) -> Result<()> {
    let workspace = workspace_ref(
        fixture.filesystem_provider.clone(),
        &format!("fork-allocation-{}", volume.storage_name(&mut None)?),
    )?;
    let bytes = fixture
        .filesystem
        .read(&workspace, None, "/claim.json", 65_536)
        .await?;
    let mut record: Value =
        crate::contract::json_from_slice(&bytes).map_err(|e| Error::Storage(e.to_string()))?;
    let mut slot = &mut record;
    for name in field {
        slot = &mut slot[*name];
    }
    *slot = value;
    fixture
        .filesystem
        .apply(
            &workspace,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/claim.json".into(),
                bytes: crate::contract::canonical_json_bytes(&record)?.into(),
            }],
            &IdempotencyKey::new(key)?,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn created_content_reauthenticates_allocation_provenance_on_every_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    assert!(matches!(
        fixture.admit_child(created.request.child.clone()).await?,
        Admission::Accepted(_)
    ));
    let task = TaskId::from_bytes([84; 16]);
    let operation = created.request.child.operation_id;
    let context = fixture.harness.durable_context(task, operation).await?;
    let file = context
        .stage_file(
            OperationId::from_bytes([95; 16]),
            "before-corruption.txt",
            b"authenticated bytes",
            "text/plain",
            "before-corruption.txt",
        )
        .await?;
    assert_eq!(
        context.read_file(&file).await?.as_ref(),
        b"authenticated bytes"
    );
    corrupt_claim(
        &fixture,
        &created.private,
        &["task_origin", "request_digest"],
        serde_json::json!(vec![0_u8; 32]),
        "corrupt-created-content-claim",
    )
    .await?;
    assert!(context.read_file(&file).await.is_err());
    assert!(
        context
            .stage_file(
                OperationId::from_bytes([96; 16]),
                "after-corruption.txt",
                b"must not publish",
                "text/plain",
                "after-corruption.txt"
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .harness
            .durable_context(task, operation)
            .await
            .is_err()
    );
    created.seed.validate()?;
    Ok(())
}

#[tokio::test]
async fn created_scope_executes_existing_workflow_and_journal_input_boundaries() -> Result<()> {
    use crate::executor::ExecutionJournal as _;
    use crate::filesystem::{
        FilesystemExecutionJournal, FilesystemTaskRuntime, TaskWorkerAttempt, TaskWorkerOutcome,
    };
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    assert!(matches!(
        fixture.admit_child(created.request.child.clone()).await?,
        Admission::Accepted(_)
    ));
    let task = TaskId::from_bytes([84; 16]);
    let observation = fixture.host.observe_task(task).await?;
    let reservation = observation
        .reservation
        .ok_or_else(|| Error::NotFound("created task lease".into()))?;
    let lease = crate::distributed::WorkLease {
        operation: observation.spec,
        reservation,
        checkpoint: observation.checkpoint,
        operation_revision: observation.revision,
    };
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let original_owner = fixture.host.journal_owner(task, fence.clone()).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let native_work = test_work(metadata_control_budget(), &cancellation)?;
    let pending = original_owner
        .outcome_with_native_work(&native_work)
        .await?;
    assert!(pending.0.is_none());
    assert_eq!(pending.1.bytes(), 0);
    drop(pending);
    let context = fixture
        .harness
        .durable_context(task, created.request.child.operation_id)
        .await?;
    let live_before = native_work.live_allocation_bytes();
    let staged = context
        .stage_file_once_with_native_work(
            OperationId::from_bytes([117; 16]),
            "native/control.txt",
            b"owned",
            "text/plain",
            "control.txt",
            &native_work,
        )
        .await?;
    assert_eq!(staged.0.volume(), &created.private);
    assert!(staged.1.bytes() > 0);
    assert_eq!(
        native_work.live_allocation_bytes(),
        live_before + staged.1.bytes()
    );
    let read = context
        .read_file_with_native_work(&staged.0, &native_work)
        .await?;
    assert_eq!(read.as_ref(), b"owned");
    assert!(native_work.live_allocation_bytes() > live_before + staged.1.bytes());
    drop(read);
    assert_eq!(
        native_work.live_allocation_bytes(),
        live_before + staged.1.bytes()
    );
    drop(staged);
    assert_eq!(native_work.live_allocation_bytes(), live_before);
    let command_payload = context
        .stage_file_once(
            OperationId::from_bytes([102; 16]),
            "ordinary/command.json",
            &crate::contract::canonical_json_bytes(
                &serde_json::json!({"text":"real created-volume command"}),
            )?,
            "application/json",
            "command.json",
        )
        .await?;
    let volume = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "scheduler".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
        &mut None,
    )?;
    let journal = FilesystemExecutionJournal::for_task(
        fixture.host.journal_owner(task, fence.clone()).await?,
        OperationId::from_bytes([103; 16]),
        OperationId::from_bytes([103; 16]),
        fixture.filesystem.clone(),
        &volume,
        &fixture.host.verifier,
        &fixture.host.owner_scope,
        65_536,
        None,
    )?;
    journal.verify_input_file(&command_payload).await?;
    let runtime = super::source_authorization::policy::runtime(&fixture).await?;
    let mut session = runtime.open_task(task, fence).await?;
    session.step(
        OperationId::from_bytes([104; 16]), IdempotencyKey::new("created-workflow-input")?,
        serde_json::json!({"created_command":crate::workflow::WorkflowCommand {
            operation_id:OperationId::from_bytes([105; 16]), kind:"test.created.content.v1".into(),
            payload:command_payload,
        }}),
    ).await?;
    let commands = CreatedIoCommands {
        output: Mutex::new(None),
    };
    assert!(matches!(
        runtime.resume_task(lease, &commands, 2, None).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task:completed }) if completed == task
    ));
    let output = commands
        .output
        .lock()
        .await
        .clone()
        .ok_or_else(|| Error::NotFound("created workflow content result".into()))?;
    assert_eq!(output.volume(), &created.private);
    assert_eq!(
        context.read_file(&output).await?.as_ref(),
        b"real created-volume command".as_ref()
    );
    assert!(
        !fixture.root_scope.grants().contains(
            &created
                .private
                .capability(VolumeOperation::Write, &mut None)?
        )
    );
    assert!(matches!(
        runtime.task_host().outcome(task).await?,
        Some(Outcome::Succeeded(_))
    ));
    let terminal = original_owner
        .outcome_with_native_work(&native_work)
        .await?;
    assert!(matches!(&terminal.0, Some(Outcome::Succeeded(_))));
    assert!(terminal.1.bytes() > 0);
    drop(terminal);
    assert!(native_work.receipt().backend_read_operations > 0);
    assert!(
        original_owner
            .verify(
                true,
                &mut Some(&mut crate::store::history::native_work_io(&native_work)?)
            )
            .await
            .is_err()
    );
    cancellation.cancel();
    let before = native_work.receipt();
    assert!(matches!(
        original_owner.outcome_with_native_work(&native_work).await,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(
        native_work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    Ok(())
}

#[tokio::test]
async fn explicit_unmounted_result_and_original_scratch_proof_use_same_created_scope() -> Result<()>
{
    let fixture = CreationFixture::new().await?;
    let created = fixture
        .create_filesystem_child_with_options(&["fork:publish", "conversation:bind"], true)
        .await?;
    assert_eq!(created.request.resources.len(), 3);
    assert_eq!(created.request.child.resource_proofs.len(), 3);
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024);
    let io = crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?;
    fixture
        .filesystem
        .read_fork_allocation(
            &created.seed,
            &created.private,
            crate::filesystem::fork_preparer::seed_digest(
                &created.seed,
                16 * 1024 * 1024,
                Some(&io),
            )?,
            4 * 1024 * 1024,
            Some(&io),
        )
        .await?;
    assert!(io.receipt().backend_read_operations > 0);
    assert_eq!(io.owned_bytes()?, 0);
    let result = created.seed.child_result_volume.as_ref().unwrap();
    for volume in [&created.private, &created.project] {
        assert_ne!(result, volume);
    }
    assert!(created.seed.child_result_generation.is_some());
    assert!(
        !created
            .seed
            .resources
            .iter()
            .any(|resource| match &resource.revision {
                crate::fork::ResourceRevision::PrivateVolume { volume, .. }
                | crate::fork::ResourceRevision::Project { volume, .. } => volume == result,
                _ => false,
            })
    );
    assert!(
        !fixture
            .root_scope
            .grants()
            .contains(&result.capability(VolumeOperation::Write, &mut None)?)
    );
    fixture.admit_child(created.request.child.clone()).await?;
    let task = TaskId::from_bytes(created.request.child.operation_id.into_bytes());
    let context = fixture
        .harness
        .durable_context(task, created.request.child.operation_id)
        .await?;
    let file = context
        .stage_file(
            OperationId::from_bytes([110; 16]),
            "scratch/control.txt",
            b"scratch",
            "text/plain",
            "control.txt",
        )
        .await?;
    let artifact = context
        .stage_artifact(
            OperationId::from_bytes([111; 16]),
            "artifacts/control.txt",
            b"result",
            "text/plain",
            "control.txt",
        )
        .await?;
    assert_eq!(file.volume(), &created.private);
    assert_eq!(artifact.volume(), result);
    assert_eq!(context.read_file(&artifact).await?.as_ref(), b"result");
    assert_eq!(
        context.scope().current_created_agent(&mut None)?,
        Some(created.seed.child_agent)
    );
    let reservation = fixture.host.observe_task(task).await?.reservation.unwrap();
    let owner = fixture
        .host
        .journal_owner(task, (&reservation).into())
        .await?;
    assert_eq!(
        owner.authenticated_binding_agent(&mut None)?,
        Some(created.seed.child_agent)
    );
    owner.authenticate_fork_parent(&created.seed.child, &mut None)?;
    assert!(
        owner
            .authenticate_fork_parent(fixture.host.verifier.audience(), &mut None)
            .is_err()
    );
    let proof = owner.child_scratch_creation_proof()?;
    assert_eq!(proof.original_task(), task);
    assert_eq!(proof.agent(), created.seed.child_agent);
    assert_eq!(proof.volume(), &created.private);
    assert!(
        fixture
            .host
            .journal_owner(fixture.root, (&fixture.root_lease.reservation).into())
            .await?
            .child_scratch_creation_proof()
            .is_err()
    );
    corrupt_claim(
        &fixture,
        result,
        &["role"],
        serde_json::json!("scratch"),
        "corrupt-result-role",
    )
    .await?;
    assert!(
        context
            .stage_artifact(
                OperationId::from_bytes([112; 16]),
                "artifacts/after-corruption.txt",
                b"must not publish",
                "text/plain",
                "after-corruption.txt"
            )
            .await
            .is_err()
    );
    let rejected = crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?;
    assert!(
        fixture
            .filesystem
            .read_fork_allocation(
                &created.seed,
                result,
                crate::filesystem::fork_preparer::seed_digest(
                    &created.seed,
                    16 * 1024 * 1024,
                    Some(&rejected)
                )?,
                4 * 1024 * 1024,
                Some(&rejected)
            )
            .await
            .is_err()
    );
    assert!(rejected.receipt().backend_read_operations > 0);
    assert_eq!(rejected.owned_bytes()?, 0);
    Ok(())
}

struct CreatedIoCommands {
    output: Mutex<Option<FileRef>>,
}

impl crate::filesystem::TaskCommandHost for CreatedIoCommands {
    fn execute<'a>(
        &'a self,
        context: crate::runtime::TaskContext,
        _: crate::scheduler::LeaseFence,
        command: crate::workflow::WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<crate::filesystem::TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind != "test.created.content.v1" {
                return Err(Error::Unsupported("unknown created-content command".into()));
            }
            let text = payload
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("created-content command lacks text".into()))?;
            let file = context
                .stage_file_once(
                    command.operation_id,
                    "ordinary/worker.txt",
                    text.as_bytes(),
                    "text/plain",
                    "worker.txt",
                )
                .await?;
            *self.output.lock().await = Some(file);
            Ok(crate::filesystem::TaskCommandProgress::Ready(
                serde_json::json!({"written":true}),
            ))
        })
    }
}
