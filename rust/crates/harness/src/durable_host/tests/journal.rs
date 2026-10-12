use super::*;

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn owned_task_journal_native_release_requires_original_settlement_and_binding() -> Result<()>
{
    use crate::executor::{ExecutionEvent, ExecutionJournal as _, ToolFailureKind};
    use crate::filesystem::{
        FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
        FilesystemSchedulerPayloadStore, FilesystemWorkflowJournal,
    };
    use crate::workflow::WorkflowJournal as _;

    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let stream = Arc::new(provider);
    let filesystem_provider = ProviderRef::new(
        "native-owned-release".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let filesystem = FilesystemHost::new(acyclic_fs::Fs::memory(), &filesystem_provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        filesystem_provider.clone(),
        "native-owned".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    filesystem.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "effect-task-owner".into(),
    };
    let issuer = AuthorityIssuer::new("effect-task", [51; 32], authority);
    let volume_grants = vec![
        volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
        volume.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
    ];
    let content_scope = issuer
        .issue(
            "native-owned-content",
            &Capabilities::new(volume_grants.clone()),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
        filesystem.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?);
    let reader = FilesystemContentVerifier::new(
        filesystem.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?;
    let (host, owner, runtime_scope) = effect_task_fixture_with_storage(
        stream.clone(),
        "test.native-release",
        volume_grants,
        payloads,
        reader,
        &volume,
    )
    .await?;
    let (task, fence) = owner.task_binding();
    let fence = fence.clone();
    let operation = OperationId::from_bytes(task.into_bytes());
    let attempt = OperationId::from_bytes([54; 16]);
    let workflow = FilesystemWorkflowJournal::for_task(
        owner.clone(),
        filesystem.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?;
    let journal = FilesystemExecutionJournal::for_task(
        owner.clone(),
        attempt,
        attempt,
        filesystem.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?;
    let units = 100_000;
    let bytes = 100_000_000;
    let budget = acyclic_fs::performance::work_budget(units, bytes);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(budget, &cancellation)?;
    assert!(
        !journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    workflow.admit(owner.workflow_admission().clone()).await?;
    assert!(
        !journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    journal
        .append(attempt, "started".into(), ExecutionEvent::Started)
        .await?;
    let invocation = crate::tool::ToolInvocation::for_model_call(
        attempt,
        0,
        "native-owned-tool".into(),
        "test".into(),
        serde_json::json!({}),
        None,
    )?;
    let invocation =
        crate::executor::stage_json(&journal, attempt, "tool-invocation", &invocation, None)
            .await?;
    journal
        .append(
            attempt,
            "tool-started".into(),
            ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "native-owned-tool".into(),
                invocation,
            },
        )
        .await?;
    let machine_identity = owner.workflow_admission().initial.machine.clone();
    let mut workflow_host = crate::workflow::DurableWorkflowHost::open(
        host.machines.clone(),
        owner.workflow_admission().initial.clone(),
        workflow.clone(),
    )
    .await?;
    let transition = workflow_host
        .step_checked_with_native_work(
            OperationId::from_bytes([57; 16]),
            IdempotencyKey::new("native-suspended-step")?,
            serde_json::json!(0),
            &work,
            |_| Ok(()),
        )
        .await?;
    assert!(matches!(transition.0.status, MachineStatus::Suspended));
    assert!(work.receipt().backend_read_operations > 0);
    assert!(work.receipt().backend_write_operations > 0);
    drop(transition);
    assert!(
        !journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    {
        let coordinator = host.coordinator.lock().await;
        let current = coordinator
            .scheduler()
            .operation(operation)
            .ok_or_else(|| Error::NotFound("original active native release task".into()))?;
        assert_eq!(current.phase, crate::scheduler::OperationPhase::Running);
        assert!(current.reservation.is_some());
    }
    journal
        .append(
            attempt,
            "tool-failed".into(),
            ExecutionEvent::ToolFailed {
                step: 0,
                call_id: "native-owned-tool".into(),
                reason: ToolFailureKind::ExecutorRejected,
            },
        )
        .await?;

    let cancelled = acyclic_fs::CancellationToken::new();
    let cancelled_work = test_work(budget, &cancelled)?;
    let cancelled_prefix = cancelled_work.receipt();
    cancelled.cancel();
    assert!(matches!(
        journal
            .release_if_quiescent_with_native_work(&cancelled_work, &workflow)
            .await,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: None
        })
    ));
    assert_eq!(cancelled_work.receipt(), cancelled_prefix);
    let quota_work = test_work(
        acyclic_fs::WorkBudget {
            backend_read_operations: 0,
            ..budget
        },
        &cancellation,
    )?;
    let quota_live = quota_work.live_allocation_bytes();
    assert!(matches!(
        journal
            .release_if_quiescent_with_native_work(&quota_work, &workflow)
            .await,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "backend_read_operations",
                    ..
                }
            ),
            custody: None
        })
    ));
    assert_eq!(quota_work.receipt().backend_read_operations, 0);
    assert_eq!(quota_work.live_allocation_bytes(), quota_live);
    assert!(
        host.coordinator
            .lock()
            .await
            .scheduler()
            .operation(operation)
            .ok_or_else(|| Error::NotFound("quota-refused original task".into()))?
            .reservation
            .is_some()
    );

    {
        let proof_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let pending = workflow
            .prove_release_with_io(&proof_io)
            .await?
            .ok_or_else(|| Error::NotFound("original Suspended source proof".into()))?;
        let execution_tail = acyclic_stream::Stream::new(
            std::sync::Arc::clone(&stream),
            format!("harness/v2/execution/{attempt}"),
        )?
        .tail()
        .await?;
        workflow_host
            .step(
                OperationId::from_bytes([58; 16]),
                IdempotencyKey::new("native-next-suspended-step")?,
                serde_json::json!(0),
                |_| Ok(()),
            )
            .await?;
        assert!(matches!(
            owner
                .release_quiescent_execution_with_native_work(
                    attempt,
                    execution_tail,
                    &pending,
                    &work,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let coordinator = host.coordinator.lock().await;
        let reservation = coordinator
            .scheduler()
            .operation(operation)
            .and_then(|current| current.reservation.as_ref())
            .ok_or_else(|| Error::NotFound("workflow-raced original lease".into()))?;
        assert_eq!(crate::scheduler::LeaseFence::from(reservation), fence);
    }
    {
        let proof_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let pending = workflow
            .prove_release_with_io(&proof_io)
            .await?
            .ok_or_else(|| Error::NotFound("current Suspended source proof".into()))?;
        let execution_tail = acyclic_stream::Stream::new(
            std::sync::Arc::clone(&stream),
            format!("harness/v2/execution/{attempt}"),
        )?
        .tail()
        .await?;
        let invocation = crate::tool::ToolInvocation::for_model_call(
            attempt,
            1,
            "interleaved-native-tool".into(),
            "test".into(),
            serde_json::json!({}),
            None,
        )?;
        let invocation = crate::executor::stage_json(
            &journal,
            attempt,
            "interleaved-tool-invocation",
            &invocation,
            None,
        )
        .await?;
        journal
            .append(
                attempt,
                "interleaved-tool-started".into(),
                ExecutionEvent::ToolStarted {
                    step: 1,
                    call_id: "interleaved-native-tool".into(),
                    invocation,
                },
            )
            .await?;
        assert!(matches!(
            owner
                .release_quiescent_execution_with_native_work(
                    attempt,
                    execution_tail,
                    &pending,
                    &work,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(
            !journal
                .release_if_quiescent_with_native_work(&work, &workflow)
                .await?
        );
        let coordinator = host.coordinator.lock().await;
        let reservation = coordinator
            .scheduler()
            .operation(operation)
            .and_then(|current| current.reservation.as_ref())
            .ok_or_else(|| Error::NotFound("execution-raced original lease".into()))?;
        assert_eq!(crate::scheduler::LeaseFence::from(reservation), fence);
    }
    journal
        .append(
            attempt,
            "interleaved-tool-failed".into(),
            ExecutionEvent::ToolFailed {
                step: 1,
                call_id: "interleaved-native-tool".into(),
                reason: ToolFailureKind::ExecutorRejected,
            },
        )
        .await?;

    let workflow_path = format!("harness/v2/workflows/{operation}");
    let execution_path = format!("harness/v2/execution/{attempt}");
    let workflow_tail =
        acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &workflow_path)?
            .tail()
            .await?;
    let execution_tail =
        acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &execution_path)?
            .tail()
            .await?;
    assert_eq!(workflow_tail, 2);
    assert_eq!(execution_tail, 5);
    assert!(
        journal
            .release_if_quiescent_with_native_work(&work, &workflow)
            .await?
    );
    assert!(
        host.coordinator
            .lock()
            .await
            .scheduler()
            .operation(operation)
            .ok_or_else(|| Error::NotFound("settled original task".into()))?
            .reservation
            .is_none()
    );
    assert_eq!(
        acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &workflow_path)?
            .tail()
            .await?,
        workflow_tail
    );
    assert_eq!(
        acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &execution_path)?
            .tail()
            .await?,
        execution_tail
    );
    assert!(work.receipt().object_bytes_read > 0);

    let worker = crate::distributed::Worker {
        id: "replacement-native-worker".into(),
        available: Default::default(),
        labels: BTreeMap::new(),
    };
    let crate::distributed::WorkPull::Claimed(replacement) =
        host.pull_operation(&worker, operation, &mut None).await?
    else {
        return Err(Error::NotFound("replacement native lease".into()).into());
    };
    host.start_task(&replacement, None).await?;
    let replacement_owner = host
        .journal_owner(
            task,
            crate::scheduler::LeaseFence::from(&replacement.reservation),
        )
        .await?;
    assert_ne!(replacement_owner.task_binding().1, &fence);
    let replacement_workflow = FilesystemWorkflowJournal::for_task(
        replacement_owner.clone(),
        filesystem.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?;
    let denied_prefix = work.receipt();
    assert!(matches!(
        journal
            .release_if_quiescent_with_native_work(&work, &replacement_workflow)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(work.receipt(), denied_prefix);
    let replacement_journal = FilesystemExecutionJournal::for_task(
        replacement_owner.clone(),
        OperationId::from_bytes([55; 16]),
        OperationId::from_bytes([55; 16]),
        filesystem.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?;
    let other_volume = VolumeRef::new(
        filesystem_provider,
        "other-native-owned".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    filesystem.create_volume(&other_volume).await?;
    let other_scope = issuer
        .issue(
            "other-native-content",
            &Capabilities::new(vec![
                other_volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                other_volume.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let other_volume_workflow = FilesystemWorkflowJournal::for_task(
        replacement_owner,
        filesystem.clone(),
        &other_volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &other_scope,
        65_536,
        None,
    )?;
    assert!(matches!(
        replacement_journal
            .release_if_quiescent_with_native_work(&work, &other_volume_workflow)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(work.receipt(), denied_prefix);
    let other_issuer = AuthorityIssuer::new(
        "other-native-owner",
        [91; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "other-native-owner".into(),
        },
    );
    assert!(matches!(
        FilesystemWorkflowJournal::for_task(
            owner,
            filesystem.clone(),
            &volume,
            &other_issuer.verifier(&mut None).expect("verifier copy"),
            &content_scope,
            65_536,
            None,
        ),
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(work.receipt(), denied_prefix);

    let other_operation = OperationId::from_bytes([56; 16]);
    host.admit(TaskAdmissionRecord {
        operation_id: other_operation,
        task: host
            .tasks
            .get::<Value, Value>("test.effect")?
            .identity()
            .clone(),
        machine: machine_identity,
        input: serde_json::json!(0),
        input_schema: serde_json::json!({"type":"integer"}),
        output_schema: serde_json::json!({"type":"integer"}),
        parent: None,
        grants: runtime_scope.grants().clone(),
        limits: runtime_scope.limits(),
        run_limits: runtime_scope.run_limits(),
        policy: None,
        extensions: None,
        execution: None,
    })
    .await?;
    let crate::distributed::WorkPull::Claimed(other_lease) = host
        .pull_operation(&worker, other_operation, &mut None)
        .await?
    else {
        return Err(Error::NotFound("other real native lease".into()).into());
    };
    host.start_task(&other_lease, None).await?;
    let other_owner = host
        .journal_owner(
            TaskId::from_bytes(other_operation.into_bytes()),
            crate::scheduler::LeaseFence::from(&other_lease.reservation),
        )
        .await?;
    let other_workflow = FilesystemWorkflowJournal::for_task(
        other_owner,
        filesystem,
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &content_scope,
        65_536,
        None,
    )?;
    assert!(matches!(
        replacement_journal
            .release_if_quiescent_with_native_work(&work, &other_workflow)
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(work.receipt(), denied_prefix);
    assert!(
        host.coordinator
            .lock()
            .await
            .scheduler()
            .operation(operation)
            .ok_or_else(|| Error::NotFound("binding-refused original lease".into()))?
            .reservation
            .is_some()
    );
    Ok(())
}
