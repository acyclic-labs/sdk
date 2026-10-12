use super::*;

pub(in crate::durable_host::tests) async fn model_claim_host<P: StreamProvider>(
    stream: Arc<P>,
) -> Result<Arc<MemoryPayloads>> {
    let prepared = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: crate::model::Model::new(
                "synthetic".into(),
                "journal-test".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            messages: vec![crate::model::ModelMessage {
                role: crate::model::ModelRole::User,
                content: crate::model::ModelContent::Text("fixture request".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )?;
    let digest = prepared.request_digest();
    let payloads = Arc::new(MemoryPayloads::new()?);
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "model-owner".into(),
    };
    let issuer = AuthorityIssuer::new("model-test", [7; 32], authority.clone());
    let verifier = issuer.verifier(&mut None)?;
    let signed = issuer.issue(
        "owner",
        &Capabilities::new([
            "operation:declare",
            "operation:observe",
            "operation:cancel",
            "task:spawn:test.model@1",
            "model:generate",
            "interaction:route",
        ]),
        None,
        None,
        &mut None,
    )?;
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
    let (task, machine, tasks, machines) = test_registries::<i64>(
        Arc::new(BatchMachine {
            identity: MachineIdentity {
                name: "test.model".into(),
                version: "1".into(),
                digest: [2; 32],
            },
            schema: serde_json::json!({"type":"integer"}),
        }),
        serde_json::json!({"type":"integer"}),
        serde_json::json!({"type":"integer"}),
    )?;
    let host = Arc::new(CoordinatorTaskHost::new(
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
        stream.clone(),
        payloads.clone(),
        payloads.clone(),
        &authority,
        signed.clone(),
        verifier.clone(),
        scope.clone(),
        tasks,
        machines,
        Arc::new(SystemUnixMillisClock),
        None,
    )?);
    let operation = OperationId::from_bytes([77; 16]);
    let task_id = TaskId::from_bytes(operation.into_bytes());
    host.admit(TaskAdmissionRecord {
        resource_proofs: Vec::new(),
        operation_id: operation,
        task,
        machine,
        input: serde_json::json!(0),
        input_schema: serde_json::json!({"type":"integer"}),
        output_schema: serde_json::json!({"type":"integer"}),
        parent: None,
        grants: scope.grants().clone(),
        limits: scope.limits(),
        run_limits: crate::runtime::TaskRunLimits {
            max_steps: Some(1),
            ..scope.run_limits()
        },
        policy: None,
        extensions: None,
        execution: None,
    })
    .await?;
    host.coordinator
        .lock()
        .await
        .configure_session(
            &authority,
            &signed,
            &verifier,
            operation,
            crate::scheduler::SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 0,
                model_steps: 2,
            },
            IdempotencyKey::new("model-session")?,
        )
        .await?;
    let worker = crate::distributed::Worker {
        id: "model-worker".into(),
        available: Default::default(),
        labels: BTreeMap::new(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = host.pull_work(&worker).await? else {
        panic!("model lease")
    };
    host.start_task(&lease, None).await?;
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let attempt = OperationId::from_bytes([78; 16]);
    host.verify_execution_owner(task_id, fence.clone()).await?;
    host.claim_model_dispatch(task_id, attempt, 0, digest, fence.clone())
        .await?;
    for (execution, step, changed) in [
        (attempt, 0, [4; 32]),
        (attempt, 1, [4; 32]),
        (
            crate::executor::ModelPurpose::Summary.attempt_operation(attempt, None)?,
            0,
            [5; 32],
        ),
    ] {
        assert!(
            host.claim_model_dispatch(task_id, execution, step, changed, fence.clone())
                .await
                .is_err()
        );
    }
    host.claim_model_dispatch(task_id, attempt, 0, digest, fence.clone())
        .await?;
    let mut stale = fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(
        host.verify_execution_owner(task_id, stale.clone())
            .await
            .is_err()
    );
    assert!(
        host.claim_model_dispatch(task_id, attempt, 0, digest, stale)
            .await
            .is_err()
    );

    #[cfg(feature = "filesystem")]
    let (
        workflow,
        execution,
        approval,
        volume,
        filesystem,
        content_scope,
        conversation_verifier,
        response_scope,
        model_start,
        observation,
        old_owner,
        pending,
    ) = {
        use crate::executor::{ExecutionEvent, ExecutionJournal};
        use crate::workflow::{DurableWorkflowHost, WorkflowJournal};
        let provider = ProviderRef::new(
            "owned-workflow".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let filesystem =
            crate::filesystem::FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        let volume = VolumeRef::new(
            provider,
            "owned-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?;
        filesystem.create_volume(&volume).await?;
        let content_scope = issuer.issue(
            "workflow-content",
            &Capabilities::new([
                volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                volume.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let old_owner = host.journal_owner(task_id, fence.clone()).await?;
        assert!(
            old_owner
                .validate_storage(&verifier, scope.limits().file_bytes + 1, &mut None)
                .is_err()
        );
        let foreign = AuthorityIssuer::new(
            "foreign-owner",
            [8; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "foreign".into(),
            },
        );
        assert!(
            old_owner
                .validate_storage(&foreign.verifier(&mut None)?, 65_536, &mut None)
                .is_err()
        );
        let admission = old_owner.workflow_admission().clone();
        let workflow = crate::filesystem::FilesystemWorkflowJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        let mut wrong = admission.clone();
        wrong.initial.state = serde_json::json!(1);
        assert!(workflow.admit(wrong).await.is_err());
        assert_eq!(workflow.admit(admission.clone()).await?, admission);
        DurableWorkflowHost::open(host.machines.clone(), admission.initial, workflow.clone())
            .await?
            .step(
                OperationId::from_bytes([85; 16]),
                IdempotencyKey::new("owned-step")?,
                serde_json::json!(0),
                |_| Ok(()),
            )
            .await?;
        let record = workflow.replay(0, 1).await?.pop().unwrap();
        assert_eq!(
            workflow
                .commit(0, record.idempotency_key.clone(), record.clone())
                .await?,
            record
        );
        let mut wrong = record.clone();
        wrong.next.state = serde_json::json!(1);
        wrong.transition.state = serde_json::json!(1);
        assert!(
            workflow
                .commit(0, wrong.idempotency_key.clone(), wrong)
                .await
                .is_err()
        );
        let execution = crate::filesystem::FilesystemExecutionJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            attempt,
            attempt,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        let request = execution
            .stage(
                attempt,
                "model-request".into(),
                prepared.bytes().to_vec(),
                "application/json",
            )
            .await?;
        let start = |step, purpose, request_digest| ExecutionEvent::ModelStarted {
            step,
            purpose,
            request_digest,
            request: request.clone(),
        };
        execution
            .append(
                attempt,
                "request".into(),
                ExecutionEvent::Started {
                    request_digest: [9; 32],
                },
            )
            .await?;
        for (step, claimed) in [(0, [4; 32]), (1, digest)] {
            assert!(
                execution
                    .append(
                        attempt,
                        "denied-model".into(),
                        start(step, crate::executor::ModelPurpose::Response, claimed)
                    )
                    .await
                    .is_err()
            );
        }
        let model_start = start(0, crate::executor::ModelPurpose::Response, digest);
        for _ in 0..2 {
            assert!(
                execution
                    .append_if_tail(attempt, 1, "model".into(), model_start.clone())
                    .await?
            );
        }
        assert!(
            execution
                .append_if_tail(
                    attempt,
                    2,
                    "uncharged-summary".into(),
                    start(0, crate::executor::ModelPurpose::Summary, digest)
                )
                .await
                .is_err()
        );
        assert_eq!(execution.replay(attempt, 0, 64).await?.len(), 2);
        assert!(
            matches!(execution.suspend_workflow_command_if_quiescent(1, attempt, None).await, Err(Error::Indeterminate(id)) if id == attempt)
        );
        let invocation = crate::tool::ToolInvocation::for_model_call(
            attempt,
            0,
            "owned-tool".into(),
            "test".into(),
            serde_json::json!({}),
            None,
        )?;
        let invocation =
            crate::executor::stage_json(&execution, attempt, "tool-invocation", &invocation, None)
                .await?;
        execution
            .append(
                attempt,
                "tool".into(),
                ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id: "owned-tool".into(),
                    invocation,
                },
            )
            .await?;
        assert!(
            matches!(execution.suspend_workflow_command_if_quiescent(1, attempt, None).await, Err(Error::Indeterminate(id)) if id == attempt)
        );
        assert!(execution.replay(operation, 0, 1).await.is_err());
        assert!(matches!(
            execution
                .interaction_outcome(InteractionId::from_bytes([91; 16]))
                .await,
            Err(Error::Unsupported(_))
        ));
        let uncharged = OperationId::from_bytes([79; 16]);
        let unbound = crate::filesystem::FilesystemExecutionJournal::new(
            stream.clone(),
            filesystem.clone(),
            &volume,
            &verifier,
            &crate::core::SchemaRegistry::new(),
            &content_scope,
            65_536,
            None,
        )?;
        unbound
            .append(
                uncharged,
                "request".into(),
                ExecutionEvent::Started {
                    request_digest: [9; 32],
                },
            )
            .await?;
        unbound
            .append(uncharged, "model".into(), model_start.clone())
            .await?;
        let adopted = crate::filesystem::FilesystemExecutionJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            uncharged,
            uncharged,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        assert!(adopted.replay(uncharged, 0, 64).await.is_err());
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: "owned-approval".into(),
        };
        let conversation_issuer =
            AuthorityIssuer::new("owned-approval", [54; 32], conversation.clone());
        let conversation_verifier = conversation_issuer.verifier(&mut None)?;
        let response_scope = conversation_issuer.issue(
            "owner",
            &Capabilities::new([
                "interaction:open".to_owned(),
                volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                volume.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let conversation_scope = conversation_issuer.issue(
            "owner",
            &Capabilities::new(
                response_scope
                    .capabilities()
                    .iter()
                    .chain(["conversation:bind"]),
            ),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let mut aggregate = crate::store::StreamAggregate::open(
            &stream,
            conversation.clone(),
            conversation_verifier.clone(),
            crate::core::SchemaRegistry::new(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        aggregate
            .execute(
                crate::core::Command {
                    operation_id: OperationId::from_bytes([90; 16]),
                    idempotency_key: IdempotencyKey::new("owned-approval-bind")?,
                    expected_revision: 0,
                    scope: conversation_scope.clone(),
                    causal_parent: None,
                    action: crate::core::Action::BindConversation {
                        agent: AgentId::from_bytes([1; 16]),
                    },
                },
                None,
                &mut None,
            )
            .await?;
        assert!(
            unbound
                .with_interaction_owner(
                    &conversation_verifier,
                    &crate::core::SchemaRegistry::new(),
                    &response_scope,
                    &volume,
                    None
                )
                .is_err()
        );
        let approval = crate::filesystem::FilesystemExecutionJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            attempt,
            attempt,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?
        .with_interaction_owner(
            &conversation_verifier,
            &crate::core::SchemaRegistry::new(),
            &conversation_scope,
            &volume,
            None,
        )?;
        let id = InteractionId::from_bytes([91; 16]);
        let approval_request =
            Interaction::approval("approve exact action".into(), attempt, [5; 32])?;
        for _ in 0..2 {
            approval
                .open_interaction(id, approval_request.clone())
                .await?;
        }
        assert_eq!(approval.interaction_outcome(id).await?, None);
        assert!(matches!(
            approval
                .open_interaction(
                    id,
                    Interaction::approval("changed".into(), attempt, [6; 32])?
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let event = crate::executor::stage_json(
            &execution,
            attempt,
            "model-observation",
            &crate::model::ModelEvent::Content {
                delta: "settled".into(),
            },
            None,
        )
        .await?;
        let observation = ExecutionEvent::Model {
            step: 0,
            purpose: crate::executor::ModelPurpose::Response,
            event,
        };
        let mut pending = Vec::new();
        for (path, tail) in [
            (format!("harness/v2/workflows/{operation}"), 1),
            (
                format!("harness/v2/execution/{}", OperationId::from_bytes([80; 16])),
                0,
            ),
            (conversation.stream_path(&mut None)?, 2),
        ] {
            pending.push(
                old_owner
                    .prepare_append_with_work(
                        &acyclic_stream::StreamPath::new(path)?,
                        tail,
                        &StreamKey::new(OwnedBytes::from_static(b"before-replacement"))?,
                        &OwnedBytes::from_static(b"never published"),
                        crate::distributed::JournalWrite::Fresh,
                        &mut None,
                    )
                    .await?
                    .0,
            );
        }
        (
            workflow,
            execution,
            approval,
            volume,
            filesystem,
            content_scope,
            conversation_verifier,
            response_scope,
            model_start,
            observation,
            old_owner,
            pending,
        )
    };
    host.coordinator
        .lock()
        .await
        .apply(
            IdempotencyKey::new("model-release")?,
            crate::scheduler::SchedulerEvent::LeaseReleased {
                operation_id: lease.operation.operation_id,
                fence: crate::scheduler::LeaseFence::from(&lease.reservation),
            },
            &mut None,
        )
        .await?;
    let worker = crate::distributed::Worker {
        id: "replacement-worker".into(),
        ..worker
    };
    let crate::distributed::WorkPull::Claimed(lease) = host.pull_work(&worker).await? else {
        panic!("replacement lease")
    };
    host.start_task(&lease, None).await?;
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    host.claim_model_dispatch(task_id, attempt, 0, digest, fence.clone())
        .await?;
    assert!(
        host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
            .await
            .is_err()
    );
    #[cfg(feature = "filesystem")]
    let (replacement_workflow, replacement_approval, pending) = {
        use crate::executor::ExecutionJournal;
        use crate::workflow::WorkflowJournal;
        for request in pending {
            assert!(matches!(
                stream.commit(request).await?,
                acyclic_stream::CommitOutcome::Conflict(_)
            ));
        }
        assert!(old_owner.verify(false, &mut None).await.is_err());
        let record = workflow.replay(0, 1).await?.pop().unwrap();
        assert!(
            workflow
                .commit(0, record.idempotency_key.clone(), record.clone())
                .await
                .is_err()
        );
        let replacement_workflow = crate::filesystem::FilesystemWorkflowJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        assert_eq!(
            replacement_workflow
                .commit(0, record.idempotency_key.clone(), record.clone())
                .await?,
            record
        );
        let id = InteractionId::from_bytes([91; 16]);
        let request = Interaction::approval("approve exact action".into(), attempt, [5; 32])?;
        assert!(
            approval
                .open_interaction(id, request.clone())
                .await
                .is_err()
        );
        assert!(approval.interaction_outcome(id).await.is_err());
        let replacement_approval = crate::filesystem::FilesystemExecutionJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            attempt,
            attempt,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?
        .with_interaction_owner(
            &conversation_verifier,
            &crate::core::SchemaRegistry::new(),
            &response_scope,
            &volume,
            None,
        )?;
        replacement_approval.open_interaction(id, request).await?;
        assert_eq!(replacement_approval.interaction_outcome(id).await?, None);
        let mut pending = Vec::new();
        for (path, tail) in [
            (format!("harness/v2/workflows/{operation}"), 1),
            (
                format!("harness/v2/execution/{}", OperationId::from_bytes([80; 16])),
                0,
            ),
            (conversation_verifier.audience().stream_path(&mut None)?, 2),
        ] {
            pending.push(
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .prepare_append_with_work(
                        &acyclic_stream::StreamPath::new(path)?,
                        tail,
                        &StreamKey::new(OwnedBytes::from_static(b"before-cancellation"))?,
                        &OwnedBytes::from_static(b"never published"),
                        crate::distributed::JournalWrite::Fresh,
                        &mut None,
                    )
                    .await?
                    .0,
            );
        }
        (replacement_workflow, replacement_approval, pending)
    };
    host.coordinator
        .lock()
        .await
        .cancel_operation(
            &authority,
            &signed,
            &verifier,
            operation,
            IdempotencyKey::new("model-cancel")?,
            false,
            None,
            &mut None,
        )
        .await?;
    host.verify_execution_owner(task_id, fence.clone()).await?;
    assert!(
        host.claim_model_dispatch(task_id, attempt, 0, digest, fence.clone())
            .await
            .is_err()
    );
    #[cfg(feature = "filesystem")]
    {
        use crate::executor::{ExecutionEvent, ExecutionJournal};
        use crate::workflow::WorkflowJournal;
        for request in pending {
            assert!(matches!(
                stream.commit(request).await?,
                acyclic_stream::CommitOutcome::Conflict(_)
            ));
        }
        let path = acyclic_stream::StreamPath::new(format!(
            "harness/v2/execution/{}",
            OperationId::from_bytes([80; 16])
        ))?;
        assert!(matches!(
            stream.bounds(path).await,
            Err(StreamError::NotFound)
        ));
        assert!(
            execution
                .append(attempt, "model".into(), model_start.clone())
                .await
                .is_err()
        );
        let settlement = |call_id: &str| ExecutionEvent::ToolFailed {
            step: 0,
            call_id: call_id.into(),
            reason: crate::executor::ToolFailureKind::ExecutorRejected,
        };
        assert!(
            execution
                .append(
                    attempt,
                    "stale-tool-settlement".into(),
                    settlement("owned-tool")
                )
                .await
                .is_err()
        );
        let reopened = crate::filesystem::FilesystemExecutionJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            attempt,
            attempt,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        assert!(
            reopened
                .append(attempt, "model".into(), model_start)
                .await
                .is_err()
        );
        assert!(
            reopened
                .append(attempt, "missing-tool".into(), settlement("missing"))
                .await
                .is_err()
        );
        for _ in 0..2 {
            reopened
                .append(attempt, "tool-settlement".into(), settlement("owned-tool"))
                .await?;
        }
        assert!(
            reopened
                .append(
                    attempt,
                    "another-tool-settlement".into(),
                    settlement("owned-tool")
                )
                .await
                .is_err()
        );
        for index in 0..70 {
            reopened
                .append(attempt, format!("settlement-{index}"), observation.clone())
                .await?;
        }
        reopened
            .append(attempt, "settlement-69".into(), observation.clone())
            .await?;
        assert_eq!(reopened.replay(attempt, 0, 64).await?.len(), 64);
        assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 10);
        let cold = crate::filesystem::FilesystemExecutionJournal::for_task(
            host.journal_owner(task_id, fence.clone()).await?,
            attempt,
            attempt,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        cold.append(attempt, "settlement-69".into(), observation.clone())
            .await?;
        let mut wrong = observation.clone();
        if let ExecutionEvent::Model { step, .. } = &mut wrong {
            *step = 1;
        }
        assert!(
            cold.append(attempt, "settlement-69".into(), wrong)
                .await
                .is_err()
        );
        assert_eq!(cold.replay(attempt, 64, 64).await?.len(), 10);
        assert!(
            matches!(cold.suspend_workflow_command_if_quiescent(1, attempt, None).await, Err(Error::Indeterminate(id)) if id == attempt)
        );
        let completed = crate::executor::stage_json(
            &cold,
            attempt,
            "model-completed",
            &crate::model::ModelEvent::Completed {
                metadata: Value::Null,
            },
            None,
        )
        .await?;
        cold.append(
            attempt,
            "model-completed".into(),
            ExecutionEvent::Model {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                event: completed,
            },
        )
        .await?;
        assert!(
            cold.append(attempt, "after-completed".into(), observation.clone())
                .await
                .is_err()
        );
        assert!(
            cold.suspend_workflow_command_if_quiescent(1, attempt, None)
                .await
                .is_err()
        );
        cold.append(attempt, "settlement-69".into(), observation)
            .await?;
        assert_eq!(cold.replay(attempt, 64, 64).await?.len(), 11);
        let record = replacement_workflow.replay(0, 1).await?.pop().unwrap();
        assert!(
            replacement_workflow
                .commit(0, record.idempotency_key.clone(), record)
                .await
                .is_err()
        );
        assert!(
            host.journal_owner(task_id, fence.clone())
                .await?
                .verify(false, &mut None)
                .await
                .is_err()
        );
        let id = InteractionId::from_bytes([91; 16]);
        assert!(
            replacement_approval
                .open_interaction(
                    id,
                    Interaction::approval("approve exact action".into(), attempt, [5; 32])?
                )
                .await
                .is_err()
        );
        assert_eq!(replacement_approval.interaction_outcome(id).await?, None);
        for (path, tail) in [
            (format!("harness/v2/workflows/{operation}"), 1),
            (conversation_verifier.audience().stream_path(&mut None)?, 2),
        ] {
            assert_eq!(
                acyclic_stream::Stream::new(stream.clone(), path)?
                    .tail()
                    .await?,
                tail
            );
        }
    }
    Ok(payloads)
}
