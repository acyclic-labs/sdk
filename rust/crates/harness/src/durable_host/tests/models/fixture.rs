use super::*;

pub(in crate::durable_host::tests) async fn model_claim_host<P: StreamProvider>(
    stream: std::sync::Arc<P>,
) -> Result<Arc<MemoryPayloads>> {
    let prepared_request = crate::model::PreparedModelRequest::prepare(
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
    let model_digest = prepared_request.request_digest();
    let payloads = Arc::new(MemoryPayloads::new()?);
    let owner = Authority {
        kind: AggregateKind::Task,
        id: "model-owner".into(),
    };
    let issuer = AuthorityIssuer::new("model-test", [7; 32], owner.clone());
    let verifier = issuer.verifier(&mut None)?;
    let owner_scope = issuer
        .issue(
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
        )
        .expect("scope proof");
    let scope = RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
    let machine = MachineIdentity {
        name: "test.model".into(),
        version: "1".into(),
        digest: [2; 32],
    };
    let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
        identity: machine.clone(),
        schema: serde_json::json!({"type": "integer"}),
    });
    let (task, machine, tasks, machines) = test_registries::<i64>(
        implementation,
        serde_json::json!({"type": "integer"}),
        serde_json::json!({"type": "integer"}),
    )?;
    let host = Arc::new(CoordinatorTaskHost::new(
        DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
        stream.clone(),
        payloads.clone(),
        payloads.clone(),
        &(owner),
        owner_scope.clone(),
        verifier.clone(),
        scope.clone(),
        tasks,
        machines,
        Arc::new(SystemUnixMillisClock),
        None,
    )?);
    let operation_id = OperationId::from_bytes([77; 16]);
    let task_id = TaskId::from_bytes(operation_id.into_bytes());
    host.admit(TaskAdmissionRecord {
        resource_proofs: Vec::new(),
        operation_id,
        task,
        machine,
        input: serde_json::json!(0),
        input_schema: serde_json::json!({"type": "integer"}),
        output_schema: serde_json::json!({"type": "integer"}),
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
    let fence = {
        let mut coordinator = host.coordinator.lock().await;
        coordinator
            .configure_session(
                &owner,
                &owner_scope,
                &verifier,
                operation_id,
                crate::scheduler::SessionLimits {
                    active_tasks: 1,
                    total_tasks: 1,
                    depth: 0,
                    model_steps: 2,
                },
                IdempotencyKey::new("model-session")?,
            )
            .await?;
        let lease = coordinator
            .pull(&crate::distributed::Worker {
                id: "model-worker".into(),
                available: crate::scheduler::ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            })
            .await?
            .ok_or_else(|| Error::NotFound("model lease".into()))?;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        coordinator
            .apply(
                IdempotencyKey::new("model-start")?,
                crate::scheduler::SchedulerEvent::Started {
                    operation_id,
                    fence: fence.clone(),
                },
                &mut None,
            )
            .await?;
        fence
    };
    host.verify_execution_owner(task_id, fence.clone()).await?;
    let attempt = OperationId::from_bytes([78; 16]);
    host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
        .await?;
    #[cfg(feature = "filesystem")]
    let mut workflow_prepare_io = None;
    #[cfg(feature = "filesystem")]
    let mut execution_prepare_io = None;
    #[cfg(feature = "filesystem")]
    let mut approval_prepare_io = None;
    #[cfg(feature = "filesystem")]
    let (workflow, filesystem, volume, content_scope, original_owner, original_pending) = {
        use crate::workflow::{DurableWorkflowHost, WorkflowJournal};
        let provider = ProviderRef::new(
            "owned-workflow".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let filesystem = Arc::new(crate::filesystem::FilesystemHost::new(
            acyclic_fs::Fs::memory(),
            provider.clone(),
        )?);
        let volume = VolumeRef::new(
            provider,
            "owned-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
        )?;
        filesystem.create_volume(&volume).await?;
        let content_scope = issuer
            .issue(
                "workflow-content",
                &Capabilities::new([
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
                Some(AgentId::from_bytes([1; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
        let original_owner = host.journal_owner(task_id, fence.clone()).await?;
        assert!(
            original_owner
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
            original_owner
                .validate_storage(
                    &foreign.verifier(&mut None).expect("verifier copy"),
                    65_536,
                    &mut None
                )
                .is_err()
        );
        let binding = host.journal_owner(task_id, fence.clone()).await?;
        let admission = binding.workflow_admission().clone();
        let journal = crate::filesystem::FilesystemWorkflowJournal::for_task(
            binding,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        let mut wrong = admission.clone();
        wrong.initial.state = serde_json::json!(1);
        assert!(journal.admit(wrong).await.is_err());
        assert_eq!(journal.admit(admission.clone()).await?, admission);
        let mut workflow =
            DurableWorkflowHost::open(host.machines.clone(), admission.initial, journal.clone())
                .await?;
        workflow
            .step(
                OperationId::from_bytes([85; 16]),
                IdempotencyKey::new("owned-step")?,
                serde_json::json!(0),
                |_| Ok(()),
            )
            .await?;
        let records = journal.replay(0, 1).await?;
        let [record] = records.as_slice() else {
            return Err(Error::Invalid("one owned transition required".into()));
        };
        assert!(matches!(
            journal
                .commit(0, record.idempotency_key.clone(), record.clone())
                .await?,
            crate::workflow::WorkflowCommitOutcome::Replayed(_)
        ));
        let mut forged = record.clone();
        forged.next.state = serde_json::json!(1);
        forged.transition.state = serde_json::json!(1);
        assert!(
            journal
                .commit(0, forged.idempotency_key.clone(), forged)
                .await
                .is_err()
        );
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
        let key = StreamKey::new(OwnedBytes::from_static(b"owned-before-replacement"))?;
        let original_pending = original_owner
            .prepare_append_with_work(
                &path,
                1,
                &key,
                &OwnedBytes::from_static(b"never published"),
                crate::distributed::JournalWrite::Fresh,
                &mut workflow_prepare_io,
            )
            .await?;
        (
            journal,
            filesystem,
            volume,
            content_scope,
            original_owner,
            original_pending,
        )
    };
    #[cfg(feature = "filesystem")]
    let (execution, execution_pending, model_request) = {
        use crate::executor::{ExecutionEvent, ExecutionJournal};
        let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
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
        let model_request = journal
            .stage(
                attempt,
                "model-request".into(),
                prepared_request.bytes().to_vec(),
                "application/json",
            )
            .await?;
        let pending_operation = OperationId::from_bytes([80; 16]);
        let digest = blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
        let key = StreamKey::new(OwnedBytes::copy_from_slice(digest.as_bytes()))?;
        let bytes = OwnedBytes::from(
            serde_json::to_vec(&serde_json::json!({
                "operation_id": pending_operation,
                "turn_operation": pending_operation,
                "version": 2,
                "retry_digest": digest.to_hex().to_string(),
                "event": ExecutionEvent::Started { request_digest: [9; 32] },
            }))
            .map_err(|error| Error::Invalid(error.to_string()))?,
        );
        let path =
            acyclic_stream::StreamPath::new(format!("harness/v2/execution/{pending_operation}"))?;
        let pending = original_owner
            .prepare_append_with_work(
                &path,
                0,
                &key,
                &bytes,
                crate::distributed::JournalWrite::Fresh,
                &mut execution_prepare_io,
            )
            .await?;
        journal
            .append(
                attempt,
                "request".into(),
                ExecutionEvent::Started {
                    request_digest: [9; 32],
                },
            )
            .await?;
        assert!(
            journal
                .append(
                    attempt,
                    "wrong-claim".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        request_digest: [4; 32],
                        request: model_request.clone(),
                    }
                )
                .await
                .is_err()
        );
        assert!(
            journal
                .append(
                    attempt,
                    "absent-claim".into(),
                    ExecutionEvent::ModelStarted {
                        step: 1,
                        purpose: crate::executor::ModelPurpose::Response,
                        request_digest: model_digest,
                        request: model_request.clone(),
                    }
                )
                .await
                .is_err()
        );
        let start = ExecutionEvent::ModelStarted {
            step: 0,
            purpose: crate::executor::ModelPurpose::Response,
            request_digest: model_digest,
            request: model_request.clone(),
        };
        assert!(
            journal
                .append_if_tail(attempt, 1, "model".into(), start.clone())
                .await?
        );
        assert!(
            journal
                .append_if_tail(attempt, 1, "model".into(), start)
                .await?
        );
        assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
        assert!(
            journal
                .append_if_tail(
                    attempt,
                    2,
                    "uncharged-summary".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Summary,
                        request_digest: model_digest,
                        request: model_request.clone(),
                    },
                )
                .await
                .is_err(),
            "a response claim must not authorize a summary admission"
        );
        assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
        assert!(matches!(
            journal.suspend_workflow_command_if_quiescent(1, attempt).await,
            Err(Error::Indeterminate(id)) if id == attempt
        ));
        let invocation = crate::tool::ToolInvocation::for_model_call(
            attempt,
            0,
            "owned-tool".into(),
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
                "tool".into(),
                ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id: "owned-tool".into(),
                    invocation,
                },
            )
            .await?;
        assert!(matches!(
            journal.suspend_workflow_command_if_quiescent(1, attempt).await,
            Err(Error::Indeterminate(id)) if id == attempt
        ));
        assert!(journal.replay(operation_id, 0, 1).await.is_err());
        assert!(matches!(
            journal
                .interaction_outcome(crate::InteractionId::from_bytes([91; 16]))
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
            .append(
                uncharged,
                "model".into(),
                ExecutionEvent::ModelStarted {
                    step: 0,
                    purpose: crate::executor::ModelPurpose::Response,
                    request_digest: model_digest,
                    request: model_request.clone(),
                },
            )
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
        assert!(
            adopted.replay(uncharged, 0, 64).await.is_err(),
            "cold replay must reject a model start lacking its retained charge"
        );
        (journal, pending, model_request)
    };
    host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
        .await?;
    assert!(
        host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
            .await
            .is_err(),
        "the task ceiling is one despite the larger session ceiling"
    );
    assert!(
        host.claim_model_dispatch(
            task_id,
            crate::executor::ModelPurpose::Summary.attempt_operation(attempt, None)?,
            0,
            [5; 32],
            fence.clone(),
        )
        .await
        .is_err(),
        "summary admissions must consume the same exhausted task ceiling"
    );
    assert!(
        host.claim_model_dispatch(task_id, attempt, 0, [4; 32], fence.clone())
            .await
            .is_err()
    );
    #[cfg(feature = "filesystem")]
    let (approval_journal, conversation_issuer, approval_id, approval_request, approval_pending) = {
        use crate::executor::ExecutionJournal;
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: "owned-approval".into(),
        };
        let conversation_issuer =
            AuthorityIssuer::new("owned-approval", [54; 32], conversation.clone());
        let conversation_scope = conversation_issuer
            .issue(
                "owner",
                &Capabilities::new([
                    "conversation:bind".to_owned(),
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
                Some(AgentId::from_bytes([1; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
        let mut aggregate = crate::store::StreamAggregate::open(
            &stream,
            conversation.clone(),
            conversation_issuer
                .verifier(&mut None)
                .expect("verifier copy"),
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
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
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
            &conversation_issuer
                .verifier(&mut None)
                .expect("verifier copy"),
            &crate::core::SchemaRegistry::new(),
            &conversation_scope,
            &volume,
            None,
        )?;
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
        let response_scope = conversation_issuer
            .issue(
                "owner",
                &Capabilities::new([
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
                Some(AgentId::from_bytes([1; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
        assert!(
            matches!(
                unbound.with_interaction_owner(
                    &conversation_issuer
                        .verifier(&mut None)
                        .expect("verifier copy"),
                    &crate::core::SchemaRegistry::new(),
                    &response_scope,
                    &volume,
                    None,
                ),
                Err(Error::Unauthorized(_))
            ),
            "a task storage owner is not a retained task lease"
        );
        let id = crate::InteractionId::from_bytes([91; 16]);
        let request = Interaction::approval("approve exact action".into(), attempt, [5; 32])?;
        journal.open_interaction(id, request.clone()).await?;
        journal.open_interaction(id, request.clone()).await?;
        assert_eq!(journal.interaction_outcome(id).await?, None);
        assert_eq!(
            acyclic_stream::Stream::new(
                std::sync::Arc::clone(&stream),
                conversation.stream_path(&mut None)?
            )?
            .tail()
            .await?,
            2
        );
        assert!(matches!(
            journal
                .open_interaction(
                    id,
                    Interaction::approval("changed".into(), attempt, [6; 32])?
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let pending = original_owner
            .prepare_append_with_work(
                &acyclic_stream::StreamPath::new(conversation.stream_path(&mut None)?)?,
                2,
                &StreamKey::new(OwnedBytes::from_static(b"approval-before-replacement"))?,
                &OwnedBytes::from_static(b"must not publish"),
                crate::distributed::JournalWrite::Fresh,
                &mut approval_prepare_io,
            )
            .await?;
        (journal, conversation_issuer, id, request, pending)
    };
    let mut stale = fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(
        host.verify_execution_owner(task_id, stale.clone())
            .await
            .is_err()
    );
    assert!(
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, stale)
            .await
            .is_err()
    );
    let fence = {
        let mut coordinator = host.coordinator.lock().await;
        coordinator
            .apply(
                IdempotencyKey::new("model-release")?,
                crate::scheduler::SchedulerEvent::LeaseReleased {
                    operation_id,
                    fence: fence.clone(),
                },
                &mut None,
            )
            .await?;
        let lease = coordinator
            .pull(&crate::distributed::Worker {
                id: "replacement-worker".into(),
                available: crate::scheduler::ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            })
            .await?
            .ok_or_else(|| Error::NotFound("replacement model lease".into()))?;
        let replacement = crate::scheduler::LeaseFence::from(&lease.reservation);
        coordinator
            .apply(
                IdempotencyKey::new("model-restart")?,
                crate::scheduler::SchedulerEvent::Started {
                    operation_id,
                    fence: replacement.clone(),
                },
                &mut None,
            )
            .await?;
        replacement
    };
    #[cfg(feature = "filesystem")]
    let (replacement_approval, cancellation_approval_pending) = {
        use crate::executor::ExecutionJournal;
        assert!(matches!(
            approval_pending
                .commit(&stream, Some(operation_id), None, None, None)
                .await?,
            (acyclic_stream::CommitOutcome::Conflict(_), 0)
        ));
        assert!(
            approval_journal
                .open_interaction(approval_id, approval_request.clone())
                .await
                .is_err()
        );
        assert!(
            approval_journal
                .interaction_outcome(approval_id)
                .await
                .is_err()
        );
        let conversation_scope = conversation_issuer
            .issue(
                "owner",
                &Capabilities::new([
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
                Some(AgentId::from_bytes([1; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
        let binding = host.journal_owner(task_id, fence.clone()).await?;
        let pending = binding
            .prepare_append_with_work(
                &acyclic_stream::StreamPath::new(
                    conversation_issuer
                        .verifier(&mut None)
                        .expect("verifier copy")
                        .audience()
                        .stream_path(&mut None)?,
                )?,
                2,
                &StreamKey::new(OwnedBytes::from_static(b"approval-before-cancellation"))?,
                &OwnedBytes::from_static(b"must not publish"),
                crate::distributed::JournalWrite::Fresh,
                &mut approval_prepare_io,
            )
            .await?;
        let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
            binding,
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
            &conversation_issuer
                .verifier(&mut None)
                .expect("verifier copy"),
            &crate::core::SchemaRegistry::new(),
            &conversation_scope,
            &volume,
            None,
        )?;
        journal
            .open_interaction(approval_id, approval_request.clone())
            .await?;
        assert_eq!(journal.interaction_outcome(approval_id).await?, None);
        assert_eq!(
            acyclic_stream::Stream::new(
                std::sync::Arc::clone(&stream),
                conversation_issuer
                    .verifier(&mut None)
                    .expect("verifier copy")
                    .audience()
                    .stream_path(&mut None)?
            )?
            .tail()
            .await?,
            2
        );
        (journal, pending)
    };
    host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
        .await?;
    assert!(
        host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
            .await
            .is_err()
    );
    #[cfg(feature = "filesystem")]
    let (replacement_workflow, pending) = {
        use crate::workflow::WorkflowJournal;
        assert!(matches!(
            original_pending
                .commit(&stream, Some(operation_id), None, None, None)
                .await?,
            (acyclic_stream::CommitOutcome::Conflict(_), 0)
        ));
        assert_eq!(
            acyclic_stream::Stream::new(
                std::sync::Arc::clone(&stream),
                format!("harness/v2/workflows/{operation_id}")
            )?
            .tail()
            .await?,
            1
        );
        let record = workflow
            .replay(0, 1)
            .await?
            .pop()
            .ok_or_else(|| Error::NotFound("owned workflow record".into()))?;
        assert!(
            workflow
                .commit(0, record.idempotency_key.clone(), record.clone())
                .await
                .is_err()
        );
        assert!(original_owner.verify(false, &mut None).await.is_err());
        let binding = host.journal_owner(task_id, fence.clone()).await?;
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
        let key = StreamKey::new(OwnedBytes::from_static(b"owned-pending"))?;
        let pending = binding
            .prepare_append_with_work(
                &path,
                1,
                &key,
                &OwnedBytes::from_static(b"never published"),
                crate::distributed::JournalWrite::Fresh,
                &mut workflow_prepare_io,
            )
            .await?;
        let journal = crate::filesystem::FilesystemWorkflowJournal::for_task(
            binding,
            filesystem.clone(),
            &volume,
            &verifier,
            &content_scope,
            65_536,
            None,
        )?;
        assert!(matches!(
            journal
                .commit(0, record.idempotency_key.clone(), record)
                .await?,
            crate::workflow::WorkflowCommitOutcome::Replayed(_)
        ));
        (journal, pending)
    };
    #[cfg(feature = "filesystem")]
    let execution_pending = {
        assert!(matches!(
            execution_pending
                .commit(&stream, Some(operation_id), None, None, None)
                .await?,
            (acyclic_stream::CommitOutcome::Conflict(_), 0)
        ));
        let pending_operation = OperationId::from_bytes([80; 16]);
        let path =
            acyclic_stream::StreamPath::new(format!("harness/v2/execution/{pending_operation}"))?;
        assert!(matches!(
            stream
                .bounds(acyclic_stream::StreamPath::new(path.as_str())?)
                .await,
            Err(StreamError::NotFound)
        ));
        let digest = blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
        let key = StreamKey::new(OwnedBytes::copy_from_slice(digest.as_bytes()))?;
        let bytes = OwnedBytes::from(
            serde_json::to_vec(&serde_json::json!({
                "operation_id": pending_operation,
                "turn_operation": pending_operation,
                "version": 2,
                "retry_digest": digest.to_hex().to_string(),
                "event": crate::executor::ExecutionEvent::Started { request_digest: [9; 32] },
            }))
            .map_err(|error| Error::Invalid(error.to_string()))?,
        );
        host.journal_owner(task_id, fence.clone())
            .await?
            .prepare_append_with_work(
                &path,
                0,
                &key,
                &bytes,
                crate::distributed::JournalWrite::Fresh,
                &mut execution_prepare_io,
            )
            .await?
    };
    host.coordinator
        .lock()
        .await
        .cancel_operation(
            &owner,
            &owner_scope,
            &verifier,
            operation_id,
            IdempotencyKey::new("model-cancel")?,
            false,
            None,
            &mut None,
        )
        .await?;
    host.verify_execution_owner(task_id, fence.clone()).await?;
    #[cfg(feature = "filesystem")]
    {
        use crate::executor::{ExecutionEvent, ExecutionJournal};
        assert!(
            matches!(
                execution_pending
                    .commit(&stream, Some(operation_id), None, None, None)
                    .await?,
                (acyclic_stream::CommitOutcome::Conflict(_), 0)
            ),
            "cancellation must atomically fence execution publication"
        );
        assert!(matches!(
            stream
                .bounds(acyclic_stream::StreamPath::new(&format!(
                    "harness/v2/execution/{}",
                    OperationId::from_bytes([80; 16])
                ))?)
                .await,
            Err(StreamError::NotFound)
        ));
        assert!(
            execution
                .append(
                    attempt,
                    "model".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        request_digest: model_digest,
                        request: model_request.clone(),
                    }
                )
                .await
                .is_err(),
            "stale lease must reject even an exact retry"
        );
        assert!(
            execution
                .append(
                    attempt,
                    "stale-tool-settlement".into(),
                    ExecutionEvent::ToolFailed {
                        step: 0,
                        call_id: "owned-tool".into(),
                        reason: crate::executor::ToolFailureKind::ExecutorRejected
                    }
                )
                .await
                .is_err(),
            "a replaced lease cannot settle the retained dispatch"
        );
        let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
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
            journal
                .append(
                    attempt,
                    "model".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        request_digest: model_digest,
                        request: model_request.clone(),
                    }
                )
                .await
                .is_err(),
            "cancellation forbids new or retried starts"
        );
        let bytes = crate::contract::canonical_json_bytes(
            &serde_json::to_value(crate::model::ModelEvent::Content {
                delta: "settled".into(),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
        )?;
        let reference = journal
            .stage(
                attempt,
                "model-observation".into(),
                bytes,
                "application/json",
            )
            .await?;
        let observation = ExecutionEvent::Model {
            step: 0,
            purpose: crate::executor::ModelPurpose::Response,
            event: reference.clone(),
        };
        assert!(
            journal
                .append(
                    attempt,
                    "missing-tool".into(),
                    ExecutionEvent::ToolFailed {
                        step: 0,
                        call_id: "missing".into(),
                        reason: crate::executor::ToolFailureKind::ExecutorRejected
                    }
                )
                .await
                .is_err()
        );
        let tool_settlement = ExecutionEvent::ToolFailed {
            step: 0,
            call_id: "owned-tool".into(),
            reason: crate::executor::ToolFailureKind::ExecutorRejected,
        };
        journal
            .append(attempt, "tool-settlement".into(), tool_settlement.clone())
            .await?;
        journal
            .append(attempt, "tool-settlement".into(), tool_settlement.clone())
            .await?;
        assert!(
            journal
                .append(attempt, "another-tool-settlement".into(), tool_settlement)
                .await
                .is_err()
        );
        for index in 0..70 {
            journal
                .append(attempt, format!("settlement-{index}"), observation.clone())
                .await?;
        }
        journal
            .append(attempt, "settlement-69".into(), observation.clone())
            .await?;
        assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 64);
        assert_eq!(journal.replay(attempt, 64, 64).await?.len(), 10);
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
        reopened
            .append(attempt, "settlement-69".into(), observation.clone())
            .await?;
        assert!(
            reopened
                .append(
                    attempt,
                    "settlement-69".into(),
                    ExecutionEvent::Model {
                        step: 1,
                        purpose: crate::executor::ModelPurpose::Response,
                        event: reference,
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 10);
        assert!(matches!(
            reopened.suspend_workflow_command_if_quiescent(1, attempt).await,
            Err(Error::Indeterminate(id)) if id == attempt
        ));
        let completed = crate::executor::stage_json(
            &reopened,
            attempt,
            "model-completed",
            &crate::model::ModelEvent::Completed {
                metadata: Value::Null,
            },
            None,
        )
        .await?;
        reopened
            .append(
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
            reopened
                .append(attempt, "after-completed".into(), observation.clone(),)
                .await
                .is_err()
        );
        assert!(
            reopened
                .suspend_workflow_command_if_quiescent(1, attempt)
                .await
                .is_err()
        );
        reopened
            .append(attempt, "settlement-69".into(), observation)
            .await?;
        assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 11);
    }
    #[cfg(feature = "filesystem")]
    {
        use crate::workflow::WorkflowJournal;
        assert!(matches!(
            pending
                .commit(&stream, Some(operation_id), None, None, None)
                .await?,
            (acyclic_stream::CommitOutcome::Conflict(_), 0)
        ));
        assert_eq!(
            acyclic_stream::Stream::new(
                std::sync::Arc::clone(&stream),
                format!("harness/v2/workflows/{operation_id}")
            )?
            .tail()
            .await?,
            1
        );
        let record = replacement_workflow
            .replay(0, 1)
            .await?
            .pop()
            .ok_or_else(|| Error::NotFound("cancelled workflow record".into()))?;
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
    }
    #[cfg(feature = "filesystem")]
    {
        use crate::executor::ExecutionJournal;
        assert!(matches!(
            cancellation_approval_pending
                .commit(&stream, Some(operation_id), None, None, None)
                .await?,
            (acyclic_stream::CommitOutcome::Conflict(_), 0)
        ));
        assert!(
            replacement_approval
                .open_interaction(approval_id, approval_request)
                .await
                .is_err()
        );
        assert_eq!(
            replacement_approval
                .interaction_outcome(approval_id)
                .await?,
            None
        );
        assert_eq!(
            acyclic_stream::Stream::new(
                std::sync::Arc::clone(&stream),
                conversation_issuer
                    .verifier(&mut None)
                    .expect("verifier copy")
                    .audience()
                    .stream_path(&mut None)?
            )?
            .tail()
            .await?,
            2
        );
    }
    assert!(
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence)
            .await
            .is_err()
    );
    Ok(payloads)
}
