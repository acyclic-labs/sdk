use super::*;
use crate::executor::{ExecutionEvent, ExecutionJournal, ModelPurpose, ToolFailureKind};
use crate::workflow::{DurableWorkflowHost, WorkflowJournal};

#[tokio::test]
async fn b13_model_claims_use_pinned_limits_and_fence() -> Result<()> {
    let f = fixture::fixture().await?;
    let mut record = f.admission(1, json!(0));
    record.run_limits.max_steps = Some(1);
    f.host.admit(record).await?;
    f.host
        .coordinator
        .lock()
        .await
        .configure_session(
            f.issuer.audience(),
            &f.signed,
            &f.issuer.verifier(&mut None)?,
            OperationId::from_bytes([1; 16]),
            crate::scheduler::SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 0,
                model_steps: 2,
            },
            IdempotencyKey::new("model-session")?,
        )
        .await?;
    let owner = f.claim(1, "model").await?;
    let task = TaskId::from_bytes([1; 16]);
    let fence = crate::scheduler::LeaseFence::from(&owner.reservation);
    let attempt = OperationId::from_bytes([2; 16]);
    for _ in 0..2 {
        f.host
            .claim_model_dispatch(task, attempt, 0, [3; 32], fence.clone())
            .await?;
    }
    for (id, step, digest) in [
        (attempt, 0, [4; 32]),
        (attempt, 1, [4; 32]),
        (
            ModelPurpose::Summary.attempt_operation(attempt, None)?,
            0,
            [5; 32],
        ),
    ] {
        assert!(
            f.host
                .claim_model_dispatch(task, id, step, digest, fence.clone())
                .await
                .is_err()
        );
    }
    let mut stale = fence;
    stale.reservation_id.push_str("-stale");
    assert!(
        f.host
            .verify_execution_owner(task, stale.clone())
            .await
            .is_err()
    );
    assert!(
        f.host
            .claim_model_dispatch(task, attempt, 0, [3; 32], stale)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn b14_workflow_pins_admission_and_transition() -> Result<()> {
    let f = fixture::fixture().await?;
    let owner = f.running(1).await?;
    let admission = owner.workflow_admission().clone();
    let workflow = f.workflow(owner)?;
    let mut wrong = admission.clone();
    wrong.initial.state = json!(1);
    assert!(workflow.admit(wrong).await.is_err());
    assert_eq!(workflow.admit(admission.clone()).await?, admission);
    DurableWorkflowHost::open(f.machines.clone(), admission.initial, workflow.clone())
        .await?
        .step(
            OperationId::from_bytes([2; 16]),
            IdempotencyKey::new("step")?,
            json!(0),
            |_| Ok(()),
        )
        .await?;
    let record = workflow.replay(0, 1).await?.pop().unwrap();
    assert!(matches!(
        workflow
            .commit(0, record.idempotency_key.clone(), record.clone())
            .await?,
        crate::workflow::WorkflowCommitOutcome::Replayed(_)
    ));
    let mut wrong = record.clone();
    wrong.next.state = json!(1);
    wrong.transition.state = json!(1);
    assert!(
        workflow
            .commit(0, wrong.idempotency_key.clone(), wrong)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn b15_replacement_rejects_stale_publications() -> Result<()> {
    let f = fixture::fixture().await?;
    let old = f.running(1).await?;
    let admission = old.workflow_admission().clone();
    let workflow = f.workflow(f.owner(1).await?)?;
    workflow.admit(admission.clone()).await?;
    DurableWorkflowHost::open(f.machines.clone(), admission.initial, workflow.clone())
        .await?
        .step(
            OperationId::from_bytes([2; 16]),
            IdempotencyKey::new("step")?,
            json!(0),
            |_| Ok(()),
        )
        .await?;
    let record = workflow.replay(0, 1).await?.pop().unwrap();
    let replacement = f.replace(1).await?;
    assert!(old.verify(false, &mut None).await.is_err());
    assert!(
        workflow
            .commit(0, record.idempotency_key.clone(), record.clone())
            .await
            .is_err()
    );
    assert!(matches!(
        f.workflow(replacement)?
            .commit(0, record.idempotency_key.clone(), record)
            .await?,
        crate::workflow::WorkflowCommitOutcome::Replayed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn b16_cancelled_owner_settles_existing_tool_once() -> Result<()> {
    let f = fixture::fixture().await?;
    f.running(1).await?;
    let operation = OperationId::from_bytes([2; 16]);
    let journal = f.execution(f.owner(1).await?, operation)?;
    journal
        .append(
            operation,
            "started".into(),
            ExecutionEvent::Started {
                request_digest: [9; 32],
            },
        )
        .await?;
    let invocation = crate::tool::ToolInvocation::for_model_call(
        operation,
        0,
        "tool".into(),
        "test".into(),
        json!({}),
        None,
    )?;
    let invocation =
        crate::executor::stage_json(&journal, operation, "invocation", &invocation, None).await?;
    journal
        .append(
            operation,
            "tool".into(),
            ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "tool".into(),
                invocation,
            },
        )
        .await?;
    f.host.cancel(TaskId::from_bytes([1; 16])).await?;
    assert!(
        journal
            .append(
                operation,
                "fresh".into(),
                ExecutionEvent::Started {
                    request_digest: [8; 32]
                }
            )
            .await
            .is_err()
    );
    let settlement = f.execution(f.owner(1).await?, operation)?;
    let event = ExecutionEvent::ToolFailed {
        step: 0,
        call_id: "tool".into(),
        reason: ToolFailureKind::ExecutorRejected,
    };
    for _ in 0..2 {
        settlement
            .append(operation, "settle".into(), event.clone())
            .await?;
    }
    assert!(
        settlement
            .append(operation, "another-settlement".into(), event)
            .await
            .is_err()
    );
    assert!(
        settlement
            .append(
                operation,
                "missing".into(),
                ExecutionEvent::ToolFailed {
                    step: 0,
                    call_id: "missing".into(),
                    reason: ToolFailureKind::ExecutorRejected
                }
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn b17_seventy_event_replay_and_completion() -> Result<()> {
    let f = fixture::fixture().await?;
    let owner = f.running(1).await?;
    let (task, fence) = owner.task_binding();
    let attempt = OperationId::from_bytes([2; 16]);
    let request = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: crate::model::Model::new(
                "synthetic".into(),
                "journal".into(),
                "1".into(),
                json!({}),
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
    f.host
        .coordinator
        .lock()
        .await
        .configure_session(
            f.issuer.audience(),
            &f.signed,
            &f.issuer.verifier(&mut None)?,
            OperationId::from_bytes([1; 16]),
            crate::scheduler::SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 0,
                model_steps: 2,
            },
            IdempotencyKey::new("model-session")?,
        )
        .await?;
    f.host
        .claim_model_dispatch(task, attempt, 0, request.request_digest(), fence.clone())
        .await?;
    let journal = f.execution(f.owner(1).await?, attempt)?;
    let reference = journal
        .stage(
            attempt,
            "request".into(),
            request.bytes().to_vec(),
            "application/json",
        )
        .await?;
    journal
        .append(
            attempt,
            "started".into(),
            ExecutionEvent::Started {
                request_digest: [9; 32],
            },
        )
        .await?;
    journal
        .append(
            attempt,
            "model".into(),
            ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: request.request_digest(),
                request: reference,
            },
        )
        .await?;
    let event = crate::executor::stage_json(
        &journal,
        attempt,
        "content",
        &crate::model::ModelEvent::Content {
            delta: "settled".into(),
        },
        None,
    )
    .await?;
    let observation = ExecutionEvent::Model {
        step: 0,
        purpose: ModelPurpose::Response,
        event,
    };
    f.host.cancel(task).await?;
    let journal = f.execution(f.owner(1).await?, attempt)?;
    for index in 0..70 {
        journal
            .append(attempt, format!("settlement-{index}"), observation.clone())
            .await?;
    }
    let cold = f.execution(f.owner(1).await?, attempt)?;
    cold.append(attempt, "settlement-69".into(), observation.clone())
        .await?;
    assert_eq!(cold.replay(attempt, 0, 64).await?.len(), 64);
    assert_eq!(cold.replay(attempt, 64, 64).await?.len(), 8);
    let mut wrong = observation.clone();
    if let ExecutionEvent::Model { step, .. } = &mut wrong {
        *step = 1;
    }
    assert!(
        cold.append(attempt, "settlement-69".into(), wrong)
            .await
            .is_err()
    );
    let completed = crate::executor::stage_json(
        &cold,
        attempt,
        "completed",
        &crate::model::ModelEvent::Completed {
            metadata: Value::Null,
        },
        None,
    )
    .await?;
    cold.append(
        attempt,
        "completed".into(),
        ExecutionEvent::Model {
            step: 0,
            purpose: ModelPurpose::Response,
            event: completed,
        },
    )
    .await?;
    assert!(
        cold.append(attempt, "after-completed".into(), observation.clone())
            .await
            .is_err()
    );
    cold.append(attempt, "settlement-69".into(), observation)
        .await?;
    assert_eq!(cold.replay(attempt, 64, 64).await?.len(), 9);
    Ok(())
}

#[tokio::test]
#[cfg(feature = "filesystem-local")]
async fn b18_local_stream_reopen_keeps_cancelled_owner() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let f = Fixture::new(
        Arc::new(provider),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let backend = f.backend.clone();
    f.running(1).await?;
    f.host.cancel(TaskId::from_bytes([1; 16])).await?;
    drop(f);
    let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let f = Fixture::new(
        Arc::new(provider),
        backend,
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    assert!(
        f.host
            .cancellation_requested(TaskId::from_bytes([1; 16]), None)
            .await?
    );
    let crate::distributed::WorkPull::Unresolved { lease, .. } = f
        .host
        .recover_work(TaskId::from_bytes([1; 16]), &mut None)
        .await?
    else {
        return Err(Error::Conflict("cancelled owner became runnable".into()));
    };
    assert!(f.host.start_task(&lease, None).await.is_err());
    f.host
        .journal_owner(TaskId::from_bytes([1; 16]), (&lease.reservation).into())
        .await?
        .verify(true, &mut None)
        .await?;
    Ok(())
}
