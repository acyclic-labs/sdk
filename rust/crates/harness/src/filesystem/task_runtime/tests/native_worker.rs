//! Native worker recovery and original ownership.

use super::*;

#[tokio::test]
async fn native_machine_without_callback_rejects_before_owner_deadline_or_provider_work()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    runtime.task_host().start_task(&lease, None).await?;
    let mut session = runtime.open_task(task, fence).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let custody = work
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(Error::from)?;
    let prefix = work.receipt();
    fixture.clock.0.store(20, Ordering::SeqCst);
    assert!(matches!(
        session
            .step_with_native_work(
                OperationId::from_bytes([118; 16]),
                IdempotencyKey::new("native-unsupported-machine")?,
                Value::Null,
                custody,
            )
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert_eq!(work.receipt(), prefix);
    assert_eq!(session.checkpoint().revision, 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    let state = runtime.task_host().observe_task(task).await?;
    assert!(!state.cancellation_requested);
    assert_eq!(state.reservation, Some(lease.reservation));
    assert!(state.outcome.is_none());
    fixture.clock.0.store(10, Ordering::SeqCst);
    let original_state = session.checkpoint().state.clone();
    session
        .step(
            OperationId::from_bytes([119; 16]),
            IdempotencyKey::new("ordinary-custom-machine")?,
            Value::Null,
        )
        .await?;
    assert_eq!(session.checkpoint().revision, 1);
    assert_eq!(session.checkpoint().state, original_state);
    assert!(fixture.machine.transitioned.load(Ordering::SeqCst) > 0);
    Ok(())
}

#[tokio::test]
async fn original_native_worker_precancel_and_metadata_denial_preserve_exact_lease() -> Result<()> {
    for precancel in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            allocation_operations: 4096,
            ..Default::default()
        };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (lease, recovery_custody) = fixture.native_lease(&runtime, &work).await?;
        let original = lease.clone();
        let recovered_bytes = recovery_custody.bytes();
        if !precancel {
            while work.receipt().allocation_operations < budget.allocation_operations {
                let text = work.display(&"allocation denial control")?;
                drop(text);
            }
        }
        let before = work.receipt();
        if precancel {
            cancellation.cancel();
        }
        let commands = runtime.commands();
        let result = runtime
            .resume_task_with_native_work(lease, recovery_custody, &commands, 4, &work)
            .await;
        match result {
            Ok((TaskWorkerAttempt::Unresolved { lease, error }, custody)) => {
                assert_eq!(lease, original);
                assert!(matches!(error, Error::NativeWork { .. }));
                assert!(custody.work().shares_ledger(&work));
                assert!(custody.bytes() >= recovered_bytes);
                drop(lease);
                drop(error);
                drop(custody);
            }
            _ => {
                return Err(Error::Invalid(
                    "native worker denial lost original attempt".into(),
                ));
            }
        }
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations
        );
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations
        );
        assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
        let state = runtime
            .task_host()
            .observe_task(TaskId::from_bytes(
                original.operation.operation_id.into_bytes(),
            ))
            .await?;
        assert_eq!(state.reservation, Some(original.reservation));
        assert!(!state.cancellation_requested);
        assert!(state.outcome.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_builtin_precancel_preserves_original_recovery_before_dispatch() -> Result<()>
{
    for kind in [
        crate::filesystem::MAIL_SEND_TASK_COMMAND_KIND,
        crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND,
        crate::filesystem::FORK_PREPARE_TASK_COMMAND_KIND,
        crate::filesystem::TASK_ADMIT_PREPARED_COMMAND_KIND,
        crate::filesystem::TASK_CANCEL_COMMAND_KIND,
        crate::filesystem::EFFECT_TASK_COMMAND_KIND,
    ] {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        let runtime = fixture.runtime().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::default(),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (lease, custody) = fixture
            .native_lease_command(&runtime, kind, Value::Null, &work)
            .await?;
        let original = lease.clone();
        let bytes = custody.bytes();
        cancellation.cancel();
        let before = work.receipt();
        let (attempt, custody) = runtime
            .resume_task_with_native_work(lease, custody, &runtime.commands(), 2, &work)
            .await?;
        match attempt {
            TaskWorkerAttempt::Unresolved { lease, error } => {
                assert_eq!(lease, original);
                assert!(matches!(error, Error::NativeWork { .. }));
                assert!(error.is_native_failure());
                drop(lease);
                drop(error);
            }
            _ => {
                return Err(Error::Invalid(
                    "stock builtin replaced original precancelled recovery".into(),
                ));
            }
        }
        assert!(custody.work().shares_allocation_context(&work));
        assert!(custody.bytes() >= bytes);
        assert_eq!(work.receipt(), before);
        assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        drop(custody);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_tool_lost_ack_reconciles_original_fence_without_redispatch() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    fixture.tool.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (lease, custody) = fixture.native_lease(&runtime, &work).await?;
    let original = lease.clone();
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let (attempt, custody) = runtime
        .resume_task_with_native_work(lease, custody, &runtime.commands(), 2, &work)
        .await?;
    let lease = match attempt {
        TaskWorkerAttempt::Unresolved {
            lease,
            error: Error::Indeterminate(operation),
        } => {
            assert_eq!(operation, original.operation.operation_id);
            assert_eq!(lease, original);
            lease
        }
        _ => {
            return Err(Error::Invalid(
                "native lost acknowledgement did not preserve original attempt".into(),
            ));
        }
    };
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(custody.work().shares_ledger(&work));
    let before = work.receipt();
    drop(runtime);
    let reopened = fixture.runtime().await?;
    let (attempt, custody) = reopened
        .reconcile_task_with_native_work(lease, custody, &reopened.commands(), 1, 5, &work)
        .await?;
    let lease = match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => {
            assert_eq!(lease, original);
            lease
        }
        _ => {
            return Err(Error::Invalid(
                "native pending receipt replaced original attempt".into(),
            ));
        }
    };
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    let state = reopened.task_host().observe_task(task).await?;
    assert!(state.outcome.is_none());
    assert_eq!(state.reservation, Some(original.reservation.clone()));
    fixture.tool.receipt_ready.store(true, Ordering::SeqCst);
    let (attempt, custody) = reopened
        .reconcile_task_with_native_work(lease, custody, &reopened.commands(), 1, 1_000, &work)
        .await?;
    assert!(
        matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task)
    );
    assert!(custody.work().shares_ledger(&work));
    assert!(work.receipt().backend_read_operations > before.backend_read_operations);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(matches!(
        reopened.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    ));
    assert!(
        reopened
            .task_host()
            .observe_task(task)
            .await?
            .reservation
            .is_none()
    );
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_stock_worker_stale_fence_and_expired_admission_precede_provider_work() -> Result<()>
{
    for stale in [true, false] {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let runtime = fixture.runtime().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::default(),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (mut lease, custody) = fixture.native_lease(&runtime, &work).await?;
        if stale {
            lease.reservation.id.replace_range(0..1, "x");
        } else {
            fixture.clock.0.store(20, Ordering::SeqCst);
        }
        let original = lease.clone();
        let (attempt, custody) = runtime
            .resume_task_with_native_work(lease, custody, &runtime.commands(), 2, &work)
            .await?;
        match attempt {
            TaskWorkerAttempt::Unresolved { lease, error } => {
                assert_eq!(lease, original);
                if stale {
                    assert!(error.is_native_failure());
                } else {
                    assert!(matches!(error, Error::Indeterminate(_)));
                }
                drop(lease);
                drop(error);
            }
            _ => {
                return Err(Error::Invalid(
                    "native stale or expired work escaped original attempt".into(),
                ));
            }
        }
        assert!(custody.work().shares_ledger(&work));
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
        drop(custody);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_model_original_lost_ack_uses_native_provider_and_settlement() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let model = Model::new(
        "synthetic".into(),
        "deadline".into(),
        "1".into(),
        Value::Null,
    )?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let input = crate::filesystem::ModelTaskCommand {
        input: crate::model::ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 1,
        max_output_tokens: None,
        selected_output_volume: None,
        mailbox_operation: None,
    };
    let (lease, custody) = fixture
        .native_lease_command(
            &runtime,
            crate::filesystem::MODEL_TASK_COMMAND_KIND,
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?,
            &work,
        )
        .await?;
    let original = lease.clone();
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let commands = runtime.commands().with_model(
        &model,
        provider.clone(),
        &context,
        &compaction,
        Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
    )?;
    let (attempt, custody) = runtime
        .resume_task_with_native_work(lease, custody, &commands, 2, &work)
        .await?;
    let lease = match attempt {
        TaskWorkerAttempt::Unresolved {
            lease,
            error: Error::Indeterminate(_),
        } => {
            assert_eq!(lease, original);
            lease
        }
        _ => {
            return Err(Error::Invalid(
                "native model lost acknowledgement did not preserve original lease".into(),
            ));
        }
    };
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    let before = work.receipt();
    drop(commands);
    drop(runtime);
    let reopened = fixture.runtime().await?;
    let commands = reopened.commands().with_model(
        &model,
        provider.clone(),
        &context,
        &compaction,
        Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
    )?;
    let (attempt, custody) = reopened
        .reconcile_task_with_native_work(lease, custody, &commands, 1, 1_000, &work)
        .await?;
    assert!(
        matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task)
    );
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(work.receipt().backend_read_operations > before.backend_read_operations);
    assert!(custody.work().shares_ledger(&work));
    assert!(matches!(
        reopened.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    ));
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let admission = reopened.admission_observed(task, Some(&io)).await?;
    let witness = reopened
        .readonly_workflow_journal(task, &admission, &work)
        .await?;
    let records =
        crate::workflow::WorkflowJournal::replay_with_native_work(witness.as_ref(), 0, 64, &work)?
            .await?;
    assert!(!records.0.is_empty());
    assert!(records.1.work().shares_ledger(&work));
    drop(records);
    drop(witness);
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_model_selected_writer_requires_original_created_binding_before_provider()
-> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let input = crate::filesystem::ModelTaskCommand {
        input: crate::model::ModelContent::Text("must not dispatch".into()),
        selected_context: None,
        max_steps: 1,
        max_output_tokens: None,
        selected_output_volume: Some(fixture.volume.clone()),
        mailbox_operation: None,
    };
    let payload = serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let model = Model::new(
        "synthetic".into(),
        "deadline".into(),
        "1".into(),
        Value::Null,
    )?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let commands = runtime.commands().with_model(
        &model,
        provider.clone(),
        &context,
        &compaction,
        Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
    )?;
    let (lease, custody) = fixture
        .native_lease_command(
            &runtime,
            crate::filesystem::MODEL_TASK_COMMAND_KIND,
            payload,
            &work,
        )
        .await?;
    let (attempt, custody) = runtime
        .resume_task_with_native_work(lease, custody, &commands, 2, &work)
        .await?;
    assert!(matches!(
        &attempt,
        TaskWorkerAttempt::Unresolved {
            error: Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            },
            ..
        }
    ));
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert!(custody.work().shares_ledger(&work));
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_stock_model_configuration_denies_before_copy_or_provider_on_original_work()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let model = Model::new(
        "synthetic".into(),
        "original-config".into(),
        "1".into(),
        serde_json::json!({"configured": true}),
    )?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::WorkBudget::default();
        if !cancelled {
            budget.peak_allocation_bytes =
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
        }
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        if cancelled {
            cancellation.cancel();
        }
        let before = work.receipt();
        let result = runtime.commands().with_model(
            &model,
            provider.clone(),
            &context,
            &compaction,
            Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
        );
        assert!(result.as_ref().is_err_and(Error::is_native_failure));
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations
        );
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
        assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
        drop(result);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_model_configuration_retains_original_custody_and_rejects_foreign_rebinding()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let model = Model::new(
        "synthetic".into(),
        "original-config".into(),
        "1".into(),
        json!({"configured": "retained"}),
    )?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = work.receipt();
    let commands = runtime.commands().with_model(
        &model,
        provider.clone(),
        &context,
        &compaction,
        Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
    )?;
    assert!(work.receipt().allocation_operations > before.allocation_operations);
    assert!(work.receipt().bytes_copied > before.bytes_copied);
    let foreign = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = foreign.receipt();
    let result = commands.with_model(
        &model,
        provider.clone(),
        &context,
        &compaction,
        Some(&crate::filesystem::PreparationIo::from_native_work(
            &foreign,
        )?),
    );
    assert!(matches!(
        result,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert_eq!(foreign.receipt(), before);
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn genuine_stock_native_context_preserves_mcp_current_and_settlement_source() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::default(),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (lease, recovery_custody) = fixture.native_lease(&runtime, &work).await?;
    runtime.task_host().start_task(&lease, Some(&work)).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let shared = runtime
        .task_context_with_native_work(task, &fence, &work)
        .await?;
    assert!(shared.as_ref().original_task_binding_matches(task, &fence));
    let operation = lease.operation.operation_id;
    let call_id = work.display(&operation)?;
    let tool =
        ToolContext::new_with_native_work(Arc::clone(&shared), operation, &call_id.0, &work)?;
    drop(call_id);
    let before = work.receipt();
    tool.validate_native_mcp_context(&work)?;
    tool.validate_native_mcp_reconciliation_context(&work)?;
    assert_eq!(work.receipt(), before);
    let read = runtime
        .harness()
        .durable_context_with_native_work(task, operation, &work)
        .await?;
    let read = read.0.into_native_shared(read.1, &work)?;
    let read_tool =
        ToolContext::new_with_native_work(read, operation, "read-context-control", &work)?;
    assert!(matches!(
        read_tool.validate_native_mcp_context(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    assert!(matches!(
        read_tool.validate_native_mcp_reconciliation_context(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    fixture.clock.0.store(20, Ordering::SeqCst);
    runtime
        .task_host()
        .expire_task_deadline(
            task,
            Some(&fence),
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&work)?,
            )),
        )
        .await?;
    let settlement = runtime
        .task_context_for_original_owner_with_native_work(task, &fence, true, &work)
        .await?;
    let settlement_tool = ToolContext::new_with_native_work(
        settlement,
        operation,
        "settlement-context-control",
        &work,
    )?;
    assert!(matches!(
        settlement_tool.validate_native_mcp_context(&work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    settlement_tool.validate_native_mcp_reconciliation_context(&work)?;
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    drop(settlement_tool);
    drop(read_tool);
    drop(tool);
    drop(shared);
    drop(lease);
    drop(recovery_custody);
    Ok(())
}
