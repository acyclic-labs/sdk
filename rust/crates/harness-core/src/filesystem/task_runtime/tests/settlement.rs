use super::*;

#[tokio::test]
async fn hanging_tool_retains_original_intent_until_explicit_reconciliation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let attempt = runtime
        .resume_task(lease.clone(), &runtime.commands(), 2, None)
        .await;
    assert!(matches!(
        attempt,
        TaskWorkerAttempt::Unresolved {
            error: Error::Indeterminate(_),
            ..
        }
    ));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    let revision = runtime
        .open_task(task, LeaseFence::from(&lease.reservation))
        .await?
        .checkpoint()
        .revision;
    assert!(fixture.scheduler_state(task).await?.outcome.is_none());
    drop(runtime);
    let reopened = fixture.runtime(None).await?;
    assert!(matches!(
        reopened
            .reconcile_task(lease.clone(), &reopened.commands(), 1, 5, None)
            .await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. })
    ));
    let unresolved = fixture.scheduler_state(task).await?;
    assert!(unresolved.outcome.is_none());
    assert_eq!(unresolved.reservation, Some(lease.reservation.clone()));
    fixture.tool.receipt_ready.store(true, Ordering::SeqCst);
    let attempt = reopened
        .reconcile_task(lease.clone(), &reopened.commands(), 1, 1_000, None)
        .await;
    assert!(
        matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task)
    );
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(revision, 1);
    assert!(matches!(
        reopened.task_host().outcome(task).await?,
        Some(Outcome::Cancelled)
    ));
    assert!(fixture.scheduler_state(task).await?.reservation.is_none());
    Ok(())
}

#[tokio::test]
async fn cancelled_mail_reconciles_original_atomic_send_without_republication() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let task = TaskId::from_bytes([104; 16]);
    let payloads = FilesystemContentPublisher::new(
        fixture.filesystem.clone(),
        &(fixture.volume),
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.signed,
        65_536,
        None,
    )?;
    let file = payloads
        .stage(OperationId::from_bytes([106; 16]), "mail-body", b"7", None)?
        .await?;
    let input = serde_json::to_value(&super::super::super::MailSendTaskCommand {
        recipient: task,
        payload: file,
    })
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let lease = fixture
        .admit_command(
            &runtime,
            super::super::super::MAIL_SEND_TASK_COMMAND_KIND,
            input.clone(),
        )
        .await?;
    let attempt = runtime
        .resume_task(lease.clone(), &runtime.commands(), 1, None)
        .await;
    if let TaskWorkerAttempt::Unresolved { error, .. } = attempt {
        return Err(error);
    }
    assert!(matches!(
        attempt,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })
    ));
    let session = runtime
        .open_task(task, LeaseFence::from(&lease.reservation))
        .await?;
    let command = serde_json::from_value(session.checkpoint().state["command"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    drop(session);
    let context = runtime
        .harness()
        .durable_context(task, lease.operation.operation_id, None)
        .await?;
    let acknowledgement = runtime
        .commands()
        .execute(
            context.as_ref().clone(),
            (&lease.reservation).into(),
            command,
            input,
        )
        .await?;
    assert!(matches!(acknowledgement, TaskCommandProgress::Ready(_)));
    drop(acknowledgement);
    assert_eq!(runtime.task_host().inbox(task, 0, 8).await?.len(), 1);
    drop(runtime);
    fixture.clock.0.store(20, Ordering::SeqCst);
    let reopened = fixture.runtime(None).await?;
    assert!(matches!(
        reopened
            .resume_task(lease.clone(), &reopened.commands(), 1, None)
            .await,
        TaskWorkerAttempt::Unresolved {
            error: Error::Indeterminate(_),
            ..
        }
    ));
    assert_eq!(
        fixture.scheduler_state(task).await?.reservation,
        Some(lease.reservation.clone())
    );
    assert!(matches!(
        reopened.reconcile_task(lease, &reopened.commands(), 1, 1_000, None).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task:completed }) if completed == task
    ));
    assert_eq!(reopened.task_host().inbox(task, 0, 8).await?.len(), 1);
    assert!(matches!(
        reopened.task_host().outcome(task).await?,
        Some(crate::Outcome::Cancelled)
    ));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_stock_builtin_precancel_preserves_original_recovery_before_dispatch() -> Result<()>
{
    for kind in [
        super::super::super::MAIL_SEND_TASK_COMMAND_KIND,
        super::super::super::MAIL_RECEIVE_TASK_COMMAND_KIND,
        super::super::super::FORK_PREPARE_TASK_COMMAND_KIND,
        super::super::super::TASK_ADMIT_PREPARED_COMMAND_KIND,
        super::super::super::TASK_CANCEL_COMMAND_KIND,
        super::super::super::EFFECT_TASK_COMMAND_KIND,
    ] {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);

        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let runtime = fixture.runtime(Some(&io)).await?;
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

    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let runtime = fixture.runtime(Some(&io)).await?;
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
        TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
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
    let reopened = fixture.runtime(Some(&io)).await?;
    let (attempt, custody) = reopened
        .reconcile_task_with_native_work(lease, custody, &reopened.commands(), 1, 1_000, &work)
        .await?;
    let lease = match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => {
            assert_eq!(lease, original);
            lease
        }
        TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
        _ => {
            return Err(Error::Invalid(
                "native pending receipt replaced original attempt".into(),
            ));
        }
    };
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    let state = fixture.scheduler_state(task).await?;
    assert!(state.outcome.is_none());
    assert_eq!(state.reservation, Some(original.reservation.clone()));
    fixture.tool.receipt_ready.store(true, Ordering::SeqCst);
    let (attempt, custody) = reopened
        .reconcile_task_with_native_work(lease, custody, &reopened.commands(), 1, 1_000, &work)
        .await?;
    if let TaskWorkerAttempt::Unresolved { error, .. } = attempt {
        return Err(error);
    }
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
    assert!(fixture.scheduler_state(task).await?.reservation.is_none());
    drop(attempt);
    drop(custody);
    Ok(())
}
