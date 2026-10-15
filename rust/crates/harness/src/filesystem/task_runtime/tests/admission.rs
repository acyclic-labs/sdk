use super::*;

#[tokio::test]
async fn native_admission_failure_precedes_post_dispatch_verification_and_deadline_cancellation()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
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
    let commands = NativeAdmissionFailure {
        clock: fixture.clock.clone(),
        work,
        calls: AtomicUsize::new(0),
    };
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    match runtime.resume_task(lease.clone(), &commands, 2, None).await {
        TaskWorkerAttempt::Unresolved {
            lease: original,
            error: Error::NativeWork { cause, custody },
        } => {
            assert_eq!(
                original.operation.operation_id,
                lease.operation.operation_id
            );
            assert_eq!(original.reservation, lease.reservation);
            assert!(matches!(
                cause,
                acyclic_fs::NativeTreeMaterializationError::Work(_)
            ));
            assert!(custody.is_none());
        }
        _ => {
            return Err(Error::Invalid(
                "native first cause or original lease was lost".into(),
            ));
        }
    }
    assert_eq!(commands.calls.load(Ordering::SeqCst), 1);
    assert_eq!(commands.work.receipt().items_examined, 0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    let state = fixture.scheduler_state(task).await?;
    assert!(!state.cancellation_requested);
    assert_eq!(state.reservation, Some(lease.reservation));
    assert!(state.outcome.is_none());
    Ok(())
}

#[tokio::test]
async fn expired_reopened_task_rejects_before_machine_or_tool_dispatch() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    drop(runtime);
    fixture.clock.0.store(20, Ordering::SeqCst);
    let reopened = fixture.runtime(None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let attempt = reopened
        .resume_task(lease.clone(), &reopened.commands(), 2, None)
        .await;
    assert!(matches!(
        attempt,
        TaskWorkerAttempt::Unresolved {
            error: Error::Indeterminate(_),
            ..
        }
    ));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    let state = fixture.scheduler_state(task).await?;
    assert!(state.reservation.is_none());
    assert_eq!(state.outcome, Some(crate::Outcome::Cancelled));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn native_machine_without_callback_rejects_before_owner_deadline_or_provider_work()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    runtime.task_host().start_task(&lease, &mut None).await?;
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
            .step(
                OperationId::from_bytes([118; 16]),
                IdempotencyKey::new("native-unsupported-machine")?,
                Value::Null,
                Some(&crate::filesystem::PreparationIo::from_native_custody(
                    custody
                ))
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
    let state = fixture.scheduler_state(task).await?;
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
            None,
        )
        .await?;
    assert_eq!(session.checkpoint().revision, 1);
    assert_eq!(session.checkpoint().state, original_state);
    assert!(fixture.machine.transitioned.load(Ordering::SeqCst) > 0);
    Ok(())
}

#[test]
fn native_step_identity_denial_precedes_string_growth() -> Result<()> {
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
    let prefix = work.receipt();
    assert!(matches!(
        native_step_key(u64::MAX, &work),
        Err(Error::NativeFilesystem { source, .. }) if matches!(*source,
            acyclic_fs::FsError::NativeKernel(acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. })))
    ));
    assert_eq!(
        work.receipt().allocation_operations,
        prefix.allocation_operations
    );
    assert_eq!(work.receipt().bytes_encoded, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn original_native_worker_precancel_and_metadata_denial_preserve_exact_lease() -> Result<()> {
    for precancel in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime(None).await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            allocation_operations: 4096,
            ..super::native_mail::BUDGET
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
        let state = fixture
            .scheduler_state(TaskId::from_bytes(
                original.operation.operation_id.into_bytes(),
            ))
            .await?;
        assert_eq!(state.reservation, Some(original.reservation));
        assert!(!state.cancellation_requested);
        assert!(state.outcome.is_none());
    }
    Ok(())
}
