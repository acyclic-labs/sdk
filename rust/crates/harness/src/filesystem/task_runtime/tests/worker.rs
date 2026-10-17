use super::*;

#[tokio::test]
async fn indeterminate_command_traces_the_underlying_failure() -> Result<()> {
    struct FailedCommand;
    impl TaskCommandHost for FailedCommand {
        fn execute<'a>(
            &'a self,
            _: TaskContext,
            _: LeaseFence,
            _: WorkflowCommand,
            _: Value,
        ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
            Box::pin(async { Err(Error::Unsupported("original command cause".into())) })
        }
    }
    let (seen, _capture) = crate::obs::capture::install();
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    runtime.run_task(lease, &FailedCommand, 2).await?;
    assert!(matches!(
        runtime.task_host().outcome(task).await?,
        Some(Outcome::Indeterminate { .. })
    ));
    assert!(seen.lock().unwrap().iter().any(|(_, field, value)| {
        *field == "cause" && value.contains("original command cause")
    }));
    Ok(())
}

#[tokio::test]
async fn native_stock_worker_stale_fence_and_expired_admission_precede_provider_work() -> Result<()>
{
    for stale in [true, false] {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let runtime = fixture.runtime(Some(&io)).await?;
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
