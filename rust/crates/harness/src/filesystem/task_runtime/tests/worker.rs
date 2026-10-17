use super::*;

#[tokio::test]
async fn failed_command_settles_with_its_cause_and_only_uncertain_effects_are_indeterminate()
-> Result<()> {
    struct FailedCommand(fn() -> Error);
    impl TaskCommandHost for FailedCommand {
        fn execute<'a>(
            &'a self,
            _: TaskContext,
            _: LeaseFence,
            _: WorkflowCommand,
            _: Value,
        ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
            let error = (self.0)();
            Box::pin(async { Err(error) })
        }
    }
    let (seen, _capture) = crate::obs::capture::install();
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    runtime
        .run_task(
            lease,
            &FailedCommand(|| Error::Unsupported("original command cause".into())),
            2,
        )
        .await?;
    assert!(matches!(
        runtime.task_host().outcome(task).await?,
        Some(Outcome::Failed { message }) if message.contains("original command cause")
    ));
    assert!(seen.lock().unwrap().iter().any(|(_, field, value)| {
        *field == "cause" && value.contains("original command cause")
    }));
    // A wrapped cause keeps its class: a task-file read that lost its acknowledgement is as
    // uncertain as the bare failure, and a wrapped definitive failure still settles Failed.
    fn wrapped(error: Error) -> Error {
        Error::from(crate::task_volume::Error::FileRead(error))
    }
    for (command, uncertain) in [
        (
            FailedCommand(|| Error::Storage("response lost after effect".into())),
            true,
        ),
        (
            FailedCommand(|| {
                wrapped(Error::Indeterminate(crate::OperationId::from_bytes(
                    [7; 16],
                )))
            }),
            true,
        ),
        (
            FailedCommand(|| wrapped(Error::Unsupported("wrapped definitive cause".into()))),
            false,
        ),
    ] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime(None).await?;
        let lease = fixture.admit(&runtime).await?;
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let code = (command.0)().code();
        runtime.run_task(lease, &command, 2).await?;
        let outcome = runtime.task_host().outcome(task).await?;
        assert_eq!(
            matches!(outcome, Some(Outcome::Indeterminate { .. })),
            uncertain,
            "{code:?}: {outcome:?}"
        );
        assert_eq!(
            matches!(outcome, Some(Outcome::Failed { .. })),
            !uncertain,
            "{code:?}: {outcome:?}"
        );
    }
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
