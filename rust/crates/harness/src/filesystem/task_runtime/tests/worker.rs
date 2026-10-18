use super::*;

// A failing command's kind and source error reach the task's outcome, the
// caller's error and the logs. Negative control: a command that reports
// indeterminate progress itself records no failure, so the cause names it
// without any source error text.
#[tokio::test]
async fn indeterminate_command_keeps_its_cause() -> Result<()> {
    struct UncertainCommand {
        fail: bool,
        kind: std::sync::Mutex<Option<String>>,
    }
    impl TaskCommandHost for UncertainCommand {
        fn execute<'a>(
            &'a self,
            _: TaskContext,
            _: LeaseFence,
            command: WorkflowCommand,
            _: Value,
        ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
            *self.kind.lock().unwrap() = Some(command.kind);
            Box::pin(async move {
                if self.fail {
                    Err(Error::Unsupported("original command cause".into()))
                } else {
                    Ok(TaskCommandProgress::Indeterminate)
                }
            })
        }
    }
    for fail in [true, false] {
        let (seen, _capture) = crate::obs::capture::install();
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime(None).await?;
        let lease = fixture.admit(&runtime).await?;
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let command = UncertainCommand {
            fail,
            kind: std::sync::Mutex::new(None),
        };
        runtime.run_task(lease, &command, 2).await?;
        let kind = command.kind.lock().unwrap().clone().expect("command ran");
        let reason = if fail {
            crate::CommandIndeterminacyReason::Failed {
                code: "ERROR_CODE_UNSUPPORTED".into(),
                error: "unsupported capability: original command cause".into(),
            }
        } else {
            crate::CommandIndeterminacyReason::Unsettled
        };
        let expected = crate::CommandIndeterminacy {
            command: kind,
            reason,
        };
        let Some(Outcome::Indeterminate {
            operation_id,
            cause,
        }) = runtime.task_host().outcome(task).await?
        else {
            panic!("command left no indeterminate outcome");
        };
        assert_eq!(cause, crate::IndeterminateCause::Command(expected.clone()));
        let error = cause.into_error(operation_id);
        assert_eq!(error.code(), crate::wire::ErrorCode::Indeterminate);
        assert!(error.to_string().contains(&expected.to_string()));
        assert_eq!(
            error.to_string().contains("original command cause"),
            fail,
            "{error}"
        );
        let logged =
            seen.lock().unwrap().iter().any(|(_, field, value)| {
                *field == "cause" && value.contains("original command cause")
            });
        assert_eq!(logged, fail);
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
