use super::*;

// A failing command's kind and source error reach the task's outcome, the
// caller's error and the logs, including after reopening the durable host.
#[tokio::test]
async fn indeterminate_command_keeps_its_cause() -> Result<()> {
    command_cause_path().await
}

#[tokio::test]
async fn indeterminate_command_negative_control_detects_erased_production_cause() {
    super::super::dispatch::ERASE_CAUSE.with(|erase| erase.set(true));
    let result = command_cause_path().await;
    super::super::dispatch::ERASE_CAUSE.with(|erase| assert!(!erase.replace(false)));
    assert!(
        matches!(result, Err(Error::Conflict(message)) if message == "indeterminate command cause was erased")
    );
}

async fn command_cause_path() -> Result<()> {
    struct UncertainCommand {
        mode: u8,
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
                match self.mode {
                    0 => Err(Error::Storage("original command cause".into())),
                    1 => Ok(TaskCommandProgress::Indeterminate),
                    _ => Ok(TaskCommandProgress::Ready(Value::String("x".repeat(8192)))),
                }
            })
        }
    }
    for mode in 0..3 {
        let fail = mode == 0;
        let (seen, _capture) = crate::obs::capture::install();
        let mut fixture = Fixture::new().await?;
        fixture.scope = RuntimeScope::new(
            fixture.scope.grants().clone(),
            Limits {
                file_bytes: 8192,
                ..Limits::default()
            },
        )?;
        let runtime = fixture.runtime(None).await?;
        let lease = fixture.admit(&runtime).await?;
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let command = UncertainCommand {
            mode,
            kind: std::sync::Mutex::new(None),
        };
        runtime.run_task(lease, &command, 2).await?;
        let kind = command.kind.lock().unwrap().clone().expect("command ran");
        let reason = match mode {
            0 => crate::CommandIndeterminacyReason::Failed {
                code: "ERROR_CODE_STORAGE".into(),
                error: "storage failure: original command cause".into(),
            },
            1 => crate::CommandIndeterminacyReason::Unsettled,
            _ => crate::CommandIndeterminacyReason::InputLimit,
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
        if cause != crate::IndeterminateCause::Command(expected.clone()) {
            return Err(Error::Conflict(
                "indeterminate command cause was erased".into(),
            ));
        }
        let reopened = fixture.runtime(None).await?;
        let durable = crate::runtime::RuntimeTask::<Value>::Durable {
            task_id: task,
            host: reopened.task_host().clone(),
            output_schema: Value::Bool(true),
            extensions: None,
        };
        assert_eq!(
            durable.result().await?,
            Outcome::Indeterminate {
                operation_id,
                cause: cause.clone()
            },
        );
        assert_eq!(
            reopened.task_host().terminal_status(task).await?,
            Some(Outcome::Indeterminate {
                operation_id,
                cause: cause.clone()
            }),
        );
        let state = fixture.scheduler_state(task).await?;
        assert_eq!(
            crate::wire_api::operation_status(&state).error,
            Some(crate::wire_codec::encode_error(
                &cause.clone().into_error(operation_id)
            )),
        );
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
        // Failures that carry their class only as a code: a task-volume storage failure and a
        // deferred search publication may both have taken effect.
        (
            FailedCommand(|| {
                Error::from(crate::task_volume::Error::Storage(
                    acyclic_stream::StreamError::DeadlineElapsed,
                ))
            }),
            true,
        ),
        (FailedCommand(|| Error::SearchPublicationDeferred), true),
        // A failed child task is settled even though its wire class is storage.
        (
            FailedCommand(|| Error::TaskFailed {
                task: TaskId::from_bytes([9; 16]),
                message: "child failed".into(),
            }),
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

/// A native provider failure is the provider's, not the ledger's: once redelivery is spent the
/// native worker settles the task Failed instead of returning the lease unresolved.
#[tokio::test]
async fn native_worker_settles_provider_failures_and_keeps_accounting_failures_unresolved()
-> Result<()> {
    struct NativeFailure(std::sync::Mutex<Option<Error>>);
    impl TaskCommandHost for NativeFailure {
        fn execute<'a>(
            &'a self,
            _: TaskContext,
            _: LeaseFence,
            _: WorkflowCommand,
            _: Value,
        ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
            let error = self.0.lock().unwrap().take().expect("one dispatch");
            Box::pin(async { Err(error) })
        }
        // The provider fails inside the funded exchange, as a real native dispatch does.
        fn execute_with_native_work<'a>(
            &'a self,
            _: Arc<crate::runtime::TaskContext>,
            _: LeaseFence,
            _: WorkflowCommand,
            _: Value,
            work: &'a acyclic_fs::NativeMaterializationWork,
        ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
            let error = self.0.lock().unwrap().take().expect("one dispatch");
            crate::NativeFutureBundle::new(work, || async move { Err(error) })
        }
    }
    // (error, settles): provider transport and refusal settle; the accounting failure they
    // used to be classified as must still stop the native attempt (negative control).
    let cases: [(fn(acyclic_fs::NativeAllocation) -> Error, bool); 4] = [
        (
            |_| Error::ModelUnavailable {
                failure: crate::ModelTransportFailure::Timeout,
                excerpt: String::new(),
            },
            true,
        ),
        (
            |custody| Error::ModelDiagnostic {
                code: crate::wire::ErrorCode::Unauthorized,
                message: String::new(),
                custody,
            },
            true,
        ),
        (
            |_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unavailable,
                reason: "model provider exchange timed out",
            },
            false,
        ),
        (
            |custody| Error::NativeDiagnostic {
                code: crate::wire::ErrorCode::Unauthorized,
                message: String::new(),
                custody,
            },
            false,
        ),
    ];
    for (error, settles) in cases {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let runtime = fixture.runtime(Some(&io)).await?;
        let (lease, mut custody) = fixture.native_lease(&runtime, &work).await?;
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let diagnostic = custody.split_off(0)?;
        let host = NativeFailure(std::sync::Mutex::new(Some(error(diagnostic))));
        let (attempt, custody) = runtime
            .resume_task_with_native_work(lease, custody, &host, 2, &work)
            .await?;
        let unresolved = matches!(attempt, TaskWorkerAttempt::Unresolved { .. });
        drop(attempt);
        let outcome = runtime.task_host().outcome(task).await?;
        assert_eq!(unresolved, !settles, "{outcome:?}");
        assert_eq!(
            matches!(outcome, Some(Outcome::Failed { .. })),
            settles,
            "{outcome:?}"
        );
        drop(custody);
    }
    Ok(())
}

/// A definitive failure's message is charged to the native ledger, as an uncertain cause's is.
#[test]
fn failed_message_is_formatted_under_the_native_ledger() -> Result<()> {
    let error = Error::Unsupported("x".repeat(4_096));
    let work = acyclic_fs::NativeMaterializationWork::new(
        super::native_mail::BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let before = work.receipt();
    let message = super::super::dispatch::failed_message(&error, Some(&io))?;
    let after = work.receipt();
    assert_eq!(message, error.to_string());
    assert!(after.allocation_operations > before.allocation_operations);
    assert!(after.peak_allocation_bytes >= message.len() as u64, "{after:?}");
    // Negative control: the unaccounted formatting this replaces charges the ledger nothing.
    let unaccounted = error.to_string();
    assert_eq!(work.receipt(), after);
    assert_eq!(unaccounted, message);
    Ok(())
}
