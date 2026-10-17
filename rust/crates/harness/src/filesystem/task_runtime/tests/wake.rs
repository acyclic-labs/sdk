use super::*;
use crate::filesystem::{TIMER_TASK_COMMAND_KIND, TimerTaskCommand};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    // Every wake input must fit the admitted directory grant after either
    // constructor path, survive reopening and retain one retry identity.
    #[test]
    fn suspended_commands_wake_with_only_the_inputs_read_grant(deadline in 11_u64..19) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(Box::pin(async move {
                for native in [false, true] {
                    Box::pin(wake_case(deadline, native)).await?;
                }
                Ok::<_, Error>(())
            })).unwrap();
    }
}

async fn wake_case(deadline: u64, native: bool) -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let input_grant = fixture
        .volume
        .directory_read_capability("inputs", &mut None)?;
    let grants = Capabilities::new(
        fixture
            .signed
            .capabilities()
            .iter()
            .map(str::to_owned)
            .chain([
                input_grant.clone(),
                "operation:wake".into(),
                "timer:wait".into(),
            ]),
    );
    fixture.signed = fixture.issuer.issue(
        "owner",
        &grants,
        Some(AgentId::from_bytes([101; 16])),
        None,
        &mut None,
    )?;
    fixture.scope = RuntimeScope::new(grants, Limits::default())?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    fixture.tool.native.store(true, Ordering::SeqCst);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(super::native_mail::BUDGET, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let runtime = Box::pin(fixture.runtime(native.then_some(&io))).await?;
    let operation = OperationId::from_bytes([104; 16]);
    let command = OperationId::from_bytes([105; 16]);
    let publisher = FilesystemContentPublisher::new(
        fixture.filesystem.clone(),
        &fixture.volume,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.signed,
        65_536,
        None,
    )?;
    let payload = publisher
        .stage_input(
            operation,
            "timer-command",
            &serde_json::to_vec(&TimerTaskCommand {
                deadline_unix_ms: deadline,
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
            None,
        )?
        .await?;
    let definition = fixture
        .tasks
        .get_version::<Value, Value>("test.deadline", "1", None)?;
    let scope = Capabilities::new([
        "task:spawn:test.deadline@1".into(),
        "timer:wait".into(),
        input_grant,
    ]);
    let admitted = runtime
        .harness()
        .scoped(scope, Limits::default())?
        .admit(
            operation,
            &definition,
            json!({"command": WorkflowCommand {
                operation_id:command, kind:TIMER_TASK_COMMAND_KIND.into(), payload,
            }}),
            None,
        )
        .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) = admitted
    else {
        return Err(Error::Conflict("wake task was not admitted".into()));
    };
    let worker = Worker {
        id: "wake-worker".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let attempt =
        Box::pin(runtime.run_operation(&worker, task, &runtime.commands(), 2, None)).await?;
    match attempt {
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended {
            task: suspended,
            revision: 1,
        })) => assert_eq!(suspended, task),
        Some(TaskWorkerAttempt::Unresolved { error, .. }) => return Err(error),
        _ => return Err(Error::Conflict("timer did not suspend".into())),
    }
    let wake_key = format!("task-command-wake:{task}:1:{command}");
    // Reproduce an interrupted old wake: its scheduler upload exists, but
    // the task cannot read it. A new workflow input must not reuse that receipt.
    let legacy = publisher
        .stage(operation, &wake_key, b"null", None)?
        .await?;
    assert!(matches!(
        runtime
            .task_host()
            .resume_workflow(
                task,
                1,
                legacy.clone(),
                IdempotencyKey::new(wake_key.clone())?,
                Some(command),
                &mut None,
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    drop(runtime);
    fixture.clock.0.store(deadline, Ordering::SeqCst);
    let reopened = Box::pin(fixture.runtime(native.then_some(&io))).await?;
    if native {
        // This ordinary timer has no native resource-preparation journal.
        // Exercise the actual native wake write/resume boundary directly,
        // rather than inventing protected readiness provenance for it.
        let mut history = crate::store::HistoryWork::new(PreparationIo::from_native_work(&work)?);
        Box::pin(
            reopened
                .task_host()
                .wake_command(task, 1, command, &mut Some(&mut history)),
        )
        .await?;
        drop(history.into_custody()?);
    } else {
        assert!(Box::pin(reopened.poll_task_wake(task, None)).await?);
    }
    // A duplicate notification is harmless and cannot create another upload.
    let workspace = crate::filesystem::workspace_ref(
        fixture.volume.provider().clone(),
        &fixture.volume.storage_name(&mut None)?,
    )?;
    let generation = fixture
        .filesystem
        .resolve(&workspace, None)
        .await?
        .generation;
    assert!(!Box::pin(reopened.poll_task_wake(task, None)).await?);
    assert_eq!(
        fixture
            .filesystem
            .resolve(&workspace, None)
            .await?
            .generation,
        generation
    );
    let suspension = fixture
        .scheduler_state(task)
        .await?
        .workflow
        .ok_or_else(|| Error::NotFound("wake suspension".into()))?;
    let input = suspension
        .input
        .ok_or_else(|| Error::NotFound("wake input".into()))?;
    assert!(input.path().starts_with("inputs/"));
    assert_eq!(
        publisher
            .stage(operation, &wake_key, b"null", None)?
            .await?,
        legacy
    );
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    Ok(())
}
