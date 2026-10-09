use super::*;
use acyclic_harness::distributed::WorkPull;
use acyclic_harness::executor::TurnOutput;
use acyclic_harness::filesystem::StockTurnMachine;

#[tokio::test]
async fn admitted_stock_turn_reconciles_original_model_after_provider_reopen() -> Result<()> {
    stock_turn_recovery(false).await
}

#[tokio::test]
async fn cancelled_stock_turn_cannot_dispatch_after_provider_reopen() -> Result<()> {
    stock_turn_recovery(true).await
}

#[expect(
    clippy::cognitive_complexity,
    reason = "one original/reopened scenario keeps provider identity, authority and uncertain reservation assertions together"
)]
async fn stock_turn_recovery(cancel: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let agent = AgentId::from_bytes([71; 16]);
    let volume = VolumeRef::new(
        ProviderRef::new("stock-turn-recovery", "filesystem", "2")?,
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let issuer = AuthorityIssuer::new(
        "stock-turn-recovery",
        [72; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "stock-owner".into(),
        },
    );
    let signed = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "operation:wake".to_owned(),
            "model:generate".to_owned(),
            "task:spawn:acyclic.stock_turn@1".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
    let machine = Arc::new(StockTurnMachine::new());
    let mut tasks = TaskRegistry::default();
    tasks.register(machine.definition()?)?;
    let definition = tasks.get::<WorkflowCommand, TurnOutput>("acyclic.stock_turn@1")?;
    let mut machines = MachineRegistry::default();
    machines.register(machine)?;
    let operation = OperationId::from_bytes([73; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let turn = OperationId::from_bytes([74; 16]);
    let worker = Worker {
        id: "stock-worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let model = Arc::new(InterruptedModel {
        approval_tool: false,
        observed_events: 1,
        output_tokens: AtomicU64::new(0),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let mut original_command = None;
    let mut original_lease = None;
    for reopened in [false, true] {
        let filesystem = Arc::new(FilesystemHost::new(
            Fs::local(options.clone())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            volume.provider().clone(),
        )?);
        if !reopened {
            filesystem.create_volume(&volume).await?;
        }
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(&stream_root, LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let payloads = FilesystemSchedulerPayloadStore::new(
            filesystem.clone(),
            volume.clone(),
            &issuer.verifier(),
            &signed,
            65_536,
        )?;
        let runtime = FilesystemTaskRuntime::open(
            stream,
            filesystem,
            volume.clone(),
            issuer.verifier(),
            signed.clone(),
            scope.clone(),
            tasks.clone(),
            machines.clone(),
            ToolRegistry::default(),
            SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 1,
                model_steps: 1,
            },
            1,
            65_536,
        )
        .await?;
        if !reopened {
            let payload = serde_json::to_vec(&ModelTaskCommand {
                input: ModelContent::Text("hello".into()),
                selected_context: None,
                max_steps: 1,
                max_output_tokens: Some(8_192),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
            let command = WorkflowCommand {
                operation_id: turn,
                kind: MODEL_TASK_COMMAND_KIND.into(),
                payload: payloads.stage(operation, "stock-command", &payload).await?,
            };
            assert!(matches!(
                runtime
                    .harness()
                    .admit(operation, &definition, command.clone(), None)
                    .await?,
                Admission::Accepted(_)
            ));
            original_command = Some(command);
        }
        let command = original_command
            .as_ref()
            .ok_or_else(|| Error::NotFound("original command".into()))?;
        // Exact admission retry consumes no additional session task or model budget.
        assert!(matches!(
            runtime
                .harness()
                .admit(operation, &definition, command.clone(), None)
                .await?,
            Admission::Accepted(_)
        ));
        let (first_retry, second_retry) = futures::join!(
            runtime
                .harness()
                .admit(operation, &definition, command.clone(), None),
            runtime
                .harness()
                .admit(operation, &definition, command.clone(), None),
        );
        assert!(matches!(first_retry?, Admission::Accepted(_)));
        assert!(matches!(second_retry?, Admission::Accepted(_)));
        let mut changed = command.clone();
        changed.operation_id = OperationId::from_bytes([75; 16]);
        assert!(
            runtime
                .harness()
                .admit(operation, &definition, changed, None)
                .await
                .is_err()
        );
        assert!(
            runtime
                .harness()
                .durable_context(task, OperationId::from_bytes([76; 16]))
                .await
                .is_err()
        );
        let commands = runtime.commands().with_model(
            Model::new("test", "interrupted", "1", Value::Null)?,
            model.clone(),
            ContextPipeline::default(),
        );
        if !reopened {
            let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease })) =
                runtime.run_operation(&worker, task, &commands, 2).await?
            else {
                return Err(Error::Invalid(
                    "stock turn did not retain uncertain reservation".into(),
                ));
            };
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
            if cancel {
                runtime.task_host().cancel(task).await?;
            }
            original_lease = Some(lease);
            continue;
        }
        let lease = original_lease
            .as_ref()
            .ok_or_else(|| Error::NotFound("original lease".into()))?;
        let recovered = runtime.task_host().recover_work(task).await?;
        if cancel {
            assert!(
                matches!(recovered, WorkPull::Unresolved { lease: retained, .. } if retained.reservation == lease.reservation)
            );
            assert!(matches!(
                runtime.resume_task(lease.clone(), &commands, 2).await,
                TaskWorkerAttempt::Unresolved { .. }
            ));
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
            assert_eq!(
                runtime.task_host().outcome(task).await?,
                Some(Outcome::Indeterminate {
                    operation_id: operation
                }),
            );
        } else {
            let WorkPull::Claimed(recovered) = recovered else {
                return Err(Error::Invalid(
                    "original stock lease was not recoverable".into(),
                ));
            };
            assert_eq!(recovered.reservation, lease.reservation);
            assert!(
                runtime
                    .run_operation(&worker, task, &commands, 2)
                    .await?
                    .is_none()
            );
            assert!(matches!(runtime.resume_task(recovered, &commands, 2).await,
                TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task));
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
            assert_eq!(model.output_tokens.load(Ordering::SeqCst), 8_192);
            let Admission::Accepted(result) = runtime
                .harness()
                .admit(operation, &definition, command.clone(), None)
                .await?
            else {
                return Err(Error::Invalid(
                    "completed stock admission was not retained".into(),
                ));
            };
            let Outcome::Succeeded(output) = result.result().await? else {
                return Err(Error::Invalid("stock output was not successful".into()));
            };
            assert_eq!(output.text, "partial-restored");
            assert_eq!(output.steps, 1);
            assert!(
                runtime
                    .run_operation(&worker, task, &commands, 2)
                    .await?
                    .is_none()
            );
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
        }
    }
    Ok(())
}
