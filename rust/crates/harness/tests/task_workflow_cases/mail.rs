use super::*;

#[tokio::test]
async fn default_stock_turn_selects_one_queued_item_and_keeps_admitted_input_on_reopen()
-> Result<()> {
    use acyclic_harness::filesystem::StockTurnMachine;
    for queued in [true, false] {
        let directory = workspace()?;
        let fs_options = LocalOptions::new(directory.path().join("filesystem"));
        let stream_root = directory.path().join("streams");
        let provider = ProviderRef::new(
            "stock-mail".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let agent = AgentId::new();
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "stock-mail",
            [81; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "stock-mail".into(),
            },
        );
        let signed = issuer
            .issue(
                "owner",
                &Capabilities::new([
                    "operation:declare".to_owned(),
                    "operation:observe".to_owned(),
                    "operation:cancel".to_owned(),
                    "task:spawn:acyclic.stock_turn@1".to_owned(),
                    "model:generate".to_owned(),
                    "mail:send".to_owned(),
                    "mail:read".to_owned(),
                    volume.capability(VolumeOperation::Read, &mut None)?,
                    volume.capability(VolumeOperation::Write, &mut None)?,
                ]),
                Some(agent),
                None,
                &mut None,
            )
            .expect("scope proof");
        let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
        let machine = Arc::new(StockTurnMachine::new());
        let definition = machine.definition()?;
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let definition = tasks.get_version::<Value, acyclic_harness::executor::TurnOutput>(
            "acyclic.stock_turn",
            "1",
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            None,
        )?;
        let mut machines = MachineRegistry::default();
        machines.register(machine, &mut None)?;
        let operation = OperationId::new();
        let task = TaskId::from_bytes(operation.into_bytes());
        let receive_operation = OperationId::new();
        let model_operation = OperationId::new();
        let model = Model::new(
            "fixture".into(),
            "stock-mail".into(),
            "1".into(),
            Value::Null,
        )?;
        let provider_model = interrupted_model(1);
        let worker = Worker {
            id: "stock-mail".into(),
            available: ResourceRequest::default(),
            labels: BTreeMap::new(),
        };
        let mut retained_lease = None;
        let mut admitted_model = None;
        let mut selected = None;
        for phase in 0..4 {
            let filesystem = FilesystemHost::new(
                Fs::local(fs_options.clone())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
                &provider,
                None,
            )?;
            if phase == 0 {
                filesystem.create_volume(&volume).await?;
            }
            let stream = Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            );
            let runtime = FilesystemTaskRuntime::open(
                stream,
                filesystem.clone(),
                &volume,
                None,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                &scope,
                &tasks,
                &machines,
                &ToolRegistry::default(),
                SessionLimits {
                    active_tasks: 1,
                    total_tasks: 1,
                    depth: 1,
                    model_steps: 1,
                },
                1,
                65536,
                Arc::new(SystemUnixMillisClock),
                None,
                &[],
                &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
                None,
            )
            .await?;
            let payloads = FilesystemContentPublisher::new(
                filesystem,
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                65536,
                None,
            )?;
            let commands = runtime.commands().with_model(
                &model.clone(),
                provider_model.clone(),
                &acyclic_harness::context::CompactionPolicy::Disabled,
                None,
            )?;
            if phase == 0 {
                let model_payload = payloads
                    .stage(
                        operation,
                        "model",
                        &serde_json::to_vec(&ModelTaskCommand {
                            summary_source: None,
                            context_manifest: acyclic_harness::context::ContextManifest::new(
                                Default::default(),
                                Vec::new(),
                                &ToolRegistry::default(),
                            )?
                            .publish(
                                payloads.as_ref(),
                                OperationId::new(),
                                "manifest.json",
                                Limits::default(),
                            )
                            .await?,
                            input: ModelContent::Text("original prompt".into()),
                            selected_context: None,
                            max_steps: 1,
                            max_output_tokens: None,
                            mailbox_operation: None,
                            selected_output_volume: None,
                        })
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                        None,
                    )?
                    .await?;
                let model_command = WorkflowCommand {
                    operation_id: model_operation,
                    kind: MODEL_TASK_COMMAND_KIND.into(),
                    payload: model_payload,
                };
                let receive = WorkflowCommand {
                    operation_id: receive_operation,
                    kind: MAIL_RECEIVE_TASK_COMMAND_KIND.into(),
                    payload: payloads
                        .stage(
                            operation,
                            "receive",
                            &serde_json::to_vec(&MailReceiveTaskCommand { after: 0 })
                                .map_err(|error| Error::Invalid(error.to_string()))?,
                            None,
                        )?
                        .await?,
                };
                assert!(matches!(
                    runtime
                        .harness()
                        .admit(
                            operation,
                            &definition,
                            json!({"receive":receive,"model":model_command}),
                            None
                        )
                        .await?,
                    Admission::Accepted(_)
                ));
                admitted_model = Some(model_command);
                let WorkPull::Claimed(lease) = runtime
                    .task_host()
                    .pull_work(&worker, None, &mut None)
                    .await?
                else {
                    return Err(Error::NotFound("stock lease".into()));
                };
                runtime.task_host().start_task(&lease, &mut None).await?;
                let fence = LeaseFence::from(&lease.reservation);
                for (path, bytes) in [
                    ("first", b"first steering".as_slice()),
                    ("second", b"later steering".as_slice()),
                ]
                .into_iter()
                .filter(|_| queued)
                {
                    let payload = payloads
                        .stage(OperationId::new(), path, bytes, None)?
                        .await?;
                    runtime
                        .task_host()
                        .send_mail(task, &fence, task, OperationId::new(), payload, None)
                        .await?;
                }
                let items = runtime.task_host().inbox(task, 0, 2).await?;
                assert_eq!(items.len(), if queued { 2 } else { 0 });
                selected = items.into_iter().next();
                retained_lease = Some(lease);
            }
            let lease = retained_lease.clone().unwrap();
            let attempt = runtime
                .resume_task(
                    lease.clone(),
                    &commands,
                    if phase == 3 { 4 } else { 1 },
                    None,
                )
                .await;
            match phase {
                0 | 1 => assert!(matches!(
                    attempt,
                    TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })
                )),
                2 => assert!(matches!(
                    attempt,
                    TaskWorkerAttempt::Unresolved {
                        error: Error::Indeterminate(_),
                        ..
                    } | TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. })
                )),
                _ => assert!(matches!(
                    attempt,
                    TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { .. })
                )),
            }
            if phase == 1 || phase == 2 {
                let journal = runtime
                    .workflow_journal(task, LeaseFence::from(&lease.reservation))
                    .await?;
                let record = journal.replay(0, 8, None)?.await?.0.pop().unwrap();
                assert_eq!(
                    record.transition.commands,
                    vec![admitted_model.clone().unwrap()]
                );
                assert_eq!(
                    record.transition.state["mail"],
                    serde_json::to_value(&selected).unwrap()
                );
                let remaining = runtime.task_host().inbox(task, 1, 1).await?;
                if queued {
                    assert_eq!(remaining[0].sequence, 2);
                } else {
                    assert!(remaining.is_empty());
                }
            }
            if phase == 2 && queued {
                let requests = provider_model.requests();
                let request = requests
                    .first()
                    .ok_or_else(|| Error::NotFound("model request".into()))?;
                let parcel_path = format!("mailbox/{receive_operation}/parcel");
                let parcel = request
                    .messages
                    .iter()
                    .flat_map(|message| message.content.file_refs())
                    .find(|file| file.path() == parcel_path)
                    .ok_or_else(|| Error::NotFound("selected model parcel".into()))?;
                assert_eq!(
                    parcel.descriptor(),
                    selected.as_ref().unwrap().payload.descriptor()
                );
                assert_eq!(
                    runtime
                        .harness()
                        .durable_context(task, operation, None)
                        .await?
                        .read_file(parcel, None)
                        .await?
                        .as_ref(),
                    b"first steering"
                );
            }
            if phase == 3 {
                let Some(Outcome::Succeeded(value)) = runtime.task_host().outcome(task).await?
                else {
                    return Err(Error::Invalid("stock model did not settle".into()));
                };
                assert_eq!(value["text"], "partial-restored");
            }
        }
        assert_eq!(provider_model.dispatches().len(), 1);
        assert_eq!(provider_model.reconciliations().len(), 1);
    }
    Ok(())
}
