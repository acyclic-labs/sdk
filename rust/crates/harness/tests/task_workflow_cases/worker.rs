use super::*;
use acyclic_harness::runtime::TaskSpawner as _;

pub(super) async fn worker_restart_with_options(
    command: WorkerCommand,
    allow_mail_read: bool,
    cancel_wait: bool,
) -> Result<()> {
    let with_model_approval = matches!(
        command,
        WorkerCommand::ApprovalWaitModel | WorkerCommand::ApprovalDeclinedModel
    );
    let declined = command == WorkerCommand::ApprovalDeclinedModel;
    let with_approval_wait = matches!(
        command,
        WorkerCommand::ApprovalWaitTool
            | WorkerCommand::ApprovalWaitModel
            | WorkerCommand::ApprovalDeclinedModel
    );
    let with_child = command == WorkerCommand::Child;
    let with_command = command != WorkerCommand::Wait;
    let with_mail_send = command == WorkerCommand::MailSend;
    let with_mail_receive = command == WorkerCommand::MailReceive;
    let with_two_waits = command == WorkerCommand::TimerThenTimer;
    let with_timer = matches!(
        command,
        WorkerCommand::Timer | WorkerCommand::TimerThenTimer
    );
    let uncertain = matches!(
        command,
        WorkerCommand::Model
            | WorkerCommand::PolicyModel
            | WorkerCommand::Tool
            | WorkerCommand::PolicyTool
    );
    let clock = Arc::new(TestClock(AtomicU64::new(100)));
    let with_tool = matches!(
        command,
        WorkerCommand::Tool
            | WorkerCommand::PolicyTool
            | WorkerCommand::ApprovalWaitTool
            | WorkerCommand::ApprovalWaitModel
            | WorkerCommand::ApprovalDeclinedModel
    );
    let with_policy = matches!(
        command,
        WorkerCommand::PolicyTool
            | WorkerCommand::PolicyModel
            | WorkerCommand::ApprovalWaitTool
            | WorkerCommand::ApprovalWaitModel
            | WorkerCommand::ApprovalDeclinedModel
    );
    let policy = Arc::new(RestartPolicy {
        evaluated: AtomicUsize::new(0),
        digest: [52; 32],
    });
    let directory = workspace()?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new(
        "worker-restart".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([21; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "worker-owner".into(),
    };
    let issuer = AuthorityIssuer::new("worker-restart", [23; 32], authority);
    let signed = issuer
        .issue(
            "owner",
            &Capabilities::new([
                "operation:declare".to_owned(),
                "operation:observe".to_owned(),
                "operation:cancel".to_owned(),
                "operation:wake".to_owned(),
                "model:generate".to_owned(),
                "timer:wait".to_owned(),
                "mail:send".to_owned(),
                "mail:read".to_owned(),
                "tool:call:test.restore".to_owned(),
                "interaction:route".to_owned(),
                "task:spawn:test.restart@1".to_owned(),
                "task:spawn:test.child@1".to_owned(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let conversation_issuer = AuthorityIssuer::new(
        "worker-approval",
        [54; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "worker-approval".into(),
        },
    );
    let conversation_scope = conversation_issuer
        .issue(
            "owner",
            &Capabilities::new([
                "conversation:bind".to_owned(),
                "interaction:open".to_owned(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let grants = if allow_mail_read {
        signed.capabilities().clone()
    } else {
        signed
            .capabilities()
            .without(&Capabilities::new(["mail:read"]))
    };
    let scope = RuntimeScope::new(grants, Limits::default())?;
    let scope = if command == WorkerCommand::PolicyModel {
        RuntimeScope::new(
            scope
                .grants()
                .without(&Capabilities::new(["interaction:route"])),
            Limits::default(),
        )?
    } else {
        scope
    };
    let base = TaskMachine {
        identity: MachineIdentity {
            name: "test.restart".into(),
            version: "1".into(),
            digest: [24; 32],
        },
        schema: if with_command {
            json!({"type":"object"})
        } else {
            json!({"type":"integer"})
        },
    };
    let machine: Arc<dyn ResumableMachine> = if with_command {
        Arc::new(CommandMachine { base, command })
    } else {
        Arc::new(base)
    };
    let input_schema = machine.state_schema().clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(TaskDefinition::<Value, u64>::resumable(
        machine.clone(),
        input_schema,
        json!({"type":"integer"}),
    )?)?;
    let definition = tasks.get_version::<Value, u64>(
        "test.restart",
        "1",
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        None,
    )?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    if with_child {
        let child = Arc::new(CompletedChildMachine(TaskMachine {
            identity: MachineIdentity {
                name: "test.child".into(),
                version: "1".into(),
                digest: [44; 32],
            },
            schema: json!({"type":"integer"}),
        }));
        tasks.register(TaskDefinition::<u64, u64>::resumable(
            child.clone(),
            json!({"type":"integer"}),
            json!({"type":"integer"}),
        )?)?;
        machines.register(child, &mut None)?;
    }
    let operation = OperationId::from_bytes([25; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let worker = Worker {
        id: "worker".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let mut old_lease: Option<acyclic_harness::distributed::WorkLease> = None;
    let mut discovery_cursor = None;
    let mut model = if with_model_approval {
        None
    } else if matches!(command, WorkerCommand::Model | WorkerCommand::PolicyModel) {
        Some(interrupted_model(1))
    } else {
        Some(Arc::new(task_model([])))
    };
    let mut approval_file = None;
    let tool = Arc::new(InterruptedTool {
        model_call: with_model_approval,
        executed: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            Tool {
                hook_names: &[],
                definition: ToolDefinition {
                    name: "test.restore".into(),
                    revision: "1".into(),
                    description: "Read and reconcile".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({"type":"string"}),
                    projection_schema: acyclic_harness::tool::json_projection_schema(
                        json!({"type":"string"}),
                    ),
                },
                executor: tool.clone(),
                projection: Arc::new(acyclic_harness::tool::JsonToolProjection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    for reopened in [false, true] {
        let (filesystem, stream, payloads, reader, runtime) = boxed_restart_phase(|| async {
            let filesystem = FilesystemHost::new(
                Fs::local(fs_options.clone())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
                &provider,
                None,
            )?;
            if !reopened {
                filesystem.create_volume(&volume).await?;
            }
            let stream = Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            );
            let payloads = FilesystemContentPublisher::new(
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                65_536,
                None,
            )?;
            let reader = FilesystemContentVerifier::new(
                filesystem.clone(),
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                65_536,
                None,
            )?;
            if with_policy && !reopened {
                Box::pin(async {
                    let mut aggregate = acyclic_harness::store::StreamAggregate::open(
                        &stream,
                        (
                            conversation_issuer
                                .verifier(&mut None)
                                .expect("verifier copy")
                                .audience()
                                .clone(),
                            conversation_issuer
                                .verifier(&mut None)
                                .expect("verifier copy"),
                            SchemaRegistry::new(),
                        ),
                        None,
                        acyclic_harness::store::default_projection_read_limits(),
                        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                        None,
                        &mut None,
                    )
                    .await?;
                    aggregate
                        .execute(
                            Command {
                                operation_id: OperationId::from_bytes([90; 16]),
                                idempotency_key: IdempotencyKey::new("worker-approval-bind")?,
                                expected_revision: 0,
                                scope: conversation_scope.clone(),
                                causal_parent: None,
                                action: Action::BindConversation { agent },
                            },
                            #[cfg(feature = "filesystem")]
                            None,
                            &mut None,
                        )
                        .await?;
                    Ok::<(), Error>(())
                })
                .await?;
            }
            let runtime = FilesystemTaskRuntime::open(
                stream.clone(),
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                &scope,
                &tasks,
                &machines,
                &tools,
                SessionLimits {
                    active_tasks: 1,
                    total_tasks: if with_child { 2 } else { 1 },
                    depth: if with_child { 2 } else { 1 },
                    model_steps: if with_model_approval { 2 } else { 1 },
                },
                1,
                65_536,
                clock.clone(),
                with_policy.then(|| policy.clone() as Arc<dyn ToolPolicy>),
                &[],
                &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
                None,
            )
            .await?;
            let runtime = if with_policy {
                runtime.with_interaction_owner(
                    conversation_issuer
                        .verifier(&mut None)
                        .expect("verifier copy"),
                    SchemaRegistry::new(),
                    conversation_scope.clone(),
                    volume.clone(),
                )?
            } else {
                runtime
            };
            if with_policy && reopened {
                let wrong = FilesystemTaskRuntime::open(
                    stream.clone(),
                    filesystem.clone(),
                    &volume,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &signed,
                    &scope,
                    &tasks,
                    &machines,
                    &tools,
                    SessionLimits {
                        active_tasks: 1,
                        total_tasks: 1,
                        depth: 1,
                        model_steps: if with_model_approval { 2 } else { 1 },
                    },
                    1,
                    65_536,
                    clock.clone(),
                    Some(Arc::new(RestartPolicy {
                        evaluated: AtomicUsize::new(0),
                        digest: [53; 32],
                    })),
                    &[],
                    &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
                    None,
                )
                .await?;
                assert!(matches!(
                    wrong.task_host().observe_admission(task).await,
                    Err(Error::Conflict(_))
                ));
                drop(wrong);
                assert_eq!(
                    tool.executed.load(Ordering::SeqCst),
                    usize::from(with_tool && !with_approval_wait)
                );
                assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
            }
            Ok::<_, Error>((filesystem, stream, payloads, reader, runtime))
        })
        .await?;
        boxed_restart_phase(|| async {
        if !reopened {
            let input = if with_child {
                let admit = serde_json::to_vec(&TaskAdmitCommand {
                    name: "test.child".into(),
                    version: "1".into(),
                    input: json!(7),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
                let observe = serde_json::to_vec(&TaskObserveCommand {
                    admission_operation: OperationId::from_bytes([26; 16]),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
                json!({"admit":payloads.stage(operation, "child-admit", &admit, None)?.await?, "observe":payloads.stage(operation, "child-observe", &observe, None)?.await?})
            } else if with_two_waits {
                let timer = serde_json::to_vec(&TimerTaskCommand {
                    deadline_unix_ms: 200,
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
                let second_timer = serde_json::to_vec(&TimerTaskCommand {
                    deadline_unix_ms: 300,
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
                json!({"timer":payloads.stage(operation, "two-waits-timer", &timer, None)?.await?,
                    "second_timer":payloads.stage(operation, "two-waits-second-timer", &second_timer, None)?.await?})
            } else if with_command {
                let payload = if with_mail_send {
                    serde_json::to_value(MailSendTaskCommand {
                        recipient: task,
                        payload: payloads.stage(operation, "mail-body", b"7", None)?.await?,
                    })
                } else if with_mail_receive {
                    serde_json::to_value(MailReceiveTaskCommand { after: 0 })
                } else if with_timer {
                    serde_json::to_value(TimerTaskCommand {
                        deadline_unix_ms: 200,
                    })
                } else if with_tool && !with_model_approval {
                    let file = payloads.stage(operation, "tool-input", b"hello", None)?.await?;
                    serde_json::to_value(ToolTaskCommand {
                        name: "test.restore".into(),
                        revision: "1".into(),
                        arguments: if with_policy {
                            json!({"file":file,"approval":true})
                        } else {
                            json!({"file":file})
                        },
                    })
                } else {
                    serde_json::to_value(ModelTaskCommand {
                        summary_source: None,
                        context_manifest: acyclic_harness::context::ContextManifest::new(
                            Default::default(), Vec::new(), &tools,
                        )?.publish(payloads.as_ref(), OperationId::new(), "manifest.json", Limits::default()).await?,
                        input: if with_model_approval {
                            let file = payloads.stage(operation, "model-input", b"hello", None)?.await?;
                            let mut turns = vec![ScriptedTurn::Events(vec![
                                ScriptedEvent::Event(ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                                    "provider-call".into(), "test.restore".into(),
                                    json!({"file":file,"approval":true}),
                                ))),
                                ScriptedEvent::Event(ModelEvent::Completed { metadata: Value::Null }),
                            ])];
                            if !declined { turns.push(ScriptedTurn::Text("partial-restored".into())); }
                            model = Some(Arc::new(task_model(turns)));
                            approval_file = Some(file.clone());
                            ModelContent::Part(acyclic_harness::model::ModelContentPart::File {
                                file,
                                policy: acyclic_harness::model::FileProjectionPolicy::Reference,
                            })
                        } else {
                            ModelContent::Text("hello".into())
                        },
                        selected_context: None,
                        max_steps: if with_model_approval { 2 } else { 1 },
                        max_output_tokens: Some(8_192),
                        selected_output_volume: None,
                        mailbox_operation: None,
                    })
                }
                .map_err(|error| Error::Invalid(error.to_string()))?;
                let bytes = serde_json::to_vec(&payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                serde_json::to_value(payloads.stage(operation, "command-payload", &bytes, None)?.await?)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            } else {
                json!(0)
            };
            assert!(matches!(
                runtime
                    .harness()
                    .admit(operation, &definition, input, None)
                    .await?,
                Admission::Accepted(_)
            ));
        }
        Ok::<_, Error>(())
        }).await?;
        let model = model
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing worker model script".into()))?;
        let mut coordinator = DistributedCoordinator::open(
            &stream,
            reader.clone(),
            Some(payloads.clone()),
            &mut None,
        )
        .await?;
        let stop = boxed_restart_phase(|| async {
        if reopened && !uncertain && !with_mail_send {
            assert!(!runtime.poll_task_wake(task, None).await?);
            if with_timer || with_mail_receive || with_child || with_approval_wait {
                let cursor = discovery_cursor
                    .as_ref()
                    .map(|bytes: &Vec<u8>| serde_json::from_slice(bytes))
                    .transpose()
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert!(discover_wakes(&runtime, cursor).await?.is_empty());
                assert!(matches!(
                    runtime.poll_task_wake_page(None, 0).await,
                    Err(Error::Invalid(_))
                ));
                assert!(
                    runtime
                        .poll_task_wake_page(None, u32::MAX)
                        .await?
                        .events_read
                        <= 64
                );
                assert!(matches!(
                    runtime
                        .poll_task_wake_page(
                            Some(TaskWakeCursor {
                                after_revision: 1,
                                through_revision: 0,
                            }),
                            1
                        )
                        .await,
                    Err(Error::Invalid(_))
                ));
                assert!(matches!(
                    runtime
                        .poll_task_wake_page(
                            Some(TaskWakeCursor {
                                after_revision: 0,
                                through_revision: u64::MAX,
                            }),
                            1
                        )
                        .await,
                    Err(Error::Invalid(_))
                ));
            }
            clock.0.store(200, Ordering::SeqCst);
            if cancel_wait {
                runtime.task_host().cancel(task).await?;
                assert!(!runtime.poll_task_wake(task, None).await?);
                assert!(discover_wakes(&runtime, None).await?.is_empty());
                assert_eq!(
                    runtime.task_host().outcome(task).await?,
                    Some(Outcome::Cancelled)
                );
                assert!(coordinator.pull(&worker).await?.is_none());
                return Ok(true);
            }
            let retained = coordinator
                .scheduler()
                .operation(operation)
                .ok_or_else(|| Error::NotFound("retained task".into()))?;
            assert_eq!(
                retained.phase,
                acyclic_harness::scheduler::OperationPhase::Suspended
            );
            assert!(retained.reservation.is_none());
            if with_child {
                let child_lease = coordinator
                    .pull(&worker)
                    .await?
                    .ok_or_else(|| Error::NotFound("admitted child lease".into()))?;
                assert_eq!(
                    child_lease
                        .operation
                        .parent
                        .as_ref()
                        .map(|parent| parent.operation_id),
                    Some(operation)
                );
                let child = TaskId::from_bytes(child_lease.operation.operation_id.into_bytes());
                let admission = runtime.task_host().observe_admission(child).await?;
                assert_eq!(admission.parent, Some(task));
                let old = old_lease
                    .as_ref()
                    .ok_or_else(|| Error::NotFound("old parent lease".into()))?;
                assert!(
                    runtime
                        .task_host()
                        .admit_owned(admission.clone(), LeaseFence::from(&old.reservation))
                        .await
                        .is_err()
                );
                assert!(
                    matches!(runtime.run_task(child_lease, &runtime.commands(), 1).await?, TaskWorkerOutcome::Completed {task: completed} if completed == child)
                );
                assert_eq!(
                    runtime.task_host().outcome(child).await?,
                    Some(Outcome::Succeeded(json!(7)))
                );
            } else {
                assert!(coordinator.pull(&worker).await?.is_none());
            }
            if with_mail_receive {
                for index in [30u8, 31] {
                    let file = payloads
                        .stage(operation, &format!("mail-incoming-{index}"), b"7", None)?
                        .await?;
                    runtime
                        .task_host()
                        .send(task, task, OperationId::from_bytes([index; 16]), file)
                        .await?;
                }
            }
            if with_approval_wait {
                assert_eq!(tool.executed.load(Ordering::SeqCst), 0);
                Box::pin(resolve_worker_approval(
                    &stream,
                    filesystem.clone(),
                    &conversation_issuer,
                    conversation_scope.clone(),
                    volume.clone(),
                    agent,
                    !declined,
                ))
                .await?;
            }
            let input = payloads.stage(operation, "wake-seven", b"7", None)?.await?;
            assert!(
                runtime
                    .task_host()
                    .resume_workflow(task, 2, input.clone(), IdempotencyKey::new("wrong-revision")?, None, &mut None)
                    .await
                    .is_err()
            );
            if with_timer || with_mail_receive || with_child || with_approval_wait {
                let evaluations = policy.evaluated.load(Ordering::SeqCst);
                            let generated = model.dispatches().len();
                let reconciled = model.reconciliations().len();
                let executed = tool.executed.load(Ordering::SeqCst);
                assert_eq!(discover_wakes(&runtime, None).await?, vec![task]);
                assert!(discover_wakes(&runtime, None).await?.is_empty());
                assert!(!runtime.poll_task_wake(task, None).await?);
                assert_eq!(policy.evaluated.load(Ordering::SeqCst), evaluations);
                assert_eq!(model.dispatches().len(), generated);
                assert_eq!(model.reconciliations().len(), reconciled);
                assert_eq!(tool.executed.load(Ordering::SeqCst), executed);
                if with_two_waits {
                    let first_wake = coordinator
                        .pull(&worker)
                        .await?
                        .ok_or_else(|| Error::NotFound("first passive wake lease".into()))?;
                    assert!(matches!(
                        runtime.run_task(first_wake, &runtime.commands(), 2).await?,
                        TaskWorkerOutcome::Suspended { revision: 1, .. }
                    ));
                    let next_wait = DistributedCoordinator::open(&stream, reader.clone(), None, &mut None).await?;
                    assert_eq!(
                        next_wait
                            .scheduler()
                            .operation(operation)
                            .and_then(|state| state.workflow.as_ref())
                            .and_then(|slot| slot.waiting_command),
                        Some(OperationId::from_bytes([27; 16]))
                    );
                    assert!(!runtime.poll_task_wake(task, None).await?);
                    clock.0.store(300, Ordering::SeqCst);
                    assert_eq!(discover_wakes(&runtime, None).await?, vec![task]);
                    assert!(!runtime.poll_task_wake(task, None).await?);
                }
                let wake_projection = DistributedCoordinator::open(&stream, reader.clone(), None, &mut None).await?;
                let woken = wake_projection
                    .scheduler()
                    .operation(operation)
                    .ok_or_else(|| Error::NotFound("woken task".into()))?;
                assert!(woken.reservation.is_none());
                assert_eq!(
                    woken.phase,
                    acyclic_harness::scheduler::OperationPhase::WaitingForCapacity
                );
                assert!(
                    woken
                        .workflow
                        .as_ref()
                        .and_then(|slot| slot.waiting_command)
                        .is_some()
                );
            } else {
                runtime
                    .task_host()
                    .resume_workflow(task, 1, input.clone(), IdempotencyKey::new("wake-seven")?, None, &mut None)
                    .await?;
                runtime
                    .task_host()
                    .resume_workflow(task, 1, input, IdempotencyKey::new("wake-seven")?, None, &mut None)
                    .await?;
            }
        }
        Ok::<_, Error>(false)
        }).await?;
        if stop {
            return Ok(());
        }
        let lease = boxed_restart_phase(|| async {
            let lease = if !reopened && with_command {
                let before = acyclic_stream::Stream::new(
                    std::sync::Arc::clone(&stream),
                    "harness/v3/coordinator/events",
                )?
                .tail()
                .await?;
                assert!(matches!(
                    runtime.worker_tick(&worker, None, &NoCommands, 1, 0).await,
                    Err(Error::Invalid(_))
                ));
                assert_eq!(
                    acyclic_stream::Stream::new(
                        std::sync::Arc::clone(&stream),
                        "harness/v3/coordinator/events"
                    )?
                    .tail()
                    .await?,
                    before
                );
                let tick = runtime
                    .worker_tick(&worker, None, &runtime.commands(), 1, 1)
                    .await?;
                assert_eq!(tick.wake.events_read, 1);
                let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) =
                    tick.work
                else {
                    return Err(Error::NotFound("initial worker tick lease".into()));
                };
                assert_eq!(lease.operation.operation_id, operation);
                let idle = runtime
                    .worker_tick(&worker, tick.wake.cursor, &NoCommands, 1, 1)
                    .await?;
                assert!(idle.work.is_none());
                lease
            } else if reopened && (uncertain || with_mail_send) {
                let retained = coordinator
                    .scheduler()
                    .operation(operation)
                    .ok_or_else(|| Error::NotFound("uncertain task".into()))?;
                assert_eq!(
                    retained.phase,
                    if uncertain {
                        acyclic_harness::scheduler::OperationPhase::Reconciling
                    } else {
                        acyclic_harness::scheduler::OperationPhase::Running
                    }
                );
                assert!(retained.reservation.is_some());
                assert!(coordinator.pull(&worker).await?.is_none());
                old_lease
                    .clone()
                    .ok_or_else(|| Error::NotFound("retained lease".into()))?
            } else {
                coordinator
                    .pull(&worker)
                    .await?
                    .ok_or_else(|| Error::NotFound("worker lease".into()))?
            };
            if let Some(old) = &old_lease
                && !uncertain
                && !with_mail_send
            {
                assert!(runtime.run_task(old.clone(), &NoCommands, 1).await.is_err());
                assert!(
                    matches!(runtime.resume_task(old.clone(), &NoCommands, 1, None).await,
                TaskWorkerAttempt::Unresolved { lease: attempted, .. } if attempted == *old)
                );
            }
            Ok::<_, Error>(lease)
        })
        .await?;
        let Some(outcome) = boxed_restart_phase(|| async {
        let outcome = if with_command {
            if !reopened {
                assert!(
                    matches!(runtime.resume_task(lease.clone(), &NoCommands, 0, None).await,
                    TaskWorkerAttempt::Unresolved { lease: retained, error: Error::Invalid(_) } if retained == lease)
                );
                assert!(
                    runtime
                        .run_task(lease.clone(), &NoCommands, 0)
                        .await
                        .is_err()
                );
                assert_eq!(model.dispatches().len(), 0);
                coordinator.refresh(&mut None).await?;
                let retained = coordinator
                    .scheduler()
                    .operation(operation)
                    .ok_or_else(|| Error::NotFound("yielded task".into()))?;
                assert_eq!(
                    retained.phase,
                    acyclic_harness::scheduler::OperationPhase::Running
                );
                assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
                if with_child {
                    let context = runtime.harness().durable_context(task, operation, None).await?;
                    let reference = runtime
                        .task_host()
                        .observe_admission(task)
                        .await?
                        .input
                        .get("admit")
                        .cloned()
                        .ok_or_else(|| Error::Invalid("missing admission file".into()))?;
                    let file = serde_json::from_value(reference)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    let command = WorkflowCommand {
                        operation_id: OperationId::from_bytes([26; 16]),
                        kind: TASK_ADMIT_COMMAND_KIND.into(),
                        payload: file,
                    };
                    let wrong = serde_json::to_value(TaskAdmitCommand {
                        name: "test.child".into(),
                        version: "2".into(),
                        input: json!(7),
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert!(matches!(
                        runtime
                            .commands()
                            .execute(context.as_ref().clone(), LeaseFence::from(&lease.reservation), command, wrong)
                            .await,
                        Err(Error::NotFound(_))
                    ));
                }
                if with_mail_send {
                    let context = runtime.harness().durable_context(task, operation, None).await?;
                    let file = serde_json::from_value(
                        runtime.task_host().observe_admission(task).await?.input,
                    )
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    let input: MailSendTaskCommand =
                        serde_json::from_slice(context.read_file(&file, None).await?.as_ref())
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                    let fence = LeaseFence::from(&lease.reservation);
                    for index in 100..170u8 {
                        runtime
                            .task_host()
                            .send(
                                task,
                                task,
                                OperationId::from_bytes([index; 16]),
                                input.payload.clone(),
                            )
                            .await?;
                    }
                    let probe = OperationId::from_bytes([99; 16]);
                    let (stock, legacy) = tokio::join!(
                        runtime.task_host().send_mail(task, &fence, task, probe, input.payload.clone(), None),
                        runtime
                            .task_host()
                            .send(task, task, probe, input.payload.clone())
                    );
                    stock?;
                    legacy?;
                    let command = WorkflowCommand {
                        operation_id: OperationId::from_bytes([26; 16]),
                        kind: MAIL_SEND_TASK_COMMAND_KIND.into(),
                        payload: file,
                    };
                    let payload = serde_json::to_value(&input)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert!(matches!(
                        runtime
                            .commands()
                            .execute(context.as_ref().clone(), fence.clone(), command.clone(), payload)
                            .await?,
                        TaskCommandProgress::Ready(_)
                    ));
                    let other = payloads.stage(operation, "mail-conflict", b"8", None)?.await?;
                    let changed = serde_json::to_value(MailSendTaskCommand {
                        payload: other,
                        ..input
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert!(matches!(
                        runtime
                            .commands()
                            .execute(context.as_ref().clone(), fence, command, changed)
                            .await,
                        Err(Error::Conflict(_))
                    ));
                    assert_eq!(
                        acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v3/mail/by-recipient/{task}"))?
                            .bounds()
                            .await?
                            .tail,
                        72
                    );
                    coordinator.refresh(&mut None).await?;
                    assert_eq!(
                        coordinator
                            .scheduler()
                            .operation(operation)
                            .and_then(|state| state.reservation.as_ref()),
                        Some(&lease.reservation)
                    );
                    old_lease = Some(lease.clone());
                    return Ok(None);
                }
                if with_timer {
                    let fence = LeaseFence::from(&lease.reservation);
                    let probe = OperationId::from_bytes([99; 16]);
                    let (stock, legacy) = tokio::join!(
                        runtime
                            .task_host()
                            .poll_timer(task, &fence, probe, 1, None),
                        runtime.task_host().wait_until(task, &fence, probe, 1, None)
                    );
                    assert!(stock?);
                    assert!(legacy?);
                    assert!(matches!(
                        runtime.task_host().wait_until(task, &fence, probe, 2, None).await,
                        Err(Error::Conflict(_))
                    ));
                    assert!(matches!(
                        runtime
                            .task_host()
                            .poll_timer(task, &fence, probe, 0, None)
                            .await,
                        Err(Error::Invalid(_))
                    ));
                    for index in 100..170u8 {
                        assert!(
                            !runtime
                                .task_host()
                                .poll_timer(task, &fence, OperationId::from_bytes([index; 16]), 200, None)
                                .await?
                        );
                    }
                    let timer_operation = OperationId::from_bytes([26; 16]);
                    assert!(
                        !runtime
                            .task_host()
                            .poll_timer(task, &fence, timer_operation, 200, None)
                            .await?
                    );
                    assert!(
                        !runtime
                            .task_host()
                            .poll_timer(task, &fence, timer_operation, 200, None)
                            .await?
                    );
                    assert!(matches!(
                        runtime
                            .task_host()
                            .poll_timer(task, &fence, timer_operation, 201, None)
                            .await,
                        Err(Error::Conflict(_))
                    ));
                }
                if with_tool && !with_model_approval {
                    let context = runtime.harness().durable_context(task, operation, None).await?;
                    let admission = runtime.task_host().observe_admission(task).await?;
                    let file = serde_json::from_value(admission.input)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    let payload: ToolTaskCommand =
                        serde_json::from_slice(context.read_file(&file, None).await?.as_ref())
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                    let command = WorkflowCommand {
                        operation_id: OperationId::from_bytes([26; 16]),
                        kind: TOOL_TASK_COMMAND_KIND.into(),
                        payload: file,
                    };
                    if with_policy {
                        Box::pin(async {
                            let denied = serde_json::to_value(ToolTaskCommand {
                                arguments: json!({"denied":true}),
                                ..payload.clone()
                            })
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                            assert!(matches!(
                                runtime
                                    .commands()
                                    .execute(context.as_ref().clone(), LeaseFence::from(&lease.reservation), command.clone(), denied)
                                    .await,
                                Err(Error::Unauthorized(_))
                            ));
                            assert_eq!(tool.executed.load(Ordering::SeqCst), 0);
                            assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
                            assert_eq!(policy.evaluated.load(Ordering::SeqCst), 1);
                            let pending = serde_json::to_value(&payload)
                                .map_err(|error| Error::Invalid(error.to_string()))?;
                            for _ in 0..2 {
                                assert!(matches!(
                                    runtime
                                        .commands()
                                        .execute(context.as_ref().clone(), LeaseFence::from(&lease.reservation), command.clone(), pending.clone())
                                        .await,
                                    Ok(TaskCommandProgress::Pending)
                                ));
                            }
                            assert_eq!(tool.executed.load(Ordering::SeqCst), 0);
                            assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
                            if !with_approval_wait {
                                Box::pin(resolve_worker_approval(
                                    &stream,
                                    filesystem.clone(),
                                    &conversation_issuer,
                                    conversation_scope.clone(),
                                    volume.clone(),
                                    agent,
                                    true,
                                ))
                                .await?;
                            }
                            Ok::<(), Error>(())
                        })
                        .await?;
                    }
                    let wrong = serde_json::to_value(ToolTaskCommand {
                        revision: "2".into(),
                        ..payload
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert!(matches!(
                        runtime
                            .commands()
                            .execute(context.as_ref().clone(), LeaseFence::from(&lease.reservation), command, wrong)
                            .await,
                        Err(Error::NotFound(_))
                    ));
                    assert_eq!(tool.executed.load(Ordering::SeqCst), 0);
                    assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
                }
            }
            let attempt = runtime
                .resume_task(
                    lease.clone(),
                    &runtime.commands().with_model(&Model::new("test".into(), "interrupted".into(), "1".into(), Value::Null)?, model.clone(), &acyclic_harness::context::CompactionPolicy::default(), None)?,
                    2, None,
                )
                .await;
            match attempt {
                TaskWorkerAttempt::Progress(progress) => progress,
                TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
            }
        } else {
            runtime.run_task(lease.clone(), &NoCommands, 1).await?
        };
        Ok::<_, Error>(Some(outcome))
        }).await? else {
            continue;
        };
        let outcome = if with_approval_wait && reopened && !declined {
            assert!(
                matches!(outcome, TaskWorkerOutcome::Reconciling { lease: retained } if retained == lease)
            );
            assert_eq!(tool.executed.load(Ordering::SeqCst), 1);
            assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
            assert!(coordinator.pull(&worker).await?.is_none());
            match runtime
                .resume_task(
                    lease.clone(),
                    &runtime.commands().with_model(
                        &Model::new("test".into(), "interrupted".into(), "1".into(), Value::Null)?,
                        model.clone(),
                        &acyclic_harness::context::CompactionPolicy::default(),
                        None,
                    )?,
                    2,
                    None,
                )
                .await
            {
                TaskWorkerAttempt::Progress(progress) => progress,
                TaskWorkerAttempt::Unresolved { error, .. } => return Err(error),
            }
        } else {
            outcome
        };
        if with_mail_receive && !allow_mail_read {
            // A missing read grant is definitive: the task settles failed and releases its lease.
            assert!(
                matches!(outcome, TaskWorkerOutcome::Completed { task: completed } if completed == task)
            );
            coordinator.refresh(&mut None).await?;
            let settled = coordinator.scheduler().operation(operation);
            assert!(matches!(settled, Some(state) if state.reservation.is_none()
                && matches!(&state.outcome, Some(acyclic_harness::Outcome::Failed { message }) if message.contains("mail:read"))));
            assert!(matches!(
                runtime.task_host().inbox(task, 0, 1).await,
                Err(Error::Unauthorized(_))
            ));
            assert!(
                acyclic_stream::Stream::new(
                    std::sync::Arc::clone(&stream),
                    format!("harness/v3/mail/by-recipient/{task}")
                )?
                .bounds()
                .await
                .is_err()
            );
            return Ok(());
        }
        if reopened {
            assert!(
                matches!(outcome, TaskWorkerOutcome::Completed { task: completed } if completed == task)
            );
            coordinator.refresh(&mut None).await?;
            let retained = coordinator
                .scheduler()
                .operation(operation)
                .ok_or_else(|| Error::NotFound("completed task".into()))?;
            assert_eq!(
                retained.phase,
                acyclic_harness::scheduler::OperationPhase::Terminal
            );
            assert!(retained.reservation.is_none());
            let Some(acyclic_harness::Outcome::Succeeded(result)) = &retained.outcome else {
                return Err(Error::Invalid("missing result".into()));
            };
            assert_eq!(reader.read(result, 65_536, None)?.await?.as_ref(), b"7");
            assert!(coordinator.pull(&worker).await?.is_none());
            assert_completed_dispatches(command, &model, &tool);
        } else {
            if uncertain {
                assert!(
                    matches!(outcome, TaskWorkerOutcome::Reconciling { lease: retained } if retained == lease)
                );
                assert_pending_dispatches(command, &model, &tool, with_tool);
            } else {
                assert!(
                    matches!(outcome, TaskWorkerOutcome::Suspended { task: suspended, revision:1 } if suspended == task)
                );
            }
            old_lease = Some(lease.clone());
        }
        if !reopened && (with_timer || with_mail_receive || with_child || with_approval_wait) {
            let evaluations = policy.evaluated.load(Ordering::SeqCst);
            let generated = model.dispatches().len();
            let first = runtime.poll_task_wake_page(None, 1).await?;
            assert_eq!(first.events_read, 1);
            assert!(first.woken.is_empty());
            let cursor = first
                .cursor
                .ok_or_else(|| Error::NotFound("cold discovery cursor".into()))?;
            discovery_cursor = Some(
                serde_json::to_vec(&cursor).map_err(|error| Error::Invalid(error.to_string()))?,
            );
            assert_eq!(policy.evaluated.load(Ordering::SeqCst), evaluations);
            assert_eq!(model.dispatches().len(), generated);
            if with_model_approval {
                assert_eq!(generated, 1);
                assert_eq!(model.reconciliations().len(), 0);
                assert_eq!(tool.executed.load(Ordering::SeqCst), 0);
                assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
            }
        }
        if with_timer {
            if let Some(old) = &old_lease {
                assert!(
                    runtime
                        .task_host()
                        .poll_timer(
                            task,
                            &LeaseFence::from(&old.reservation),
                            OperationId::from_bytes([26; 16]),
                            200,
                            None
                        )
                        .await
                        .is_err()
                );
            }
        }
        if with_mail_send || (with_mail_receive && reopened) {
            mail::assert_mail(&runtime, task, reader.as_ref(), with_mail_send, &lease).await?;
        }
    }
    let model = model
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing worker model script".into()))?;
    if let Some(expected) = &approval_file {
        for request in model.requests() {
            assert_eq!(
                request
                    .messages
                    .iter()
                    .flat_map(|message| message.content.file_refs())
                    .next(),
                Some(expected)
            );
        }
    }
    assert_eq!(model.remaining(), 0);
    assert!(model.recoveries().is_empty());
    if with_policy && with_tool {
        assert_policy_dispatches(&policy, &tool, declined);
    }
    Ok(())
}

#[path = "worker_mail.rs"]
mod mail;
