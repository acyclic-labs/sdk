use super::*;

#[tokio::test]
async fn direct_stock_turn_reconciles_through_runtime_after_provider_reopen() -> Result<()> {
    stock_restart_with_publication_fault(None, false, true).await
}

#[tokio::test]
async fn stock_model_publication_faults_reconcile_after_provider_reopen() -> Result<()> {
    futures::future::try_join_all(
        [
            ExecutionFaultMode::Before,
            ExecutionFaultMode::AfterVisible,
            ExecutionFaultMode::AfterHidden,
        ]
        .map(|mode| stock_restart_with_publication_fault(Some(mode), false, false)),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_uncertain_model_retains_ownership_until_fenced_release_after_reopen()
-> Result<()> {
    futures::future::try_join_all(
        [
            ExecutionFaultMode::Before,
            ExecutionFaultMode::AfterVisible,
            ExecutionFaultMode::AfterHidden,
        ]
        .map(|mode| stock_restart_with_publication_fault(Some(mode), true, false)),
    )
    .await?;
    Ok(())
}

#[allow(
    clippy::cognitive_complexity,
    reason = "one restart ownership scenario shared by normal, publication-fault and cancellation cases"
)]
pub(super) async fn stock_restart_with_publication_fault(
    fault: Option<ExecutionFaultMode>,
    cancel_after_failure: bool,
    direct_executor: bool,
) -> Result<()> {
    let directory = workspace()?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new(
        "task-restart".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "task-owner".into(),
    };
    let issuer = AuthorityIssuer::new("task-restart", [7; 32], authority.clone());
    let signed = issuer
        .issue(
            "owner",
            &Capabilities::new([
                "operation:declare".to_owned(),
                "operation:observe".to_owned(),
                "operation:cancel".to_owned(),
                "task:spawn:test.restart@1".to_owned(),
                "model:generate".to_owned(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?
        .with_run_limits(
            TaskRunLimits {
                max_steps: Some(1),
                ..TaskRunLimits::default()
            },
            &mut None,
        )?;
    let machine: Arc<dyn ResumableMachine> = Arc::new(TaskMachine {
        identity: MachineIdentity {
            name: "test.restart".into(),
            version: "1".into(),
            digest: [2; 32],
        },
        schema: json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<u64, u64>::resumable(
        machine.clone(),
        json!({"type":"integer"}),
        json!({"type":"integer"}),
    )?;
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let definition = tasks.get_version::<u64, u64>(
        "test.restart",
        "1",
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        None,
    )?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let operation = OperationId::from_bytes([3; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let step = OperationId::from_bytes([4; 16]);
    let worker = Worker {
        id: "worker".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let mut previous_lease: Option<acyclic_harness::distributed::WorkLease> = None;
    let model = interrupted_model(usize::from(fault != Some(ExecutionFaultMode::Before)));
    let session_limits = SessionLimits {
        active_tasks: 1,
        total_tasks: 2,
        depth: 1,
        model_steps: if cancel_after_failure { 2 } else { 1 },
    };
    for reopened in [false, true] {
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
        let stream_provider = Arc::new(LostSessionAck::new(
            LocalStream::open(&stream_root, LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let stream = stream_provider.clone();
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
        let host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(
                &stream,
                reader.clone(),
                Some(payloads.clone()),
                &mut None,
            )
            .await?,
            stream.clone(),
            payloads.clone(),
            reader.clone(),
            authority.clone(),
            signed.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?
        .with_session_limits(if reopened {
            SessionLimits {
                model_steps: session_limits.model_steps + 1,
                ..session_limits
            }
        } else {
            session_limits
        })?;
        if reopened {
            assert!(host.observe_admission(task).await.is_err());
        }
        drop(host);
        let runtime = FilesystemTaskRuntime::open(
            stream.clone(),
            filesystem.clone(),
            &volume,
            None,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            &scope,
            &tasks,
            &machines,
            &ToolRegistry::default(),
            session_limits,
            1,
            65_536,
            Arc::new(SystemUnixMillisClock),
            None,
            &[],
            &acyclic_harness::durable_tool::ResumableToolRegistry::default(),
            None,
        )
        .await?;
        let host = runtime.task_host();
        let harness = runtime.harness();
        if !reopened {
            let task_grants = scope.grants().without(&Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?
            ]));
            assert!(matches!(
                harness
                    .scoped(task_grants, scope.limits())?
                    .admit(operation, &definition, 0, None)
                    .await?,
                Admission::Accepted(_)
            ));
        }
        let input_file = payloads
            .stage(operation, "owner-readable-input", b"\"secret\"", None)?
            .await?;
        reader.verify(&input_file, 65_536, None)?.await?;
        assert!(matches!(
            harness
                .durable_context(task, operation, None)
                .await?
                .read_file(&input_file, None)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut coordinator =
            DistributedCoordinator::open(&stream, reader, Some(payloads), &mut None).await?;
        assert_eq!(
            coordinator
                .scheduler()
                .session_limits(operation, &mut None)?,
            session_limits
        );
        let lease = if reopened && cancel_after_failure {
            let old = previous_lease
                .as_ref()
                .ok_or_else(|| Error::NotFound("cancelled task lease".into()))?;
            let retained = coordinator
                .scheduler()
                .operation(operation)
                .ok_or_else(|| Error::NotFound("cancelled task ownership".into()))?;
            assert!(retained.cancellation_requested);
            assert_eq!(retained.reservation.as_ref(), Some(&old.reservation));
            assert_eq!(
                retained.phase,
                acyclic_harness::scheduler::OperationPhase::Running
            );
            assert!(coordinator.pull(&worker).await?.is_none());
            old.clone()
        } else {
            if let Some(old) = &previous_lease {
                coordinator
                    .apply(
                        IdempotencyKey::new("release-crashed")?,
                        acyclic_harness::scheduler::SchedulerEvent::LeaseReleased {
                            operation_id: old.operation.operation_id,
                            fence: acyclic_harness::scheduler::LeaseFence::from(&old.reservation),
                        },
                        &mut None,
                    )
                    .await?;
            }
            coordinator
                .pull(&worker)
                .await?
                .ok_or_else(|| Error::NotFound("task lease".into()))?
        };
        let fence = LeaseFence::from(&lease.reservation);
        if !reopened || !cancel_after_failure {
            coordinator
                .apply(
                    IdempotencyKey::new(if reopened { "restart" } else { "start" })?,
                    SchedulerEvent::Started {
                        operation_id: operation,
                        fence: fence.clone(),
                    },
                    &mut None,
                )
                .await?;
        }
        if let Some(old) = &previous_lease
            && !cancel_after_failure
        {
            assert!(
                host.journal_owner(task, &LeaseFence::from(&old.reservation), None)
                    .await
                    .is_err()
            );
        }
        assert!(
            definition
                .open(
                    harness.durable_context(task, operation, None).await?,
                    &fence,
                    Arc::new(MemoryWorkflowJournal::default())
                )?
                .await
                .is_err()
        );
        let compaction_policy = if fault.is_none() {
            acyclic_harness::context::CompactionPolicy::Threshold(
                acyclic_harness::context::ThresholdCompaction {
                    response_reserve_tokens: 8_192,
                    ..acyclic_harness::context::ThresholdCompaction::default()
                },
            )
        } else {
            acyclic_harness::context::CompactionPolicy::default()
        };
        let execution = runtime
            .stock_execution(
                task,
                fence.clone(),
                OperationId::from_bytes([8; 16]),
                Model::new("test".into(), "interrupted".into(), "1".into(), Value::Null)?,
                model.clone(),
                ContextPipeline::default(),
                ToolRegistry::default(),
                None,
            )
            .await?
            .with_compaction_policy(compaction_policy.clone());
        let turn = TurnInput {
            operation_id: execution.operation_id(),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 1,
        };
        assert!(matches!(execution.execute(TurnInput {
            input: ModelContent::Part(ModelContentPart::File { file: input_file, policy: FileProjectionPolicy::Native(Box::new(acyclic_harness::model::NativeMediaPolicy { intent: acyclic_harness::model::NativeMediaIntent::Image { detail: acyclic_harness::model::ImageDetail::Auto }, maximum_bytes: acyclic_harness::conversation::MAX_EXACT_JS_INTEGER, maximum_work: 4096, configuration: None })) }),
            ..turn.clone()
        }).await, Err(Error::Unauthorized(message)) if message == "attenuated task cannot read model content"));
        assert!(
            execution
                .execute(TurnInput {
                    operation_id: OperationId::from_bytes([9; 16]),
                    ..turn.clone()
                })
                .await
                .is_err()
        );
        assert!(
            execution
                .execute(TurnInput {
                    max_steps: 2,
                    ..turn.clone()
                })
                .await
                .is_err()
        );
        if reopened {
            if direct_executor {
                let drifted = runtime
                    .stock_execution(
                        task,
                        fence.clone(),
                        OperationId::from_bytes([8; 16]),
                        Model::new("test".into(), "interrupted".into(), "1".into(), Value::Null)?,
                        model.clone(),
                        ContextPipeline::default(),
                        ToolRegistry::default(),
                        None,
                    )
                    .await?
                    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled);
                assert!(matches!(
                    drifted.execute(turn.clone()).await,
                    Err(Error::Conflict(message))
                        if message == "execution identity is bound to another request or configuration"
                ));
                assert_eq!(model.dispatches().len(), 1);
                assert_eq!(model.reconciliations().len(), 0);
            }
            let restored = execution.execute(turn.clone()).await?;
            assert_eq!(
                restored.text,
                if fault == Some(ExecutionFaultMode::Before) {
                    "restored"
                } else {
                    "partial-restored"
                }
            );
            assert_eq!(execution.execute(turn).await?, restored);
            assert_eq!(model.dispatches().len(), 1);
            assert_eq!(model.reconciliations().len(), 1);
            if fault.is_none() {
                assert_eq!(
                    model
                        .requests()
                        .last()
                        .and_then(|request| request.max_output_tokens),
                    Some(8_192)
                );
            }
            let another = runtime
                .stock_execution(
                    task,
                    fence.clone(),
                    OperationId::from_bytes([10; 16]),
                    Model::new("test".into(), "interrupted".into(), "1".into(), Value::Null)?,
                    model.clone(),
                    ContextPipeline::default(),
                    ToolRegistry::default(),
                    None,
                )
                .await?;
            assert_ne!(another.operation_id(), execution.operation_id());
            let before_fresh_attempt = coordinator.scheduler().clone();
            let rejected = another
                .execute(TurnInput {
                    operation_id: another.operation_id(),
                    input: ModelContent::Text("another".into()),
                    selected_context: None,
                    max_steps: 1,
                })
                .await;
            if cancel_after_failure {
                assert!(matches!(rejected, Err(Error::Conflict(message))
                    if message == "cancelled task cannot publish new execution work"));
                coordinator.refresh(&mut None).await?;
                assert_eq!(coordinator.scheduler(), &before_fresh_attempt);
            } else {
                assert!(rejected.is_err());
            }
            assert_eq!(model.dispatches().len(), 1);
            assert_eq!(model.reconciliations().len(), 1);
        } else {
            if let Some(mode) = fault {
                stream_provider.arm_execution(3, mode);
            }
            let interrupted = if direct_executor {
                let admitted_context = harness.durable_context(task, operation, None).await?;
                let previous = StockExecutor::new(
                    Model::new("test".into(), "interrupted".into(), "1".into(), Value::Null)?,
                    model.clone(),
                    ContextPipeline::default(),
                    ToolRegistry::default(),
                )
                .with_limits(admitted_context.scope().limits())
                .with_compaction_policy(compaction_policy.clone())
                .with_tool_authority(admitted_context.scope().clone(), None)?
                .with_durable_task(host.clone(), task, fence.clone())
                .with_task_context(&admitted_context, execution.operation_id())?;
                let journal = FilesystemExecutionJournal::for_task(
                    host.journal_owner(task, &fence.clone(), None).await?,
                    execution.operation_id(),
                    OperationId::from_bytes([8; 16]),
                    filesystem.clone(),
                    acyclic_harness::filesystem::FilesystemJournalBinding {
                        volume: &volume,
                        verifier: &issuer.verifier(&mut None).expect("verifier copy"),
                        scope: &signed,
                        maximum_bytes: 65_536,
                        io: None,
                    },
                )?;
                previous.execute(turn, &journal).await
            } else {
                execution.execute(turn).await
            };
            assert!(
                match fault {
                    Some(ExecutionFaultMode::Before | ExecutionFaultMode::AfterHidden) =>
                        matches!(interrupted, Err(Error::Indeterminate(id)) if id == operation),
                    _ => matches!(interrupted, Err(Error::Storage(_))),
                },
                "{interrupted:?}"
            );
            assert_eq!(
                stream_provider.execution_faults.load(Ordering::SeqCst),
                usize::from(fault.is_some())
            );
            assert_eq!(model.dispatches().len(), 1);
            assert_eq!(model.reconciliations().len(), 0);
            coordinator.refresh(&mut None).await?;
            let retained = coordinator
                .scheduler()
                .operation(operation)
                .ok_or_else(|| Error::NotFound("faulted task ownership".into()))?;
            assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
            assert_eq!(
                retained.phase,
                acyclic_harness::scheduler::OperationPhase::Running
            );
        }
        drop(execution);
        if cancel_after_failure {
            if reopened {
                let old = previous_lease
                    .as_ref()
                    .ok_or_else(|| Error::NotFound("cancelled task lease".into()))?;
                coordinator.refresh(&mut None).await?;
                assert_eq!(
                    coordinator
                        .scheduler()
                        .operation(operation)
                        .and_then(|state| state.reservation.as_ref()),
                    Some(&old.reservation)
                );
                let mut stale = old.clone();
                stale.reservation.id = "stale".into();
                assert!(
                    coordinator
                        .apply(
                            IdempotencyKey::new("stale-cancelled-release")?,
                            acyclic_harness::scheduler::SchedulerEvent::LeaseReleased {
                                operation_id: stale.operation.operation_id,
                                fence: acyclic_harness::scheduler::LeaseFence::from(
                                    &stale.reservation
                                )
                            },
                            &mut None
                        )
                        .await
                        .is_err()
                );
                coordinator
                    .apply(
                        IdempotencyKey::new("cancelled-reconciled-release")?,
                        acyclic_harness::scheduler::SchedulerEvent::LeaseReleased {
                            operation_id: old.operation.operation_id,
                            fence: acyclic_harness::scheduler::LeaseFence::from(&old.reservation),
                        },
                        &mut None,
                    )
                    .await?;
                let terminal = coordinator
                    .scheduler()
                    .operation(operation)
                    .ok_or_else(|| Error::NotFound("cancelled terminal task".into()))?;
                assert_eq!(terminal.outcome, Some(Outcome::Cancelled));
                assert!(terminal.reservation.is_none());
                assert!(coordinator.pull(&worker).await?.is_none());
                assert!(
                    host.journal_owner(task, &LeaseFence::from(&old.reservation), None)
                        .await
                        .is_err()
                );
                return Ok(());
            }
            coordinator
                .cancel_operation(
                    &authority,
                    &signed,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    operation,
                    IdempotencyKey::new("cancel-uncertain-model")?,
                    false,
                    None,
                    &mut None,
                )
                .await?;
            assert!(coordinator.pull(&worker).await?.is_none());
            previous_lease = Some(lease);
            continue;
        }
        let journal = runtime.workflow_journal(task, fence.clone()).await?;
        let mut session = harness
            .open_registered_task(
                task,
                &fence,
                journal.clone(),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                None,
            )
            .await?;
        assert_eq!(session.task_id(), task);
        if reopened {
            assert_eq!(session.checkpoint().revision, 1);
            assert_eq!(session.checkpoint().state, json!(1));
            assert_eq!(
                session
                    .latest_transition(None)
                    .await?
                    .0
                    .map(|value| value.status),
                Some(MachineStatus::Suspended)
            );
        } else {
            assert!(session.latest_transition(None).await?.0.is_none());
            let invalid_input = json!("bad");
            let prior = session.checkpoint().clone();
            let (_, transition, _, _) =
                machines.step(&prior, std::borrow::Cow::Borrowed(&invalid_input), None)?;
            let invalid_record = WorkflowRecord {
                operation_id: OperationId::from_bytes([8; 16]),
                idempotency_key: IdempotencyKey::new("direct-invalid-output")?,
                prior,
                input: invalid_input,
                transition,
            };
            assert!(
                journal
                    .commit(0, invalid_record.idempotency_key.clone(), invalid_record)
                    .await
                    .is_err()
            );
            assert!(journal.replay(0, 64, None)?.await?.0.is_empty());
            assert!(
                session
                    .step(
                        OperationId::from_bytes([5; 16]),
                        IdempotencyKey::new("invalid-output")?,
                        json!("bad"),
                        None
                    )
                    .await
                    .is_err()
            );
            assert_eq!(session.checkpoint().revision, 0);
        }
        assert_eq!(
            session
                .step(step, IdempotencyKey::new("suspend")?, Value::Null, None)
                .await?
                .0
                .status,
            MachineStatus::Suspended
        );
        assert_eq!(session.checkpoint().revision, 1);
        assert_eq!(journal.replay(0, 64, None)?.await?.0.len(), 1);
        assert!(
            session
                .step(step, IdempotencyKey::new("suspend")?, json!(9), None)
                .await
                .is_err()
        );
        if reopened {
            let completed_step = OperationId::from_bytes([6; 16]);
            assert_eq!(
                session
                    .step(
                        completed_step,
                        IdempotencyKey::new("complete")?,
                        json!(7),
                        None
                    )
                    .await?
                    .0
                    .status,
                MachineStatus::Completed { value: json!(7) }
            );
            assert_eq!(session.checkpoint().revision, 2);
            coordinator
                .cancel_operation(
                    &authority,
                    &signed,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    operation,
                    IdempotencyKey::new("cancel")?,
                    false,
                    None,
                    &mut None,
                )
                .await?;
            assert!(
                session
                    .step(
                        completed_step,
                        IdempotencyKey::new("complete")?,
                        json!(7),
                        None
                    )
                    .await
                    .is_err()
            );
            drop(session);
            let recovery_journal = runtime.workflow_journal(task, fence.clone()).await?;
            let mut recovered = harness
                .open_registered_task(
                    task,
                    &fence,
                    recovery_journal.clone(),
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    None,
                )
                .await?;
            assert_eq!(
                recovered
                    .latest_transition(None)
                    .await?
                    .0
                    .map(|value| value.status),
                Some(MachineStatus::Completed { value: json!(7) })
            );
            assert!(
                recovered
                    .step(
                        completed_step,
                        IdempotencyKey::new("complete")?,
                        json!(7),
                        None
                    )
                    .await
                    .is_err()
            );
            assert_eq!(recovery_journal.replay(0, 64, None)?.await?.0.len(), 2);
        }
        previous_lease = Some(lease);
    }
    Ok(())
}
