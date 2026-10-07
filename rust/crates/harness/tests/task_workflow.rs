//! Registered task checkpoint recovery through real durable-local providers.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{Fs, LocalOptions};
use acyclic_harness::context::ContextPipeline;
use acyclic_harness::conversation::{
    ContentResidencyVerifier, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{AggregateKind, Authority, AuthorityIssuer};
use acyclic_harness::distributed::{DistributedCoordinator, SchedulerPayloadStore, Worker};
use acyclic_harness::durable_host::CoordinatorTaskHost;
use acyclic_harness::executor::TurnInput;
use acyclic_harness::filesystem::{
    FilesystemContentVerifier, FilesystemHost, FilesystemSchedulerPayloadStore,
    FilesystemTaskRuntime,
};
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent,
    ModelProvider, ModelRequest,
};
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::runtime::{
    DurableTaskHost, RuntimeScope, TaskDefinition, TaskRegistry, TaskRunLimits,
};
use acyclic_harness::scheduler::{LeaseFence, ResourceSnapshot, SchedulerEvent, SessionLimits};
use acyclic_harness::tool::ToolRegistry;
use acyclic_harness::workflow::{
    MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, MemoryWorkflowJournal,
    ResumableMachine, WorkflowJournal, WorkflowRecord,
};
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result, TaskId,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient, SystemUnixMillisClock};
use futures::{future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeMap, sync::Arc};

struct InterruptedModel {
    generated: AtomicUsize,
    reconciled: AtomicUsize,
}

impl ModelProvider for InterruptedModel {
    fn generate<'a>(&'a self, _: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        self.generated.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "partial-".into(),
            }),
            Err(Error::Storage("forced model interruption".into())),
        ]))
    }
    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            assert_eq!(
                attempt.observed,
                vec![ModelEvent::Content {
                    delta: "partial-".into()
                }]
            );
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            Ok(Some(vec![
                ModelEvent::Content {
                    delta: "restored".into(),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]))
        })
    }
}

struct TaskMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for TaskMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let state = state
            .as_u64()
            .ok_or_else(|| Error::Invalid("integer state required".into()))?;
        Ok(MachineTransition {
            state: json!(state + 1),
            commands: Vec::new(),
            status: if input.is_null() {
                MachineStatus::Suspended
            } else {
                MachineStatus::Completed {
                    value: input.clone(),
                }
            },
        })
    }
}

#[tokio::test]
async fn registered_task_reopens_checkpoint_under_replacement_lease() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("task-restart", "filesystem", "2")?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "task-owner".into(),
    };
    let issuer = AuthorityIssuer::new("task-restart", [7; 32], authority.clone());
    let signed = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:test.restart@1".to_owned(),
            "model:generate".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?
        .with_run_limits(TaskRunLimits {
            max_steps: Some(1),
            ..TaskRunLimits::default()
        })?;
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
    let definition = tasks.get_version::<u64, u64>("test.restart", "1")?;
    let mut machines = MachineRegistry::default();
    machines.register(machine)?;
    let operation = OperationId::from_bytes([3; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let step = OperationId::from_bytes([4; 16]);
    let worker = Worker {
        id: "worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let mut previous_lease = None;
    let model = Arc::new(InterruptedModel {
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let session_limits = SessionLimits {
        active_tasks: 1,
        total_tasks: 2,
        depth: 1,
        model_steps: 1,
    };
    // Each iteration drops all provider, coordinator, harness, and workflow handles.
    for reopened in [false, true] {
        let filesystem = Arc::new(FilesystemHost::new(
            Fs::local(fs_options.clone())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            provider.clone(),
        )?);
        if !reopened {
            filesystem.create_volume(&volume).await?;
        }
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(&stream_root, LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(),
            volume.clone(),
            &issuer.verifier(),
            &signed,
            65_536,
        )?);
        let reader = Arc::new(FilesystemContentVerifier::new(
            filesystem.clone(),
            issuer.verifier(),
            signed.clone(),
            65_536,
        )?);
        let host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, reader.clone())
                .await?
                .with_payload_store(payloads.clone()),
            stream.clone(),
            payloads.clone(),
            reader.clone(),
            authority.clone(),
            signed.clone(),
            issuer.verifier(),
            scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?
        .with_session_limits(if reopened {
            SessionLimits {
                model_steps: 2,
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
            volume.clone(),
            issuer.verifier(),
            signed.clone(),
            scope.clone(),
            tasks.clone(),
            machines.clone(),
            ToolRegistry::default(),
            session_limits,
            1,
            65_536,
        )
        .await?;
        let host = runtime.task_host();
        let harness = runtime.harness();
        if !reopened {
            let task_grants = scope.grants().without(&Capabilities::new([
                volume.capability(VolumeOperation::Read)?
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
            .stage(operation, "owner-readable-input", b"\"secret\"")
            .await?;
        // The composition owner can read this actual committed file, but the
        // task deliberately lacks that owner's private-volume read grant.
        reader.verify(&input_file).await?;
        let mut coordinator = DistributedCoordinator::open(&stream, reader)
            .await?
            .with_payload_store(payloads);
        assert_eq!(
            coordinator.scheduler().session_limits(operation)?,
            session_limits
        );
        if let Some(old) = &previous_lease {
            coordinator
                .release_lease(old, IdempotencyKey::new("release-crashed")?)
                .await?;
        }
        let lease = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("task lease".into()))?;
        let fence = LeaseFence::from(&lease.reservation);
        coordinator
            .apply(
                operation,
                IdempotencyKey::new(if reopened { "restart" } else { "start" })?,
                SchedulerEvent::Started {
                    operation_id: operation,
                    fence: fence.clone(),
                },
            )
            .await?;
        if let Some(old) = &previous_lease {
            assert!(
                host.journal_owner(task, LeaseFence::from(&old.reservation))
                    .await
                    .is_err()
            );
        }
        assert!(
            harness
                .open_task(
                    task,
                    &definition,
                    fence.clone(),
                    Arc::new(MemoryWorkflowJournal::default())
                )
                .await
                .is_err()
        );
        let execution = runtime
            .stock_execution(
                task,
                fence.clone(),
                OperationId::from_bytes([8; 16]),
                Model::new("test", "interrupted", "1", Value::Null)?,
                model.clone(),
                ContextPipeline::default(),
            )
            .await?;
        let turn = TurnInput {
            operation_id: execution.operation_id(),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 1,
        };
        assert!(matches!(execution.execute(TurnInput {
            input: ModelContent::Part(ModelContentPart::File { file: input_file, policy: FileProjectionPolicy::Native }),
            ..turn.clone()
        }).await, Err(Error::Unauthorized(message)) if message == "task cannot read the execution input file"));
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
            let restored = execution.execute(turn.clone()).await?;
            assert_eq!(restored.text, "partial-restored");
            assert_eq!(execution.execute(turn).await?, restored);
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
            let another = runtime
                .stock_execution(
                    task,
                    fence.clone(),
                    OperationId::from_bytes([10; 16]),
                    Model::new("test", "interrupted", "1", Value::Null)?,
                    model.clone(),
                    ContextPipeline::default(),
                )
                .await?;
            assert_ne!(another.operation_id(), execution.operation_id());
            assert!(
                another
                    .execute(TurnInput {
                        operation_id: another.operation_id(),
                        input: ModelContent::Text("another".into()),
                        selected_context: None,
                        max_steps: 1,
                    })
                    .await
                    .is_err()
            );
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
        } else {
            let interrupted = execution.execute(turn).await;
            assert!(
                matches!(interrupted, Err(Error::Storage(_))),
                "{interrupted:?}"
            );
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
        }
        drop(execution);
        let journal = runtime.workflow_journal(task, fence.clone()).await?;
        let mut session = harness
            .open_task(task, &definition, fence.clone(), journal.clone())
            .await?;
        assert_eq!(session.task_id(), task);
        if reopened {
            assert_eq!(session.checkpoint().revision, 1);
            assert_eq!(session.checkpoint().state, json!(1));
            assert_eq!(
                session.latest_transition().await?.map(|value| value.status),
                Some(MachineStatus::Suspended)
            );
        } else {
            assert!(session.latest_transition().await?.is_none());
            // Direct journal calls must also enforce the admitted output schema.
            let invalid_input = json!("bad");
            let prior = session.checkpoint().clone();
            let (next, transition) = machines.step(&prior, &invalid_input)?;
            let invalid_record = WorkflowRecord {
                operation_id: OperationId::from_bytes([8; 16]),
                idempotency_key: IdempotencyKey::new("direct-invalid-output")?,
                input_digest: Sha256::digest(
                    serde_json::to_vec(&invalid_input)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
                .into(),
                prior,
                input: invalid_input,
                transition,
                next,
            };
            assert!(
                journal
                    .commit(0, invalid_record.idempotency_key.clone(), invalid_record)
                    .await
                    .is_err()
            );
            assert!(journal.replay(0, 64).await?.is_empty());
            assert!(
                session
                    .step(
                        OperationId::from_bytes([5; 16]),
                        IdempotencyKey::new("invalid-output")?,
                        json!("bad")
                    )
                    .await
                    .is_err()
            );
            assert_eq!(session.checkpoint().revision, 0);
        }
        assert_eq!(
            session
                .step(step, IdempotencyKey::new("suspend")?, Value::Null)
                .await?
                .status,
            MachineStatus::Suspended
        );
        assert_eq!(session.checkpoint().revision, 1);
        assert_eq!(journal.replay(0, 64).await?.len(), 1);
        assert!(
            session
                .step(step, IdempotencyKey::new("suspend")?, json!(9))
                .await
                .is_err()
        );
        if reopened {
            let completed_step = OperationId::from_bytes([6; 16]);
            assert_eq!(
                session
                    .step(completed_step, IdempotencyKey::new("complete")?, json!(7))
                    .await?
                    .status,
                MachineStatus::Completed { value: json!(7) }
            );
            assert_eq!(session.checkpoint().revision, 2);
            coordinator
                .cancel_operation(
                    &authority,
                    &signed,
                    &issuer.verifier(),
                    operation,
                    IdempotencyKey::new("cancel")?,
                    false,
                )
                .await?;
            assert!(
                session
                    .step(completed_step, IdempotencyKey::new("complete")?, json!(7))
                    .await
                    .is_err()
            );
            drop(session);
            let recovery_journal = runtime.workflow_journal(task, fence.clone()).await?;
            let mut recovered = harness
                .open_task(task, &definition, fence, recovery_journal.clone())
                .await?;
            assert_eq!(
                recovered
                    .latest_transition()
                    .await?
                    .map(|value| value.status),
                Some(MachineStatus::Completed { value: json!(7) })
            );
            assert!(
                recovered
                    .step(completed_step, IdempotencyKey::new("complete")?, json!(7))
                    .await
                    .is_err()
            );
            assert_eq!(recovery_journal.replay(0, 64).await?.len(), 2);
        }
        previous_lease = Some(lease);
    }
    Ok(())
}
