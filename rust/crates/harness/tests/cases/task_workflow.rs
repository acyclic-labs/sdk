//! Registered task checkpoint recovery through real durable-local providers.
#![cfg(feature = "filesystem-local")]
#![allow(
    clippy::too_many_lines,
    reason = "durable recovery scenarios retain setup, fault injection and reopen assertions together"
)]
use acyclic_harness::runtime::InteractionResolver as _;

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, LocalOptions};
use acyclic_harness::context::ContextPipeline;
use acyclic_harness::conversation::{
    ContentResidencyVerifier, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, EventPayload, SchemaRegistry,
};
use acyclic_harness::distributed::{
    DistributedCoordinator, SchedulerPayloadStore, WorkPull, Worker,
};
use acyclic_harness::durable_host::CoordinatorTaskHost;
use acyclic_harness::executor::{Executor, StockExecutor, TurnInput};
use acyclic_harness::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemInteractionHost, FilesystemTaskRuntime,
    MAIL_RECEIVE_TASK_COMMAND_KIND, MAIL_SEND_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND,
    MailReceiveTaskCommand, MailSendTaskCommand, ModelTaskCommand, TASK_ADMIT_COMMAND_KIND,
    TASK_OBSERVE_COMMAND_KIND, TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskAdmitCommand,
    TaskCommandHost, TaskCommandProgress, TaskObserveCommand, TaskWakeCursor, TaskWorkerAttempt,
    TaskWorkerOutcome, TimerTaskCommand, ToolTaskCommand,
};
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent,
};
use acyclic_harness::registry::ComponentIdentity;
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::runtime::{
    RuntimeScope, TaskContext, TaskDefinition, TaskRegistry, TaskRunLimits, ToolContext,
    ToolPolicy, ToolPolicyDecision,
};
use acyclic_harness::scheduler::{LeaseFence, ResourceRequest, SchedulerEvent, SessionLimits};
use acyclic_harness::test_support::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn,
};
use acyclic_harness::tool::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry, ToolResult,
};
use acyclic_harness::workflow::{
    MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, MemoryWorkflowJournal,
    ResumableMachine, WorkflowCommand, WorkflowJournal, WorkflowRecord,
};
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
};
use acyclic_stream::{
    LocalStream, LocalStreamLimits, StreamProvider, SystemUnixMillisClock, UnixMillisClock,
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::{collections::BTreeMap, sync::Arc};

async fn discover_wakes<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    initial_cursor: Option<TaskWakeCursor>,
) -> Result<Vec<TaskId>>
where
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let mut cursor = initial_cursor;
    let mut snapshot = initial_cursor.map(|cursor| cursor.through_revision);
    let mut inspected = initial_cursor.map_or(0, |cursor| cursor.after_revision);
    let mut woken = Vec::new();
    loop {
        let page = runtime.poll_task_wake_page(cursor, 1).await?;
        assert!(page.events_read <= 1);
        assert!(page.tasks_polled <= page.events_read);
        assert!(page.woken.len() <= page.tasks_polled as usize);
        if let Some(snapshot) = snapshot {
            assert_eq!(page.through_revision, snapshot);
        } else {
            snapshot = Some(page.through_revision);
        }
        inspected += u64::from(page.events_read);
        woken.extend(page.woken);
        let Some(next) = page.cursor else {
            assert_eq!(Some(inspected), snapshot);
            return Ok(woken);
        };
        assert_eq!(next.after_revision, inspected);
        let bytes = serde_json::to_vec(&next).map_err(|error| Error::Invalid(error.to_string()))?;
        cursor = Some(
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?,
        );
    }
}

struct InterruptedTool {
    model_call: bool,
    executed: AtomicUsize,
    reconciled: AtomicUsize,
}

impl ToolExecutor for InterruptedTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        let Some(context) = context else {
            return {
                if self.model_call {
                    self.executed.fetch_add(1, Ordering::SeqCst);
                    return Box::pin(async {
                        Err(Error::Storage("lost model tool result".into()))
                    });
                }
                Box::pin(async { Err(Error::Unsupported("scoped tool context required".into())) })
            };
        };
        Box::pin(async move {
            let file = serde_json::from_value(
                invocation
                    .arguments
                    .get("file")
                    .cloned()
                    .ok_or_else(|| Error::Invalid("missing file".into()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(
                context.task().read_file(&file, None).await?.as_ref(),
                b"hello"
            );
            self.executed.fetch_add(1, Ordering::SeqCst);
            Err(Error::Storage("lost tool result".into()))
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        let Some(context) = context else {
            return {
                if self.model_call {
                    self.reconciled.fetch_add(1, Ordering::SeqCst);
                    return Box::pin(async {
                        Ok(Some(ToolResult {
                            value: json!("tool-restored"),
                        }))
                    });
                }
                Box::pin(async { Err(Error::Unsupported("scoped reconciliation required".into())) })
            };
        };
        Box::pin(async move {
            let file = serde_json::from_value(
                invocation
                    .arguments
                    .get("file")
                    .cloned()
                    .ok_or_else(|| Error::Invalid("missing file".into()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(
                context.task().read_file(&file, None).await?.as_ref(),
                b"hello"
            );
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            Ok(Some(ToolResult {
                value: json!("tool-restored"),
            }))
        })
    }
}

fn task_model(turns: impl IntoIterator<Item = ScriptedTurn>) -> ScriptedModelProvider {
    ScriptedModelProvider::new(turns)
        .with_context_capacity(acyclic_harness::context::ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: Some(16_384),
        })
        .with_token_counter(|request| {
            let message_tokens = request
                .request()
                .messages
                .iter()
                .map(|message| {
                    let bytes = message.content.file_refs().try_fold(
                        serde_json::to_vec(message)
                            .map_err(|error| Error::Invalid(error.to_string()))?
                            .len() as u64,
                        |total, file| {
                            total
                                .checked_add(file.descriptor().byte_length())
                                .ok_or_else(|| {
                                    Error::Invalid("synthetic token count overflows".into())
                                })
                        },
                    )?;
                    u32::try_from(bytes)
                        .map_err(|_| Error::Invalid("synthetic token count exceeds u32".into()))
                })
                .collect::<Result<Vec<_>>>()?;
            let fixed = serde_json::to_vec(&request.request().tools)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len();
            Ok(
                acyclic_harness::context::ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    u32::try_from(fixed)
                        .map_err(|_| Error::Invalid("synthetic framing exceeds u32".into()))?,
                    message_tokens,
                ),
            )
        })
}

fn interrupted_model(observed_events: usize) -> Arc<ScriptedModelProvider> {
    Arc::new(
        task_model([ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "partial-".into(),
            }),
            ScriptedEvent::Error {
                error: "forced model interruption".into(),
            },
        ])])
        .with_reconciliations([ScriptedReconciliation {
            orphan: false,
            observed: if observed_events == 0 {
                vec![]
            } else {
                vec![ModelEvent::Content {
                    delta: "partial-".into(),
                }]
            },
            continuation: Some(vec![
                ModelEvent::Content {
                    delta: "restored".into(),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]),
        }]),
    )
}

fn workspace() -> Result<tempfile::TempDir> {
    tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))
}

struct TaskMachine {
    identity: MachineIdentity,
    schema: Value,
}

struct NoCommands;

impl TaskCommandHost for NoCommands {
    fn execute<'a>(
        &'a self,
        _: TaskContext,
        _: LeaseFence,
        _: WorkflowCommand,
        _: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "empty outbox dispatched a command".into(),
            ))
        })
    }
}

#[tokio::test]
async fn bounded_worker_suspends_and_consumes_wake_after_full_reopen() -> Result<()> {
    worker_restart(WorkerCommand::Wait).await
}

#[tokio::test]
async fn worker_reconciles_journaled_model_without_releasing_ownership() -> Result<()> {
    worker_restart(WorkerCommand::Model).await
}

#[tokio::test]
async fn worker_reconciles_pinned_tool_without_reexecuting_after_reopen() -> Result<()> {
    worker_restart(WorkerCommand::Tool).await
}

#[tokio::test]
async fn worker_timer_releases_capacity_and_completes_after_reopen() -> Result<()> {
    worker_restart(WorkerCommand::Timer).await
}

#[tokio::test]
async fn worker_mail_send_replays_after_publication_before_checkpoint() -> Result<()> {
    worker_restart(WorkerCommand::MailSend).await
}

#[tokio::test]
async fn worker_mail_receive_releases_capacity_until_committed_item() -> Result<()> {
    worker_restart(WorkerCommand::MailReceive).await
}

#[tokio::test]
async fn worker_mail_receive_requires_retained_read_grant() -> Result<()> {
    Box::pin(worker_restart_with_options(
        WorkerCommand::MailReceive,
        false,
        false,
    ))
    .await
}

#[tokio::test]
async fn worker_child_admission_and_observation_share_one_slot_after_reopen() -> Result<()> {
    worker_restart(WorkerCommand::Child).await
}

#[tokio::test]
async fn passive_wakes_distinguish_successive_commands_at_one_checkpoint() -> Result<()> {
    worker_restart(WorkerCommand::TimerThenTimer).await
}

#[tokio::test]
async fn cancelled_timer_is_not_woken_after_reopen() -> Result<()> {
    Box::pin(worker_restart_with_options(
        WorkerCommand::Timer,
        true,
        true,
    ))
    .await
}

struct CompletedChildMachine(TaskMachine);
impl ResumableMachine for CompletedChildMachine {
    fn identity(&self) -> &MachineIdentity {
        self.0.identity()
    }
    fn state_schema(&self) -> &Value {
        self.0.state_schema()
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Completed {
                value: state.clone(),
            },
        })
    }
}

struct RestartPolicy {
    evaluated: AtomicUsize,
    digest: [u8; 32],
}
impl ToolPolicy for RestartPolicy {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "test.restart-policy".into(),
            version: "1".into(),
            digest: self.digest,
        }
    }
    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _scope: &'a RuntimeScope,
        _io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<
        acyclic_harness::store::HistoryContentFuture<
            'a,
            (ToolPolicyDecision, acyclic_harness::NativeCustody),
        >,
    > {
        Ok(acyclic_harness::store::HistoryContentFuture::Ordinary {
            future: Box::pin(async move {
                self.evaluated.fetch_add(1, Ordering::SeqCst);
                let decision = if invocation.arguments.get("denied") == Some(&json!(true)) {
                    ToolPolicyDecision::Deny {
                        reason: "pinned policy denied".into(),
                    }
                } else if invocation.arguments.get("approval") == Some(&json!(true)) {
                    ToolPolicyDecision::RequireApproval {
                        prompt: "Approve this exact restore".into(),
                    }
                } else {
                    ToolPolicyDecision::Allow
                };
                Ok((decision, None))
            }),
            frame_bytes: 0,
        })
    }
}

#[tokio::test]
async fn worker_pinned_policy_approval_denies_dispatch_and_reconciles_after_reopen() -> Result<()> {
    worker_restart(WorkerCommand::PolicyTool).await
}

#[tokio::test]
async fn worker_pinned_policy_model_binding_survives_reopen() -> Result<()> {
    worker_restart(WorkerCommand::PolicyModel).await
}

async fn resolve_worker_approval<P, A, O>(
    stream: &std::sync::Arc<P>,
    filesystem: Arc<FilesystemHost<A, O>>,
    issuer: &AuthorityIssuer,
    owner_scope: acyclic_harness::core::Scope,
    volume: VolumeRef,
    agent: AgentId,
    approved: bool,
) -> Result<()>
where
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let aggregate = acyclic_harness::store::StreamAggregate::open(
        stream,
        (
            issuer
                .verifier(&mut None)
                .expect("verifier copy")
                .audience()
                .clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    let events = aggregate.reducer().events_after(0, 64)?;
    assert_eq!(events.len(), 2, "request retry must reuse its ticket");
    let ticket = events
        .into_iter()
        .find_map(|event| match event.payload {
            EventPayload::InteractionOpened { ticket } => Some(ticket),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("approval ticket".into()))?;
    let interactions = FilesystemInteractionHost::new(
        stream.clone(),
        filesystem.clone(),
        &issuer
            .verifier(&mut None)
            .expect("verifier copy")
            .audience()
            .clone(),
        &SchemaRegistry::new(),
        acyclic_harness::filesystem::FilesystemJournalBinding {
            volume: &volume.clone(),
            verifier: &issuer.verifier(&mut None).expect("verifier copy"),
            scope: &owner_scope.clone(),
            maximum_bytes: 65_536,
            io: None,
        },
    )?;
    assert!(matches!(
        interactions
            .resolve_approval(
                OperationId::from_bytes([91; 16]),
                owner_scope.clone(),
                acyclic_harness::InteractionId::from_bytes(*ticket.id.as_bytes()),
                1,
                true,
                None
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    let responder = issuer
        .issue(
            "approver",
            &Capabilities::new(["interaction:resolve".to_owned(), ticket.responder_grant()]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    interactions
        .resolve_approval(
            OperationId::from_bytes([92; 16]),
            responder,
            acyclic_harness::InteractionId::from_bytes(*ticket.id.as_bytes()),
            1,
            approved,
            None,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn pending_approval_releases_slot_and_wakes_after_store_reopen() -> Result<()> {
    worker_restart(WorkerCommand::ApprovalWaitTool).await
}

#[tokio::test]
async fn model_approval_releases_slot_and_reconciles_tool_after_store_reopen() -> Result<()> {
    worker_restart(WorkerCommand::ApprovalWaitModel).await
}

#[tokio::test]
async fn declined_model_approval_wakes_without_tool_dispatch_after_store_reopen() -> Result<()> {
    worker_restart(WorkerCommand::ApprovalDeclinedModel).await
}

#[tokio::test]
async fn cancelled_model_approval_is_not_woken_after_store_reopen() -> Result<()> {
    Box::pin(worker_restart_with_options(
        WorkerCommand::ApprovalWaitModel,
        true,
        true,
    ))
    .await
}

#[tokio::test]
async fn cancelled_approval_is_not_woken_after_store_reopen() -> Result<()> {
    Box::pin(worker_restart_with_options(
        WorkerCommand::ApprovalWaitTool,
        true,
        true,
    ))
    .await
}

struct TestClock(AtomicU64);
impl UnixMillisClock for TestClock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WorkerCommand {
    Wait,
    Model,
    PolicyModel,
    Tool,
    PolicyTool,
    ApprovalWaitTool,
    ApprovalWaitModel,
    ApprovalDeclinedModel,
    Timer,
    TimerThenTimer,
    MailSend,
    MailReceive,
    Child,
}

struct CommandMachine {
    base: TaskMachine,
    command: WorkerCommand,
}

impl ResumableMachine for CommandMachine {
    fn identity(&self) -> &MachineIdentity {
        self.base.identity()
    }
    fn state_schema(&self) -> &Value {
        self.base.state_schema()
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if input.is_null() {
            if matches!(
                self.command,
                WorkerCommand::Child | WorkerCommand::TimerThenTimer
            ) {
                let commands =
                    if self.command == WorkerCommand::Child {
                        [
                            ("admit", TASK_ADMIT_COMMAND_KIND, 26u8),
                            ("observe", TASK_OBSERVE_COMMAND_KIND, 27),
                        ]
                    } else {
                        [
                            ("timer", TIMER_TASK_COMMAND_KIND, 26u8),
                            ("second_timer", TIMER_TASK_COMMAND_KIND, 27),
                        ]
                    }
                    .into_iter()
                    .map(|(field, kind, identity)| {
                        Ok(WorkflowCommand {
                            operation_id: OperationId::from_bytes([identity; 16]),
                            kind: kind.into(),
                            payload: serde_json::from_value(state.get(field).cloned().ok_or_else(
                                || Error::Invalid("missing child command file".into()),
                            )?)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(MachineTransition {
                    state: state.clone(),
                    commands,
                    status: MachineStatus::Suspended,
                });
            }
            Ok(MachineTransition {
                state: state.clone(),
                commands: vec![WorkflowCommand {
                    operation_id: OperationId::from_bytes([26; 16]),
                    kind: if self.command == WorkerCommand::MailSend {
                        MAIL_SEND_TASK_COMMAND_KIND
                    } else if self.command == WorkerCommand::MailReceive {
                        MAIL_RECEIVE_TASK_COMMAND_KIND
                    } else if self.command == WorkerCommand::Timer {
                        TIMER_TASK_COMMAND_KIND
                    } else if matches!(
                        self.command,
                        WorkerCommand::Tool
                            | WorkerCommand::PolicyTool
                            | WorkerCommand::ApprovalWaitTool
                    ) {
                        TOOL_TASK_COMMAND_KIND
                    } else {
                        MODEL_TASK_COMMAND_KIND
                    }
                    .into(),
                    payload: serde_json::from_value(state.clone())
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                }],
                status: MachineStatus::Suspended,
            })
        } else {
            let outcome: Outcome<Value> = serde_json::from_value(
                input
                    .get("commands")
                    .and_then(|values| {
                        values.get(usize::from(matches!(
                            self.command,
                            WorkerCommand::Child | WorkerCommand::TimerThenTimer
                        )))
                    })
                    .and_then(|value| value.get("value"))
                    .cloned()
                    .ok_or_else(|| Error::Invalid("missing command result".into()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            if self.command == WorkerCommand::ApprovalDeclinedModel {
                assert_eq!(
                    outcome,
                    Outcome::Failed {
                        message: "declined".into()
                    }
                );
                return Ok(MachineTransition {
                    state: state.clone(),
                    commands: Vec::new(),
                    status: MachineStatus::Completed { value: json!(7) },
                });
            }
            let Outcome::Succeeded(value) = outcome else {
                return Err(Error::Invalid("unexpected command outcome".into()));
            };
            if self.command == WorkerCommand::Child {
                assert_eq!(value, json!(7));
            } else if self.command == WorkerCommand::MailReceive {
                assert_eq!(value.get("sequence"), Some(&json!(1)));
                assert!(value.get("payload").is_some());
            } else if matches!(
                self.command,
                WorkerCommand::Timer | WorkerCommand::TimerThenTimer | WorkerCommand::MailSend
            ) {
                assert!(value.is_null());
            } else if matches!(
                self.command,
                WorkerCommand::Tool | WorkerCommand::PolicyTool | WorkerCommand::ApprovalWaitTool
            ) {
                assert_eq!(value, json!("tool-restored"));
            } else {
                assert_eq!(value.get("text"), Some(&json!("partial-restored")));
            }
            Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Completed { value: json!(7) },
            })
        }
    }
}

#[allow(
    clippy::cognitive_complexity,
    reason = "parallel restart assertions for suspended and uncertain ownership"
)]
async fn worker_restart(command: WorkerCommand) -> Result<()> {
    Box::pin(worker_restart_with_options(command, true, false)).await
}

#[inline(never)]
fn boxed_restart_phase<'a, F: std::future::Future + 'a>(
    create: impl FnOnce() -> F,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = F::Output> + 'a>> {
    Box::pin(create())
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

#[path = "../support/stream/mod.rs"]
mod stream_fixture;
use fault_stream::{ExecutionFaultMode, LostSessionAck};
use stream_fixture as fault_stream;

#[tokio::test]
async fn registered_task_reopens_checkpoint_under_replacement_lease() -> Result<()> {
    stock_restart_with_publication_fault(None, false, false).await
}

fn assert_completed_dispatches(
    command: WorkerCommand,
    model: &ScriptedModelProvider,
    tool: &InterruptedTool,
) {
    if command == WorkerCommand::Wait {
        return;
    }
    let with_model_approval = matches!(
        command,
        WorkerCommand::ApprovalWaitModel | WorkerCommand::ApprovalDeclinedModel
    );
    let declined = command == WorkerCommand::ApprovalDeclinedModel;
    let with_tool = matches!(
        command,
        WorkerCommand::Tool
            | WorkerCommand::PolicyTool
            | WorkerCommand::ApprovalWaitTool
            | WorkerCommand::ApprovalWaitModel
            | WorkerCommand::ApprovalDeclinedModel
    );
    if matches!(
        command,
        WorkerCommand::Model
            | WorkerCommand::PolicyModel
            | WorkerCommand::ApprovalWaitModel
            | WorkerCommand::ApprovalDeclinedModel
    ) {
        assert_eq!(
            model
                .requests()
                .last()
                .and_then(|request| request.max_output_tokens),
            Some(8_192)
        );
    }
    assert_eq!(
        model.dispatches().len(),
        if with_model_approval {
            if declined { 1 } else { 2 }
        } else {
            usize::from(matches!(
                command,
                WorkerCommand::Model | WorkerCommand::PolicyModel
            ))
        }
    );
    assert_eq!(
        tool.executed.load(Ordering::SeqCst),
        usize::from(with_tool && !declined)
    );
    assert_eq!(
        model.reconciliations().len(),
        usize::from(matches!(
            command,
            WorkerCommand::Model | WorkerCommand::PolicyModel
        ))
    );
    assert_eq!(
        tool.reconciled.load(Ordering::SeqCst),
        usize::from(with_tool && !declined)
    );
}

#[path = "../task_workflow_cases/worker.rs"]
mod worker;
use worker::*;
#[path = "../task_workflow_cases/merge.rs"]
mod merge;
#[path = "../task_workflow_cases/stock.rs"]
mod stock;
use stock::*;
#[path = "../task_workflow_cases/mail.rs"]
mod mail;

fn assert_pending_dispatches(
    command: WorkerCommand,
    model: &ScriptedModelProvider,
    tool: &InterruptedTool,
    with_tool: bool,
) {
    assert_eq!(
        model.dispatches().len(),
        usize::from(matches!(
            command,
            WorkerCommand::Model | WorkerCommand::PolicyModel
        ))
    );
    assert_eq!(tool.executed.load(Ordering::SeqCst), usize::from(with_tool));
    assert_eq!(model.reconciliations().len(), 0);
}

fn assert_policy_dispatches(policy: &RestartPolicy, tool: &InterruptedTool, declined: bool) {
    if declined {
        assert_eq!(policy.evaluated.load(Ordering::SeqCst), 2);
    } else {
        assert!(policy.evaluated.load(Ordering::SeqCst) >= 3);
    }
    assert_eq!(tool.executed.load(Ordering::SeqCst), usize::from(!declined));
    assert_eq!(
        tool.reconciled.load(Ordering::SeqCst),
        usize::from(!declined)
    );
}
