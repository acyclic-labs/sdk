//! Registered task checkpoint recovery through real durable-local providers.
#![cfg(feature = "filesystem-local")]
#![allow(
    clippy::too_many_lines,
    reason = "durable recovery scenarios retain setup, fault injection and reopen assertions together"
)]

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, LocalOptions};
use acyclic_harness::context::ContextPipeline;
use acyclic_harness::conversation::{
    ContentResidencyVerifier, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, EventPayload, SchemaRegistry,
};
use acyclic_harness::distributed::{DistributedCoordinator, SchedulerPayloadStore, Worker};
use acyclic_harness::durable_host::CoordinatorTaskHost;
use acyclic_harness::executor::{Executor, StockExecutor, TurnInput};
use acyclic_harness::filesystem::{
    FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
    FilesystemInteractionHost, FilesystemSchedulerPayloadStore, FilesystemTaskRuntime,
    MAIL_RECEIVE_TASK_COMMAND_KIND, MAIL_SEND_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND,
    MailReceiveTaskCommand, MailSendTaskCommand, ModelTaskCommand, TASK_ADMIT_COMMAND_KIND,
    TASK_OBSERVE_COMMAND_KIND, TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskAdmitCommand,
    TaskCommandHost, TaskCommandProgress, TaskObserveCommand, TaskWakeCursor, TaskWorkerAttempt,
    TaskWorkerOutcome, TimerTaskCommand, ToolTaskCommand,
};
use acyclic_harness::model::{
    FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent,
    ModelProvider, PreparedModelRequest,
};
use acyclic_harness::registry::ComponentIdentity;
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::runtime::{
    DurableTaskHost, RuntimeScope, TaskContext, TaskDefinition, TaskRegistry, TaskRunLimits,
    ToolContext, ToolPolicy, ToolPolicyDecision,
};
use acyclic_harness::scheduler::{LeaseFence, ResourceSnapshot, SchedulerEvent, SessionLimits};
use acyclic_harness::tool::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry, ToolResult,
};
use acyclic_harness::workflow::{
    MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, MemoryWorkflowJournal,
    ResumableMachine, WorkflowCommand, WorkflowJournal, WorkflowRecord,
};
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
};
use acyclic_stream::{
    LocalStream, LocalStreamLimits, StreamClient, StreamProvider, SystemUnixMillisClock,
    UnixMillisClock,
};
use futures::{future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::{collections::BTreeMap, sync::Arc};

struct InterruptedModel {
    approval_tool: bool,
    observed_events: usize,
    output_tokens: AtomicU64,
    generated: AtomicUsize,
    reconciled: AtomicUsize,
}

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
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        if self.model_call {
            self.executed.fetch_add(1, Ordering::SeqCst);
            return Box::pin(async { Err(Error::Storage("lost model tool result".into())) });
        }
        Box::pin(async { Err(Error::Unsupported("scoped tool context required".into())) })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let file = serde_json::from_value(
                invocation
                    .arguments
                    .get("file")
                    .cloned()
                    .ok_or_else(|| Error::Invalid("missing file".into()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(context.task().read_file(&file).await?, b"hello");
            self.executed.fetch_add(1, Ordering::SeqCst);
            Err(Error::Storage("lost tool result".into()))
        })
    }

    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        if self.model_call {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            return Box::pin(async {
                Ok(Some(ToolResult {
                    value: json!("tool-restored"),
                }))
            });
        }
        Box::pin(async { Err(Error::Unsupported("scoped reconciliation required".into())) })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let file = serde_json::from_value(
                invocation
                    .arguments
                    .get("file")
                    .cloned()
                    .ok_or_else(|| Error::Invalid("missing file".into()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(context.task().read_file(&file).await?, b"hello");
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            Ok(Some(ToolResult {
                value: json!("tool-restored"),
            }))
        })
    }
}

impl ToolProjection for InterruptedTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(serde_json::json!({"kind":"json","value":result.value}))
    }
}

impl ModelProvider for InterruptedModel {
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        _dispatch: acyclic_harness::model::ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        self.output_tokens.store(
            u64::from(request.request().max_output_tokens.unwrap_or(0)),
            Ordering::SeqCst,
        );
        let attempt = self.generated.fetch_add(1, Ordering::SeqCst);
        if self.approval_tool {
            let Some(file) = request
                .request()
                .messages
                .iter()
                .flat_map(|message| message.content.file_refs())
                .next()
            else {
                return Box::pin(futures::stream::iter([Err(Error::Invalid(
                    "approval tool requires the original input file".into(),
                ))]));
            };
            return Box::pin(futures::stream::iter([
                Ok(if attempt == 0 {
                    ModelEvent::ToolCall {
                        call_id: "provider-call".into(),
                        name: "test.restore".into(),
                        arguments: json!({"file":file,"approval":true}),
                    }
                } else {
                    ModelEvent::Content {
                        delta: "partial-restored".into(),
                    }
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
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
                if self.observed_events == 0 {
                    Vec::new()
                } else {
                    vec![ModelEvent::Content {
                        delta: "partial-".into(),
                    }]
                }
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
    worker_restart(WorkerCommand::TimerThenMail).await
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
    ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
        Box::pin(async move {
            self.evaluated.fetch_add(1, Ordering::SeqCst);
            Ok(
                if invocation.arguments.get("denied") == Some(&json!(true)) {
                    ToolPolicyDecision::Deny {
                        reason: "pinned policy denied".into(),
                    }
                } else if invocation.arguments.get("approval") == Some(&json!(true)) {
                    ToolPolicyDecision::RequireApproval {
                        prompt: "Approve this exact restore".into(),
                    }
                } else {
                    ToolPolicyDecision::Allow
                },
            )
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
    stream: &StreamClient<P>,
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
        issuer.verifier().audience().clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
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
        issuer.verifier().audience().clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
        owner_scope.clone(),
        volume.clone(),
        65_536,
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
    let responder = issuer.root_for_agent(
        agent,
        "approver",
        Capabilities::new(["interaction:resolve".to_owned(), ticket.responder_grant()]),
    );
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
    TimerThenMail,
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
                WorkerCommand::Child | WorkerCommand::TimerThenMail
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
                            ("mail", MAIL_RECEIVE_TASK_COMMAND_KIND, 27),
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
                            WorkerCommand::Child | WorkerCommand::TimerThenMail
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
            } else if matches!(
                self.command,
                WorkerCommand::MailReceive | WorkerCommand::TimerThenMail
            ) {
                assert_eq!(value.get("sequence"), Some(&json!(1)));
                assert!(value.get("payload").is_some());
            } else if matches!(self.command, WorkerCommand::Timer | WorkerCommand::MailSend) {
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

// Construct one phase in a short frame that returns before it is polled.
// Otherwise this large scenario's unoptimized poll frame reserves temporaries
// for every phase on top of the production call chain's default-thread stack.
#[inline(never)]
fn boxed_restart_phase<F: std::future::Future>(
    create: impl FnOnce() -> F,
) -> std::pin::Pin<Box<F>> {
    Box::pin(create())
}

#[allow(
    clippy::cognitive_complexity,
    reason = "restart ownership and authority boundary cases"
)]
async fn worker_restart_with_options(
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
    let with_two_waits = command == WorkerCommand::TimerThenMail;
    let with_timer = matches!(command, WorkerCommand::Timer | WorkerCommand::TimerThenMail);
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
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("worker-restart", "filesystem", "2")?;
    let agent = AgentId::from_bytes([21; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "worker-owner".into(),
    };
    let issuer = AuthorityIssuer::new("worker-restart", [23; 32], authority);
    let signed = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([
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
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let conversation_issuer = AuthorityIssuer::new(
        "worker-approval",
        [54; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "worker-approval".into(),
        },
    );
    let conversation_scope = conversation_issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([
            "conversation:bind".to_owned(),
            "interaction:open".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
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
    let definition = tasks.get_version::<Value, u64>("test.restart", "1")?;
    let mut machines = MachineRegistry::default();
    machines.register(machine)?;
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
        machines.register(child)?;
    }
    let operation = OperationId::from_bytes([25; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let worker = Worker {
        id: "worker".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let mut old_lease: Option<acyclic_harness::distributed::WorkLease> = None;
    let mut model_execution = None;
    let mut discovery_cursor = None;
    let model = Arc::new(InterruptedModel {
        approval_tool: with_model_approval,
        observed_events: 1,
        output_tokens: AtomicU64::new(0),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let tool = Arc::new(InterruptedTool {
        model_call: with_model_approval,
        executed: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let mut tools = ToolRegistry::default();
    tools.register(Tool {
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
        projection: tool.clone(),
    })?;
    for reopened in [false, true] {
        let (filesystem, stream, payloads, reader, runtime) = boxed_restart_phase(|| async {
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
            if with_policy && !reopened {
                Box::pin(async {
                    let mut aggregate = acyclic_harness::store::StreamAggregate::open(
                        &stream,
                        conversation_issuer.verifier().audience().clone(),
                        conversation_issuer.verifier(),
                        SchemaRegistry::new(),
                    )
                    .await?;
                    aggregate
                        .execute(Command {
                            operation_id: OperationId::from_bytes([90; 16]),
                            idempotency_key: IdempotencyKey::new("worker-approval-bind")?,
                            expected_revision: 0,
                            scope: conversation_scope.clone(),
                            causal_parent: None,
                            action: Action::BindConversation { agent },
                        })
                        .await?;
                    Ok::<(), Error>(())
                })
                .await?;
            }
            let runtime = FilesystemTaskRuntime::open_with_policy_and_clock(
                stream.clone(),
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                signed.clone(),
                scope.clone(),
                tasks.clone(),
                machines.clone(),
                tools.clone(),
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
            )
            .await?;
            let runtime = if with_policy {
                runtime.with_interaction_owner(
                    conversation_issuer.verifier(),
                    SchemaRegistry::new(),
                    conversation_scope.clone(),
                    volume.clone(),
                )?
            } else {
                runtime
            };
            if with_policy && reopened {
                let wrong = FilesystemTaskRuntime::open_with_policy_and_clock(
                    stream.clone(),
                    filesystem.clone(),
                    volume.clone(),
                    issuer.verifier(),
                    signed.clone(),
                    scope.clone(),
                    tasks.clone(),
                    machines.clone(),
                    tools.clone(),
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
                json!({"admit":payloads.stage(operation, "child-admit", &admit).await?, "observe":payloads.stage(operation, "child-observe", &observe).await?})
            } else if with_two_waits {
                let timer = serde_json::to_vec(&TimerTaskCommand {
                    deadline_unix_ms: 200,
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
                let mail = serde_json::to_vec(&MailReceiveTaskCommand { after: 0 })
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                json!({"timer":payloads.stage(operation, "two-waits-timer", &timer).await?,
                    "mail":payloads.stage(operation, "two-waits-mail", &mail).await?})
            } else if with_command {
                let payload = if with_mail_send {
                    serde_json::to_value(MailSendTaskCommand {
                        recipient: task,
                        payload: payloads.stage(operation, "mail-body", b"7").await?,
                    })
                } else if with_mail_receive {
                    serde_json::to_value(MailReceiveTaskCommand { after: 0 })
                } else if with_timer {
                    serde_json::to_value(TimerTaskCommand {
                        deadline_unix_ms: 200,
                    })
                } else if with_tool && !with_model_approval {
                    let file = payloads.stage(operation, "tool-input", b"hello").await?;
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
                        input: if with_model_approval {
                            ModelContent::Part(acyclic_harness::model::ModelContentPart::File {
                                file: payloads.stage(operation, "model-input", b"hello").await?,
                                policy: acyclic_harness::model::FileProjectionPolicy::Reference,
                            })
                        } else {
                            ModelContent::Text("hello".into())
                        },
                        selected_context: None,
                        max_steps: if with_model_approval { 2 } else { 1 },
                        max_output_tokens: Some(8_192),
                    })
                }
                .map_err(|error| Error::Invalid(error.to_string()))?;
                let bytes = serde_json::to_vec(&payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                serde_json::to_value(payloads.stage(operation, "command-payload", &bytes).await?)
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
        let mut coordinator = DistributedCoordinator::open(&stream, reader.clone())
            .await?
            .with_payload_store(payloads.clone());
        let stop = boxed_restart_phase(|| async {
        if reopened && !uncertain && !with_mail_send {
            // A passive model approval has no reservation. Inspecting its
            // original execution must neither acquire authority nor publish.
            let passive = if with_model_approval {
                let execution = model_execution.ok_or_else(|| Error::NotFound("model execution".into()))?;
                let journal = stream.stream(format!("harness/v2/execution/{execution}"))?;
                let workspace = acyclic_harness::filesystem::workspace_ref(provider.clone(), &volume.storage_name()?)?;
                let before = (journal.tail().await?, filesystem.resolve(&workspace).await?,
                    stream.stream("harness/v2/coordinator/events")?.tail().await?);
                let callbacks = (model.generated.load(Ordering::SeqCst), model.reconciled.load(Ordering::SeqCst),
                    tool.executed.load(Ordering::SeqCst), tool.reconciled.load(Ordering::SeqCst));
                assert!(coordinator.scheduler().operation(operation)
                    .ok_or_else(|| Error::NotFound("passive task".into()))?.reservation.is_none());
                let old = old_lease.as_ref().ok_or_else(|| Error::NotFound("original approval lease".into()))?;
                assert!(runtime.task_host().journal_owner(task, LeaseFence::from(&old.reservation)).await.is_err());
                Some((journal, workspace, before, callbacks))
            } else { None };
            assert!(!runtime.poll_task_wake(task).await?);
            if let Some((journal, workspace, before, callbacks)) = passive {
                assert_eq!(journal.tail().await?, before.0);
                assert_eq!(filesystem.resolve(&workspace).await?, before.1);
                assert_eq!(stream.stream("harness/v2/coordinator/events")?.tail().await?, before.2);
                assert_eq!((model.generated.load(Ordering::SeqCst), model.reconciled.load(Ordering::SeqCst),
                    tool.executed.load(Ordering::SeqCst), tool.reconciled.load(Ordering::SeqCst)), callbacks);
                coordinator.refresh().await?;
                let retained = coordinator.scheduler().operation(operation)
                    .ok_or_else(|| Error::NotFound("passive task".into()))?;
                assert_eq!(retained.phase, acyclic_harness::scheduler::OperationPhase::Suspended);
                assert!(retained.reservation.is_none());
            }
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
                assert!(!runtime.poll_task_wake(task).await?);
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
                        .stage(operation, &format!("mail-incoming-{index}"), b"7")
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
            let input = payloads.stage(operation, "wake-seven", b"7").await?;
            assert!(
                runtime
                    .task_host()
                    .resume_workflow(
                        task,
                        2,
                        input.clone(),
                        IdempotencyKey::new("wrong-revision")?
                    )
                    .await
                    .is_err()
            );
            if with_timer || with_mail_receive || with_child || with_approval_wait {
                let evaluations = policy.evaluated.load(Ordering::SeqCst);
                let generated = model.generated.load(Ordering::SeqCst);
                let reconciled = model.reconciled.load(Ordering::SeqCst);
                let executed = tool.executed.load(Ordering::SeqCst);
                assert_eq!(discover_wakes(&runtime, None).await?, vec![task]);
                assert!(discover_wakes(&runtime, None).await?.is_empty());
                assert!(!runtime.poll_task_wake(task).await?);
                assert_eq!(policy.evaluated.load(Ordering::SeqCst), evaluations);
                assert_eq!(model.generated.load(Ordering::SeqCst), generated);
                assert_eq!(model.reconciled.load(Ordering::SeqCst), reconciled);
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
                    let next_wait = DistributedCoordinator::open(&stream, reader.clone()).await?;
                    assert_eq!(
                        next_wait
                            .scheduler()
                            .operation(operation)
                            .and_then(|state| state.workflow.as_ref())
                            .and_then(|slot| slot.waiting_command),
                        Some(OperationId::from_bytes([27; 16]))
                    );
                    assert!(!runtime.poll_task_wake(task).await?);
                    let file = payloads.stage(operation, "second-wait-mail", b"7").await?;
                    runtime
                        .task_host()
                        .send(task, task, OperationId::from_bytes([30; 16]), file)
                        .await?;
                    assert_eq!(discover_wakes(&runtime, None).await?, vec![task]);
                    assert!(!runtime.poll_task_wake(task).await?);
                }
                let wake_projection = DistributedCoordinator::open(&stream, reader.clone()).await?;
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
                    .resume_workflow(task, 1, input.clone(), IdempotencyKey::new("wake-seven")?)
                    .await?;
                // Exact publication retries do not deliver another wake.
                runtime
                    .task_host()
                    .resume_workflow(task, 1, input, IdempotencyKey::new("wake-seven")?)
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
                let before = stream
                    .stream("harness/v2/coordinator/events")?
                    .tail()
                    .await?;
                assert!(matches!(
                    runtime.worker_tick(&worker, None, &NoCommands, 1, 0).await,
                    Err(Error::Invalid(_))
                ));
                assert_eq!(
                    stream
                        .stream("harness/v2/coordinator/events")?
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
                    matches!(runtime.resume_task(old.clone(), &NoCommands, 1).await,
                TaskWorkerAttempt::Unresolved { lease: attempted, .. } if attempted == *old)
                );
            }
            Ok::<_, Error>(lease)
        })
        .await?;
        let Some(outcome) = boxed_restart_phase(|| async {
        let outcome = if with_command {
            if !reopened {
                if with_model_approval {
                    // Obtain the namespace from the public, effect-free
                    // production constructor rather than repeat its hash rule.
                    model_execution = Some(runtime.stock_execution(task, LeaseFence::from(&lease.reservation),
                        OperationId::from_bytes([26;16]), Model::new("test", "interrupted", "1", Value::Null)?,
                        model.clone(), ContextPipeline::default()).await?.operation_id());
                }
                assert!(
                    matches!(runtime.resume_task(lease.clone(), &NoCommands, 0).await,
                    TaskWorkerAttempt::Unresolved { lease: retained, error: Error::Invalid(_) } if retained == lease)
                );
                assert!(
                    runtime
                        .run_task(lease.clone(), &NoCommands, 0)
                        .await
                        .is_err()
                );
                assert_eq!(model.generated.load(Ordering::SeqCst), 0);
                coordinator.refresh().await?;
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
                    let context = runtime.harness().durable_context(task, operation).await?;
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
                            .execute(
                                context,
                                LeaseFence::from(&lease.reservation),
                                command,
                                wrong
                            )
                            .await,
                        Err(Error::NotFound(_))
                    ));
                }
                if with_mail_send {
                    let context = runtime.harness().durable_context(task, operation).await?;
                    let file = serde_json::from_value(
                        runtime.task_host().observe_admission(task).await?.input,
                    )
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    let input: MailSendTaskCommand =
                        serde_json::from_slice(&context.read_file(&file).await?)
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
                        runtime.task_host().send_owned(
                            task,
                            fence.clone(),
                            task,
                            probe,
                            input.payload.clone()
                        ),
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
                            .execute(context.clone(), fence.clone(), command.clone(), payload)
                            .await?,
                        TaskCommandProgress::Ready(_)
                    ));
                    let other = payloads.stage(operation, "mail-conflict", b"8").await?;
                    let changed = serde_json::to_value(MailSendTaskCommand {
                        payload: other,
                        ..input
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert!(matches!(
                        runtime
                            .commands()
                            .execute(context, fence, command, changed)
                            .await,
                        Err(Error::Conflict(_))
                    ));
                    assert_eq!(
                        stream
                            .stream(format!("harness/v2/mail/{task}"))?
                            .bounds()
                            .await?
                            .tail,
                        72
                    );
                    coordinator.refresh().await?;
                    assert_eq!(
                        coordinator
                            .scheduler()
                            .operation(operation)
                            .and_then(|state| state.reservation.as_ref()),
                        Some(&lease.reservation)
                    );
                    // Simulate process loss after committed send, before the
                    // machine consumes its result. All handles drop on continue.
                    old_lease = Some(lease.clone());
                    return Ok(None);
                }
                if with_timer {
                    let fence = LeaseFence::from(&lease.reservation);
                    let probe = OperationId::from_bytes([99; 16]);
                    let (stock, legacy) = tokio::join!(
                        runtime
                            .task_host()
                            .poll_timer(task, fence.clone(), probe, 1),
                        runtime.task_host().wait_until(task, probe, 1)
                    );
                    assert!(stock?);
                    legacy?;
                    assert!(matches!(
                        runtime.task_host().wait_until(task, probe, 2).await,
                        Err(Error::Conflict(_))
                    ));
                    assert!(matches!(
                        runtime
                            .task_host()
                            .poll_timer(task, fence.clone(), probe, 0)
                            .await,
                        Err(Error::Invalid(_))
                    ));
                    for index in 100..170u8 {
                        assert!(
                            !runtime
                                .task_host()
                                .poll_timer(
                                    task,
                                    fence.clone(),
                                    OperationId::from_bytes([index; 16]),
                                    200
                                )
                                .await?
                        );
                    }
                    let timer_operation = OperationId::from_bytes([26; 16]);
                    assert!(
                        !runtime
                            .task_host()
                            .poll_timer(task, fence.clone(), timer_operation, 200)
                            .await?
                    );
                    assert!(
                        !runtime
                            .task_host()
                            .poll_timer(task, fence.clone(), timer_operation, 200)
                            .await?
                    );
                    assert!(matches!(
                        runtime
                            .task_host()
                            .poll_timer(task, fence, timer_operation, 201)
                            .await,
                        Err(Error::Conflict(_))
                    ));
                }
                if with_tool && !with_model_approval {
                    let context = runtime.harness().durable_context(task, operation).await?;
                    let admission = runtime.task_host().observe_admission(task).await?;
                    let file = serde_json::from_value(admission.input)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    let payload: ToolTaskCommand =
                        serde_json::from_slice(&context.read_file(&file).await?)
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
                                    .execute(
                                        context.clone(),
                                        LeaseFence::from(&lease.reservation),
                                        command.clone(),
                                        denied
                                    )
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
                                        .execute(
                                            context.clone(),
                                            LeaseFence::from(&lease.reservation),
                                            command.clone(),
                                            pending.clone()
                                        )
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
                            .execute(
                                context,
                                LeaseFence::from(&lease.reservation),
                                command,
                                wrong
                            )
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
                    &runtime.commands().with_model(
                        Model::new("test", "interrupted", "1", Value::Null)?,
                        model.clone(),
                        ContextPipeline::default(),
                    ),
                    2,
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
                        Model::new("test", "interrupted", "1", Value::Null)?,
                        model.clone(),
                        ContextPipeline::default(),
                    ),
                    2,
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
            assert!(
                matches!(outcome, TaskWorkerOutcome::Reconciling { lease: retained } if retained == lease)
            );
            coordinator.refresh().await?;
            assert_eq!(
                coordinator
                    .scheduler()
                    .operation(operation)
                    .and_then(|state| state.reservation.as_ref()),
                Some(&lease.reservation)
            );
            assert!(matches!(
                runtime.task_host().inbox(task, 0, 1).await,
                Err(Error::Unauthorized(_))
            ));
            assert!(
                stream
                    .stream(format!("harness/v2/mail/{task}"))?
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
            coordinator.refresh().await?;
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
            assert_eq!(reader.read(result).await?, b"7");
            assert!(coordinator.pull(&worker).await?.is_none());
            if with_command {
                if matches!(
                    command,
                    WorkerCommand::Model
                        | WorkerCommand::PolicyModel
                        | WorkerCommand::ApprovalWaitModel
                        | WorkerCommand::ApprovalDeclinedModel
                ) {
                    assert_eq!(model.output_tokens.load(Ordering::SeqCst), 8_192);
                }
                assert_eq!(
                    model.generated.load(Ordering::SeqCst),
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
                    model.reconciled.load(Ordering::SeqCst),
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
        } else {
            if uncertain {
                assert!(
                    matches!(outcome, TaskWorkerOutcome::Reconciling { lease: retained } if retained == lease)
                );
                assert_eq!(
                    model.generated.load(Ordering::SeqCst),
                    usize::from(matches!(
                        command,
                        WorkerCommand::Model | WorkerCommand::PolicyModel
                    ))
                );
                assert_eq!(tool.executed.load(Ordering::SeqCst), usize::from(with_tool));
                assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
            } else {
                assert!(
                    matches!(outcome, TaskWorkerOutcome::Suspended { task: suspended, revision:1 } if suspended == task)
                );
            }
            old_lease = Some(lease.clone());
        }
        if !reopened && (with_timer || with_mail_receive || with_child || with_approval_wait) {
            let evaluations = policy.evaluated.load(Ordering::SeqCst);
            let generated = model.generated.load(Ordering::SeqCst);
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
            assert_eq!(model.generated.load(Ordering::SeqCst), generated);
            if with_model_approval {
                assert_eq!(generated, 1);
                assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
                assert_eq!(tool.executed.load(Ordering::SeqCst), 0);
                assert_eq!(tool.reconciled.load(Ordering::SeqCst), 0);
            }
        }
        if with_timer {
            let mut retained_timers = 0;
            for index in std::iter::once(99_u8)
                .chain(100..170_u8)
                .chain(std::iter::once(26_u8))
            {
                let timer_operation = OperationId::from_bytes([index; 16]);
                let timer = stream.stream(format!("harness/v2/timers/{task}/{timer_operation}"))?;
                assert_eq!(timer.bounds().await?.tail, 1);
                retained_timers += 1;
            }
            assert_eq!(retained_timers, 72);
            if let Some(old) = &old_lease {
                assert!(
                    runtime
                        .task_host()
                        .poll_timer(
                            task,
                            LeaseFence::from(&old.reservation),
                            OperationId::from_bytes([26; 16]),
                            200
                        )
                        .await
                        .is_err()
                );
            }
        }
        if with_mail_send || (with_mail_receive && reopened) {
            let inbox = runtime.task_host().inbox(task, 0, 1024).await?;
            assert_eq!(inbox.len(), if with_mail_send { 72 } else { 2 });
            let first = inbox
                .first()
                .ok_or_else(|| Error::NotFound("committed mail item".into()))?;
            assert_eq!(reader.read(&first.payload).await?, b"7");
            if with_mail_send {
                assert!(
                    runtime
                        .task_host()
                        .send_owned(
                            task,
                            LeaseFence::from(&lease.reservation),
                            task,
                            OperationId::from_bytes([99; 16]),
                            first.payload.clone()
                        )
                        .await
                        .is_err()
                );
            }
        }
        // All provider, runtime and coordinator handles are dropped here.
    }
    if with_policy && with_tool {
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
    Ok(())
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

#[path = "support/stream.rs"]
mod fault_stream;
use fault_stream::{ExecutionFaultMode, LostSessionAck};

#[tokio::test]
async fn registered_task_reopens_checkpoint_under_replacement_lease() -> Result<()> {
    stock_restart_with_publication_fault(None, false, false).await
}

#[tokio::test]
async fn admitted_stock_turn_without_selected_context_reconciles_after_reopen() -> Result<()> {
    stock_restart_with_publication_fault(None, false, true).await
}

#[tokio::test]
async fn stock_model_publication_faults_reconcile_after_provider_reopen() -> Result<()> {
    for mode in [
        ExecutionFaultMode::Before,
        ExecutionFaultMode::AfterVisible,
        ExecutionFaultMode::AfterHidden,
    ] {
        stock_restart_with_publication_fault(Some(mode), false, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_uncertain_model_retains_ownership_until_fenced_release_after_reopen()
-> Result<()> {
    for mode in [
        ExecutionFaultMode::Before,
        ExecutionFaultMode::AfterVisible,
        ExecutionFaultMode::AfterHidden,
    ] {
        stock_restart_with_publication_fault(Some(mode), true, false).await?;
    }
    Ok(())
}

#[allow(
    clippy::cognitive_complexity,
    reason = "one restart ownership scenario shared by normal, publication-fault and cancellation cases"
)]
async fn stock_restart_with_publication_fault(
    fault: Option<ExecutionFaultMode>,
    cancel_after_failure: bool,
    unselected_context: bool,
) -> Result<()> {
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
    let mut previous_lease: Option<acyclic_harness::distributed::WorkLease> = None;
    let model = Arc::new(InterruptedModel {
        approval_tool: false,
        observed_events: usize::from(fault != Some(ExecutionFaultMode::Before)),
        output_tokens: AtomicU64::new(0),
        generated: AtomicUsize::new(0),
        reconciled: AtomicUsize::new(0),
    });
    let session_limits = SessionLimits {
        active_tasks: 1,
        total_tasks: 2,
        depth: 1,
        model_steps: if cancel_after_failure { 2 } else { 1 },
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
        let stream_provider = Arc::new(LostSessionAck::new(
            LocalStream::open(&stream_root, LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let stream = StreamClient::new(stream_provider.clone());
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
        assert!(matches!(
            harness
                .durable_context(task, operation)
                .await?
                .read_file(&input_file)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut coordinator = DistributedCoordinator::open(&stream, reader)
            .await?
            .with_payload_store(payloads);
        assert_eq!(
            coordinator.scheduler().session_limits(operation)?,
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
                // This fixture's finite in-process model stream is quiescent after
                // dropping it. Replacement is explicit, never inferred from a
                // failed publication or an unknown external provider state.
                coordinator
                    .release_lease(old, IdempotencyKey::new("release-crashed")?)
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
                    operation,
                    IdempotencyKey::new(if reopened { "restart" } else { "start" })?,
                    SchedulerEvent::Started {
                        operation_id: operation,
                        fence: fence.clone(),
                    },
                )
                .await?;
        }
        if let Some(old) = &previous_lease
            && !cancel_after_failure
        {
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
            input: ModelContent::Part(ModelContentPart::File { file: input_file, policy: FileProjectionPolicy::Native(Box::new(acyclic_harness::model::NativeMediaPolicy { intent: acyclic_harness::model::NativeMediaIntent::Image { detail: acyclic_harness::model::ImageDetail::Auto }, maximum_bytes: acyclic_harness::conversation::MAX_LIMIT_FILE_BYTES, maximum_work: 4096, configuration: None })) }),
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
                // This session still has a model step available: cancellation,
                // rather than an exhausted caller budget, rejects fresh work.
                assert!(matches!(rejected, Err(Error::Conflict(message))
                    if message == "cancelled task cannot publish new execution work"));
                coordinator.refresh().await?;
                assert_eq!(coordinator.scheduler(), &before_fresh_attempt);
            } else {
                assert!(rejected.is_err());
            }
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
        } else {
            if let Some(mode) = fault {
                stream_provider.arm_execution(2, mode);
            }
            let interrupted = if unselected_context {
                // Produce current stock Started/request bytes with explicit
                // unselected context through the same fenced disk journal.
                let admission = host.observe_admission(task).await?;
                let admitted_context = harness
                    .durable_context(task, admission.operation_id)
                    .await?;
                let admitted_scope = admitted_context.scope().clone();
                let previous = StockExecutor::new(
                    Model::new("test", "interrupted", "1", Value::Null)?,
                    model.clone(),
                    ContextPipeline::default(),
                    ToolRegistry::default(),
                )
                .with_limits(admitted_scope.limits())
                .with_tool_authority(admitted_scope, None)?
                .with_durable_task(host.clone(), task, fence.clone())
                .with_task_context(&admitted_context, execution.operation_id())?;
                let journal = FilesystemExecutionJournal::for_task(
                    host.journal_owner(task, fence.clone()).await?,
                    execution.operation_id(),
                    OperationId::from_bytes([8; 16]),
                    filesystem.clone(),
                    volume.clone(),
                    issuer.verifier(),
                    signed.clone(),
                    65_536,
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
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
            coordinator.refresh().await?;
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
                coordinator.refresh().await?;
                assert_eq!(
                    coordinator
                        .scheduler()
                        .operation(operation)
                        .and_then(|state| state.reservation.as_ref()),
                    Some(&old.reservation)
                );
                // Reconciliation observed the finite provider's completion. Only
                // this explicit fenced acknowledgement may now release ownership.
                let mut stale = old.clone();
                stale.reservation.id = "stale".into();
                assert!(
                    coordinator
                        .release_lease(&stale, IdempotencyKey::new("stale-cancelled-release")?)
                        .await
                        .is_err()
                );
                coordinator
                    .release_lease(old, IdempotencyKey::new("cancelled-reconciled-release")?)
                    .await?;
                let terminal = coordinator
                    .scheduler()
                    .operation(operation)
                    .ok_or_else(|| Error::NotFound("cancelled terminal task".into()))?;
                assert_eq!(terminal.outcome, Some(Outcome::Cancelled));
                assert!(terminal.reservation.is_none());
                assert!(coordinator.pull(&worker).await?.is_none());
                assert!(
                    host.journal_owner(task, LeaseFence::from(&old.reservation))
                        .await
                        .is_err()
                );
                return Ok(());
            }
            coordinator
                .cancel_operation(
                    &authority,
                    &signed,
                    &issuer.verifier(),
                    operation,
                    IdempotencyKey::new("cancel-uncertain-model")?,
                    false,
                )
                .await?;
            assert!(coordinator.pull(&worker).await?.is_none());
            previous_lease = Some(lease);
            continue;
        }
        let journal = runtime.workflow_journal(task, fence.clone()).await?;
        let mut session = harness
            .open_registered_task(task, fence.clone(), journal.clone())
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
                .open_registered_task(task, fence, recovery_journal.clone())
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

#[path = "task_workflow/stock_turn.rs"]
mod stock_turn;
