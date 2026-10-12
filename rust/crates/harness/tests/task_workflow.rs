//! Registered task checkpoint recovery through real durable-local providers.
#![cfg(feature = "filesystem-local")]
#![allow(
    clippy::too_many_lines,
    reason = "durable recovery scenarios retain setup, fault injection and reopen assertions together"
)]

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, LocalOptions};
use acyclic_harness::runtime::TaskStateProvider;
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
    FileProjectionPolicy, Model, ModelContent, ModelContentPart, ModelEvent,
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
    LocalStream, LocalStreamLimits, StreamProvider, SystemUnixMillisClock,
    UnixMillisClock,
};
use futures::{future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::{collections::BTreeMap, sync::{Arc, Mutex}};

struct InterruptedModel {
    requests: Mutex<Vec<Vec<u8>>>,
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
            assert_eq!(context.task().read_file(&file).await?.as_ref(), b"hello");
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
            assert_eq!(context.task().read_file(&file).await?.as_ref(), b"hello");
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
    fn context_capacity(
        &self,
        _: &Model,
    ) -> Result<acyclic_harness::context::ModelContextCapacity> {
        Ok(acyclic_harness::context::ModelContextCapacity {
            context_tokens: 131_072,
            output_tokens: 16_384,
        })
    }

    fn count_tokens(
        &self,
        request: &PreparedModelRequest,
    ) -> Result<acyclic_harness::context::ModelTokenCount> {
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
                            .ok_or_else(|| Error::Invalid("synthetic token count overflows".into()))
                    },
                )?;
                u32::try_from(bytes)
                    .map_err(|_| Error::Invalid("synthetic token count exceeds u32".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let fixed = serde_json::to_vec(&request.request().tools)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len();
        Ok(acyclic_harness::context::ModelTokenCount {
            request_digest: request.request_digest(),
            fixed_tokens: u32::try_from(fixed)
                .map_err(|_| Error::Invalid("synthetic framing exceeds u32".into()))?,
            message_tokens,
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        _dispatch: acyclic_harness::model::ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        self.requests.lock().unwrap().push(request.bytes().to_vec());
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
    fn reconcile<'a>(&'a self, _dispatch: acyclic_harness::model::ModelDispatch, observed: &'a [acyclic_harness::model::ModelEvent]) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            assert_eq!(
                observed,
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
        issuer.verifier(&mut None).expect("verifier copy").audience().clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
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
        issuer.verifier(&mut None).expect("verifier copy").audience().clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
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
    let responder = issuer.issue("approver", &Capabilities::new(["interaction:resolve".to_owned(), ticket.responder_grant()]), Some(agent), None, &mut None).expect("scope proof");
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
    let signed = issuer.issue("owner", &Capabilities::new([
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
        ]), Some(agent), None, &mut None).expect("scope proof");
    let conversation_issuer = AuthorityIssuer::new(
        "worker-approval",
        [54; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "worker-approval".into(),
        },
    );
    let conversation_scope = conversation_issuer.issue("owner", &Capabilities::new([
            "conversation:bind".to_owned(),
            "interaction:open".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
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
    let definition = tasks.get_version::<Value, u64>("test.restart", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
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
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let mut old_lease: Option<acyclic_harness::distributed::WorkLease> = None;
    let mut discovery_cursor = None;
    let model = Arc::new(InterruptedModel {
        requests: Mutex::new(Vec::new()),
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
            let stream = Arc::new(
                LocalStream::open(&stream_root, LocalStreamLimits::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            );
            let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                65_536, None,
            )?);
            let reader = Arc::new(FilesystemContentVerifier::new(
                filesystem.clone(),
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                65_536, None,
            )?);
            if with_policy && !reopened {
                Box::pin(async {
                    let mut aggregate = acyclic_harness::store::StreamAggregate::open(
                        &stream,
                        conversation_issuer.verifier(&mut None).expect("verifier copy").audience().clone(),
                        conversation_issuer.verifier(&mut None).expect("verifier copy"),
                        SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
                    .await?;
                    aggregate
                        .execute(Command {
                            operation_id: OperationId::from_bytes([90; 16]),
                            idempotency_key: IdempotencyKey::new("worker-approval-bind")?,
                            expected_revision: 0,
                            scope: conversation_scope.clone(),
                            causal_parent: None,
                            action: Action::BindConversation { agent },
                        }, #[cfg(feature = "filesystem")] None, &mut None)
                        .await?;
                    Ok::<(), Error>(())
                })
                .await?;
            }
            let runtime = FilesystemTaskRuntime::open_with_policy_and_clock(
                stream.clone(),
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(&mut None).expect("verifier copy"),
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
                    conversation_issuer.verifier(&mut None).expect("verifier copy"),
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
                    issuer.verifier(&mut None).expect("verifier copy"),
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
                        selected_output_volume: None,
                        mailbox_operation: None,
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
        let mut coordinator = DistributedCoordinator::open(&stream, reader.clone(), Some(payloads.clone()), &mut None)
            .await?
            ;
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
                runtime.task_host().cancel(task, None).await?;
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
                    .resume_workflow(task, 2, input.clone(), IdempotencyKey::new("wrong-revision")?, None)
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
                assert!(!runtime.poll_task_wake(task, None).await?);
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
                    let file = payloads.stage(operation, "second-wait-mail", b"7").await?;
                    runtime
                        .task_host()
                        .send(task, task, OperationId::from_bytes([30; 16]), file)
                        .await?;
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
                    .resume_workflow(task, 1, input.clone(), IdempotencyKey::new("wake-seven")?, None)
                    .await?;
                runtime
                    .task_host()
                    .resume_workflow(task, 1, input, IdempotencyKey::new("wake-seven")?, None)
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
                let before = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), "harness/v3/coordinator/events")?
                    .tail()
                    .await?;
                assert!(matches!(
                    runtime.worker_tick(&worker, None, &NoCommands, 1, 0).await,
                    Err(Error::Invalid(_))
                ));
                assert_eq!(
                    acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), "harness/v3/coordinator/events")?
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
                assert_eq!(model.generated.load(Ordering::SeqCst), 0);
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
                        acyclic_harness::context::CompactionPolicy::default(),
                    ),
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
                        Model::new("test", "interrupted", "1", Value::Null)?,
                        model.clone(),
                        ContextPipeline::default(),
                        acyclic_harness::context::CompactionPolicy::default(),
                    ),
                    2, None,
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
            coordinator.refresh(&mut None).await?;
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
                acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v3/mail/by-recipient/{task}"))?
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
            assert_eq!(reader.read(result).await?.as_ref(), b"7");
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
            let timers = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v2/timers/{task}"))?;
            assert_eq!(timers.bounds().await?.tail, 72);
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
            assert_eq!(reader.read(&first.payload).await?.as_ref(), b"7");
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
async fn admitted_custom_merge_candidate_reopens_without_rerunning_driver() -> Result<()> {
    use acyclic_fs::{ConflictKey, ConflictSide, JoinOutcome, MergeDriver, MergeDriverRegistry,
        MergeResolution, MemoryMergeResolutionCache, resolve_merge_plan};
    use acyclic_harness::conversation::ContentGrant;
    use acyclic_harness::core::Reducer;
    use acyclic_harness::filesystem::{FilesystemWorkflowJournal, ParentProjectController, WorkspaceMutation, workspace_ref};
    use acyclic_harness::workflow::{MachineCheckpoint, WorkflowAdmission};

    struct Driver(AtomicUsize);
    impl MergeDriver for Driver {
        fn fingerprint(&self) -> &[u8] { b"review-policy@7" }
        fn resolve(&self, conflict: &acyclic_fs::ConflictView)
            -> std::result::Result<MergeResolution, acyclic_fs::DriverError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(match conflict.key {
                ConflictKey::File(_) | ConflictKey::ContentRange { .. } => MergeResolution::Text("custom resolution".into()),
                _ => MergeResolution::Select(ConflictSide::Theirs),
            })
        }
    }
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("custom-merge", "filesystem", "2")?;
    let agent = AgentId::from_bytes([141; 16]);
    let project = VolumeRef::new(provider.clone(), "project", VolumeClass::Project, VolumeOwner::Project("review".into()))?;
    let source = VolumeRef::new(provider.clone(), "source", VolumeClass::Project, project.owner().clone())?;
    let private = VolumeRef::new(provider.clone(), "private", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    let authority = Authority { kind: AggregateKind::Conversation, id: "custom-merge-parent".into() };
    let issuer = AuthorityIssuer::new("custom-merge", [142; 32], authority.clone());
    let signed = issuer.issue("owner", &Capabilities::new([
        "conversation:bind".into(), "fork:publish".into(), "project:merge".into(),
        project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
        source.capability(VolumeOperation::Read)?, source.capability(VolumeOperation::Write)?,
        private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?,
    ]), Some(agent), None, &mut None).expect("scope proof");
    let mut parent = Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new());
    parent.apply(Command { operation_id: OperationId::from_bytes([143; 16]), idempotency_key: IdempotencyKey::new("bind")?,
        expected_revision: 0, scope: signed.clone(), causal_parent: None, action: Action::BindConversation { agent } })?;
    let driver = Arc::new(Driver(AtomicUsize::new(0)));
    let operation = OperationId::from_bytes([144; 16]);
    let admission;
    let resolution_record;
    let source_head;
    let target_head;
    {
        let host = Arc::new(FilesystemHost::new(Fs::local(options.clone()).await.map_err(|e| Error::Storage(e.to_string()))?, provider.clone())?);
        host.create_volume(&project).await?;
        host.create_volume(&private).await?;
        let write = ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &signed, &project, VolumeOperation::Write)?;
        host.put_content(&project, &write, "shared.txt", b"base", "text/plain", "shared.txt", 128,
            &IdempotencyKey::new("base")?).await?;
        let controller = ParentProjectController::new(&host, &parent, &issuer.verifier(&mut None).expect("verifier copy"), &signed, &project, None)?;
        let base = host.resolve(&workspace_ref(provider.clone(), &project.storage_name()?)?).await?;
        let fork = controller.fork_project(&base.generation, &source, &IdempotencyKey::new("fork")?).await?;
        source_head = host.apply(&fork.workspace, Some(&fork.generation), &[WorkspaceMutation::PutFile {
            path: "/shared.txt".into(), bytes: b"source".as_slice().into(),
        }], &IdempotencyKey::new("source-edit")?).await?.generation;
        let target = workspace_ref(provider.clone(), &project.storage_name()?)?;
        target_head = host.apply(&target, None, &[WorkspaceMutation::PutFile {
            path: "/shared.txt".into(), bytes: b"target".as_slice().into(),
        }], &IdempotencyKey::new("target-edit")?).await?.generation;
        let plan = controller.prepare_project_import(&source, &source_head).await?;
        let JoinOutcome::Conflicted { conflicts, truncated } = controller.apply_project_merge(&plan, operation).await? else {
            return Err(Error::Invalid("custom merge must have a real conflict".into()));
        };
        let described = controller.describe_project_merge_conflicts(&plan, &conflicts, truncated).await?;
        let state = json!({"policy":"review", "revision":"7", "plan":acyclic_fs::encode_merge_plan(&described)
            .map_err(|e| Error::Invalid(e.to_string()))?});
        admission = WorkflowAdmission { operation_id: operation, request_digest: acyclic_harness::contract::canonical_json_digest(&state)?,
            initial: MachineCheckpoint { machine: MachineIdentity { name: "test.custom-merge".into(), version: "7".into(), digest: [145; 32] },
                revision: 0, state } };
        let journal = FilesystemWorkflowJournal::new(Arc::new(LocalStream::open(&stream_root, LocalStreamLimits::default()).await
            .map_err(|e| Error::Storage(e.to_string()))?), host.clone(), "custom-merge", private.clone(), issuer.verifier(&mut None).expect("verifier copy"), signed.clone(), 65_536)?;
        assert_eq!(journal.admit(admission.clone()).await?, admission);
        let mut registry = MergeDriverRegistry::new();
        registry.register("review", driver.clone()).map_err(|e| Error::Invalid(e.to_string()))?;
        registry.set_default("review").map_err(|e| Error::Invalid(e.to_string()))?;
        let candidate = resolve_merge_plan(described, &registry, &mut MemoryMergeResolutionCache::default(), false)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let input = json!(acyclic_fs::encode_merge_candidate(&candidate).map_err(|e| Error::Invalid(e.to_string()))?);
        let state = json!({"candidate":input});
        resolution_record = WorkflowRecord { operation_id: OperationId::from_bytes([146; 16]),
            idempotency_key: IdempotencyKey::new("driver-result")?, input_digest: acyclic_harness::contract::canonical_json_digest(&input)?,
            prior: admission.initial.clone(), input,
            transition: MachineTransition { state: state.clone(), commands: Vec::new(), status: MachineStatus::Suspended },
            next: MachineCheckpoint { machine: admission.initial.machine.clone(), revision: 1, state } };
        journal.commit(0, resolution_record.idempotency_key.clone(), resolution_record.clone()).await?;
    }
    let calls = driver.0.load(Ordering::SeqCst);
    assert!(calls > 0);
    for retry in 0..2 {
        let host = Arc::new(FilesystemHost::new(Fs::local(options.clone()).await.map_err(|e| Error::Storage(e.to_string()))?, provider.clone())?);
        let journal = FilesystemWorkflowJournal::new(Arc::new(LocalStream::open(&stream_root, LocalStreamLimits::default()).await
            .map_err(|e| Error::Storage(e.to_string()))?), host.clone(), "custom-merge", private.clone(), issuer.verifier(&mut None).expect("verifier copy"), signed.clone(), 65_536)?;
        let retained = journal.admission().await?.ok_or_else(|| Error::NotFound("merge admission".into()))?;
        assert_eq!(retained, admission);
        let records = journal.replay(0, 1).await?;
        assert_eq!(records, vec![resolution_record.clone()]);
        let record = records.first().ok_or_else(|| Error::NotFound("driver result".into()))?;
        let candidate = acyclic_fs::decode_merge_candidate(record.next.state["candidate"].as_str()
            .ok_or_else(|| Error::Invalid("candidate missing".into()))?).map_err(|e| Error::Invalid(e.to_string()))?;
        let controller = ParentProjectController::new(&host, &parent, &issuer.verifier(&mut None).expect("verifier copy"), &signed, &project, None)?;
        let plan = controller.prepare_project_import_at(&source, &source_head, &target_head).await?;
        let denied = issuer.issue("no-write", &Capabilities::new([source.capability(VolumeOperation::Read, &mut None)?]), Some(agent), None, &mut None).expect("scope proof");
        let unauthorized = ParentProjectController::new(&host, &parent, &issuer.verifier(&mut None).expect("verifier copy"), &denied, &project, None)?;
        assert!(unauthorized.apply_project_merge_candidate(&plan, operation, &candidate).await.is_err());
        let mut changed = candidate.clone();
        changed.plan.ours = candidate.plan.base;
        assert!(controller.apply_project_merge_candidate(&plan, operation, &changed).await.is_err());
        let outcome = controller.apply_project_merge_candidate(&plan, operation, &candidate).await?;
        assert!(if retry == 0 { matches!(outcome, JoinOutcome::Applied(_)) } else { matches!(outcome, JoinOutcome::AlreadyApplied(_)) });
        if retry == 0 {
            let write = ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &signed, &project, VolumeOperation::Write)?;
            host.put_content(&project, &write, "concurrent.txt", b"user edit", "text/plain", "concurrent.txt", 128,
                &IdempotencyKey::new("concurrent")?).await?;
        }
        assert_eq!(host.read(&workspace_ref(provider.clone(), &project.storage_name()?)?, None, "/concurrent.txt", 128).await?.as_ref(), b"user edit");
        assert_eq!(host.read(&workspace_ref(provider.clone(), &project.storage_name()?)?, None, "/shared.txt", 128).await?.as_ref(), b"custom resolution");
        assert_eq!(host.resolve(&workspace_ref(provider.clone(), &source.storage_name()?)?).await?.generation, source_head);
    }
    assert_eq!(driver.0.load(Ordering::SeqCst), calls);
    Ok(())
}

#[tokio::test]
async fn direct_stock_turn_reconciles_through_runtime_after_provider_reopen() -> Result<()> {
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
    direct_executor: bool,
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
    let signed = issuer.issue("owner", &Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:test.restart@1".to_owned(),
            "model:generate".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?
        .with_run_limits(TaskRunLimits {
            max_steps: Some(1),
            ..TaskRunLimits::default()
        }, &mut None)?;
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
    let definition = tasks.get_version::<u64, u64>("test.restart", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
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
        requests: Mutex::new(Vec::new()),
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
        let stream = stream_provider.clone();
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(),
            &volume,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            65_536, None,
        )?);
        let reader = Arc::new(FilesystemContentVerifier::new(
            filesystem.clone(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            65_536, None,
        )?);
        let host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, reader.clone(), Some(payloads.clone()), &mut None)
                .await?
                ,
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
        }, None)?;
        if reopened {
            assert!(host.observe_admission(task).await.is_err());
        }
        drop(host);
        let runtime = FilesystemTaskRuntime::open(
            stream.clone(),
            filesystem.clone(),
            volume.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
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
        reader.verify(&input_file).await?;
        assert!(matches!(
            harness
                .durable_context(task, operation)
                .await?
                .read_file(&input_file)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut coordinator = DistributedCoordinator::open(&stream, reader, Some(payloads), &mut None)
            .await?
            ;
        assert_eq!(
            coordinator.scheduler().session_limits(operation, &mut None)?,
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
                    }, &mut None,
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
                Model::new("test", "interrupted", "1", Value::Null)?,
                model.clone(),
                ContextPipeline::default(), None,
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
                        Model::new("test", "interrupted", "1", Value::Null)?,
                        model.clone(),
                        ContextPipeline::default(), None,
                    )
                    .await?
                    .with_compaction_policy(acyclic_harness::context::CompactionPolicy::Disabled);
                assert!(matches!(
                    drifted.execute(turn.clone()).await,
                    Err(Error::Conflict(message))
                        if message == "execution identity is bound to another request or configuration"
                ));
                assert_eq!(model.generated.load(Ordering::SeqCst), 1);
                assert_eq!(model.reconciled.load(Ordering::SeqCst), 0);
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
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
            if fault.is_none() {
                assert_eq!(model.output_tokens.load(Ordering::SeqCst), 8_192);
            }
            let another = runtime
                .stock_execution(
                    task,
                    fence.clone(),
                    OperationId::from_bytes([10; 16]),
                    Model::new("test", "interrupted", "1", Value::Null)?,
                    model.clone(),
                    ContextPipeline::default(), None,
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
            assert_eq!(model.generated.load(Ordering::SeqCst), 1);
            assert_eq!(model.reconciled.load(Ordering::SeqCst), 1);
        } else {
            if let Some(mode) = fault {
                stream_provider.arm_execution(3, mode);
            }
            let interrupted = if direct_executor {
                let admitted_context = harness.durable_context(task, operation).await?;
                let previous = StockExecutor::new(
                    Model::new("test", "interrupted", "1", Value::Null)?,
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
                    host.journal_owner(task, fence.clone()).await?,
                    execution.operation_id(),
                    OperationId::from_bytes([8; 16]),
                    filesystem.clone(),
                    &volume,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &signed,
                    65_536, None,
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
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    operation,
                    IdempotencyKey::new("cancel-uncertain-model")?,
                    false, &mut None,
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
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    operation,
                    IdempotencyKey::new("cancel")?,
                    false, &mut None,
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

#[tokio::test]
async fn default_stock_turn_selects_one_queued_item_and_keeps_admitted_input_on_reopen() -> Result<()> {
    use acyclic_harness::filesystem::StockTurnMachine;
    for queued in [true, false] {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new("stock-mail", "filesystem", "2")?;
    let agent = AgentId::new();
    let volume = VolumeRef::new(provider.clone(), "private", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    let issuer = AuthorityIssuer::new("stock-mail", [81;32], Authority { kind: AggregateKind::Task, id: "stock-mail".into() });
    let signed = issuer.issue("owner", &Capabilities::new([
        "operation:declare".to_owned(), "operation:observe".to_owned(), "operation:cancel".to_owned(),
        "task:spawn:acyclic.stock_turn@1".to_owned(), "model:generate".to_owned(),
        "mail:send".to_owned(), "mail:read".to_owned(),
        volume.capability(VolumeOperation::Read)?, volume.capability(VolumeOperation::Write)?,
    ]), Some(agent), None, &mut None).expect("scope proof");
    let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?;
    let machine = Arc::new(StockTurnMachine::new());
    let definition = machine.definition()?;
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let definition = tasks.get_version::<Value, acyclic_harness::executor::TurnOutput>("acyclic.stock_turn", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let operation = OperationId::new();
    let task = TaskId::from_bytes(operation.into_bytes());
    let receive_operation = OperationId::new();
    let model_operation = OperationId::new();
    let model = Model::new("fixture", "stock-mail", "1", Value::Null)?;
    let provider_model = Arc::new(InterruptedModel {
        requests: Mutex::new(Vec::new()), approval_tool: false, observed_events: 1,
        output_tokens: AtomicU64::new(0), generated: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
    });
    let worker = Worker { id: "stock-mail".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let mut retained_lease = None;
    let mut admitted_model = None;
    let mut selected = None;
    for phase in 0..4 {
        let filesystem = Arc::new(FilesystemHost::new(Fs::local(fs_options.clone()).await
            .map_err(|error| Error::Storage(error.to_string()))?, provider.clone())?);
        if phase == 0 { filesystem.create_volume(&volume).await?; }
        let stream = Arc::new(LocalStream::open(&stream_root, LocalStreamLimits::default()).await?);
        let runtime = FilesystemTaskRuntime::open(stream, filesystem.clone(), volume.clone(), issuer.verifier(&mut None).expect("verifier copy"), signed.clone(),
            scope.clone(), tasks.clone(), machines.clone(), ToolRegistry::default(),
            SessionLimits { active_tasks: 1, total_tasks: 1, depth: 1, model_steps: 1 }, 1, 65536).await?;
        let payloads = FilesystemSchedulerPayloadStore::new(filesystem, &volume, &issuer.verifier(&mut None).expect("verifier copy"), &signed, 65536, None)?;
        let commands = runtime.commands().with_model(model.clone(), provider_model.clone(), ContextPipeline::default(),
            acyclic_harness::context::CompactionPolicy::Disabled);
        if phase == 0 {
            let model_payload = payloads.stage(operation, "model", &serde_json::to_vec(&ModelTaskCommand {
                input: ModelContent::Text("original prompt".into()), selected_context: None, max_steps: 1,
                max_output_tokens: None, mailbox_operation: None, selected_output_volume: None,
            }).map_err(|error| Error::Invalid(error.to_string()))?).await?;
            let model_command = WorkflowCommand { operation_id: model_operation, kind: MODEL_TASK_COMMAND_KIND.into(), payload: model_payload };
            let receive = WorkflowCommand { operation_id: receive_operation, kind: MAIL_RECEIVE_TASK_COMMAND_KIND.into(),
                payload: payloads.stage(operation, "receive", &serde_json::to_vec(&MailReceiveTaskCommand { after: 0 })
                    .map_err(|error| Error::Invalid(error.to_string()))?).await? };
            assert!(matches!(runtime.harness().admit(operation, &definition, json!({"receive":receive,"model":model_command}), None).await?, Admission::Accepted(_)));
            admitted_model = Some(model_command);
            let WorkPull::Claimed(lease) = runtime.task_host().pull_work(&worker).await? else {
                return Err(Error::NotFound("stock lease".into()));
            };
            runtime.task_host().start_task(&lease, None).await?;
            let fence = LeaseFence::from(&lease.reservation);
            for (path, bytes) in [("first", b"first steering".as_slice()), ("second", b"later steering".as_slice())].into_iter().filter(|_| queued) {
                let payload = payloads.stage(OperationId::new(), path, bytes).await?;
                runtime.task_host().send_owned(task, fence.clone(), task, OperationId::new(), payload).await?;
            }
            let items = runtime.task_host().inbox(task, 0, 2).await?;
            assert_eq!(items.len(), if queued { 2 } else { 0 });
            selected = items.into_iter().next();
            retained_lease = Some(lease);
        }
        let lease = retained_lease.clone().unwrap();
        let attempt = runtime.resume_task(lease.clone(), &commands, if phase == 3 { 4 } else { 1 }, None).await;
        match phase {
            0 | 1 => assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. }))),
            2 => assert!(matches!(attempt, TaskWorkerAttempt::Unresolved { error: Error::Indeterminate(_), .. }
                | TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. }))),
            _ => assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { .. }))),
        }
        if phase == 1 || phase == 2 {
            let journal = runtime.workflow_journal(task, LeaseFence::from(&lease.reservation)).await?;
            let record = journal.replay(0, 8).await?.pop().unwrap();
            assert_eq!(record.transition.commands, vec![admitted_model.clone().unwrap()]);
            assert_eq!(record.next.state["mail"], serde_json::to_value(&selected).unwrap());
            let remaining = runtime.task_host().inbox(task, 1, 1).await?;
            if queued { assert_eq!(remaining[0].sequence, 2); } else { assert!(remaining.is_empty()); }
        }
        if phase == 2 && queued {
            let request: acyclic_harness::model::ModelRequest = serde_json::from_slice(
                &provider_model.requests.lock().unwrap()[0],
            ).map_err(|error| Error::Invalid(error.to_string()))?;
            let parcel_path = format!("mailbox/{receive_operation}/parcel");
            let parcel = request.messages.iter().flat_map(|message| message.content.file_refs())
                .find(|file| file.path() == parcel_path).ok_or_else(|| Error::NotFound("selected model parcel".into()))?;
            assert_eq!(parcel.descriptor(), selected.as_ref().unwrap().payload.descriptor());
            assert_eq!(runtime.harness().durable_context(task, operation).await?.read_file(parcel).await?.as_ref(), b"first steering");
        }
        if phase == 3 {
            let Some(Outcome::Succeeded(value)) = runtime.task_host().outcome(task).await? else {
                return Err(Error::Invalid("stock model did not settle".into()));
            };
            assert_eq!(value["text"], "partial-restored");
        }
    }
    assert_eq!(provider_model.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider_model.reconciled.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
