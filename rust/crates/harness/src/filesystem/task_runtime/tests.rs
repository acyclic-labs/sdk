//! Native controls exercise the ordinary admitted runtime and real journals.

use super::*;
use crate::{
    Admission, AgentId, Capabilities,
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{SchedulerPayloadStore as _, WorkLease, WorkPull, Worker},
    resources::ProviderRef,
    runtime::{TaskDefinition, ToolContext},
    scheduler::ResourceSnapshot,
    tool::{Tool, ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
    workflow::{MachineIdentity, MachineTransition, ResumableMachine},
};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::MemoryStream;
use std::{collections::BTreeMap, sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}};

mod headless;

struct Clock(AtomicU64);
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct PendingTool {
    clock: Arc<Clock>,
    executed: AtomicUsize,
    reconciled: AtomicUsize,
    receipt_ready: AtomicBool,
}
impl ToolExecutor for PendingTool {
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Unauthorized("task context required".into())) })
    }
    fn execute_with_context<'a>(
        &'a self,
        _: ToolContext,
        _: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            // The provider effect exists, but its acknowledgement never arrives.
            self.executed.fetch_add(1, Ordering::SeqCst);
            self.clock.0.store(20, Ordering::SeqCst);
            futures::future::pending().await
        })
    }
    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Err(Error::Unauthorized("task context required".into())) })
    }
    fn reconcile_with_context<'a>(
        &'a self,
        _: ToolContext,
        _: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.executed.load(Ordering::SeqCst), 1);
            if !self.receipt_ready.load(Ordering::SeqCst) {
                return futures::future::pending().await;
            }
            Ok(Some(ToolResult { value: json!("original receipt") }))
        })
    }
}
impl ToolProjection for PendingTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(json!({"kind":"json","value":result.value}))
    }
}

struct Machine {
    identity: MachineIdentity,
    schema: Value,
    transitioned: AtomicUsize,
}
impl ResumableMachine for Machine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> { Ok(input.clone()) }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        self.transitioned.fetch_add(1, Ordering::SeqCst);
        let command: WorkflowCommand = serde_json::from_value(state["command"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(MachineTransition {
            state: state.clone(),
            commands: vec![command],
            status: MachineStatus::Suspended,
        })
    }
}

type Runtime = FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;
struct Fixture {
    stream: StreamClient<MemoryStream>,
    filesystem: Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>,
    volume: VolumeRef,
    issuer: AuthorityIssuer,
    signed: Scope,
    scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    tools: ToolRegistry,
    clock: Arc<Clock>,
    tool: Arc<PendingTool>,
    machine: Arc<Machine>,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let provider = ProviderRef::new("deadline-control", "filesystem", "2")?;
        let filesystem = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
        let agent = AgentId::from_bytes([101; 16]);
        let volume = VolumeRef::new(provider, "private", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        filesystem.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new("deadline-control", [102; 32], Authority {
            kind: AggregateKind::Task,
            id: "deadline-owner".into(),
        });
        let signed = issuer.root_for_agent(agent, "owner", Capabilities::new([
            "operation:declare".to_owned(), "operation:observe".to_owned(),
            "operation:cancel".to_owned(), "task:spawn:test.deadline@1".to_owned(),
            "tool:call:test.deadline".to_owned(), "model:generate".to_owned(),
            "mail:send".to_owned(), "mail:read".to_owned(),
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]));
        let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?
            .with_run_limits(TaskRunLimits { deadline_epoch_ms: Some(20), ..TaskRunLimits::default() })?;
        let machine = Arc::new(Machine {
            identity: MachineIdentity {
                name: "test.deadline".into(), version: "1".into(),
                digest: *blake3::hash(include_str!("tests.rs").as_bytes()).as_bytes(),
            },
            schema: json!({"type":"object"}),
            transitioned: AtomicUsize::new(0),
        });
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::<Value, Value>::resumable(
            machine.clone(), json!({"type":"object"}), json!({}),
        )?)?;
        let mut machines = MachineRegistry::default();
        machines.register(machine.clone())?;
        let clock = Arc::new(Clock(AtomicU64::new(10)));
        let tool = Arc::new(PendingTool {
            clock: clock.clone(), executed: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
            receipt_ready: AtomicBool::new(false),
        });
        let mut tools = ToolRegistry::default();
        tools.register(Tool {
            definition: ToolDefinition {
                name: "test.deadline".into(), revision: "1".into(), description: "Original receipt".into(),
                input_schema: json!({"type":"null"}), output_schema: json!({"type":"string"}),
                projection_schema: crate::tool::json_projection_schema(json!({"type":"string"})),
            },
            executor: tool.clone(), projection: tool.clone(),
        })?;
        Ok(Self { stream: StreamClient::new(Arc::new(MemoryStream::default())), filesystem,
            volume, issuer, signed, scope, tasks, machines, tools, clock, tool, machine })
    }
    async fn runtime(&self) -> Result<Runtime> {
        FilesystemTaskRuntime::open_with_clock(
            self.stream.clone(), self.filesystem.clone(), self.volume.clone(), self.issuer.verifier(),
            self.signed.clone(), self.scope.clone(), self.tasks.clone(), self.machines.clone(),
            self.tools.clone(), SessionLimits { active_tasks: 1, total_tasks: 1, depth: 1, model_steps: 1 },
            1, 65_536, self.clock.clone(),
        ).await
    }
    async fn admit(&self, runtime: &Runtime) -> Result<WorkLease> {
        self.admit_command(runtime, super::super::TOOL_TASK_COMMAND_KIND, serde_json::to_value(
            &super::super::ToolTaskCommand {
                name:"test.deadline".into(), revision:"1".into(), arguments:Value::Null,
            },
        ).map_err(|error| Error::Invalid(error.to_string()))?).await
    }
    async fn admit_command(&self, runtime: &Runtime, kind: &str, command: Value) -> Result<WorkLease> {
        let operation = OperationId::from_bytes([104; 16]);
        let payloads = FilesystemSchedulerPayloadStore::new(
            self.filesystem.clone(), self.volume.clone(), &self.issuer.verifier(), &self.signed, 65_536,
        )?;
        let payload = payloads.stage(operation, "command", &crate::contract::canonical_json_bytes(&command)?).await?;
        let definition = self.tasks.get_version::<Value, Value>("test.deadline", "1")?;
        let input = json!({"command":WorkflowCommand {
            operation_id: OperationId::from_bytes([105; 16]),
            kind: kind.into(),
            payload,
        }});
        assert!(matches!(
            runtime.harness().admit(operation, &definition, input, None).await?,
            Admission::Accepted(_)
        ));
        match runtime.task_host().pull_work(&Worker {
            id: "deadline-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
        }).await? {
            WorkPull::Claimed(lease) => Ok(lease),
            _ => Err(Error::NotFound("deadline lease".into())),
        }
    }
}

struct NativeAdmissionFailure {
    clock: Arc<Clock>,
    work: acyclic_fs::NativeMaterializationWork,
    calls: AtomicUsize,
}

impl TaskCommandHost for NativeAdmissionFailure {
    fn execute<'a>(
        &'a self,
        _context: TaskContext,
        _fence: LeaseFence,
        command: WorkflowCommand,
        _payload: Value,
    ) -> BoxFuture<'a,Result<TaskCommandProgress>> {
        Box::pin(async move {
            assert_eq!(command.operation_id,OperationId::from_bytes([105;16]));
            self.calls.fetch_add(1,Ordering::SeqCst);
            // A synchronously failed real ledger admission also crosses the
            // deadline. Neither failure is evidence of a stopped effect.
            self.clock.0.store(20,Ordering::SeqCst);
            match self.work.charge(acyclic_fs::WorkCounters {items_examined:1,..Default::default()}) {
                Err(cause) => Err(Error::NativeWork {cause,custody:None}),
                Ok(()) => Err(Error::Invalid("native negative control unexpectedly admitted".into())),
            }
        })
    }
}

#[tokio::test]
async fn native_admission_failure_precedes_post_dispatch_verification_and_deadline_cancellation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let lease = fixture.admit(&runtime).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            allocation_operations:1,
            peak_allocation_bytes:acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        },&cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let commands = NativeAdmissionFailure {clock:fixture.clock.clone(),work,calls:AtomicUsize::new(0)};
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    match runtime.resume_task(lease.clone(),&commands,2).await {
        TaskWorkerAttempt::Unresolved {lease:original,error:Error::NativeWork {cause,custody}} => {
            assert_eq!(original.operation.operation_id,lease.operation.operation_id);
            assert_eq!(original.reservation,lease.reservation);
            assert!(matches!(cause,acyclic_fs::NativeTreeMaterializationError::Work(_)));
            assert!(custody.is_none());
        },
        _ => return Err(Error::Invalid("native first cause or original lease was lost".into())),
    }
    assert_eq!(commands.calls.load(Ordering::SeqCst),1);
    assert_eq!(commands.work.receipt().items_examined,0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst),0);
    let state = runtime.task_host().observe_task(task).await?;
    assert!(!state.cancellation_requested);
    assert_eq!(state.reservation,Some(lease.reservation));
    assert!(state.outcome.is_none());
    Ok(())
}

#[tokio::test]
async fn expired_reopened_task_rejects_before_machine_or_tool_dispatch() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let lease = fixture.admit(&runtime).await?;
    drop(runtime);
    fixture.clock.0.store(20, Ordering::SeqCst);
    let reopened = fixture.runtime().await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let attempt = reopened.resume_task(lease.clone(), &reopened.commands(), 2).await;
    assert!(matches!(attempt, TaskWorkerAttempt::Unresolved { error: Error::Indeterminate(_), .. }));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    let state = reopened.task_host().observe_task(task).await?;
    assert!(state.cancellation_requested);
    assert_eq!(state.reservation, Some(lease.reservation.clone()));
    assert!(state.outcome.is_none());
    assert!(matches!(
        reopened.reconcile_task(lease, &reopened.commands(), 1, 1_000).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task:completed }) if completed == task
    ));
    assert!(matches!(reopened.task_host().outcome(task).await?, Some(crate::Outcome::Cancelled)));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn hanging_tool_retains_original_intent_until_explicit_reconciliation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let attempt = runtime.resume_task(lease.clone(), &runtime.commands(), 2).await;
    assert!(matches!(attempt, TaskWorkerAttempt::Unresolved { error: Error::Indeterminate(_), .. }));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    let revision = runtime.open_task(task, LeaseFence::from(&lease.reservation)).await?.checkpoint().revision;
    assert!(runtime.task_host().observe_task(task).await?.outcome.is_none());
    drop(runtime);
    let reopened = fixture.runtime().await?;
    assert!(matches!(
        reopened.reconcile_task(lease.clone(), &reopened.commands(), 1, 5).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. })
    ));
    let unresolved = reopened.task_host().observe_task(task).await?;
    assert!(unresolved.outcome.is_none());
    assert_eq!(unresolved.reservation, Some(lease.reservation.clone()));
    fixture.tool.receipt_ready.store(true, Ordering::SeqCst);
    let attempt = reopened.reconcile_task(lease.clone(), &reopened.commands(), 1, 1_000).await;
    assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert_eq!(revision, 1);
    assert!(matches!(reopened.task_host().outcome(task).await?, Some(Outcome::Cancelled)));
    assert!(reopened.task_host().observe_task(task).await?.reservation.is_none());
    Ok(())
}

struct PendingModel {
    clock: Arc<Clock>,
    generated: AtomicUsize,
    reconciled: AtomicUsize,
}
impl ModelProvider for PendingModel {
    fn generate<'a>(
        &'a self,
        _: crate::model::PreparedModelRequest,
        _: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<crate::model::ModelEvent>> {
        self.generated.fetch_add(1, Ordering::SeqCst);
        self.clock.0.store(20, Ordering::SeqCst);
        Box::pin(futures::stream::pending())
    }
    fn reconcile<'a>(
        &'a self,
        _: crate::model::ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        Box::pin(async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.generated.load(Ordering::SeqCst), 1);
            Ok(Some(vec![
                crate::model::ModelEvent::Content { delta: "original model receipt".into() },
                crate::model::ModelEvent::Completed { metadata: Value::Null },
            ]))
        })
    }
}

#[tokio::test]
async fn stock_model_deadline_preserves_original_attempt_and_never_dispatches_expired_work() -> Result<()> {
    for expired_before_dispatch in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime().await?;
        let lease = fixture.admit(&runtime).await?;
        let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        runtime.task_host().start_task(&lease).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let turn = OperationId::from_bytes([106; 16]);
        let model = Model::new("synthetic", "deadline", "1", Value::Null)?;
        let provider = Arc::new(PendingModel {
            clock: fixture.clock.clone(),
            generated: AtomicUsize::new(0),
            reconciled: AtomicUsize::new(0),
        });
        let execution = runtime.stock_execution(
            task, fence.clone(), turn, model.clone(), provider.clone(), ContextPipeline::default(),
        ).await?.with_compaction_policy(crate::context::CompactionPolicy::Disabled);
        let input = TurnInput {
            operation_id: execution.operation_id(),
            input: crate::model::ModelContent::Text("question".into()),
            selected_context: None,
            max_steps: 1,
        };
        if expired_before_dispatch {
            fixture.clock.0.store(20, Ordering::SeqCst);
        }
        assert!(matches!(execution.execute(input.clone()).await,
            Err(Error::Indeterminate(operation)) if operation == input.operation_id));
        assert_eq!(provider.generated.load(Ordering::SeqCst), usize::from(!expired_before_dispatch));
        let state = runtime.task_host().observe_task(task).await?;
        assert!(state.cancellation_requested);
        assert_eq!(state.reservation, Some(lease.reservation.clone()));
        assert!(state.outcome.is_none());
        assert!(runtime.task_host().verify_dispatch_owner(task, fence.clone()).await.is_err());
        drop(execution);
        drop(runtime);
        let reopened = fixture.runtime().await?;
        let execution = reopened.stock_execution(
            task, fence.clone(), turn, model, provider.clone(), ContextPipeline::default(),
        ).await?.with_compaction_policy(crate::context::CompactionPolicy::Disabled);
        if expired_before_dispatch {
            assert!(matches!(execution.reconcile(input, 1_000).await, Err(Error::Indeterminate(_))));
            assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
        } else {
            assert!(execution.reconcile(input, 1_000).await?.is_some());
            assert_eq!(provider.reconciled.load(Ordering::SeqCst), 1);
            assert!(reopened.execution_journal(task, fence, turn).await?.is_quiescent().await?);
        }
        assert_eq!(provider.generated.load(Ordering::SeqCst), usize::from(!expired_before_dispatch));
        assert!(reopened.task_host().observe_task(task).await?.outcome.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_mail_reconciles_original_atomic_send_without_republication() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let task = TaskId::from_bytes([104; 16]);
    let payloads = FilesystemSchedulerPayloadStore::new(
        fixture.filesystem.clone(), fixture.volume.clone(), &fixture.issuer.verifier(),
        &fixture.signed, 65_536,
    )?;
    let file = payloads.stage(OperationId::from_bytes([106; 16]), "mail-body", b"7").await?;
    let lease = fixture.admit_command(
        &runtime, super::super::MAIL_SEND_TASK_COMMAND_KIND,
        serde_json::to_value(&super::super::MailSendTaskCommand { recipient:task, payload:file })
            .map_err(|error| Error::Invalid(error.to_string()))?,
    ).await?;
    assert!(matches!(
        runtime.resume_task(lease.clone(), &runtime.commands(), 1).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })
    ));
    assert_eq!(runtime.task_host().inbox(task, 0, 8).await?.len(), 1);
    drop(runtime);
    fixture.clock.0.store(20, Ordering::SeqCst);
    let reopened = fixture.runtime().await?;
    assert!(matches!(
        reopened.resume_task(lease.clone(), &reopened.commands(), 1).await,
        TaskWorkerAttempt::Unresolved { error:Error::Indeterminate(_), .. }
    ));
    assert_eq!(reopened.task_host().observe_task(task).await?.reservation, Some(lease.reservation.clone()));
    assert!(matches!(
        reopened.reconcile_task(lease, &reopened.commands(), 1, 1_000).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task:completed }) if completed == task
    ));
    assert_eq!(reopened.task_host().inbox(task, 0, 8).await?.len(), 1);
    assert!(matches!(reopened.task_host().outcome(task).await?, Some(crate::Outcome::Cancelled)));
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    Ok(())
}

async fn mailbox_runtime(fixture: &Fixture) -> Result<Runtime> {
    FilesystemTaskRuntime::open_with_clock(
        fixture.stream.clone(), fixture.filesystem.clone(), fixture.volume.clone(),
        fixture.issuer.verifier(), fixture.signed.clone(), fixture.scope.clone(),
        fixture.tasks.clone(), fixture.machines.clone(), fixture.tools.clone(),
        SessionLimits { active_tasks: 8, total_tasks: 8, depth: 4, model_steps: 8 },
        8, 65_536, fixture.clock.clone(),
    ).await
}

async fn pull_mailbox(runtime: &Runtime) -> Result<WorkLease> {
    match runtime.task_host().pull_work(&Worker {
        id: "mailbox-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
    }).await? {
        WorkPull::Claimed(lease) => {
            runtime.task_host().start_task(&lease).await?;
            Ok(lease)
        }
        _ => Err(Error::NotFound("mailbox lease".into())),
    }
}

async fn mailbox_child(
    fixture: &Fixture, runtime: &Runtime, parent: TaskId, fence: LeaseFence,
    byte: u8, scope: &RuntimeScope,
) -> Result<TaskId> {
    let initial = runtime.task_host().observe_admission(parent).await?.initial.state;
    let request = runtime.harness().registered_child_admission(
        OperationId::from_bytes([byte; 16]), "test.deadline", "1",
        initial, parent, scope,
    )?;
    match runtime.task_host().admit_owned(request, fence).await? {
        Admission::Accepted(child) => Ok(child),
        _ => Err(Error::NotFound(format!("child of {}", fixture.signed.id()))),
    }
}

#[tokio::test]
async fn default_topology_mail_retains_exact_ack_and_rejects_mutations_after_restart() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.scope = fixture.scope.narrow(
        fixture.scope.grants().without(&Capabilities::new(["mail:send", "mail:read"])),
        fixture.scope.limits(),
    )?;
    let runtime = mailbox_runtime(&fixture).await?;
    let parent_lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&parent_lease).await?;
    let parent = TaskId::from_bytes(parent_lease.operation.operation_id.into_bytes());
    let parent_fence = LeaseFence::from(&parent_lease.reservation);
    let child = mailbox_child(&fixture, &runtime, parent, parent_fence.clone(), 120, &fixture.scope).await?;
    let other_child = mailbox_child(&fixture, &runtime, parent, parent_fence.clone(), 121, &fixture.scope).await?;
    let payloads = FilesystemSchedulerPayloadStore::new(
        fixture.filesystem.clone(), fixture.volume.clone(), &fixture.issuer.verifier(), &fixture.signed, 65_536,
    )?;
    let operation = OperationId::from_bytes([122; 16]);
    let payload = payloads.stage(operation, "mail", b"steer-one").await?;
    assert!(matches!(runtime.harness().durable_context(parent, parent_lease.operation.operation_id).await?
        .send(child, operation, payload.clone()).await, Err(Error::Unauthorized(_))));
    assert!(matches!(runtime.task_host().send(parent, child, operation, payload.clone()).await,
        Err(Error::Unauthorized(_))));
    let expected = runtime.task_context(parent, parent_fence.clone()).await?
        .send(child, operation, payload.clone()).await?;
    {
        let bound = runtime.task_context(parent, parent_fence.clone()).await?;
        let narrowed = bound.scoped(bound.scope().grants().without(&Capabilities::new([
            fixture.volume.capability(VolumeOperation::Read)?,
        ])), bound.scope().limits())?;
        assert!(matches!(narrowed.send(child, OperationId::from_bytes([127; 16]), payload.clone()).await,
            Err(Error::Unauthorized(_))));
        assert!(matches!(narrowed.inbox(0, 1).await, Err(Error::Unauthorized(_))));
    }
    assert_eq!(expected.recipient, child);
    assert_eq!(expected.operation_id, operation);
    assert_eq!(expected.payload, payload);
    assert_eq!(runtime.task_host().inbox(child, 0, 1).await?.len(), 1);
    // The caller loses the successful response; the original atomic receipt survives.
    drop(runtime);
    let reopened = mailbox_runtime(&fixture).await?;
    assert_eq!(reopened.task_host().send_owned(
        parent, parent_fence.clone(), child, operation, payload.clone(),
    ).await?, expected);
    assert_eq!(reopened.task_host().inbox(child, 0, 8).await?.len(), 1);
    assert!(matches!(reopened.task_host().send_owned(
        parent, parent_fence.clone(), other_child, operation, payload.clone(),
    ).await, Err(Error::Conflict(_))));
    let changed = payloads.stage(operation, "changed-mail", b"steer-two").await?;
    assert!(matches!(reopened.task_host().send_owned(
        parent, parent_fence.clone(), child, operation, changed,
    ).await, Err(Error::Conflict(_))));
    let mut stale = parent_fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(reopened.task_host().send_owned(parent, stale, child, operation, payload.clone()).await.is_err());
    assert!(reopened.task_host().send_owned(child, parent_fence.clone(), parent, operation, payload.clone()).await.is_err());
    let child_lease = pull_mailbox(&reopened).await?;
    assert_eq!(child_lease.operation.operation_id.into_bytes(), child.into_bytes());
    let child_fence = LeaseFence::from(&child_lease.reservation);
    let reply = OperationId::from_bytes([123; 16]);
    let child_context = reopened.task_context(child, child_fence.clone()).await?;
    assert_eq!(child_context.inbox(0, 1).await?.len(), 1);
    child_context.send(parent, reply, payload.clone()).await?;
    assert_eq!(reopened.task_host().inbox(parent, 0, 1).await?.len(), 1);
    // Siblings are not a default route, despite their common retained parent.
    assert!(matches!(reopened.task_host().send_owned(
        child, child_fence, other_child, OperationId::from_bytes([124; 16]), payload.clone(),
    ).await, Err(Error::Unauthorized(_))));
    let unreadable = fixture.scope.narrow(
        fixture.scope.grants().without(&Capabilities::new([fixture.volume.capability(VolumeOperation::Read)?])),
        fixture.scope.limits(),
    )?;
    let unread_child = mailbox_child(&fixture, &reopened, parent, parent_fence.clone(), 125, &unreadable).await?;
    assert!(matches!(reopened.task_host().send_owned(
        parent, parent_fence, unread_child, OperationId::from_bytes([126; 16]), payload,
    ).await, Err(Error::Unauthorized(_))));
    assert!(reopened.task_host().inbox(unread_child, 0, 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn subtree_cancellation_recovers_intent_without_claiming_provider_stop() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.scope = RuntimeScope::new(fixture.scope.grants().clone(), fixture.scope.limits())?;
    let runtime = mailbox_runtime(&fixture).await?;
    let parent_lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&parent_lease).await?;
    let parent = TaskId::from_bytes(parent_lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&parent_lease.reservation);
    let child = mailbox_child(&fixture, &runtime, parent, fence.clone(), 130, &fixture.scope).await?;
    let child_lease = pull_mailbox(&runtime).await?;
    let grandchild = mailbox_child(&fixture, &runtime, child,
        LeaseFence::from(&child_lease.reservation), 131, &fixture.scope).await?;
    let sibling = mailbox_child(&fixture, &runtime, parent, fence.clone(), 132, &fixture.scope).await?;
    // A real tool dispatch has happened, but its acknowledgement is unavailable.
    // Requesting cancellation must not turn that uncertainty into proven stop.
    {
        let context = runtime.task_context(child, LeaseFence::from(&child_lease.reservation)).await?;
        let command: WorkflowCommand = serde_json::from_value(
            runtime.task_host().observe_admission(child).await?.initial.state["command"].clone(),
        ).map_err(|error| Error::Invalid(error.to_string()))?;
        let payload: Value = crate::contract::json_from_slice(
            &context.read_file(&command.payload).await?,
        ).map_err(|error| Error::Invalid(error.to_string()))?;
        let commands = runtime.commands();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20),
            commands.execute(context, LeaseFence::from(&child_lease.reservation), command, payload),
        ).await.is_err());
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    }
    let operation = OperationId::from_bytes([133; 16]);
    let mut stale = fence.clone();
    stale.placement.push_str("-forged");
    assert!(runtime.task_host().cancel_child_owned(parent, stale, child, operation).await.is_err());
    assert!(matches!(runtime.task_host().cancel_child_owned(
        parent, fence.clone(), grandchild, operation,
    ).await, Err(Error::Unauthorized(_))));
    let state = runtime.task_host().cancel_child_owned(parent, fence.clone(), child, operation).await?;
    assert!(state.cancellation_requested);
    assert!(state.outcome.is_none());
    assert_eq!(state.reservation, Some(child_lease.reservation.clone()));
    assert!(runtime.task_host().observe_task(grandchild).await?.cancellation_requested);
    assert!(!runtime.task_host().observe_task(sibling).await?.cancellation_requested);
    assert!(!runtime.task_host().observe_task(parent).await?.cancellation_requested);
    drop(runtime);
    let reopened = mailbox_runtime(&fixture).await?;
    assert!(reopened.task_host().cancel_child_owned(parent, fence.clone(), child, operation).await?.cancellation_requested);
    assert!(matches!(reopened.task_host().cancel_child_owned(
        parent, fence.clone(), sibling, operation,
    ).await, Err(Error::Conflict(_))));
    assert!(!reopened.task_host().observe_task(sibling).await?.cancellation_requested);
    reopened.task_host().cancel(parent).await?;
    assert!(reopened.task_host().cancel_child_owned(parent, fence, child, operation).await.is_err());
    assert!(reopened.task_host().observe_task(child).await?.outcome.is_none());
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
    Ok(())
}

struct SteeringModel {
    generated: AtomicUsize,
}
impl ModelProvider for SteeringModel {
    fn generate<'a>(
        &'a self, request: crate::model::PreparedModelRequest, _: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<crate::model::ModelEvent>> {
        self.generated.fetch_add(1, Ordering::SeqCst);
        assert!(String::from_utf8_lossy(request.bytes()).contains("steer-one"));
        assert!(!String::from_utf8_lossy(request.bytes()).contains("steer-two"));
        Box::pin(futures::stream::iter([
            Ok(crate::model::ModelEvent::Content { delta: "steered".into() }),
            Ok(crate::model::ModelEvent::Completed { metadata: Value::Null }),
        ]))
    }
    fn reconcile<'a>(
        &'a self, _: crate::model::ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        Box::pin(async { Err(Error::Conflict("unexpected steering reconciliation".into())) })
    }
}

#[tokio::test]
async fn mailbox_model_checkpoints_one_item_before_new_admission_and_restarts_bounded() -> Result<()> {
    use super::super::{MailboxModelMachine, MailReceiveTaskCommand, ModelTaskCommand};
    let mut fixture = Fixture::new().await?;
    let machine = Arc::new(MailboxModelMachine::default());
    let definition = TaskDefinition::<Value, Value>::resumable(
        machine.clone(), json!({"type":"object"}), json!({"type":"object"}),
    )?;
    fixture.tasks.register(definition.clone())?;
    fixture.machines.register(machine)?;
    let runtime = mailbox_runtime(&fixture).await?;
    let operation = OperationId::from_bytes([140; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let receive_operation = OperationId::from_bytes([141; 16]);
    let model_operation = OperationId::from_bytes([142; 16]);
    let payloads = FilesystemSchedulerPayloadStore::new(
        fixture.filesystem.clone(), fixture.volume.clone(), &fixture.issuer.verifier(), &fixture.signed, 65_536,
    )?;
    let receive = WorkflowCommand {
        operation_id: receive_operation, kind: super::super::MAIL_RECEIVE_TASK_COMMAND_KIND.into(),
        payload: payloads.stage(operation, "receive", &crate::contract::canonical_json_bytes(
            &MailReceiveTaskCommand { after: 0 },
        )?).await?,
    };
    let model = WorkflowCommand {
        operation_id: model_operation, kind: super::super::MODEL_TASK_COMMAND_KIND.into(),
        payload: payloads.stage(operation, "model", &crate::contract::canonical_json_bytes(
            &ModelTaskCommand { input: crate::model::ModelContent::Text("question".into()),
                selected_context: None, max_steps: 1, max_output_tokens: None,
                mailbox_operation: Some(receive_operation) },
        )?).await?,
    };
    assert!(matches!(runtime.harness().admit(operation, &definition,
        json!({"receive":receive,"model":model}), None).await?, Admission::Accepted(_)));
    let lease = pull_mailbox(&runtime).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let original = payloads.stage(operation, "one", b"steer-one").await?;
    runtime.task_host().send_owned(task, fence.clone(), task,
        OperationId::from_bytes([143; 16]), original.clone()).await?;
    let provider = Arc::new(SteeringModel { generated: AtomicUsize::new(0) });
    let binding = Model::new("synthetic", "steering", "1", Value::Null)?;
    let commands = runtime.commands().with_model(binding.clone(), provider.clone(),
        ContextPipeline::default(), crate::context::CompactionPolicy::Disabled);
    let context = runtime.harness().durable_context(task, operation).await?;
    assert!(matches!(runtime.dispatch_commands(task, &context, &fence,
        &[receive.clone(), model.clone()], &commands).await, Err(Error::Invalid(_))));
    let model_payload: Value = crate::contract::json_from_slice(
        &context.read_file(&model.payload).await?,
    ).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(commands.execute(context.clone(), fence.clone(), model.clone(), model_payload.clone()).await,
        Err(Error::Unauthorized(_))));
    for _ in 0..2 {
        assert!(matches!(runtime.resume_task(lease.clone(), &commands, 1).await,
            TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })));
    }
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    let latest = runtime.workflow_journal(task, fence.clone()).await?.observe_latest_record().await?
        .ok_or_else(|| Error::NotFound("mail checkpoint".into()))?;
    assert_eq!(latest.next.revision, 2);
    assert_eq!(latest.next.state["mail"]["sequence"], json!(1));
    assert_eq!(latest.next.state["mail"]["payload"], serde_json::to_value(original)
        .map_err(|error| Error::Invalid(error.to_string()))?);
    let mut changed_receive = model_payload;
    changed_receive["mailbox_operation"] = json!(OperationId::from_bytes([149; 16]));
    assert!(matches!(commands.execute(context, fence.clone(), model.clone(), changed_receive).await,
        Err(Error::Unauthorized(_))));
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert!(runtime.execution_journal(task, fence.clone(), model_operation).await?
        .replay(execution_operation(task, model_operation), 0, 1).await?.is_empty());
    drop(commands);
    drop(runtime);
    let reopened = mailbox_runtime(&fixture).await?;
    let later = payloads.stage(operation, "two", b"steer-two").await?;
    reopened.task_host().send_owned(task, fence, task, OperationId::from_bytes([144; 16]), later).await?;
    let commands = reopened.commands().with_model(binding, provider.clone(),
        ContextPipeline::default(), crate::context::CompactionPolicy::Disabled);
    assert!(matches!(reopened.resume_task(lease.clone(), &commands, 1).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { .. })));
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert!(matches!(reopened.task_host().outcome(task).await?, Some(Outcome::Succeeded(_))));
    assert_eq!(reopened.task_host().inbox(task, 0, 8).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn child_cancellation_cannot_borrow_the_hosts_broader_control_grant() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.scope = fixture.scope.narrow(
        fixture.scope.grants().without(&Capabilities::new(["operation:cancel"])),
        fixture.scope.limits(),
    )?;
    let runtime = mailbox_runtime(&fixture).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease).await?;
    let parent = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let child = mailbox_child(&fixture, &runtime, parent, fence.clone(), 151, &fixture.scope).await?;
    assert!(matches!(runtime.task_host().cancel_child_owned(
        parent, fence, child, OperationId::from_bytes([152; 16]),
    ).await, Err(Error::Unauthorized(_))));
    assert!(!runtime.task_host().observe_task(child).await?.cancellation_requested);
    Ok(())
}

#[tokio::test]
async fn native_machine_without_callback_rejects_before_owner_deadline_or_provider_work() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    runtime.task_host().start_task(&lease).await?;
    let mut session = runtime.open_task(task, fence).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        }, &cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let custody = work.retain_owned(0).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let prefix = work.receipt();
    fixture.clock.0.store(20, Ordering::SeqCst);
    assert!(matches!(session.step_with_native_work(
        OperationId::from_bytes([118; 16]),
        IdempotencyKey::new("native-unsupported-machine")?,
        Value::Null,
        custody,
    ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
    assert_eq!(work.receipt(), prefix);
    assert_eq!(session.checkpoint().revision, 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    let state = runtime.task_host().observe_task(task).await?;
    assert!(!state.cancellation_requested);
    assert_eq!(state.reservation, Some(lease.reservation));
    assert!(state.outcome.is_none());
    // The same custom state machine still works through ordinary None.
    fixture.clock.0.store(10, Ordering::SeqCst);
    let original_state = session.checkpoint().state.clone();
    session.step(
        OperationId::from_bytes([119; 16]),
        IdempotencyKey::new("ordinary-custom-machine")?,
        Value::Null,
    ).await?;
    assert_eq!(session.checkpoint().revision, 1);
    assert_eq!(session.checkpoint().state, original_state);
    // The real journal may rerun the callback to prove the committed record.
    assert!(fixture.machine.transitioned.load(Ordering::SeqCst) > 0);
    Ok(())
}

#[test]
fn native_step_identity_denial_precedes_string_growth() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        }, &cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let prefix = work.receipt();
    assert!(matches!(native_step_key(u64::MAX, &work), Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
    assert_eq!(work.receipt().bytes_encoded, 0);
    assert_eq!(work.receipt().authority_records_acknowledged, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}
