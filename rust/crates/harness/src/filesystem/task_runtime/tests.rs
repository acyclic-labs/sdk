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
