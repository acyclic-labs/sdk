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
#[cfg(not(target_arch = "wasm32"))]
mod native_mail;

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
    native: AtomicBool,
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
    fn supports_native_execution(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self.native.load(Ordering::SeqCst) { Ok(()) }
        else { Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "control native tool disabled" }) }
    }
    fn execute_with_native_context<'a>(
        &'a self, _: &'a ToolContext, _: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.executed.fetch_add(1, Ordering::SeqCst);
            self.clock.0.store(20, Ordering::SeqCst);
            futures::future::pending().await
        })
    }
    fn reconcile_with_native_context<'a>(
        &'a self, _: &'a ToolContext, _: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.executed.load(Ordering::SeqCst), 1);
            if !self.receipt_ready.load(Ordering::SeqCst) { return futures::future::pending().await; }
            let (value, custody) = crate::tool::value(&"original receipt", work.remaining()?.bytes_encoded, work)?;
            Ok((Some(ToolResult { value }), custody))
        })
    }
}
impl ToolProjection for PendingTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(json!({"kind":"json","value":result.value}))
    }
    fn supports_native_projection(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.supports_native_execution(work)
    }
    fn project_with_native_work(
        &self, _: &ToolInvocation, result: &ToolResult, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        #[derive(serde::Serialize)]
        struct Projection<'a> { kind: &'static str, value: &'a Value }
        crate::tool::value(&Projection { kind: "json", value: &result.value }, work.remaining()?.bytes_encoded, work)
    }
}

struct Machine {
    identity: MachineIdentity,
    schema: Value,
    transitioned: AtomicUsize,
    native: AtomicBool,
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
    fn supports_native_work(&self) -> bool { self.native.load(Ordering::SeqCst) }
    fn initialize_with_native_work(
        &self, input: &Value, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        crate::tool::clone_value(input, None, work)
    }
    fn transition_with_native_work(
        &self, state: &Value, _: Value, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let (state, custody) = crate::tool::clone_value(state, None, work)?;
        io.absorb_native_custody(custody)?;
        let command = {
            let encoded_hold;
            let encoded = io.encode(&state["command"], io.remaining()?.bytes_encoded, false)?;
            encoded_hold = io.guard_owned(encoded.capacity() as u64)?;
            let command = crate::filesystem::retain_decoded(io.decode_preparation::<WorkflowCommand>(&encoded, crate::wire::ErrorCode::Storage)?);
            drop(encoded);
            drop(encoded_hold);
            command
        };
        io.retain_owned(std::mem::size_of::<MachineTransition>().saturating_sub(std::mem::size_of::<Value>()) as u64)?;
        let bytes = std::mem::size_of::<WorkflowCommand>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
        let mut commands = Vec::new();
        commands.try_reserve_exact(1).map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "control command vector allocation failed",
        })?;
        if commands.capacity() > 1 { io.retain_owned((commands.capacity() as u64 - 1) * bytes)?; }
        commands.push(command);
        self.transitioned.fetch_add(1, Ordering::SeqCst);
        let transition = MachineTransition { state, commands, status: MachineStatus::Suspended };
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok((transition, custody))
    }
}

type Runtime = FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;
struct Fixture {
    stream: Arc<MemoryStream>,
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
        let signed = issuer.issue("owner", &Capabilities::new([
            "operation:declare".to_owned(), "operation:observe".to_owned(),
            "operation:cancel".to_owned(), "task:spawn:test.deadline@1".to_owned(),
            "tool:call:test.deadline".to_owned(), "model:generate".to_owned(),
            "mail:send".to_owned(), "mail:read".to_owned(),
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
        let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?
            .with_run_limits(TaskRunLimits { deadline_epoch_ms: Some(20), ..TaskRunLimits::default() }, &mut None)?;
        let machine = Arc::new(Machine {
            identity: MachineIdentity {
                name: "test.deadline".into(), version: "1".into(),
                digest: *blake3::hash(include_str!("tests.rs").as_bytes()).as_bytes(),
            },
            schema: json!({"type":"object"}),
            transitioned: AtomicUsize::new(0),
            native: AtomicBool::new(false),
        });
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::<Value, Value>::resumable(
            machine.clone(), json!({"type":"object"}), json!({}),
        )?)?;
        let mut machines = MachineRegistry::default();
        machines.register(machine.clone(), &mut None)?;
        let clock = Arc::new(Clock(AtomicU64::new(10)));
        let tool = Arc::new(PendingTool {
            clock: clock.clone(), executed: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
            receipt_ready: AtomicBool::new(false),
            native: AtomicBool::new(false),
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
        Ok(Self { stream: Arc::new(MemoryStream::default()), filesystem,
            volume, issuer, signed, scope, tasks, machines, tools, clock, tool, machine })
    }
    async fn runtime(&self) -> Result<Runtime> {
        FilesystemTaskRuntime::open(
            self.stream.clone(), self.filesystem.clone(), &self.volume, &self.issuer.verifier(&mut None).expect("verifier copy"),
            &self.signed, &self.scope, &self.tasks, &self.machines,
            &self.tools, SessionLimits { active_tasks: 1, total_tasks: 1, depth: 1, model_steps: 1 },
            1, 65_536, self.clock.clone(), None, &[], &ResumableToolRegistry::default(), None,
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
        self.admit_only(runtime, kind, command).await?;
        match runtime.task_host().pull_work(&Worker {
            id: "deadline-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
        }).await? {
            WorkPull::Claimed(lease) => Ok(lease),
            _ => Err(Error::NotFound("deadline lease".into())),
        }
    }

    async fn admit_only(&self, runtime: &Runtime, kind: &str, command: Value) -> Result<OperationId> {
        let operation = OperationId::from_bytes([104; 16]);
        let payloads = FilesystemSchedulerPayloadStore::new(self.filesystem.clone(), &(self.volume), &self.issuer.verifier(&mut None).expect("verifier copy"), &self.signed, 65_536, None)?;
        let payload = payloads.stage(operation, "command", &crate::contract::canonical_json_bytes(&command)?).await?;
        let definition = self.tasks.get_version::<Value, Value>("test.deadline", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
        let input = json!({"command":WorkflowCommand {
            operation_id: OperationId::from_bytes([105; 16]),
            kind: kind.into(),
            payload,
        }});
        assert!(matches!(
            runtime.harness().admit(operation, &definition, input, None).await?,
            Admission::Accepted(_)
        ));
        Ok(operation)
    }

    async fn native_lease(
        &self, runtime: &Runtime, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkLease, acyclic_fs::NativeAllocation)> {
        self.native_lease_command(runtime, super::super::TOOL_TASK_COMMAND_KIND, serde_json::to_value(
            &super::super::ToolTaskCommand { name: "test.deadline".into(), revision: "1".into(), arguments: Value::Null },
        )?, work).await
    }

    async fn native_lease_command(
        &self, runtime: &Runtime, kind: &str, command: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkLease, acyclic_fs::NativeAllocation)> {
        let operation = self.admit_only(runtime, kind, command).await?;
        let pulled = runtime.task_host().pull_operation_with_native_work(&Worker {
            id: "deadline-worker".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
        }, operation, work).await?;
        match pulled {
            (WorkPull::Claimed(lease), custody) => Ok((lease, custody)),
            (other, custody) => {
                drop(other);
                drop(custody);
                Err(Error::NotFound("native deadline lease".into()))
            }
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
    match runtime.resume_task(lease.clone(),&commands,2, None).await {
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
    let attempt = reopened.resume_task(lease.clone(), &reopened.commands(), 2, None).await;
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
    let attempt = runtime.resume_task(lease.clone(), &runtime.commands(), 2, None).await;
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
    fn reconcile<'a>(&'a self, _dispatch: crate::model::ModelDispatch, _observed: &'a [crate::model::ModelEvent]) -> BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        Box::pin(async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.generated.load(Ordering::SeqCst), 1);
            Ok(Some(vec![
                crate::model::ModelEvent::Content { delta: "original model receipt".into() },
                crate::model::ModelEvent::Completed { metadata: Value::Null },
            ]))
        })
    }
    fn supports_native_model(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<bool> { Ok(true) }
    fn generate_with_native_work<'a>(
        &'a self, _: &'a crate::model::PreparedModelRequest, _: crate::model::ModelDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, crate::model::NativeModelStream<'a>>> {
        crate::NativeFutureBundle::new(work, || async move {
            let stream = crate::model::NativeModelStream::new(work, || {
                futures::stream::pending::<Result<(crate::model::ModelEvent, acyclic_fs::NativeAllocation)>>()
            })?;
            self.generated.fetch_add(1, Ordering::SeqCst);
            self.clock.0.store(20, Ordering::SeqCst);
            Ok(stream)
        })
    }
    fn reconcile_with_native_work<'a>(
        &'a self, _: crate::model::ModelDispatch, _: &'a [crate::model::ModelEvent], work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Vec<crate::model::ModelEvent>>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.generated.load(Ordering::SeqCst), 1);
            let io = super::super::PreparationIo::from_native_work(work)?;
            let (delta, custody) = work.display(&"original model receipt")?;
            io.absorb_native_custody(custody)?;
            let bytes = std::mem::size_of::<crate::model::ModelEvent>() as u64;
            io.retain_owned(2 * bytes + std::mem::size_of::<Vec<crate::model::ModelEvent>>() as u64)?;
            io.allocation_attempt(2 * bytes)?;
            let mut events = Vec::new();
            events.try_reserve_exact(2).map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "native model control event allocation failed",
            })?;
            if events.capacity() > 2 { io.retain_owned((events.capacity() as u64 - 2) * bytes)?; }
            events.push(crate::model::ModelEvent::Content { delta });
            events.push(crate::model::ModelEvent::Completed { metadata: Value::Null });
            let custody = io.take_native_custody(io.owned_bytes()?)?;
            Ok((Some(events), custody))
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
        runtime.task_host().start_task(&lease, None).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let turn = OperationId::from_bytes([106; 16]);
        let model = Model::new("synthetic", "deadline", "1", Value::Null)?;
        let provider = Arc::new(PendingModel {
            clock: fixture.clock.clone(),
            generated: AtomicUsize::new(0),
            reconciled: AtomicUsize::new(0),
        });
        let execution = runtime.stock_execution(
            task, fence.clone(), turn, model.clone(), provider.clone(), ContextPipeline::default(), None,
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
            task, fence.clone(), turn, model, provider.clone(), ContextPipeline::default(), None,
        ).await?.with_compaction_policy(crate::context::CompactionPolicy::Disabled);
        if expired_before_dispatch {
            assert!(matches!(execution.reconcile(input, 1_000).await, Err(Error::Indeterminate(_))));
            assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
        } else {
            assert!(execution.reconcile(input, 1_000).await?.is_some());
            assert_eq!(provider.reconciled.load(Ordering::SeqCst), 1);
            assert!(reopened.execution_journal(task, fence, turn).await?.is_quiescent(None).await?);
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
    let payloads = FilesystemSchedulerPayloadStore::new(fixture.filesystem.clone(), &(fixture.volume), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.signed, 65_536, None)?;
    let file = payloads.stage(OperationId::from_bytes([106; 16]), "mail-body", b"7").await?;
    let lease = fixture.admit_command(
        &runtime, super::super::MAIL_SEND_TASK_COMMAND_KIND,
        serde_json::to_value(&super::super::MailSendTaskCommand { recipient:task, payload:file })
            .map_err(|error| Error::Invalid(error.to_string()))?,
    ).await?;
    assert!(matches!(
        runtime.resume_task(lease.clone(), &runtime.commands(), 1, None).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })
    ));
    assert_eq!(runtime.task_host().inbox(task, 0, 8).await?.len(), 1);
    drop(runtime);
    fixture.clock.0.store(20, Ordering::SeqCst);
    let reopened = fixture.runtime().await?;
    assert!(matches!(
        reopened.resume_task(lease.clone(), &reopened.commands(), 1, None).await,
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

#[path = "mail_tests.rs"]
mod mail_tests;

#[tokio::test]
async fn native_machine_without_callback_rejects_before_owner_deadline_or_provider_work() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let lease = fixture.admit(&runtime).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    runtime.task_host().start_task(&lease, None).await?;
    let mut session = runtime.open_task(task, fence).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        }, &cancellation,
    ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let custody = work.retain_owned(0).map_err(Error::from)?;
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
    fixture.clock.0.store(10, Ordering::SeqCst);
    let original_state = session.checkpoint().state.clone();
    session.step(
        OperationId::from_bytes([119; 16]),
        IdempotencyKey::new("ordinary-custom-machine")?,
        Value::Null,
    ).await?;
    assert_eq!(session.checkpoint().revision, 1);
    assert_eq!(session.checkpoint().state, original_state);
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
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn original_native_worker_precancel_and_metadata_denial_preserve_exact_lease() -> Result<()> {
    for precancel in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget { allocation_operations: 4096, ..Default::default() };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (lease, recovery_custody) = fixture.native_lease(&runtime, &work).await?;
        let original = lease.clone();
        let recovered_bytes = recovery_custody.bytes();
        if !precancel {
            while work.receipt().allocation_operations < budget.allocation_operations {
                let text = work.display(&"allocation denial control")?;
                drop(text);
            }
        }
        let before = work.receipt();
        if precancel { cancellation.cancel(); }
        let commands = runtime.commands();
        let result = runtime.resume_task_with_native_work(lease, recovery_custody, &commands, 4, &work).await;
        match result {
            Ok((TaskWorkerAttempt::Unresolved { lease, error }, custody)) => {
                assert_eq!(lease, original);
                assert!(matches!(error, Error::NativeWork { .. }));
                assert!(custody.work().shares_ledger(&work));
                assert!(custody.bytes() >= recovered_bytes);
                drop(lease);
                drop(error);
                drop(custody);
            }
            _ => return Err(Error::Invalid("native worker denial lost original attempt".into())),
        }
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
        assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
        let state = runtime.task_host().observe_task(TaskId::from_bytes(original.operation.operation_id.into_bytes())).await?;
        assert_eq!(state.reservation, Some(original.reservation));
        assert!(!state.cancellation_requested);
        assert!(state.outcome.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_builtin_precancel_preserves_original_recovery_before_dispatch() -> Result<()> {
    for kind in [
        super::super::MAIL_SEND_TASK_COMMAND_KIND,
        super::super::MAIL_RECEIVE_TASK_COMMAND_KIND,
        super::super::FORK_PREPARE_TASK_COMMAND_KIND,
        super::super::TASK_ADMIT_PREPARED_COMMAND_KIND,
        super::super::TASK_CANCEL_COMMAND_KIND,
        super::super::EFFECT_TASK_COMMAND_KIND,
    ] {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        let runtime = fixture.runtime().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (lease, custody) = fixture.native_lease_command(&runtime, kind, Value::Null, &work).await?;
        let original = lease.clone();
        let bytes = custody.bytes();
        cancellation.cancel();
        let before = work.receipt();
        let (attempt, custody) = runtime.resume_task_with_native_work(
            lease, custody, &runtime.commands(), 2, &work,
        ).await?;
        match attempt {
            TaskWorkerAttempt::Unresolved { lease, error } => {
                assert_eq!(lease, original);
                assert!(matches!(error, Error::NativeWork { .. }));
                assert!(error.is_native_failure());
                drop(lease);
                drop(error);
            }
            _ => return Err(Error::Invalid("stock builtin replaced original precancelled recovery".into())),
        }
        assert!(custody.matches_work(&work));
        assert!(custody.bytes() >= bytes);
        assert_eq!(work.receipt(), before);
        assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        drop(custody);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_tool_lost_ack_reconciles_original_fence_without_redispatch() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    fixture.tool.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (lease, custody) = fixture.native_lease(&runtime, &work).await?;
    let original = lease.clone();
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let (attempt, custody) = runtime.resume_task_with_native_work(lease, custody, &runtime.commands(), 2, &work).await?;
    let lease = match attempt {
        TaskWorkerAttempt::Unresolved { lease, error: Error::Indeterminate(operation) } => {
            assert_eq!(operation, original.operation.operation_id);
            assert_eq!(lease, original);
            lease
        }
        _ => return Err(Error::Invalid("native lost acknowledgement did not preserve original attempt".into())),
    };
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(custody.work().shares_ledger(&work));
    let before = work.receipt();
    drop(runtime);
    let reopened = fixture.runtime().await?;
    let (attempt, custody) = reopened.reconcile_task_with_native_work(lease, custody, &reopened.commands(), 1, 5, &work).await?;
    let lease = match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => {
            assert_eq!(lease, original);
            lease
        }
        _ => return Err(Error::Invalid("native pending receipt replaced original attempt".into())),
    };
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    let state = reopened.task_host().observe_task(task).await?;
    assert!(state.outcome.is_none());
    assert_eq!(state.reservation, Some(original.reservation.clone()));
    fixture.tool.receipt_ready.store(true, Ordering::SeqCst);
    let (attempt, custody) = reopened.reconcile_task_with_native_work(lease, custody, &reopened.commands(), 1, 1_000, &work).await?;
    assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task));
    assert!(custody.work().shares_ledger(&work));
    assert!(work.receipt().backend_read_operations > before.backend_read_operations);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(matches!(reopened.task_host().outcome(task).await?, Some(Outcome::Cancelled)));
    assert!(reopened.task_host().observe_task(task).await?.reservation.is_none());
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_stock_worker_stale_fence_and_expired_admission_precede_provider_work() -> Result<()> {
    for stale in [true, false] {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
        let runtime = fixture.runtime().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (mut lease, custody) = fixture.native_lease(&runtime, &work).await?;
        if stale { lease.reservation.id.replace_range(0..1, "x"); }
        else { fixture.clock.0.store(20, Ordering::SeqCst); }
        let original = lease.clone();
        let (attempt, custody) = runtime.resume_task_with_native_work(lease, custody, &runtime.commands(), 2, &work).await?;
        match attempt {
            TaskWorkerAttempt::Unresolved { lease, error } => {
                assert_eq!(lease, original);
                if stale { assert!(error.is_native_failure()); }
                else { assert!(matches!(error, Error::Indeterminate(_))); }
                drop(lease);
                drop(error);
            }
            _ => return Err(Error::Invalid("native stale or expired work escaped original attempt".into())),
        }
        assert!(custody.work().shares_ledger(&work));
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
        drop(custody);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_model_original_lost_ack_uses_native_provider_and_settlement() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock), generated: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
    });
    let model = Model::new("synthetic", "deadline", "1", Value::Null)?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let input = super::super::ModelTaskCommand {
        input: crate::model::ModelContent::Text("question".into()),
        selected_context: None, max_steps: 1, max_output_tokens: None,
        selected_output_volume: None,
        mailbox_operation: None,
    };
    let (lease, custody) = fixture.native_lease_command(
        &runtime, super::super::MODEL_TASK_COMMAND_KIND, serde_json::to_value(input)?, &work,
    ).await?;
    let original = lease.clone();
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let commands = runtime.commands().with_model(&model, provider.clone(), &context, &compaction, Some(&super::super::PreparationIo::from_native_work(&work)?))?;
    let (attempt, custody) = runtime.resume_task_with_native_work(lease, custody, &commands, 2, &work).await?;
    let lease = match attempt {
        TaskWorkerAttempt::Unresolved { lease, error: Error::Indeterminate(_) } => {
            assert_eq!(lease, original);
            lease
        }
        _ => return Err(Error::Invalid("native model lost acknowledgement did not preserve original lease".into())),
    };
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    let before = work.receipt();
    drop(commands);
    drop(runtime);
    let reopened = fixture.runtime().await?;
    let commands = reopened.commands().with_model(&model, provider.clone(), &context, &compaction, Some(&super::super::PreparationIo::from_native_work(&work)?))?;
    let (attempt, custody) = reopened.reconcile_task_with_native_work(lease, custody, &commands, 1, 1_000, &work).await?;
    assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed }) if completed == task));
    assert_eq!(provider.generated.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 1);
    assert!(work.receipt().backend_read_operations > before.backend_read_operations);
    assert!(custody.work().shares_ledger(&work));
    assert!(matches!(reopened.task_host().outcome(task).await?, Some(Outcome::Cancelled)));
    let witness = reopened.readonly_workflow_journal_with_native_work(task, &work).await?;
    let records = crate::workflow::WorkflowJournal::replay_with_native_work(
        witness.as_ref(), 0, 64, &work,
    )?.await?;
    assert!(!records.0.is_empty());
    assert!(records.1.work().shares_ledger(&work));
    drop(records);
    drop(witness);
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_model_selected_writer_requires_original_created_binding_before_provider() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let input = super::super::ModelTaskCommand {
        input: crate::model::ModelContent::Text("must not dispatch".into()),
        selected_context: None, max_steps: 1, max_output_tokens: None,
        selected_output_volume: Some(fixture.volume.clone()),
        mailbox_operation: None,
    };
    let payload = serde_json::to_value(input)?;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock), generated: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
    });
    let model = Model::new("synthetic", "deadline", "1", Value::Null)?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let commands = runtime.commands().with_model(&model, provider.clone(), &context, &compaction, Some(&super::super::PreparationIo::from_native_work(&work)?))?;
    let (lease, custody) = fixture.native_lease_command(
        &runtime, super::super::MODEL_TASK_COMMAND_KIND, payload, &work,
    ).await?;
    let (attempt, custody) = runtime.resume_task_with_native_work(lease, custody, &commands, 2, &work).await?;
    assert!(matches!(&attempt, TaskWorkerAttempt::Unresolved {
        error: Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. }, ..
    }));
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    assert!(custody.work().shares_ledger(&work));
    drop(attempt);
    drop(custody);
    Ok(())
}

#[tokio::test]
async fn native_stock_model_configuration_denies_before_copy_or_provider_on_original_work() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let model = Model::new("synthetic", "original-config", "1", serde_json::json!({"configured": true}))?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock), generated: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
    });
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::WorkBudget::default();
        if !cancelled { budget.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes(); }
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        if cancelled { cancellation.cancel(); }
        let before = work.receipt();
        let result = runtime.commands().with_model(&model, provider.clone(), &context, &compaction, Some(&super::super::PreparationIo::from_native_work(&work)?));
        assert!(result.as_ref().is_err_and(Error::is_native_failure));
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
        assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
        drop(result);
    }
    Ok(())
}

#[tokio::test]
async fn native_stock_model_configuration_retains_original_custody_and_rejects_foreign_rebinding() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let model = Model::new("synthetic", "original-config", "1", json!({"configured": "retained"}))?;
    let context = ContextPipeline::default();
    let compaction = crate::context::CompactionPolicy::Disabled;
    let provider = Arc::new(PendingModel {
        clock: Arc::clone(&fixture.clock), generated: AtomicUsize::new(0), reconciled: AtomicUsize::new(0),
    });
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = work.receipt();
    let commands = runtime.commands().with_model(&model, provider.clone(), &context, &compaction, Some(&super::super::PreparationIo::from_native_work(&work)?))?;
    assert!(work.receipt().allocation_operations > before.allocation_operations);
    assert!(work.receipt().bytes_copied > before.bytes_copied);
    let foreign = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = foreign.receipt();
    let result = commands.with_model(&model, provider.clone(), &context, &compaction, Some(&super::super::PreparationIo::from_native_work(&foreign)?));
    assert!(matches!(result, Err(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unauthorized, ..
    })));
    assert_eq!(foreign.receipt(), before);
    assert_eq!(provider.generated.load(Ordering::SeqCst), 0);
    assert_eq!(provider.reconciled.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn genuine_stock_native_context_preserves_mcp_current_and_settlement_source() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (lease, recovery_custody) = fixture.native_lease(&runtime, &work).await?;
    runtime.task_host().start_task(&lease, Some(&work)).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let shared = runtime.task_context_with_native_work(task, &fence, &work).await?;
    assert!(shared.context().original_task_binding_matches(task, &fence));
    let operation = lease.operation.operation_id;
    let call_id = work.display(&operation)?;
    let tool = ToolContext::new_with_native_work(Arc::clone(&shared), operation, &call_id.0, &work)?;
    drop(call_id);
    let before = work.receipt();
    tool.validate_native_mcp_context(&work)?;
    tool.validate_native_mcp_reconciliation_context(&work)?;
    assert_eq!(work.receipt(), before);
    let read = runtime.harness().durable_context_with_native_work(task, operation, &work).await?;
    let read = read.0.into_native_shared(read.1, &work)?;
    let read_tool = ToolContext::new_with_native_work(read, operation, "read-context-control", &work)?;
    assert!(matches!(read_tool.validate_native_mcp_context(&work), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    assert!(matches!(read_tool.validate_native_mcp_reconciliation_context(&work), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    fixture.clock.0.store(20, Ordering::SeqCst);
    runtime.task_host().expire_task_deadline_with_native_work(task, Some(fence.clone()), &work).await?;
    let settlement = runtime.task_context_for_original_owner_with_native_work(task, &fence, true, &work).await?;
    let settlement_tool = ToolContext::new_with_native_work(settlement, operation, "settlement-context-control", &work)?;
    assert!(matches!(settlement_tool.validate_native_mcp_context(&work), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
    settlement_tool.validate_native_mcp_reconciliation_context(&work)?;
    assert_eq!(fixture.machine.transitioned.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
    drop(settlement_tool);
    drop(read_tool);
    drop(tool);
    drop(shared);
    drop(lease);
    drop(recovery_custody);
    Ok(())
}
