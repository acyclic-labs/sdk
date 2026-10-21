//! Native controls exercise the ordinary admitted runtime and real journals.

use super::*;
use crate::filesystem::PreparationIo;
use crate::runtime::TaskStateProvider;
use crate::{
    Admission, AgentId, Capabilities,
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{SchedulerPayloadStore, WorkLease, WorkPull, Worker},
    resources::ProviderRef,
    runtime::ToolContext,
    scheduler::ResourceRequest,
    tool::{Tool, ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
    workflow::{MachineIdentity, MachineTransition, ResumableMachine},
};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::MemoryStream;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

mod canonical_compaction;
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
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let denied = || Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "pending tool requires its original admitted call",
        };
        let context = context.ok_or_else(denied)?;
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || invocation.name != "test.deadline"
            || !context
                .effective_scope()
                .grants()
                .contains("tool:call:test.deadline")
        {
            return Err(denied());
        }
        if let Some(work) = work {
            self.supports_native_execution(work)?;
            context.task().require_original_native_work(work)?;
        }
        Ok(())
    }

    fn resumable_machine_identity(
        &self,
        work: crate::NativeWork<'_>,
    ) -> Result<Option<&MachineIdentity>> {
        if let Some(work) = work {
            self.supports_native_execution(work)?;
        }
        Ok(None)
    }

    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        if context.is_none() {
            return Box::pin(async { Err(Error::Unauthorized("task context required".into())) });
        }
        Box::pin(async move {
            self.executed.fetch_add(1, Ordering::SeqCst);
            self.clock.0.store(20, Ordering::SeqCst);
            futures::future::pending().await
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        if context.is_none() {
            return Box::pin(async { Err(Error::Unauthorized("task context required".into())) });
        }
        Box::pin(async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.executed.load(Ordering::SeqCst), 1);
            if !self.receipt_ready.load(Ordering::SeqCst) {
                return futures::future::pending().await;
            }
            Ok(Some(ToolResult {
                value: json!("original receipt"),
            }))
        })
    }
    fn supports_native_execution(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self.native.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "control native tool disabled",
            })
        }
    }
    fn execute_with_native_context<'a>(
        &'a self,
        _: &'a ToolContext,
        _: &'a ToolInvocation,
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
        &'a self,
        _: &'a ToolContext,
        _: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        self.supports_native_execution(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.executed.load(Ordering::SeqCst), 1);
            if !self.receipt_ready.load(Ordering::SeqCst) {
                return futures::future::pending().await;
            }
            let (value, custody) = crate::tool::value(
                &"original receipt",
                work.remaining()?.bytes_encoded,
                Some(work),
            )?;
            Ok((
                Some(ToolResult { value }),
                custody.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native tool result has no custody",
                })?,
            ))
        })
    }
}
impl ToolProjection for PendingTool {
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        if let Some(work) = work {
            #[derive(serde::Serialize)]
            struct Projection<'a> {
                kind: &'static str,
                value: &'a Value,
            }
            return crate::tool::value(
                &Projection {
                    kind: "json",
                    value: &result.value,
                },
                work.remaining()?.bytes_encoded,
                Some(work),
            );
        }
        Ok((json!({"kind":"json","value":result.value}), None))
    }
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            self.supports_native_execution(work)?;
        }
        Ok(())
    }
}

struct Machine {
    identity: MachineIdentity,
    schema: Value,
    transitioned: AtomicUsize,
    admitted: std::sync::Mutex<std::collections::BTreeSet<OperationId>>,
    native: AtomicBool,
}
impl Machine {
    fn admit(&self, command: &WorkflowCommand) {
        if self
            .admitted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(command.operation_id)
        {
            self.transitioned.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl ResumableMachine for Machine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        let command: WorkflowCommand = serde_json::from_value(state["command"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        self.admit(&command);
        Ok(MachineTransition {
            state: state.clone(),
            commands: vec![command],
            status: MachineStatus::Suspended,
        })
    }
    fn supports_native_work(&self) -> bool {
        self.native.load(Ordering::SeqCst)
    }
    fn identity_with_native_work<'a>(
        &'a self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        if !self.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "control native machine disabled",
            });
        }
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        Ok(&self.identity)
    }
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        crate::tool::clone_value(input, None, work)
    }
    fn transition_with_native_work(
        &self,
        state: &Value,
        _: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let (state, custody) = crate::tool::clone_value(state, None, work)?;
        io.absorb_native_custody(custody)?;
        let command = {
            let encoded = io.encode(&state["command"], io.remaining()?.bytes_encoded, false)?;
            let command = crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<WorkflowCommand>,
                &encoded,
                crate::wire::ErrorCode::Storage,
            )?)?;
            drop(encoded);
            command
        };
        io.retain_owned(
            std::mem::size_of::<MachineTransition>().saturating_sub(std::mem::size_of::<Value>())
                as u64,
        )?;
        let bytes = std::mem::size_of::<WorkflowCommand>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
        let mut commands = Vec::new();
        commands
            .try_reserve_exact(1)
            .map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "control command vector allocation failed",
            })?;
        if commands.capacity() > 1 {
            io.retain_owned((commands.capacity() as u64 - 1) * bytes)?;
        }
        self.admit(&command);
        commands.push(command);
        let transition = MachineTransition {
            state,
            commands,
            status: MachineStatus::Suspended,
        };
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
        let provider = ProviderRef::new(
            "deadline-control".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let filesystem = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([101; 16]);
        let volume = VolumeRef::new(
            provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        filesystem.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "deadline-control",
            [102; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "deadline-owner".into(),
            },
        );
        let signed = issuer.issue(
            "owner",
            &Capabilities::new([
                "operation:declare".to_owned(),
                "operation:observe".to_owned(),
                "operation:cancel".to_owned(),
                "task:spawn:test.deadline@1".to_owned(),
                "tool:call:test.deadline".to_owned(),
                "model:generate".to_owned(),
                "mail:send".to_owned(),
                "mail:read".to_owned(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )?;
        let scope = RuntimeScope::new(signed.capabilities().clone(), Limits::default())?
            .with_run_limits(
                TaskRunLimits {
                    deadline_epoch_ms: Some(20),
                    ..TaskRunLimits::default()
                },
                &mut None,
            )?;
        let machine = Arc::new(Machine {
            identity: MachineIdentity {
                name: "test.deadline".into(),
                version: "1".into(),
                digest: *blake3::hash(include_str!("tests.rs").as_bytes()).as_bytes(),
            },
            schema: json!({"type":"object"}),
            transitioned: AtomicUsize::new(0),
            admitted: std::sync::Mutex::default(),
            native: AtomicBool::new(true),
        });
        let metadata_work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let metadata_io = PreparationIo::from_native_work(&metadata_work)?;
        let (_, _, tasks, machines) = crate::durable_host::tests::created_fixture::test_registries(
            machine.clone(),
            json!({"type":"object"}),
            json!({"type":"string"}),
            Some(&metadata_io),
        )?;
        machine.native.store(false, Ordering::SeqCst);
        let clock = Arc::new(Clock(AtomicU64::new(10)));
        let tool = Arc::new(PendingTool {
            clock: clock.clone(),
            executed: AtomicUsize::new(0),
            reconciled: AtomicUsize::new(0),
            receipt_ready: AtomicBool::new(false),
            native: AtomicBool::new(false),
        });
        let mut tools = ToolRegistry::default();
        tools.register(
            (
                Tool {
                    hook_names: &[],
                    definition: ToolDefinition {
                        name: "test.deadline".into(),
                        revision: "1".into(),
                        description: "Original receipt".into(),
                        input_schema: json!({"type":"null"}),
                        output_schema: json!({"type":"string"}),
                        projection_schema: crate::tool::json_projection_schema(
                            json!({"type":"string"}),
                        ),
                    },
                    executor: tool.clone(),
                    projection: tool.clone(),
                },
                None,
            ),
            &mut None,
            None,
        )?;
        Ok(Self {
            stream: Arc::new(MemoryStream::default()),
            filesystem,
            volume,
            issuer,
            signed,
            scope,
            tasks,
            machines,
            tools,
            clock,
            tool,
            machine,
        })
    }
    async fn scheduler_state(&self, task: TaskId) -> Result<crate::scheduler::OperationState> {
        let reader = crate::filesystem::FilesystemContentVerifier::new(
            self.filesystem.clone(),
            &self.issuer.verifier(&mut None)?,
            &self.signed,
            self.scope.limits().file_bytes,
            None,
        )?;
        let coordinator =
            crate::distributed::DistributedCoordinator::open(&self.stream, reader, None, &mut None)
                .await?;
        coordinator
            .scheduler()
            .operation(OperationId::from_bytes(task.into_bytes()))
            .cloned()
            .ok_or_else(|| Error::NotFound("fixture task".into()))
    }
    async fn runtime(&self, io: Option<&PreparationIo>) -> Result<Runtime> {
        FilesystemTaskRuntime::open(
            self.stream.clone(),
            self.filesystem.clone(),
            &self.volume,
            None,
            &self.issuer.verifier(&mut None)?,
            &self.signed,
            &self.scope,
            &self.tasks,
            &self.machines,
            &self.tools,
            SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 1,
                model_steps: 1,
            },
            1,
            65_536,
            self.clock.clone(),
            None,
            &[],
            &ResumableToolRegistry::default(),
            io,
        )
        .await
    }
    async fn admit(&self, runtime: &Runtime) -> Result<WorkLease> {
        self.admit_command(
            runtime,
            super::super::TOOL_TASK_COMMAND_KIND,
            serde_json::to_value(&super::super::ToolTaskCommand {
                name: "test.deadline".into(),
                revision: "1".into(),
                arguments: Value::Null,
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .await
    }
    async fn admit_command(
        &self,
        runtime: &Runtime,
        kind: &str,
        command: Value,
    ) -> Result<WorkLease> {
        self.admit_only(runtime, kind, command).await?;
        match runtime
            .task_host()
            .pull_work(
                &Worker {
                    id: "deadline-worker".into(),
                    available: ResourceRequest::default(),
                    labels: BTreeMap::new(),
                },
                None,
                &mut None,
            )
            .await?
        {
            WorkPull::Claimed(lease) => Ok(lease),
            _ => Err(Error::NotFound("deadline lease".into())),
        }
    }

    async fn admit_only(
        &self,
        runtime: &Runtime,
        kind: &str,
        command: Value,
    ) -> Result<OperationId> {
        let operation = OperationId::from_bytes([104; 16]);
        let payloads = FilesystemContentPublisher::new(
            self.filesystem.clone(),
            &(self.volume),
            &self.issuer.verifier(&mut None)?,
            &self.signed,
            65_536,
            None,
        )?;
        let payload = payloads
            .stage(
                operation,
                "command",
                &crate::contract::canonical_json_bytes(&command)?,
                None,
            )?
            .await?;
        let definition = self.tasks.get_version::<Value, Value>(
            "test.deadline",
            "1",
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            None,
        )?;
        let input = json!({"command":WorkflowCommand {
            operation_id: OperationId::from_bytes([105; 16]),
            kind: kind.into(),
            payload,
        }});
        assert!(matches!(
            runtime
                .harness()
                .admit(operation, &definition, input, None)
                .await?,
            Admission::Accepted(_)
        ));
        Ok(operation)
    }

    async fn native_lease(
        &self,
        runtime: &Runtime,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkLease, acyclic_fs::NativeAllocation)> {
        self.native_lease_command(
            runtime,
            super::super::TOOL_TASK_COMMAND_KIND,
            serde_json::to_value(&super::super::ToolTaskCommand {
                name: "test.deadline".into(),
                revision: "1".into(),
                arguments: Value::Null,
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
            work,
        )
        .await
    }

    async fn native_lease_command(
        &self,
        runtime: &Runtime,
        kind: &str,
        command: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkLease, acyclic_fs::NativeAllocation)> {
        let operation = self.admit_only(runtime, kind, command).await?;
        let pulled = {
            let mut operation_io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            );
            crate::store::HistoryIo::retain_owned(
                &mut operation_io,
                std::mem::size_of::<crate::distributed::WorkPull>()
                    .max(std::mem::size_of::<crate::filesystem::TaskWorkerAttempt>())
                    as u64,
            )?;
            let output = runtime
                .task_host()
                .pull_work(
                    &Worker {
                        id: "deadline-worker".into(),
                        available: ResourceRequest::default(),
                        labels: BTreeMap::new(),
                    },
                    Some(operation),
                    &mut Some(&mut operation_io),
                )
                .await?;
            (output, operation_io.into_custody()?)
        };
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
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            assert_eq!(command.operation_id, OperationId::from_bytes([105; 16]));
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.clock.0.store(20, Ordering::SeqCst);
            match self.work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            }) {
                Err(cause) => Err(Error::NativeWork {
                    cause,
                    custody: None,
                }),
                Ok(()) => Err(Error::Invalid(
                    "native negative control unexpectedly admitted".into(),
                )),
            }
        })
    }
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
        _dispatch: crate::model::ModelDispatch,
        _observed: &'a [crate::model::ModelEvent],
    ) -> BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        Box::pin(async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.generated.load(Ordering::SeqCst), 1);
            Ok(Some(vec![
                crate::model::ModelEvent::Content {
                    delta: "original model receipt".into(),
                },
                crate::model::ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]))
        })
    }
    fn supports_native_model(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        Ok(true)
    }
    fn generate_with_native_work<'a>(
        &'a self,
        _: &'a crate::model::PreparedModelRequest,
        _: crate::model::ModelDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, crate::model::NativeModelStream<'a>>> {
        crate::NativeFutureBundle::new(work, || async move {
            let stream = crate::model::NativeModelStream::new(work, || {
                futures::stream::pending::<
                    Result<(crate::model::ModelEvent, acyclic_fs::NativeAllocation)>,
                >()
            })?;
            self.generated.fetch_add(1, Ordering::SeqCst);
            self.clock.0.store(20, Ordering::SeqCst);
            Ok(stream)
        })
    }
    fn reconcile_with_native_work<'a>(
        &'a self,
        _: crate::model::ModelDispatch,
        _: &'a [crate::model::ModelEvent],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                Option<Vec<crate::model::ModelEvent>>,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            self.reconciled.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.generated.load(Ordering::SeqCst), 1);
            let io = super::super::PreparationIo::from_native_work(work)?;
            let (delta, custody) = work.display(&"original model receipt")?;
            io.absorb_native_custody(custody)?;
            let bytes = std::mem::size_of::<crate::model::ModelEvent>() as u64;
            io.retain_owned(
                2 * bytes + std::mem::size_of::<Vec<crate::model::ModelEvent>>() as u64,
            )?;
            io.allocation_attempt(2 * bytes)?;
            let mut events = Vec::new();
            events
                .try_reserve_exact(2)
                .map_err(|_| Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "native model control event allocation failed",
                })?;
            if events.capacity() > 2 {
                io.retain_owned((events.capacity() as u64 - 2) * bytes)?;
            }
            events.push(crate::model::ModelEvent::Content { delta });
            events.push(crate::model::ModelEvent::Completed {
                metadata: Value::Null,
            });
            let custody = io.take_native_custody(io.owned_bytes()?)?;
            Ok((Some(events), custody))
        })
    }
}

#[path = "mail_tests.rs"]
mod mail_tests;

mod admission;
mod future_sizes;
mod manifest;
mod model;
mod settlement;
mod worker;

#[cfg(all(windows, feature = "native-execution"))]
mod native_mount;
