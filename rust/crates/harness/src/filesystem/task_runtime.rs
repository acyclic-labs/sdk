//! Optional persistent task composition using the existing runtime authorities.

use super::{
    FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
    FilesystemSchedulerPayloadStore, FilesystemWorkflowJournal,
};
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::VolumeRef,
    core::{AuthorityVerifier, Scope},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    executor::{Executor, StockExecutor, TurnInput, TurnOutput},
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, DurableTaskHost, ResumableTaskSession, RuntimeScope, TaskContext,
        TaskRegistry, TaskRunLimits,
    },
    scheduler::{LeaseFence, SessionLimits},
    tool::ToolRegistry,
    workflow::{MachineRegistry, MachineStatus, WorkflowCommand},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{StreamClient, StreamProvider, SystemUnixMillisClock};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

/// Trusted command adapter. Implementations must dispatch/reconcile each stable
/// command identity through the existing journaled provider APIs. A ready result
/// certifies that no uncertain dispatch remains; pending retains durable wait
/// state rather than a future. This trait does not provide an effects ledger.
pub trait TaskCommandHost: Send + Sync {
    /// Executes or reconciles a retained command under the exact task lease.
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>>;
}

/// Explicit dispatch ownership returned by a trusted command adapter.
pub enum TaskCommandProgress {
    /// Durable result, safe to deliver to the machine.
    Ready(Value),
    /// Durable wait with no active uncertain dispatch.
    Pending,
    /// A dispatch may still be active; retain the existing reservation.
    Indeterminate,
}

/// A bounded worker turn retains no suspended future or second queue.
pub enum TaskWorkerOutcome {
    /// Checkpoint retained and reservation released until an authorized wake.
    Suspended {
        /// Admitted task identity.
        task: TaskId,
        /// Committed checkpoint awaiting a wake.
        revision: u64,
    },
    /// Terminal result published through the existing task host.
    Completed {
        /// Admitted task identity.
        task: TaskId,
    },
    /// Transition allowance exhausted; caller still owns this exact lease.
    Yielded {
        /// Exact retained reservation.
        lease: crate::distributed::WorkLease,
    },
    /// Ordered reconciliation required; no new lease may be pulled for this task.
    Reconciling {
        /// Exact reservation required for ordered reconciliation.
        lease: crate::distributed::WorkLease,
    },
}

/// Composes caller-owned persistent providers without creating another scheduler
/// or registry. Callers create/reopen the private volume before opening this
/// composition. Persistence and atomicity are properties of those providers.
pub struct FilesystemTaskRuntime<P, A, O> {
    host: Arc<CoordinatorTaskHost<P>>,
    harness: Arc<AgentHarness>,
    filesystem: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    tools: ToolRegistry,
    maximum_payload_bytes: u64,
}

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Uses one Stream provider for admission, accounting and both journals.
    /// Root admission always includes immutable session ceilings atomically.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit existing provider and authority boundaries"
    )]
    pub async fn open(
        stream: StreamClient<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        signed: Scope,
        scope: RuntimeScope,
        tasks: TaskRegistry,
        machines: MachineRegistry,
        tools: ToolRegistry,
        session_limits: SessionLimits,
        concurrency: usize,
        maximum_payload_bytes: u64,
    ) -> Result<Self> {
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(),
            volume.clone(),
            &verifier,
            &signed,
            maximum_payload_bytes,
        )?);
        let reader = Arc::new(FilesystemContentVerifier::new(
            filesystem.clone(),
            verifier.clone(),
            signed.clone(),
            maximum_payload_bytes,
        )?);
        let host = Arc::new(
            CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, reader.clone())
                    .await?
                    .with_payload_store(payloads.clone()),
                stream,
                payloads,
                reader,
                verifier.audience().clone(),
                signed.clone(),
                verifier.clone(),
                scope.clone(),
                tasks.clone(),
                machines,
                Arc::new(SystemUnixMillisClock),
            )?
            .with_session_limits(session_limits)?,
        );
        let harness =
            AgentHarness::new(tasks, tools.clone(), scope, concurrency, Some(host.clone()))?;
        Ok(Self {
            host,
            harness,
            filesystem,
            volume,
            verifier,
            signed,
            tools,
            maximum_payload_bytes,
        })
    }

    /// The existing typed admission, recovery and cancellation API.
    #[must_use]
    pub const fn harness(&self) -> &Arc<AgentHarness> {
        &self.harness
    }

    /// The existing authoritative task host, also used by the bound journals.
    #[must_use]
    pub const fn task_host(&self) -> &Arc<CoordinatorTaskHost<P>> {
        &self.host
    }

    /// Opens a workflow journal under the exact retained lease and admission.
    pub async fn workflow_journal(
        &self,
        task: TaskId,
        fence: LeaseFence,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let admission = self.host.observe_admission(task).await?;
        Ok(Arc::new(FilesystemWorkflowJournal::for_task(
            self.host.journal_owner(task, fence).await?,
            self.filesystem.clone(),
            self.volume.clone(),
            self.verifier.clone(),
            self.signed.clone(),
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
        )?))
    }

    /// Reopens the admitted registered task and its bound workflow without
    /// requiring a typed definition at the worker boundary.
    pub async fn open_task(&self, task: TaskId, fence: LeaseFence) -> Result<ResumableTaskSession> {
        let journal = self.workflow_journal(task, fence.clone()).await?;
        self.harness
            .open_registered_task(task, fence, journal)
            .await
    }

    /// Drives an already-claimed lease through the registered machine. The first
    /// trigger is null; wake values and ordered command results are subsequent
    /// machine inputs. Commands use `{"commands":[{"operation_id":...,"value":...}]}`.
    /// Callers choose the invocation allowance; suspended work consumes no slot.
    pub async fn run_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> Result<TaskWorkerOutcome> {
        if maximum_transitions == 0
            || u64::from(maximum_transitions) > crate::workflow::MAX_WORKFLOW_RECORDS
        {
            return Err(Error::Invalid("invalid worker transition allowance".into()));
        }
        let operation_id = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation_id.into_bytes());
        let fence = LeaseFence::from(&lease.reservation);
        let context = self.harness.durable_context(task, operation_id).await?;
        if context.scope().run_limits().deadline_epoch_ms.is_some() {
            return Err(Error::Unsupported(
                "persistent worker deadline runner is not composed".into(),
            ));
        }
        self.host.start_task(&lease).await?;
        let mut session = self.open_task(task, fence.clone()).await?;
        let wake = self.host.workflow_input(task, &fence).await?;
        let mut advanced = 0;
        loop {
            let revision = session.checkpoint().revision;
            let transition = session.latest_transition().await?;
            // Check the allowance before provider work whose result requires a
            // further step. Terminal transitions may still settle at the bound.
            if advanced == maximum_transitions
                && transition.as_ref().is_none_or(|value| {
                    matches!(value.status, MachineStatus::Suspended) && !value.commands.is_empty()
                })
            {
                return Ok(TaskWorkerOutcome::Yielded { lease });
            }
            let mut command_input = None;
            if let Some(transition) = &transition {
                if !transition.commands.is_empty() {
                    match self
                        .dispatch_commands(task, &context, &fence, &transition.commands, commands)
                        .await?
                    {
                        TaskCommandProgress::Ready(value) => command_input = Some(value),
                        TaskCommandProgress::Pending => {
                            self.host.suspend_workflow(task, fence, revision).await?;
                            return Ok(TaskWorkerOutcome::Suspended { task, revision });
                        }
                        TaskCommandProgress::Indeterminate => {
                            self.host
                                .settle_task(task, fence, Outcome::Indeterminate { operation_id })
                                .await?;
                            return Ok(TaskWorkerOutcome::Reconciling { lease });
                        }
                    }
                }
                let outcome = match &transition.status {
                    MachineStatus::Completed { value } => Some(Outcome::Succeeded(value.clone())),
                    MachineStatus::Failed { message } => Some(Outcome::Failed {
                        message: message.clone(),
                    }),
                    MachineStatus::Suspended => None,
                };
                if let Some(outcome) = outcome {
                    self.host.settle_task(task, fence, outcome).await?;
                    return Ok(TaskWorkerOutcome::Completed { task });
                }
            }
            let input = if transition.is_none() {
                Value::Null
            } else if let Some(value) = command_input {
                value
            } else if let Some((wake_revision, value)) = &wake {
                if *wake_revision == revision {
                    value.clone()
                } else {
                    self.host.suspend_workflow(task, fence, revision).await?;
                    return Ok(TaskWorkerOutcome::Suspended { task, revision });
                }
            } else {
                self.host.suspend_workflow(task, fence, revision).await?;
                return Ok(TaskWorkerOutcome::Suspended { task, revision });
            };
            if advanced == maximum_transitions {
                return Ok(TaskWorkerOutcome::Yielded { lease });
            }
            // The checkpoint itself is the durable consumption marker. Recovery
            // retries this identity rather than creating another input ledger.
            let mut digest = blake3::Hasher::new();
            digest.update(b"harness/v2/task-worker-step\0");
            digest.update(&task.into_bytes());
            digest.update(&revision.to_be_bytes());
            let mut bytes = [0; 16];
            bytes.copy_from_slice(&digest.finalize().as_bytes()[..16]);
            session
                .step(
                    OperationId::from_bytes(bytes),
                    IdempotencyKey::new(format!("task-step:{revision}"))?,
                    input,
                )
                .await?;
            advanced += 1;
        }
    }

    async fn dispatch_commands(
        &self,
        task: TaskId,
        context: &TaskContext,
        fence: &LeaseFence,
        outbox: &[WorkflowCommand],
        commands: &dyn TaskCommandHost,
    ) -> Result<TaskCommandProgress> {
        use crate::conversation::ContentResidencyVerifier as _;
        let reader = FilesystemContentVerifier::new(
            self.filesystem.clone(),
            self.verifier.clone(),
            self.signed.clone(),
            self.maximum_payload_bytes
                .min(context.scope().limits().file_bytes),
        )?;
        let mut results = Vec::new();
        for command in outbox {
            self.host.verify_dispatch_owner(task, fence.clone()).await?;
            context.scope().limits().validate_file(&command.payload)?;
            if !crate::runtime::read_granted(context.scope().grants(), &command.payload)? {
                return Err(Error::Unauthorized(
                    "task cannot read command payload".into(),
                ));
            }
            let payload = serde_json::from_slice(&reader.read(&command.payload).await?)
                .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
            let progress = commands
                .execute(context.clone(), fence.clone(), command.clone(), payload)
                .await;
            self.host.verify_dispatch_owner(task, fence.clone()).await?;
            match progress {
                Ok(TaskCommandProgress::Ready(value)) => {
                    results.push(json!({"operation_id":command.operation_id,"value":value}));
                    // Include the machine-input envelope in the admitted bound.
                    if crate::contract::canonical_json_bytes(&json!({"commands":&results}))?.len()
                        as u64
                        > context.scope().limits().file_bytes
                    {
                        return Ok(TaskCommandProgress::Indeterminate);
                    }
                }
                Ok(progress) => return Ok(progress),
                Err(_) => return Ok(TaskCommandProgress::Indeterminate),
            }
        }
        Ok(TaskCommandProgress::Ready(json!({"commands":results})))
    }

    /// Constructs stock execution with mandatory shared accounting, retained
    /// task scope and a journal fenced by that same host. The logical turn is
    /// namespaced by task identity, so another task cannot alias its journal.
    pub async fn stock_execution(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
    ) -> Result<FilesystemTaskExecution<P, A, O>> {
        let admission = self.host.observe_admission(task).await?;
        if !admission.grants.contains("model:generate") {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        if admission.policy.is_some() || admission.execution.is_some() {
            return Err(Error::Unsupported(
                "persistent local stock composition has no policy or routed execution provider"
                    .into(),
            ));
        }
        let task_context = self
            .harness
            .durable_context(task, admission.operation_id)
            .await?;
        let scope = task_context.scope().clone();
        let limits = scope.limits();
        // Deadline enforcement belongs to the existing task runner; until that
        // runner is composed, never silently ignore an admitted deadline.
        let run_limits = scope.run_limits();
        if run_limits.deadline_epoch_ms.is_some() {
            return Err(Error::Unsupported(
                "persistent stock task deadline runner is not composed".into(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness/v2/task-execution\0");
        hasher.update(&task.into_bytes());
        hasher.update(&turn.into_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        let operation_id = OperationId::from_bytes(bytes);
        let journal = FilesystemExecutionJournal::for_task(
            self.host.journal_owner(task, fence.clone()).await?,
            operation_id,
            self.filesystem.clone(),
            self.volume.clone(),
            self.verifier.clone(),
            self.signed.clone(),
            self.maximum_payload_bytes.min(limits.file_bytes),
        )?;
        let executor = StockExecutor::new(model, provider, context, self.tools.clone())
            .with_limits(limits)
            .with_tool_authority(scope, None)?
            .with_durable_task(self.host.clone(), task, fence);
        Ok(FilesystemTaskExecution {
            operation_id,
            executor,
            journal,
            run_limits,
        })
    }
}

/// A stock executor and its mandatory task-owned journal. No caller-selected
/// journal or accounting override is accepted by this execution entry point.
pub struct FilesystemTaskExecution<P, A, O> {
    operation_id: OperationId,
    executor: StockExecutor,
    journal: FilesystemExecutionJournal<P, A, O>,
    run_limits: TaskRunLimits,
}

impl<P, A, O> FilesystemTaskExecution<P, A, O>
where
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Stable task-namespaced turn identity to use in `TurnInput` after reopen.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Executes or reconciles using the bound journal and existing stock loop.
    pub async fn execute(&self, input: TurnInput) -> Result<TurnOutput> {
        if input.operation_id != self.operation_id {
            return Err(Error::Unauthorized(
                "turn belongs to another task execution".into(),
            ));
        }
        if self
            .run_limits
            .max_steps
            .is_some_and(|bound| u64::from(input.max_steps) > bound as u64)
        {
            return Err(Error::Unauthorized(
                "turn exceeds admitted task step ceiling".into(),
            ));
        }
        self.executor.execute(input, &self.journal).await
    }
}
