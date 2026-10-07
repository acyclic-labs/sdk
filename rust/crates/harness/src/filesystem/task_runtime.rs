//! Optional persistent task composition using the existing runtime authorities.

use super::{
    FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
    FilesystemSchedulerPayloadStore, FilesystemWorkflowJournal,
};
use crate::{
    Error, OperationId, Result, TaskId,
    context::ContextPipeline,
    conversation::VolumeRef,
    core::{AuthorityVerifier, Scope},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    executor::{Executor, StockExecutor, TurnInput, TurnOutput},
    model::{Model, ModelProvider},
    runtime::{AgentHarness, DurableTaskHost, RuntimeScope, TaskRegistry, TaskRunLimits},
    scheduler::{LeaseFence, SessionLimits},
    tool::ToolRegistry,
    workflow::MachineRegistry,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{StreamClient, StreamProvider, SystemUnixMillisClock};
use std::sync::Arc;

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
