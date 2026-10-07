//! Optional persistent task composition using the existing runtime authorities.

use super::{
    FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
    FilesystemInteractionHost, FilesystemSchedulerPayloadStore, FilesystemWorkflowJournal,
};
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::VolumeRef,
    core::{AuthorityVerifier, SchemaRegistry, Scope},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput, TurnOutput},
    interaction::{Interaction, InteractionOutcome},
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, ContentBindings, DurableTaskHost, InteractionRouter, ResumableTaskSession,
        RuntimeScope, TaskAdmissionRecord, TaskContext, TaskRegistry, TaskRunLimits, ToolPolicy,
    },
    scheduler::{LeaseFence, SessionLimits},
    tool::{ToolDefinition, ToolRegistry},
    workflow::{MachineRegistry, MachineStatus, WorkflowCommand},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{StreamClient, StreamProvider, SystemUnixMillisClock, UnixMillisClock};
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

enum CommandDispatch {
    Ready(Value),
    Pending {
        command: OperationId,
        guard_execution: bool,
    },
    Indeterminate,
}

/// Resume hint for one finite coordinator discovery sweep. It conveys no
/// execution authority and can be serialized by the caller across restarts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskWakeCursor {
    /// Last inspected coordinator revision.
    pub after_revision: u64,
    /// Fixed inclusive revision at which this sweep ends.
    pub through_revision: u64,
}

/// One bounded discovery/poll result; no task queue or watcher is retained.
#[derive(Debug)]
pub struct TaskWakePage {
    /// Next page of the same snapshot, or none when the sweep is complete.
    pub cursor: Option<TaskWakeCursor>,
    /// Inclusive coordinator revision captured for this sweep.
    pub through_revision: u64,
    /// Coordinator events inspected, at most the requested page allowance.
    pub events_read: u32,
    /// Distinct currently owned passive tasks polled, at most `events_read`.
    pub tasks_polled: u32,
    /// Tasks whose existing authenticated wake slot was filled.
    pub woken: Vec<TaskId>,
}

/// Worker attempt, always preserving a known lease when work is unresolved.
pub enum TaskWorkerAttempt {
    /// Normal worker progress, including explicit yielded/reconciling leases.
    Progress(TaskWorkerOutcome),
    /// Admission observation or execution failed. This exact lease is a resume
    /// attempt, not proof of ownership or provider quiescence. Resumption checks
    /// the retained coordinator reservation before dispatch.
    Unresolved {
        /// Exact attempted or retained lease.
        lease: crate::distributed::WorkLease,
        /// Admission or execution error.
        error: Error,
    },
}

/// One caller-driven service tick. It retains no background future.
pub struct TaskWorkerTick {
    /// Bounded discovery page; carry its cursor into the next tick.
    pub wake: TaskWakePage,
    /// At most one owned task attempt; none means no work was admitted.
    pub work: Option<TaskWorkerAttempt>,
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
    stream: StreamClient<P>,
    host: Arc<CoordinatorTaskHost<P>>,
    harness: Arc<AgentHarness>,
    filesystem: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    tools: ToolRegistry,
    maximum_payload_bytes: u64,
    policy: Option<Arc<dyn ToolPolicy>>,
    interaction_owner: Option<TaskInteractionOwner>,
}

struct TaskInteractionOwner {
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    scope: Scope,
    volume: VolumeRef,
}

struct TaskJournalInteractions {
    task: TaskId,
    journal: Arc<dyn ExecutionJournal>,
}

impl InteractionRouter for TaskJournalInteractions {
    fn route<'a>(
        &'a self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        Box::pin(async move {
            let id = crate::durable_host::task_interaction_id(self.task, operation_id);
            self.journal.open_interaction(id, interaction).await?;
            Ok(self
                .journal
                .interaction_outcome(id)
                .await?
                .unwrap_or(InteractionOutcome::Indeterminate { operation_id }))
        })
    }
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
        Self::open_with_clock(
            stream,
            filesystem,
            volume,
            verifier,
            signed,
            scope,
            tasks,
            machines,
            tools,
            session_limits,
            concurrency,
            maximum_payload_bytes,
            Arc::new(SystemUnixMillisClock),
        )
        .await
    }

    /// Composes the existing host with a trusted clock, including restart tests.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit existing provider and authority boundaries"
    )]
    pub async fn open_with_clock(
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
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self> {
        Self::open_with_policy_and_clock(
            stream,
            filesystem,
            volume,
            verifier,
            signed,
            scope,
            tasks,
            machines,
            tools,
            session_limits,
            concurrency,
            maximum_payload_bytes,
            clock,
            None,
        )
        .await
    }

    /// Composes one exact policy into the existing task host, runtime and stock
    /// executor. The policy identity is retained at admission and must match
    /// after reopening. Approval decisions still need an interaction binding.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit existing provider and authority boundaries"
    )]
    pub async fn open_with_policy_and_clock(
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
        clock: Arc<dyn UnixMillisClock>,
        policy: Option<Arc<dyn ToolPolicy>>,
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
        let mut host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, reader.clone())
                .await?
                .with_payload_store(payloads.clone()),
            stream.clone(),
            payloads,
            reader.clone(),
            verifier.audience().clone(),
            signed.clone(),
            verifier.clone(),
            scope.clone(),
            tasks.clone(),
            machines,
            clock,
        )?
        .with_session_limits(session_limits)?;
        if let Some(policy) = &policy {
            host = host.with_policy(policy.clone())?;
        }
        let host = Arc::new(host);
        let harness = AgentHarness::with_policy(
            tasks,
            tools.clone(),
            scope,
            concurrency,
            Some(host.clone()),
            None,
            Some(ContentBindings {
                reader,
                writer: None,
            }),
            None,
            policy.clone(),
        )?;
        Ok(Self {
            stream,
            host,
            harness,
            filesystem,
            volume,
            verifier,
            signed,
            tools,
            maximum_payload_bytes,
            policy,
            interaction_owner: None,
        })
    }

    /// Binds the existing conversation owner for task approval/question tickets.
    /// Each dispatch uses its admitted task journal and exact lease; this scope
    /// permits request publication and conveys no participant response rights.
    pub fn with_interaction_owner(
        mut self,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        private_volume: VolumeRef,
    ) -> Result<Self> {
        verifier.verify(&scope)?;
        if !scope
            .capabilities()
            .contains(crate::contract::capability::INTERACTION_OPEN)
        {
            return Err(Error::Unauthorized(
                "interaction owner lacks interaction:open".into(),
            ));
        }
        self.interaction_owner = Some(TaskInteractionOwner {
            verifier,
            schemas,
            scope,
            volume: private_volume,
        });
        self.interaction_host()?;
        Ok(self)
    }

    fn interaction_host(&self) -> Result<FilesystemInteractionHost<P, A, O>> {
        let owner = self
            .interaction_owner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("task conversation owner is not bound".into()))?;
        FilesystemInteractionHost::new(
            self.stream.clone(),
            self.filesystem.clone(),
            owner.verifier.audience().clone(),
            owner.verifier.clone(),
            owner.schemas.clone(),
            owner.scope.clone(),
            owner.volume.clone(),
            self.maximum_payload_bytes,
        )
    }

    pub(super) async fn tool_approval_ready(
        &self,
        context: &TaskContext,
        definition: &ToolDefinition,
        invocation: &crate::tool::ToolInvocation,
    ) -> Result<bool> {
        if !context
            .scope()
            .grants()
            .contains(crate::contract::capability::INTERACTION_ROUTE)
        {
            return Err(Error::Unauthorized(
                "task scope lacks interaction:route".into(),
            ));
        }
        let Some(policy) = &self.policy else {
            return Ok(false);
        };
        let Some((operation, request)) = context
            .policy_approval(policy.as_ref(), definition, invocation)
            .await?
        else {
            return Ok(false);
        };
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("approval has no task".into()))?;
        let id = crate::durable_host::task_interaction_id(task, operation);
        let host = self.interaction_host()?;
        match host.read_request(id).await? {
            None => return Ok(false),
            Some(original) if original != request => {
                return Err(Error::Conflict("approval request changed".into()));
            }
            Some(_) => {}
        }
        Ok(host
            .read(id)
            .await?
            .and_then(|(_, resolution)| resolution)
            .is_some_and(|resolution| resolution.outcome.is_terminal()))
    }

    fn bind_interaction_owner(
        &self,
        journal: FilesystemExecutionJournal<P, A, O>,
        admitted_route: bool,
    ) -> Result<FilesystemExecutionJournal<P, A, O>> {
        if !admitted_route {
            return Ok(journal);
        }
        if let Some(owner) = &self.interaction_owner {
            journal.with_interaction_owner(
                owner.verifier.clone(),
                owner.schemas.clone(),
                owner.scope.clone(),
                owner.volume.clone(),
            )
        } else {
            Ok(journal)
        }
    }

    pub(super) fn bind_context_interactions(
        &self,
        context: TaskContext,
        task: TaskId,
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
    ) -> Result<TaskContext> {
        if self.interaction_owner.is_some() {
            context
                .with_owned_interactions(task, Arc::new(TaskJournalInteractions { task, journal }))
        } else {
            Ok(context)
        }
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

    /// Stock model/tool command adapters using this runtime's existing bindings.
    #[must_use]
    pub const fn commands(&self) -> super::FilesystemTaskCommands<'_, P, A, O> {
        super::FilesystemTaskCommands::new(self)
    }

    pub(super) fn tool_definition(&self, name: &str, revision: &str) -> Result<ToolDefinition> {
        self.tools
            .get_version(name, revision)
            .map(|tool| tool.definition.clone())
            .ok_or_else(|| Error::NotFound(format!("tool {name}@{revision}")))
    }

    pub(super) fn tool_registry(&self) -> ToolRegistry {
        self.tools.clone()
    }

    pub(super) async fn execution_journal(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
    ) -> Result<Arc<FilesystemExecutionJournal<P, A, O>>> {
        let admission = self.host.observe_admission(task).await?;
        Ok(Arc::new(
            self.open_execution_journal(task, fence, turn, &admission)
                .await?,
        ))
    }

    async fn open_execution_journal(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        admission: &TaskAdmissionRecord,
    ) -> Result<FilesystemExecutionJournal<P, A, O>> {
        self.bind_interaction_owner(
            FilesystemExecutionJournal::for_task(
                self.host.journal_owner(task, fence).await?,
                execution_operation(task, turn),
                self.filesystem.clone(),
                self.volume.clone(),
                self.verifier.clone(),
                self.signed.clone(),
                self.maximum_payload_bytes.min(admission.limits.file_bytes),
            )?,
            admission
                .grants
                .contains(crate::contract::capability::INTERACTION_ROUTE),
        )
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

    /// Performs one bounded discovery page followed by at most one owner-scoped
    /// admission and worker turn through the trusted command adapter. Allowances are
    /// checked before publication. Resource accounting uses the existing
    /// coordinator; discovery/refresh costs retain their documented limits.
    ///
    /// Carry the wake cursor between ticks and repeat completed sweeps later.
    /// Retain yielded, reconciling or unresolved leases and call `resume_task`
    /// explicitly; another tick never rediscovers an active lease or assumes
    /// that its provider stopped. Errors after an attempted admission return
    /// that exact attempt in `work` rather than discarding its identity.
    pub async fn worker_tick(
        &self,
        worker: &crate::distributed::Worker,
        cursor: Option<TaskWakeCursor>,
        commands: &dyn TaskCommandHost,
        maximum_events: u32,
        maximum_transitions: u32,
    ) -> Result<TaskWorkerTick> {
        if worker.id.trim().is_empty()
            || maximum_transitions == 0
            || u64::from(maximum_transitions) > crate::workflow::MAX_WORKFLOW_RECORDS
        {
            return Err(Error::Invalid(
                "invalid worker tick allowance or identity".into(),
            ));
        }
        let wake = self.poll_task_wake_page(cursor, maximum_events).await?;
        let work = match self.host.pull_work(worker).await? {
            crate::distributed::WorkPull::Idle => None,
            crate::distributed::WorkPull::Claimed(lease) => {
                Some(self.resume_task(lease, commands, maximum_transitions).await)
            }
            crate::distributed::WorkPull::Unresolved { lease, error } => {
                Some(TaskWorkerAttempt::Unresolved { lease, error })
            }
        };
        Ok(TaskWorkerTick { wake, work })
    }

    /// Resumes an explicit lease; every error returns the original attempt
    /// so callers can reconcile or verify it without silently releasing capacity.
    pub async fn resume_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> TaskWorkerAttempt {
        match self
            .run_task(lease.clone(), commands, maximum_transitions)
            .await
        {
            Ok(progress) => TaskWorkerAttempt::Progress(progress),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        }
    }

    /// Inspects one page of a finite coordinator snapshot and polls its owned
    /// passive waits. The allowance must be 1..=64. A cursor is only a resume
    /// hint: each candidate's current owner, wait and command are rechecked.
    ///
    /// Pass None to begin a sweep; persist/reuse the returned cursor until None
    /// marks completion. Start another sweep later to revisit waits that were
    /// not ready, including timers and inboxes without new coordinator events.
    /// A failed page may be retried with the same cursor: earlier wake fills are
    /// idempotent and are not another execution or accounting charge. This page
    /// bound does not bound the existing resident coordinator's refresh costs.
    pub async fn poll_task_wake_page(
        &self,
        cursor: Option<TaskWakeCursor>,
        maximum_events: u32,
    ) -> Result<TaskWakePage> {
        let page = self
            .host
            .workflow_wait_page(
                cursor.map_or(0, |cursor| cursor.after_revision),
                cursor.map(|cursor| cursor.through_revision),
                maximum_events,
            )
            .await?;
        let tasks_polled = u32::try_from(page.tasks.len())
            .map_err(|_| Error::Storage("wake candidate count exceeds its allowance".into()))?;
        let mut woken = Vec::new();
        for task in page.tasks {
            if self.poll_task_wake(task).await? {
                woken.push(task);
            }
        }
        Ok(TaskWakePage {
            cursor: (page.next_revision < page.through_revision).then_some(TaskWakeCursor {
                after_revision: page.next_revision,
                through_revision: page.through_revision,
            }),
            through_revision: page.through_revision,
            events_read: page.events_read,
            tasks_polled,
            woken,
        })
    }

    /// Polls one durable passive wait and fills its existing authenticated wake
    /// slot when ready. Callers drive bounded task discovery; this keeps no
    /// watcher, sleep future, execution reservation or second queue.
    pub async fn poll_task_wake(&self, task: TaskId) -> Result<bool> {
        use crate::workflow::WorkflowJournal as _;
        let Some(slot) = self.host.workflow_suspension(task).await? else {
            return Ok(false);
        };
        let Some(waiting) = slot.waiting_command else {
            return Ok(false);
        };
        let admission = self.host.observe_admission(task).await?;
        let journal = FilesystemWorkflowJournal::new(
            self.host.workflow_stream(),
            self.filesystem.clone(),
            &admission.operation_id.to_string(),
            self.volume.clone(),
            self.verifier.clone(),
            self.signed.clone(),
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
        )?;
        let retained = journal
            .admission()
            .await?
            .ok_or_else(|| Error::Storage("suspended workflow admission is absent".into()))?;
        if retained.operation_id != admission.operation_id
            || retained.request_digest != crate::contract::canonical_json_digest(&admission)?
            || retained.initial.machine != admission.machine
        {
            return Err(Error::Unauthorized(
                "wake workflow differs from admitted task".into(),
            ));
        }
        let record = journal.observe_transition(slot.revision).await?;
        if record.next.revision != slot.revision
            || record.next.machine != admission.machine
            || !matches!(record.transition.status, MachineStatus::Suspended)
        {
            return Err(Error::Conflict(
                "wake checkpoint differs from suspension".into(),
            ));
        }
        let command = record
            .transition
            .commands
            .iter()
            .find(|command| command.operation_id == waiting)
            .ok_or_else(|| Error::Conflict("suspended command is absent from outbox".into()))?;
        let context = self
            .harness
            .durable_context(task, admission.operation_id)
            .await?;
        let payload = serde_json::from_slice(&context.read_file(&command.payload).await?)
            .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?;
        if !self
            .commands()
            .wait_ready(&context, command, payload)
            .await?
        {
            return Ok(false);
        }
        // The authenticated publication rechecks the exact revision and phase
        // after every asynchronous readiness read, including cancellation races.
        self.host.wake_command(task, slot.revision, waiting).await?;
        Ok(true)
    }

    async fn suspend_command(
        &self,
        task: TaskId,
        fence: LeaseFence,
        revision: u64,
        command: OperationId,
        guard_execution: bool,
    ) -> Result<()> {
        if guard_execution {
            self.host
                .suspend_workflow_command_if_execution_idle(
                    task,
                    fence,
                    revision,
                    command,
                    execution_operation(task, command),
                )
                .await
        } else {
            self.host
                .suspend_workflow_command(task, fence, revision, Some(command))
                .await
        }
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
                        CommandDispatch::Ready(value) => command_input = Some(value),
                        CommandDispatch::Pending {
                            command,
                            guard_execution,
                        } => {
                            self.suspend_command(task, fence, revision, command, guard_execution)
                                .await?;
                            return Ok(TaskWorkerOutcome::Suspended { task, revision });
                        }
                        CommandDispatch::Indeterminate => {
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
    ) -> Result<CommandDispatch> {
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
                        return Ok(CommandDispatch::Indeterminate);
                    }
                }
                Ok(TaskCommandProgress::Pending) => {
                    return Ok(CommandDispatch::Pending {
                        command: command.operation_id,
                        guard_execution: command.kind == super::TOOL_TASK_COMMAND_KIND,
                    });
                }
                Ok(TaskCommandProgress::Indeterminate) | Err(_) => {
                    return Ok(CommandDispatch::Indeterminate);
                }
            }
        }
        Ok(CommandDispatch::Ready(json!({"commands":results})))
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
        if admission.execution.is_some() {
            return Err(Error::Unsupported(
                "persistent local stock composition has no routed execution provider".into(),
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
        let operation_id = execution_operation(task, turn);
        let journal = self
            .open_execution_journal(task, fence.clone(), turn, &admission)
            .await?;
        let executor = StockExecutor::new(model, provider, context, self.tools.clone())
            .with_limits(limits)
            .with_tool_authority(scope, self.policy.clone())?
            .with_durable_task(self.host.clone(), task, fence);
        Ok(FilesystemTaskExecution {
            operation_id,
            executor,
            journal,
            run_limits,
        })
    }
}

pub(super) fn execution_operation(task: TaskId, turn: OperationId) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-execution\0");
    hasher.update(&task.into_bytes());
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
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
