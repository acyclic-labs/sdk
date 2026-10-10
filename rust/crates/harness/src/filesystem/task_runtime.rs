//! Optional persistent task composition using the existing runtime authorities.
//!
//! Admitted absolute deadlines use the original owner's injected clock on both
//! native and browser workers. Expiry cancels publication authority, not unknown
//! provider effects: the exact lease and started intents survive for explicit,
//! bounded `reconcile_task` observation through the ordinary command executors.

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
    executor::{ExecutionJournal, StockExecutor, StockTurnProgress, TurnInput, TurnOutput},
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
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{StreamClient, StreamProvider, SystemUnixMillisClock, UnixMillisClock};
use serde_json::{Value, json};
use std::sync::Arc;

mod deadline;
mod reconciliation;
mod inspection;
pub use inspection::TaskSourceInspection;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Trusted command adapter. Implementations must dispatch/reconcile each stable
/// command identity through the existing journaled provider APIs. A ready result
/// certifies that no uncertain dispatch remains; pending retains durable wait
/// state rather than a future. This trait does not provide an effects ledger.
pub trait TaskCommandHost: acyclic_stream::ProviderPlatform {
    /// Executes or reconciles a retained command under the exact task lease.
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>>;

    /// Observes only a retained dispatch. Missing intent must never execute.
    fn reconcile<'a>(
        &'a self,
        _context: TaskContext,
        _fence: LeaseFence,
        _command: WorkflowCommand,
        _payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "task command reconciliation is not bound".into(),
            ))
        })
    }
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
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
    recursive_source: Option<Arc<dyn super::RecursiveDefaultToolSource<P, A, O>>>,
}

struct TaskInteractionOwner {
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    scope: Scope,
    volume: VolumeRef,
}

struct TaskJournalMailbox<P: StreamProvider> {
    host: Arc<CoordinatorTaskHost<P>>,
    owner: crate::durable_host::TaskJournalOwner<P>,
}

impl<P: StreamProvider + 'static> crate::runtime::OwnedTaskMailbox for TaskJournalMailbox<P> {
    fn validate_scope(&self, scope: &RuntimeScope) -> Result<()> {
        self.owner.require_mail_scope(scope)
    }
    fn task_binding(&self) -> (TaskId, crate::scheduler::LeaseFence) {
        self.owner.task_binding()
    }
    fn send<'a>(
        &'a self, recipient: TaskId, operation: OperationId, payload: crate::conversation::FileRef,
    ) -> BoxFuture<'a, Result<crate::durable_host::MailSendAck>> {
        Box::pin(async move {
            let (task, fence) = self.owner.task_binding();
            self.host.send_owned(task, fence, recipient, operation, payload).await
        })
    }
    fn inbox<'a>(
        &'a self, after: u64, maximum: usize,
    ) -> BoxFuture<'a, Result<Vec<crate::scheduler::InboxItem>>> {
        Box::pin(async move {
            self.owner.verify(true).await?;
            let (task, _) = self.owner.task_binding();
            let items = self.host.inbox(task, after, maximum).await?;
            self.owner.verify(true).await?;
            Ok(items)
        })
    }
    fn read_received<'a>(
        &'a self, receive_operation: OperationId, item: &'a crate::scheduler::InboxItem,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let (task, fence) = self.owner.task_binding();
            self.host.read_received_mail_owned(task, fence, receive_operation, item).await
        })
    }
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
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
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
        Self::open_with_creation_providers(
            stream, filesystem, volume, verifier, signed, scope, tasks, machines, tools,
            session_limits, concurrency, maximum_payload_bytes, clock, policy, Vec::new(),
            crate::durable_tool::ResumableToolRegistry::default(),
        ).await
    }

    /// Composes a fixed set of trusted resource creation verifiers into the
    /// ordinary owner before it is shared. Proof refs alone never install access.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit existing provider, policy, clock and authority boundaries"
    )]
    pub async fn open_with_creation_providers(
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
        creation_providers: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
        resumable_tools: crate::durable_tool::ResumableToolRegistry,
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
        .with_session_limits(session_limits)?
        .with_resource_creation_providers(creation_providers)?;
        if let Some(policy) = &policy {
            host = host.with_policy(policy.clone())?;
        }
        let host = Arc::new(host);
        let harness = crate::runtime::HarnessBuilder {
            tasks,
            tools: tools.clone(),
            resumable_tools,
            scope,
            concurrency,
            durable_host: Some(host.clone()),
            content: Some(ContentBindings { reader, writer: None }),
            policy: policy.clone(),
            ..crate::runtime::HarnessBuilder::local()
        }.build()?;
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
            recursive_source: None,
        })
    }

    /// Installs the same configured recursive source used by registered default
    /// tools. Preparation dispatch resolves its exact original request per task.
    #[must_use]
    pub fn with_recursive_source(
        mut self,
        source: Arc<dyn super::RecursiveDefaultToolSource<P, A, O>>,
    ) -> Self {
        self.recursive_source = Some(source);
        self
    }

    pub(super) fn recursive_source(
        &self,
    ) -> Option<&Arc<dyn super::RecursiveDefaultToolSource<P, A, O>>> {
        self.recursive_source.as_ref()
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
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("approval has no task".into()))?;
        let admission = self.host.observe_admission(task).await?;
        let identity = policy.identity();
        if admission.policy.as_ref() != Some(&identity) {
            return Err(Error::Conflict(
                "approval policy differs from admission".into(),
            ));
        }
        let (operation, digest) =
            crate::runtime::tool_approval_binding(task, &identity, definition, invocation)?;
        let id = crate::durable_host::task_interaction_id(task, operation);
        let host = self.interaction_host()?;
        match host.read_request(id).await? {
            None => return Ok(false),
            Some(Interaction::Approval {
                operation_id,
                action_digest,
                ..
            }) if operation_id == invocation.operation_id && action_digest == digest => {}
            Some(_) => {
                return Err(Error::Conflict("approval request changed".into()));
            }
        }
        Ok(host
            .read(id)
            .await?
            .and_then(|(_, resolution)| resolution)
            .is_some_and(|resolution| resolution.outcome.is_terminal()))
    }

    /// Inspects retained model/tool artifacts without constructing an executor
    /// or granting a passive task a lease. Wake publication rechecks suspension.
    #[allow(
        clippy::too_many_lines,
        reason = "one read-only readiness proof keeps claims, retained request and tool settlement checks in order"
    )]
    pub(super) async fn model_approval_ready(
        &self,
        context: &TaskContext,
        turn: OperationId,
        input: &super::ModelTaskCommand,
    ) -> Result<bool> {
        use crate::executor::{
            ExecutionEvent, ExecutionReplay, completed_tool_projection, replay_execution,
            retained_model_step,
        };
        let scope = context.scope();
        if !scope.grants().contains("model:generate") {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        let limits = scope.limits();
        if input.max_steps == 0
            || u64::from(input.max_steps) > limits.model_steps as u64
            || scope
                .run_limits()
                .max_steps
                .is_some_and(|bound| u64::from(input.max_steps) > bound as u64)
        {
            return Err(Error::Unauthorized(
                "model wait exceeds admitted steps".into(),
            ));
        }
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("model wait has no task".into()))?;
        let operation = execution_operation(task, turn);
        let journal = FilesystemExecutionJournal::new(
            self.stream.clone(),
            self.filesystem.clone(),
            self.volume.clone(),
            self.verifier.clone(),
            self.signed.clone(),
            self.maximum_payload_bytes.min(limits.file_bytes),
        )?
        .with_turn(operation, turn);
        let mut replay = ExecutionReplay::new(operation);
        let mut latest = None;
        while let Some(page) = replay.next_page(&journal).await? {
            self.host
                .verify_model_history(task, operation, &page)
                .await?;
            for record in page {
                if let ExecutionEvent::ModelStarted {
                    step,
                    purpose: crate::executor::ModelPurpose::Response,
                    ..
                } = record.event
                {
                    if step >= input.max_steps {
                        return Err(Error::Conflict(
                            "retained model step exceeds command".into(),
                        ));
                    }
                    latest = Some(latest.map_or(step, |previous: u32| previous.max(step)));
                }
            }
        }
        let Some(step) = latest else {
            return Ok(false);
        };
        let (_, records) = replay_execution(
            &journal,
            operation,
            limits.model_events_per_step.saturating_add(1),
            |event| {
                matches!(event,
                ExecutionEvent::ModelStarted { step: recorded, purpose: crate::executor::ModelPurpose::Response, .. }
                | ExecutionEvent::Model { step: recorded, purpose: crate::executor::ModelPurpose::Response, .. } if *recorded == step)
            },
        )
        .await?;
        let retained = retained_model_step(
            &journal,
            &records,
            step,
            crate::executor::ModelPurpose::Response,
            limits,
        )
        .await?;
        if !retained.admission.completed() {
            return Err(Error::Indeterminate(operation));
        }
        let request = retained.request.ok_or(Error::Indeterminate(operation))?;
        for message in &request.request().messages {
            for file in message.content.file_refs() {
                context.read_file(file).await?;
            }
        }
        for event in retained.events {
            if let crate::model::ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            } = event
            {
                let invocation = crate::tool::ToolInvocation::for_model_call(
                    operation, step, call_id, name, arguments,
                );
                let (_, records) = replay_execution(&journal, operation, 3, |event| {
                    matches!(event,
                        ExecutionEvent::ToolStarted { step: recorded, call_id, .. }
                        | ExecutionEvent::ToolCompleted { step: recorded, call_id, .. }
                        | ExecutionEvent::ToolFailed { step: recorded, call_id, .. }
                        if *recorded == step && call_id == &invocation.call_id)
                })
                .await?;
                if records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. }))
                {
                    return Err(Error::Invalid("retained model tool failed".into()));
                }
                if records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
                {
                    completed_tool_projection(
                        &journal,
                        &records,
                        step,
                        &invocation,
                        &request.request().tools,
                    )
                    .await?;
                    continue;
                }
                if !records.is_empty() {
                    return Err(Error::Indeterminate(invocation.operation_id));
                }
                let pinned = request
                    .request()
                    .tools
                    .iter()
                    .find(|definition| definition.name == invocation.name)
                    .ok_or_else(|| {
                        Error::Conflict("approval tool is absent from model request".into())
                    })?;
                let definition = self.tool_definition(&pinned.name, &pinned.revision)?;
                if &definition != pinned {
                    return Err(Error::Conflict(
                        "approval tool differs from model request".into(),
                    ));
                }
                if !scope
                    .grants()
                    .contains(&crate::contract::capability::tool_call(&pinned.name))
                {
                    return Err(Error::Unauthorized(
                        "model wait lacks tool capability".into(),
                    ));
                }
                return self
                    .tool_approval_ready(context, &definition, &invocation)
                    .await;
            }
        }
        Ok(false)
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

    /// Constructs the ordinary retained scope/parent context with the exact
    /// current mailbox owner token. Unbound context constructors cannot send.
    pub async fn task_context(&self, task: TaskId, fence: LeaseFence) -> Result<TaskContext> {
        let owner = self.host.journal_owner(task, fence).await?;
        let context = self.harness.durable_context(
            task, OperationId::from_bytes(task.into_bytes()),
        ).await?;
        context.with_owned_mailbox(task, Arc::new(TaskJournalMailbox {
            host: self.host.clone(), owner,
        }))
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
                turn,
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

    /// Opens the canonical tool-effect workflow under the original caller
    /// reservation. This does not acquire a replacement fence or root scope.
    pub async fn workflow_journal_for_tool(
        &self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let task = context.task().durable_task_id()
            .ok_or_else(|| Error::Unauthorized("tool has no admitted caller".into()))?;
        let owner = self.host.journal_owner(task, context.task().owned_task_fence()?).await?
            .for_tool_workflow(context, tool, invocation, cancellation).await?;
        let maximum_payload_bytes = self.maximum_payload_bytes.min(owner.input_limits().file_bytes);
        Ok(Arc::new(FilesystemWorkflowJournal::for_task(
            owner, self.filesystem.clone(), self.volume.clone(),
            self.verifier.clone(), self.signed.clone(), maximum_payload_bytes,
        )?))
    }

    /// Actual caller/source route for portable recursive tool adapters.
    pub async fn recursive_tool_owner(
        self: &Arc<Self>,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<super::RecursiveToolOwner<P, A, O>> {
        let fence = context.task().owned_task_fence()?;
        let workflow = self.workflow_journal_for_tool(context, tool, invocation, cancellation).await?;
        let journal_owner = workflow.task_journal_owner()
            .ok_or_else(|| Error::Unauthorized("recursive tool lacks its original task owner".into()))?;
        Ok(super::RecursiveToolOwner { runtime:self.clone(), fence, workflow, journal_owner, cancellation:cancellation.clone() })
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
        if worker.id.trim().is_empty() || maximum_transitions == 0 {
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

    /// Polls and claims only the original admitted task, then uses the same
    /// fenced worker execution path. An active uncertain lease must instead be
    /// reconciled and supplied to `resume_task`; this never guesses ownership.
    pub async fn run_operation(
        &self,
        worker: &crate::distributed::Worker,
        task: TaskId,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> Result<Option<TaskWorkerAttempt>> {
        if worker.id.trim().is_empty() || maximum_transitions == 0 {
            return Err(Error::Invalid(
                "invalid targeted worker allowance or identity".into(),
            ));
        }
        // Verify the original admission before touching this task's passive wake.
        let admission = self.host.observe_admission(task).await?;
        if matches!(
            self.host.observe_task(task).await?.outcome,
            Some(Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled)
        ) {
            return Ok(None);
        }
        let work = deadline::run(
            &self.host,
            task,
            None,
            admission.run_limits.deadline_epoch_ms,
            admission.operation_id,
            async {
                self.poll_task_wake(task).await?;
                self.host.pull_operation(worker, admission.operation_id).await
            },
        )
        .await?;
        Ok(match work {
            crate::distributed::WorkPull::Idle => None,
            crate::distributed::WorkPull::Claimed(lease) => {
                Some(self.resume_task(lease, commands, maximum_transitions).await)
            }
            crate::distributed::WorkPull::Unresolved { lease, error } => {
                Some(TaskWorkerAttempt::Unresolved { lease, error })
            }
        })
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
    /// passive waits. The caller selects a positive allowance. A cursor is only a resume
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

    /// Reads the exact suspended workflow checkpoint under this runtime's
    /// protected service scope. No lease is acquired and no machine is advanced.
    /// This is not a task-level grant to read another task's private state.
    pub async fn observe_workflow_checkpoint(
        &self,
        task: TaskId,
    ) -> Result<crate::workflow::MachineCheckpoint> {
        use crate::workflow::WorkflowJournal as _;
        let slot = self.host.workflow_suspension(task).await?
            .ok_or_else(|| Error::Conflict("workflow is not passively suspended".into()))?;
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
        let retained = journal.admission().await?
            .ok_or_else(|| Error::Storage("suspended workflow admission is absent".into()))?;
        if retained.operation_id != admission.operation_id
            || retained.request_digest != crate::contract::canonical_json_digest(&admission)?
            || retained.initial.machine != admission.machine
        {
            return Err(Error::Unauthorized("observed workflow differs from admitted task".into()));
        }
        let record = journal.observe_transition(slot.revision).await?;
        if record.next.revision != slot.revision
            || record.next.machine != admission.machine
            || !matches!(record.transition.status, MachineStatus::Suspended)
        {
            return Err(Error::Conflict("observed checkpoint differs from suspension".into()));
        }
        // A concurrently consumed/replaced resume slot cannot be represented as
        // the same passive checkpoint to the caller.
        if self.host.workflow_suspension(task).await?.as_ref() != Some(&slot) {
            return Err(Error::Conflict("workflow suspension changed during observation".into()));
        }
        Ok(record.next)
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
        let payload = crate::contract::json_from_slice(&context.read_file(&command.payload).await?)
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
            self.execution_journal(task, fence, command)
                .await?
                .suspend_workflow_command_if_quiescent(revision, command)
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
        if maximum_transitions == 0 {
            return Err(Error::Invalid("invalid worker transition allowance".into()));
        }
        let operation_id = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation_id.into_bytes());
        let fence = LeaseFence::from(&lease.reservation);
        let admission = self.host.observe_admission(task).await?;
        deadline::run(
            &self.host,
            task,
            Some(&fence),
            admission.run_limits.deadline_epoch_ms,
            operation_id,
            self.run_task_owned(lease, commands, maximum_transitions),
        )
        .await
    }

    async fn run_task_owned(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> Result<TaskWorkerOutcome> {
        let operation_id = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation_id.into_bytes());
        let fence = LeaseFence::from(&lease.reservation);
        self.host.start_task(&lease).await?;
        let context = self.task_context(task, fence.clone()).await?;
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
        if outbox.len() != 1 && outbox.iter().any(|command|
            command.kind == super::MAIL_RECEIVE_TASK_COMMAND_KIND)
        {
            return Err(Error::Invalid(
                "mail receive must checkpoint alone before the next command".into(),
            ));
        }
        let mut results = Vec::new();
        for command in outbox {
            self.host.verify_dispatch_owner(task, fence.clone()).await?;
            if command.payload.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(Error::Invalid("command exceeds payload limit".into()));
            }
            let payload =
                crate::contract::json_from_slice(&context.read_file(&command.payload).await?)
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
                        guard_execution: matches!(
                            command.kind.as_str(),
                            super::TOOL_TASK_COMMAND_KIND | super::MODEL_TASK_COMMAND_KIND
                        ),
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
        let task_context = self.task_context(task, fence.clone()).await?;
        let scope = task_context.scope().clone();
        let limits = scope.limits();
        let run_limits = scope.run_limits();
        let operation_id = execution_operation(task, turn);
        let journal = self
            .open_execution_journal(task, fence.clone(), turn, &admission)
            .await?;
        let executor = StockExecutor::new(model, provider, context, self.tools.clone())
            .with_limits(limits)
            .with_tool_authority(scope, self.policy.clone())?
            .with_durable_task(self.host.clone(), task, fence.clone())
            .with_task_context(&task_context, operation_id)?;
        Ok(FilesystemTaskExecution {
            operation_id,
            executor,
            journal,
            run_limits,
            host: self.host.clone(),
            task,
            fence,
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
    host: Arc<CoordinatorTaskHost<P>>,
    task: TaskId,
    fence: LeaseFence,
}

impl<P, A, O> FilesystemTaskExecution<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Stable task-namespaced turn identity to use in `TurnInput` after reopen.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Replaces or disables compaction while retaining task accounting and fencing.
    #[must_use]
    pub fn with_compaction_policy(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.executor = self.executor.with_compaction_policy(policy);
        self
    }

    /// Sets the output token budget without replacing task ownership or its journal.
    pub fn with_max_output_tokens(mut self, maximum: u32) -> Result<Self> {
        self.executor = self.executor.with_max_output_tokens(maximum)?;
        Ok(self)
    }

    /// Executes or reconciles using the bound journal and existing stock loop.
    pub async fn execute(&self, input: TurnInput) -> Result<TurnOutput> {
        self.execute_progress(input).await?.into_output()
    }

    pub(crate) async fn execute_progress(&self, input: TurnInput) -> Result<StockTurnProgress> {
        self.validate_input(&input)?;
        deadline::run(
            &self.host,
            self.task,
            Some(&self.fence),
            self.run_limits.deadline_epoch_ms,
            self.operation_id,
            self.executor.execute_progress(input, &self.journal),
        )
        .await
    }

    /// Observes retained model/tool attempts without starting another step.
    /// Consumes this composition so the existing executor changes mode in place.
    /// `None` means the observation allowance elapsed; effects remain uncertain.
    pub async fn reconcile(
        self,
        input: TurnInput,
        maximum_wait_millis: u32,
    ) -> Result<Option<TurnOutput>> {
        if maximum_wait_millis == 0 {
            return Err(Error::Invalid("invalid reconciliation allowance".into()));
        }
        deadline::observe(maximum_wait_millis, async {
            self.reconcile_progress(input).await?.into_output()
        })
        .await
    }

    pub(crate) async fn reconcile_progress(
        mut self,
        input: TurnInput,
    ) -> Result<StockTurnProgress> {
        self.validate_input(&input)?;
        self.host.verify_execution_owner(self.task, self.fence.clone()).await?;
        if self.run_limits.deadline_epoch_ms.is_some_and(|deadline| {
            self.host.now_unix_millis() >= deadline
        }) {
            self.host.expire_task_deadline(self.task, Some(self.fence.clone())).await?;
        }
        self.executor = self.executor.into_reconciliation();
        self.executor.execute_progress(input, &self.journal).await
    }

    fn validate_input(&self, input: &TurnInput) -> Result<()> {
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
        Ok(())
    }
}
