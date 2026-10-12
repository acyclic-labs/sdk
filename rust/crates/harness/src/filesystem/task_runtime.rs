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
    durable_tool::ResumableToolRegistry,
    executor::{ExecutionJournal, StockExecutor, StockTurnProgress, TurnInput, TurnOutput},
    interaction::{Interaction, InteractionOutcome},
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, ContentBindings, DurableTaskHost, InteractionRouter, ResumableTaskSession,
        OwnedTaskMailbox, RuntimeScope, TaskAdmissionRecord, TaskContext, TaskRegistry, TaskRunLimits, ToolPolicy,
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
mod command_input;
pub(super) mod observed;
#[cfg(not(target_arch = "wasm32"))]
mod native_mailbox;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Trusted command adapter.
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

    /// Executes on the original work ledger without constructing a legacy future.
    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_work<'a>(
        &'a self,
        _context: Arc<crate::runtime::NativeTaskContext>,
        _fence: LeaseFence,
        _command: WorkflowCommand,
        _payload: Value,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task command host has no native execution observer",
        })
    }

    /// Observes the original dispatch; unsupported hosts fail before allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        _context: Arc<crate::runtime::NativeTaskContext>,
        _fence: LeaseFence,
        _command: WorkflowCommand,
        _payload: Value,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task command host has no native reconciliation observer",
        })
    }
}

/// Explicit dispatch ownership returned by a trusted command adapter.
pub enum TaskCommandProgress {
    /// Durable result, safe to deliver to the machine.
    Ready(Value),
    /// Native result whose existing same-operation custody survives consumption.
    #[cfg(not(target_arch = "wasm32"))]
    ReadyWithNativeCustody {
        /// Owned response, dropped before its custody.
        value: Value,
        /// Actual original-ledger custody, not a post-allocation reconstruction.
        custody: acyclic_fs::NativeAllocation,
    },
    /// Durable wait with no active uncertain dispatch.
    Pending,
    /// A dispatch may still be active; retain the existing reservation.
    Indeterminate,
}

enum CommandDispatch {
    Ready {
        value: Value,
        #[cfg(not(target_arch = "wasm32"))]
        array_capacity_claimed: u64,
        #[cfg(not(target_arch = "wasm32"))]
        additional_capacity_claimed: u64,
        #[cfg(not(target_arch = "wasm32"))]
        additional_custody: Vec<acyclic_fs::NativeAllocation>,
        #[cfg(not(target_arch = "wasm32"))]
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    Pending {
        command: OperationId,
        guard_execution: bool,
    },
    Indeterminate,
}

/// Resume hint for one finite coordinator discovery sweep.
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
    /// Admission observation or execution failed.
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

/// Invocation progress before moving the original lease into its public result.
enum WorkerTurn {
    Suspended { task: TaskId, revision: u64 },
    Completed { task: TaskId },
    Yielded,
    Reconciling,
}

impl WorkerTurn {
    fn with_lease(self, lease: crate::distributed::WorkLease) -> TaskWorkerOutcome {
        match self {
            Self::Suspended { task, revision } => TaskWorkerOutcome::Suspended { task, revision },
            Self::Completed { task } => TaskWorkerOutcome::Completed { task },
            Self::Yielded => TaskWorkerOutcome::Yielded { lease },
            Self::Reconciling => TaskWorkerOutcome::Reconciling { lease },
        }
    }
}

enum WorkerContext {
    Ordinary(TaskContext),
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<crate::runtime::NativeTaskContext>),
}

impl WorkerContext {
    fn context(&self) -> &TaskContext {
        match self {
            Self::Ordinary(context) => context,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(context) => context.context(),
        }
    }
}

/// Composes caller-owned persistent providers without creating another scheduler or registry.
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
    #[cfg(not(target_arch = "wasm32"))]
    native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
}

struct TaskInteractionOwner {
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    scope: Scope,
    volume: VolumeRef,
}

struct TaskJournalMailbox<P: StreamProvider> {
    host: Arc<CoordinatorTaskHost<P>>,
    owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<P: StreamProvider + 'static> crate::runtime::OwnedTaskMailbox for TaskJournalMailbox<P> {
    fn validate_scope(&self, scope: &RuntimeScope) -> Result<()> {
        self.owner.as_ref().require_mail_scope(scope)
    }
    fn task_binding(&self) -> (TaskId, &LeaseFence) {
        self.owner.as_ref().task_binding()
    }
    fn matches_task_binding(&self, task: TaskId, fence: &LeaseFence) -> bool {
        self.owner.as_ref().matches_task_binding(task, fence)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_scope_with_native_work(&self, scope: &RuntimeScope, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !self.native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "mailbox metadata differs from original work",
            });
        }
        self.owner.as_ref().require_mail_scope_with_native_work(scope, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self, recipient: TaskId, operation: OperationId,
        payload: &'a crate::conversation::FileRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::durable_host::MailSendAck, acyclic_fs::NativeAllocation)>> {
        native_mailbox::send(self, recipient, operation, payload, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn inbox_with_native_work<'a>(
        &'a self, after: u64, maximum: usize, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<crate::scheduler::InboxItem>, acyclic_fs::NativeAllocation)>> {
        native_mailbox::inbox(self, after, maximum, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_received_with_native_work<'a>(
        &'a self, receive_operation: OperationId, item: &'a crate::scheduler::InboxItem,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        native_mailbox::read_received(self, receive_operation, item, work)
    }
    fn send<'a>(
        &'a self, recipient: TaskId, operation: OperationId, payload: crate::conversation::FileRef,
    ) -> BoxFuture<'a, Result<crate::durable_host::MailSendAck>> {
        Box::pin(async move {
            let (task, fence) = self.owner.as_ref().task_binding();
            self.host.send_owned(task, fence.clone(), recipient, operation, payload).await
        })
    }
    fn inbox<'a>(&'a self, after: u64, maximum: usize) -> BoxFuture<'a, Result<Vec<crate::scheduler::InboxItem>>> {
        Box::pin(async move {
            self.owner.as_ref().verify(true, &mut None).await?;
            let (task, _) = self.owner.as_ref().task_binding();
            let items = self.host.inbox(task, after, maximum).await?;
            self.owner.as_ref().verify(true, &mut None).await?;
            Ok(items)
        })
    }
    fn read_received<'a>(
        &'a self, receive_operation: OperationId, item: &'a crate::scheduler::InboxItem,
    ) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move {
            let (task, fence) = self.owner.as_ref().task_binding();
            self.host.read_received_mail_owned(task, fence.clone(), receive_operation, item).await
        })
    }
}

struct TaskJournalInteractions {
    task: TaskId,
    journal: Arc<dyn ExecutionJournal>,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
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

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_interactions(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self.native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "task journal router has no original native custody",
            })
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn route_with_native_work<'a>(
        &'a self, operation_id: OperationId, interaction: Interaction,
        request_custody: acyclic_fs::NativeAllocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (InteractionOutcome, acyclic_fs::NativeAllocation)>> {
        let paid = (interaction, request_custody);
        self.supports_native_interactions(work)?;
        if !paid.1.matches_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "interaction request differs from original work",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let paid = paid;
            let id = crate::durable_host::task_interaction_id_with_native_work(
                self.task, operation_id, work,
            )?;
            self.journal.open_interaction_with_native_work(id, &paid.0, work)?.await?;
            let (outcome, custody) = self.journal.interaction_outcome_with_native_work(id, work)?.await?;
            Ok((outcome.unwrap_or(InteractionOutcome::Indeterminate { operation_id }), custody))
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

    /// Composes one exact policy into the existing task host, runtime and stock executor.
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

    /// Composes a fixed set of trusted resource creation verifiers into the ordinary owner before it is shared.
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
        let harness = crate::runtime::Bindings {
            tasks,
            tools: tools.clone(),
            resumable_tools,
            scope,
            concurrency,
            durable_host: Some(host.clone()),
            content: Some(ContentBindings { reader, writer: None }),
            policy: policy.clone(),
            ..crate::runtime::Bindings::local()
        }.build()?;
        Self::from_host(
            stream, harness, host, filesystem, volume, verifier, signed, tools,
            maximum_payload_bytes, policy,
        )
    }

    /// Installs the same configured recursive source used by registered default tools.
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

    /// Opens the same stock composition using its original native producers.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "the original explicit stock configuration plus native work")]
    pub async fn open_with_creation_providers_with_native_work(
        stream: StreamClient<P>, filesystem: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef, verifier: &AuthorityVerifier, signed: &Scope,
        scope: &RuntimeScope, tasks: &TaskRegistry, machines: &MachineRegistry,
        tools: &ToolRegistry, resumable_tools: &ResumableToolRegistry,
        session_limits: SessionLimits, concurrency: usize,
        maximum_payload_bytes: u64, clock: Arc<dyn UnixMillisClock>,
        policy: Option<Arc<dyn ToolPolicy>>,
        creation_providers: &[Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let io = super::PreparationIo::from_native_work(work)?;
        volume.validate_with_native_work(work)?;
        if maximum_payload_bytes == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "stock runtime requires a positive payload limit" });
        }
        if let Some(policy) = &policy { policy.supports_native_policy(work)?; }
        let payloads = FilesystemSchedulerPayloadStore::new_with_native_work(
            filesystem.clone(), volume, verifier, signed, maximum_payload_bytes, work,
        )?.into_native_shared(work)?;
        let reader = FilesystemContentVerifier::new_with_native_work(
            filesystem.clone(), verifier, signed, maximum_payload_bytes, work,
        )?.into_native_shared(work)?;
        let coordinator = DistributedCoordinator::open_with_native_work(
            &stream, reader.clone(), work,
        ).await?.with_payload_store(payloads.clone());
        let mut host = CoordinatorTaskHost::new_with_native_work(
            coordinator, stream.clone(), payloads, reader.clone(),
            verifier.audience(), signed, verifier, scope, tasks, machines, clock, work,
        )?.with_session_limits_with_native_work(session_limits, work)?
            .with_resource_creation_providers_with_native_work(creation_providers, work)?;
        if let Some(policy) = &policy {
            host = host.with_policy_with_native_work(policy.clone(), work)?;
        }
        let host = host.into_native_shared(work)?;
        let harness = AgentHarness::with_policy_with_native_work(
            tasks, tools, resumable_tools, scope, concurrency, Some(host.clone()), None,
            Some(ContentBindings { reader, writer: None }), None, policy.clone(), work,
        )?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let volume = volume.clone_with_io(&io)?;
        let verifier = verifier.clone_with_native_work(work)?;
        io.absorb_native_custody(verifier.1)?;
        let signed = signed.clone_with_native_work(work)?;
        io.absorb_native_custody(signed.1)?;
        let tools = tools.clone_with_native_work(work)?;
        io.absorb_native_custody(tools.1)?;
        Self::from_host_observed(
            stream, harness, host, filesystem, volume, verifier.0, signed.0, tools.0,
            maximum_payload_bytes, policy, Some(&io),
        )
    }

    /// Attaches stock commands to the same already-configured host and harness.
    #[allow(clippy::too_many_arguments, reason = "the original explicit configured provider and authority boundaries")]
    pub fn from_host(
        stream: StreamClient<P>, harness: Arc<AgentHarness>,
        host: Arc<CoordinatorTaskHost<P>>, filesystem: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef, verifier: AuthorityVerifier, signed: Scope,
        tools: ToolRegistry, maximum_payload_bytes: u64,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        Self::from_host_observed(
            stream, harness, host, filesystem, volume, verifier, signed, tools,
            maximum_payload_bytes, policy, None,
        )
    }

    #[allow(clippy::too_many_arguments, reason = "the original configured stock initializer and its optional observer")]
    fn from_host_observed(
        stream: StreamClient<P>, harness: Arc<AgentHarness>,
        host: Arc<CoordinatorTaskHost<P>>, filesystem: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef, verifier: AuthorityVerifier, signed: Scope,
        tools: ToolRegistry, maximum_payload_bytes: u64,
        policy: Option<Arc<dyn ToolPolicy>>, io: Option<&super::PreparationIo>,
    ) -> Result<Self> {
        if !harness.original_host_binding_matches(host.as_ref(), policy.as_deref()) {
            return Err(match io {
                Some(_) => observed::boundary(io, crate::wire::ErrorCode::Invalid,
                    "stock runtime differs from the original host or policy binding"),
                None => Error::Invalid("stock runtime differs from the harness's original host or policy binding".into()),
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "native runtime configuration has no original work",
            })?;
            volume.validate_with_native_work(work)?;
        } else { volume.validate()?; }
        #[cfg(target_arch = "wasm32")]
        volume.validate()?;
        if maximum_payload_bytes == 0 {
            return Err(match io {
                Some(_) => observed::boundary(io, crate::wire::ErrorCode::Invalid,
                    "stock runtime requires a positive payload limit"),
                None => Error::Invalid("stock runtime requires a positive payload limit".into()),
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        let native_configuration_custody = io.map(|io| {
            io.take_native_custody(io.owned_bytes()?)
        }).transpose()?;
        Ok(Self {
            stream, host, harness, filesystem, volume, verifier, signed, tools,
            maximum_payload_bytes, policy, interaction_owner: None, recursive_source: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_configuration_custody,
        })
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn original_lease_fence_with_io(
        lease: &crate::distributed::WorkLease, io: &super::PreparationIo,
    ) -> Result<LeaseFence> {
        io.retain_owned(std::mem::size_of::<LeaseFence>() as u64)?;
        observed::lease_fence(lease, Some(io))
    }

    /// Binds the existing conversation owner for task approval/question tickets.
    pub fn with_interaction_owner(
        mut self,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        private_volume: VolumeRef,
    ) -> Result<Self> {
        verifier.verify(&scope, &mut None)?;
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

    #[cfg(not(target_arch = "wasm32"))]
    fn interaction_host_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<FilesystemInteractionHost<P, A, O>> {
        let owner = self.interaction_owner.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported, reason: "task conversation owner is not bound",
        })?;
        FilesystemInteractionHost::new_with_native_work(
            self.stream.clone(), Arc::clone(&self.filesystem), owner.verifier.audience(),
            &owner.verifier, &owner.schemas, &owner.scope, &owner.volume,
            self.maximum_payload_bytes, work,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn tool_approval_ready_with_native_work(
        &self, context: &TaskContext, definition: &ToolDefinition,
        invocation: &crate::tool::ToolInvocation, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        let io = super::PreparationIo::from_native_work(work)?;
        if !context.scope().grants().contains(crate::contract::capability::INTERACTION_ROUTE) {
            return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Unauthorized, "task scope lacks interaction:route"));
        }
        let Some(policy) = &self.policy else { return Ok(false); };
        let task = context.durable_task_id().ok_or_else(|| observed::boundary(
            Some(&io), crate::wire::ErrorCode::Unauthorized, "approval has no task",
        ))?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        policy.supports_native_policy(work)?;
        let (identity, custody) = policy.identity_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        if admission.policy.as_ref() != Some(&identity) {
            return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Conflict, "approval policy differs from admission"));
        }
        let (operation, digest) = crate::runtime::tool_approval_binding_with_native_work(task, &identity, definition, invocation, work)?;
        let hashed = acyclic_fs::WorkCounters {
            bytes_hashed: b"harness/v2/task-interaction\0".len() as u64 + 32,
            ..Default::default()
        };
        io.preflight(hashed)?;
        io.charge(hashed)?;
        let id = crate::durable_host::task_interaction_id(task, operation);
        let host = self.interaction_host_with_native_work(work)?;
        let (request, custody) = host.read_request_with_native_work(id, work)?.await?;
        io.absorb_native_custody(custody)?;
        match request {
            None => return Ok(false),
            Some(Interaction::Approval { operation_id, action_digest, .. })
                if operation_id == invocation.operation_id && action_digest == digest => {}
            Some(_) => return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Conflict, "approval request changed")),
        }
        let read = host.read_with_native_work(id, work)?.await?;
        let ready = read.0.as_ref().and_then(|(_, resolution)| resolution.as_ref())
            .is_some_and(|resolution| resolution.outcome.is_terminal());
        drop(read);
        Ok(ready)
    }

    /// Inspects retained model/tool artifacts without constructing an executor or granting a passive task a lease.
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
                .verify_model_history(task, operation, page.iter(), &mut None)
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

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn model_approval_ready_with_native_work(
        &self, context: &TaskContext, turn: OperationId,
        input: &super::ModelTaskCommand, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        use crate::executor::{ExecutionEvent, ExecutionReplay, ModelPurpose};
        let io = super::PreparationIo::from_native_work(work)?;
        let scope = context.scope();
        if !scope.grants().contains("model:generate") {
            return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Unauthorized, "task scope lacks model:generate"));
        }
        let limits = scope.limits();
        if input.max_steps == 0 || u64::from(input.max_steps) > limits.model_steps as u64
            || scope.run_limits().max_steps.is_some_and(|bound| u64::from(input.max_steps) > bound as u64)
        {
            return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Unauthorized, "model wait exceeds admitted steps"));
        }
        let task = context.durable_task_id().ok_or_else(|| observed::boundary(
            Some(&io), crate::wire::ErrorCode::Unauthorized, "model wait has no task",
        ))?;
        let (journal, operation) = self.readonly_execution_journal_with_native_work(task, turn, work).await?;
        let mut replay = ExecutionReplay::new(operation);
        let mut latest = None;
        while let Some(page) = replay.next_page_with_io(journal.as_ref(), Some(&io)).await? {
            self.host.verify_model_history_with_native_work(task, operation, &page, work).await?;
            for record in page {
                if let ExecutionEvent::ModelStarted { step, purpose: ModelPurpose::Response, .. } = record.event {
                    if step >= input.max_steps {
                        return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Conflict, "retained model step exceeds command"));
                    }
                    latest = Some(latest.map_or(step, |previous: u32| previous.max(step)));
                }
            }
        }
        let Some(step) = latest else { return Ok(false); };
        let (_, records, custody) = crate::executor::replay_execution_with_native_work(
            journal.as_ref(), operation, limits.model_events_per_step.saturating_add(1),
            |event| matches!(event,
                ExecutionEvent::ModelStarted { step: recorded, purpose: ModelPurpose::Response, .. }
                | ExecutionEvent::Model { step: recorded, purpose: ModelPurpose::Response, .. } if *recorded == step),
            work,
        )?.await?;
        io.absorb_native_custody(custody)?;
        let (retained, custody) = crate::executor::retained_model_step_with_native_work(
            journal.as_ref(), &records, step, ModelPurpose::Response, limits, work,
        )?.await?;
        io.absorb_native_custody(custody)?;
        drop(records);
        if !retained.admission.completed() { return Err(Error::Indeterminate(operation)); }
        let request = retained.request.ok_or(Error::Indeterminate(operation))?;
        for message in &request.request().messages {
            verify_native_model_files(context, &message.content, work).await?;
        }
        for event in retained.events {
            if let crate::model::ModelEvent::ToolCall { call_id, name, arguments } = event {
                let invocation = crate::tool::ToolInvocation::for_model_call_with_native_work(operation, step, call_id, name, arguments, work)?;
                let (_, records, custody) = crate::executor::replay_execution_with_native_work(
                    journal.as_ref(), operation, 3,
                    |event| matches!(event,
                        ExecutionEvent::ToolStarted { step: recorded, call_id, .. }
                        | ExecutionEvent::ToolCompleted { step: recorded, call_id, .. }
                        | ExecutionEvent::ToolFailed { step: recorded, call_id, .. }
                        if *recorded == step && call_id == &invocation.call_id), work,
                )?.await?;
                io.absorb_native_custody(custody)?;
                if records.iter().any(|record| matches!(record.event, ExecutionEvent::ToolFailed { .. })) {
                    return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Invalid, "retained model tool failed"));
                }
                if records.iter().any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. })) {
                    let projection = crate::executor::completed_tool_projection_with_native_work(
                        journal.as_ref(), &records, step, &invocation, &request.request().tools,
                        context.scope().limits(), work,
                    )?.await?;
                    drop(projection);
                    continue;
                }
                if !records.is_empty() { return Err(Error::Indeterminate(invocation.operation_id)); }
                let pinned = request.request().tools.iter().find(|definition| definition.name == invocation.name)
                    .ok_or_else(|| observed::boundary(Some(&io), crate::wire::ErrorCode::Conflict, "approval tool is absent from model request"))?;
                let definition = self.tool_definition_with_native_work(&pinned.name, &pinned.revision, work)?;
                if definition != pinned {
                    return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Conflict, "approval tool differs from model request"));
                }
                let grant = work.display(&format_args!("tool:call:{}", pinned.name))?;
                if !scope.grants().contains(&grant.0) {
                    return Err(observed::boundary(Some(&io), crate::wire::ErrorCode::Unauthorized, "model wait lacks tool capability"));
                }
                drop(grant);
                return self.tool_approval_ready_with_native_work(context, definition, &invocation, work).await;
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
                .with_owned_interactions(task, Arc::new(TaskJournalInteractions {
                    task, journal,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_custody: None,
                }))
        } else {
            Ok(context)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_native_tool_interactions(
        &self, context: crate::runtime::ToolContext, task: TaskId,
        fence: &LeaseFence, turn: OperationId,
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::runtime::ToolContext> {
        if !context.native_metadata_matches(work)
            || !context.task().original_task_binding_matches(task, fence)
            || !journal.original_native_execution_work_matches(work)
            || !journal.original_task_execution_binding_matches(
                task, fence, turn, context.operation_id(), self.host.as_ref(),
            )
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "journal interaction route differs from original task",
            });
        }
        match self.native_journal_interactions(task, journal, work)? {
            Some(router) => context.with_native_interactions(router, work),
            None => Ok(context),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_journal_interactions(
        &self, task: TaskId, journal: Arc<FilesystemExecutionJournal<P, A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Arc<dyn InteractionRouter>>> {
        if self.interaction_owner.is_none() { return Ok(None); }
        if !journal.supports_native_execution(work)? {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "interaction journal has no original native owner",
            });
        }
        let io = super::PreparationIo::from_native_work(work)?;
        let bytes = std::mem::size_of::<TaskJournalInteractions>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
        let custody = io.take_native_custody(bytes)?;
        Ok(Some(Arc::new(TaskJournalInteractions { task, journal, native_custody: Some(custody) })))
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

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn tool_definition_with_native_work<'a>(
        &'a self, name: &str, revision: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a ToolDefinition> {
        self.tools.get_version_with_native_work(name, revision, work)?
            .map(|tool| &tool.definition)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::NotFound,
                reason: "pinned tool revision is absent",
            })
    }

    pub(super) fn tool_registry(&self) -> &ToolRegistry {
        &self.tools
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

    /// Constructs the ordinary retained scope/parent context with the exact current mailbox owner token.
    pub async fn task_context(&self, task: TaskId, fence: LeaseFence) -> Result<TaskContext> {
        let owner = self.host.journal_owner(task, fence).await?;
        let context = self.harness.durable_context(
            task, OperationId::from_bytes(task.into_bytes()),
        ).await?;
        context.with_owned_mailbox(task, Arc::new(TaskJournalMailbox {
            host: Arc::clone(&self.host), owner: Arc::new(owner),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        }))
    }

    /// Issues the stock runtime's genuine current-owner native task context.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn task_context_with_native_work(
        &self, task: TaskId, fence: &LeaseFence, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::runtime::NativeTaskContext>> {
        self.task_context_for_original_owner_with_native_work(task, fence, false, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn readonly_fork_preparation_with_native_work(
        &self, task: TaskId, query: &Arc<crate::runtime::NativeTaskContext>,
        admitted: &crate::resource_preparation::ResourcePreparationRequest,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Arc<crate::resource_preparation::TaskResourcePreparation>>> {
        let witness = self.host.task_preparation_readonly_witness_with_native_work(
            query, work,
        )?.await?;
        if witness.task() != task {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "readonly fork witness belongs to another task" });
        }
        let original = witness.original_preparation_request_with_native_work(query, work)?.await?;
        if &original.0 != admitted {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "readonly fork request differs from its original admission" });
        }
        let claim = self.filesystem.fork_allocation(
            &witness, query, &original.0, &self.volume, work,
        )?.await?;
        let Some(claim) = claim else { return Ok(None); };
        let (owner, request) = self.host.readonly_fork_journal_owner_with_native_work(
            &witness, query, claim, work,
        )?.await?;
        let preparation = owner.resource_preparation_reconciliation_with_native_work(
            query, request, work,
        )?.await?;
        Ok(Some(preparation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn task_mailbox_with_native_work(
        &self, owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<dyn OwnedTaskMailbox>> {
        if !owner.original_host_binding_matches(&self.host) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native mailbox owner belongs to another original host" });
        }
        let io = super::PreparationIo::from_native_work(work)?;
        let bytes = std::mem::size_of::<TaskJournalMailbox<P>>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok(Arc::new(TaskJournalMailbox {
            host: Arc::clone(&self.host), owner,
            native_custody: Some(custody),
        }))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn task_context_for_original_owner_with_native_work(
        &self, task: TaskId, fence: &LeaseFence, allow_settlement: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::runtime::NativeTaskContext>> {
        let owner = self.host.journal_owner_with_native_work(task, fence, work).await?;
        owner.verify_with_native_work(allow_settlement, work).await?;
        let owner = owner.into_native_shared(work)?;
        let mailbox = self.task_mailbox_with_native_work(owner, work)?;
        let operation = OperationId::from_bytes(task.into_bytes());
        let (context, custody) = if allow_settlement {
            self.harness.durable_context_for_reconciliation_with_native_work(
                task, operation, fence, mailbox, work,
            ).await?
        } else {
            self.harness.durable_context_for_dispatch_with_native_work(
                task, operation, fence, mailbox, work,
            ).await?
        };
        context.into_native_shared(custody, work)
    }

    /// Opens the original owner-fenced journal and returns its exact task/turn namespace.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn execution_journal_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        let operation = execution_operation_with_native_work(task, turn, work)?;
        let owner = self.host.journal_owner_with_native_work(task, fence, work).await?;
        let journal = FilesystemExecutionJournal::for_task_with_native_work(
            owner, operation, turn, Arc::clone(&self.filesystem), &self.volume,
            &self.verifier, &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes), work,
        )?;
        let journal = if admission.grants.contains(crate::contract::capability::INTERACTION_ROUTE) {
            if let Some(owner) = &self.interaction_owner {
                journal.with_interaction_owner_with_native_work(
                    &owner.verifier, &owner.schemas, &owner.scope, &owner.volume, work,
                )?
            } else { journal }
        } else { journal };
        Ok((journal.into_native_shared(work)?, operation))
    }

    /// Opens the protected original readonly journal for native witnesses.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn readonly_execution_journal_with_native_work(
        &self, task: TaskId, turn: OperationId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        let operation = execution_operation_with_native_work(task, turn, work)?;
        let owner = self.host.readonly_journal_owner_with_native_work(task, work).await?;
        let journal = FilesystemExecutionJournal::for_task_with_native_work(
            owner, operation, turn, Arc::clone(&self.filesystem), &self.volume,
            &self.verifier, &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes), work,
        )?;
        Ok((journal.into_native_shared(work)?, operation))
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

    /// Opens the canonical tool-effect workflow under the original caller reservation.
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

    /// Constructs the same owner-fenced journal with intrinsic native metadata custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn workflow_journal_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let io = super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        let owner = self.host.journal_owner_with_native_work(task, fence, work).await?;
        FilesystemWorkflowJournal::for_task_with_native_work(
            owner, Arc::clone(&self.filesystem), &self.volume, &self.verifier,
            &self.signed, self.maximum_payload_bytes.min(admission.limits.file_bytes), work,
        )?.into_native_shared(work)
    }

    /// Observes the original workflow under its protected readonly owner, including terminal records, without claiming a new lease.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn readonly_workflow_journal_with_native_work(
        &self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let io = super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        self.readonly_workflow_journal_observed(task, &admission, work).await?
            .into_native_shared(work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn readonly_workflow_journal_observed(
        &self, task: TaskId, admission: &TaskAdmissionRecord,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<FilesystemWorkflowJournal<P, A, O>> {
        let owner = self.host.readonly_journal_owner_with_native_work(task, work).await?;
        FilesystemWorkflowJournal::for_task_with_native_work(
            owner, Arc::clone(&self.filesystem), &self.volume, &self.verifier,
            &self.signed, self.maximum_payload_bytes.min(admission.limits.file_bytes), work,
        )
    }

    /// Reopens the admitted registered task and its bound workflow without requiring a typed definition at the worker boundary.
    pub async fn open_task(&self, task: TaskId, fence: LeaseFence) -> Result<ResumableTaskSession> {
        let journal = self.workflow_journal(task, fence.clone()).await?;
        self.harness
            .open_registered_task(task, fence, journal)
            .await
    }

    /// Opens the original registered task using its one issued shared context.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_task_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        context: Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ResumableTaskSession> {
        if !context.context().original_task_binding_matches(task, fence) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native task context differs from original task lease",
            });
        }
        let journal = self.workflow_journal_with_native_work(task, fence, work).await?;
        self.harness.open_registered_task_with_native_work(
            task, fence, journal, context, work,
        ).await
    }

    /// Performs one bounded discovery page followed by at most one owner-scoped admission and worker turn through the trusted command adapter.
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

    /// Polls and claims only the original admitted task, then uses the same fenced worker execution path.
    pub async fn run_operation(
        &self,
        worker: &crate::distributed::Worker,
        task: TaskId,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> Result<Option<TaskWorkerAttempt>> {
        self.run_operation_observed(worker, task, commands, maximum_transitions, None).await
    }

    async fn run_operation_observed(
        &self,
        worker: &crate::distributed::Worker,
        task: TaskId,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&super::PreparationIo>,
    ) -> Result<Option<TaskWorkerAttempt>> {
        if let Some(io) = io { io.check_cancellation()?; }
        if worker.id.trim().is_empty() || maximum_transitions == 0 {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid targeted worker allowance or identity",
                });
            }
            return Err(Error::Invalid("invalid targeted worker allowance or identity".into()));
        }
        let admission = self.admission_observed(task, io).await?;
        let terminal = if io.is_some() {
            self.outcome_observed(task, io).await?.is_some_and(|outcome|
                matches!(outcome, Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled))
        } else {
            self.host.observe_task(task).await?.outcome.is_some_and(|outcome|
                matches!(outcome, Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled))
        };
        if terminal { return Ok(None); }
        let pulled = deadline::run(
            &self.host, task, None, admission.run_limits.deadline_epoch_ms,
            admission.operation_id,
            async {
                self.poll_task_wake_observed(task, io).await?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    let work = io.original_native_work().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "worker pull requires original native work",
                    })?;
                    let (pull, custody) = self.host.pull_operation_with_native_work(worker, admission.operation_id, work).await?;
                    io.absorb_native_custody(custody)?;
                    return Ok(pull);
                }
                self.host.pull_operation(worker, admission.operation_id).await
            }, io, observed::pull_has_native_failure,
        ).await?;
        Ok(match pulled {
            crate::distributed::WorkPull::Idle => None,
            crate::distributed::WorkPull::Claimed(lease) => {
                Some(self.resume_task_observed(lease, commands, maximum_transitions, io).await)
            }
            crate::distributed::WorkPull::Unresolved { lease, error } => {
                Some(TaskWorkerAttempt::Unresolved { lease, error })
            }
        })
    }

    /// Resumes an explicit lease; every error returns the original attempt so callers can reconcile or verify it without silently releasing capacity.
    pub async fn resume_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> TaskWorkerAttempt {
        self.resume_task_observed(lease, commands, maximum_transitions, None).await
    }

    /// Resumes only this original lease and returns it unchanged on failure.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resume_task_with_native_work(
        &self,
        lease: crate::distributed::WorkLease,
        recovery_custody: acyclic_fs::NativeAllocation,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskWorkerAttempt, acyclic_fs::NativeAllocation)> {
        if !work.shares_ledger(recovery_custody.work()) {
            return Ok((TaskWorkerAttempt::Unresolved {
                lease, error: Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "recovered lease custody differs from original work",
                },
            }, recovery_custody));
        }
        let io = super::PreparationIo::from_native_custody(recovery_custody);
        let value = self.resume_task_observed(lease, commands, maximum_transitions, Some(&io)).await;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok((value, custody))
    }

    async fn resume_task_observed(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&super::PreparationIo>,
    ) -> TaskWorkerAttempt {
        match self.run_task_observed(&lease, commands, maximum_transitions, io).await {
            Ok(progress) => TaskWorkerAttempt::Progress(progress.with_lease(lease)),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        }
    }

    /// Inspects one page of a finite coordinator snapshot and polls its owned passive waits.
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

    /// Polls one durable passive wait and fills its existing authenticated wake slot when ready.
    pub async fn poll_task_wake(&self, task: TaskId) -> Result<bool> {
        self.poll_task_wake_observed(task, None).await
    }

    async fn poll_task_wake_observed(
        &self, task: TaskId, io: Option<&super::PreparationIo>,
    ) -> Result<bool> {
        use crate::workflow::WorkflowJournal as _;
        #[cfg(not(target_arch = "wasm32"))]
        let slot = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wake requires original observer",
            ))?;
            let (slot, custody) = self.host.workflow_suspension_with_native_work(task, work).await?;
            io.absorb_native_custody(custody)?;
            slot
        } else { self.host.workflow_suspension(task).await? };
        #[cfg(target_arch = "wasm32")]
        let slot = self.host.workflow_suspension(task).await?;
        let Some(slot) = slot else {
            return Ok(false);
        };
        let Some(waiting) = slot.waiting_command else {
            return Ok(false);
        };
        let admission = self.admission_observed(task, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let journal = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wake journal requires original observer",
            ))?;
            self.readonly_workflow_journal_observed(task, &admission, work).await?
        } else {
            FilesystemWorkflowJournal::new(
                self.host.workflow_stream(), Arc::clone(&self.filesystem),
                &admission.operation_id.to_string(), self.volume.clone(),
                self.verifier.clone(), self.signed.clone(),
                self.maximum_payload_bytes.min(admission.limits.file_bytes),
            )?
        };
        #[cfg(target_arch = "wasm32")]
        let journal = FilesystemWorkflowJournal::new(
            self.host.workflow_stream(), Arc::clone(&self.filesystem),
            &admission.operation_id.to_string(), self.volume.clone(),
            self.verifier.clone(), self.signed.clone(),
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let retained = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wake admission requires original observer",
            ))?;
            let (retained, custody) = journal.admission_with_native_work(work)?.await?;
            io.absorb_native_custody(custody)?;
            retained
        } else { journal.admission().await? };
        #[cfg(target_arch = "wasm32")]
        let retained = journal.admission().await?;
        let retained = retained.ok_or_else(|| observed::boundary(
            io, crate::wire::ErrorCode::Storage, "suspended workflow admission is absent",
        ))?;
        if retained.operation_id != admission.operation_id
            || retained.request_digest != observed::digest(&admission, io)?
            || retained.initial.machine != admission.machine
        {
            return Err(observed::boundary(io, crate::wire::ErrorCode::Unauthorized,
                "wake workflow differs from admitted task"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let record = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wake transition requires original observer",
            ))?;
            let (record, custody) = journal.observe_transition_with_native_work(slot.revision, work).await?;
            io.absorb_native_custody(custody)?;
            record
        } else { journal.observe_transition(slot.revision).await? };
        #[cfg(target_arch = "wasm32")]
        let record = journal.observe_transition(slot.revision).await?;
        if record.next.revision != slot.revision
            || record.next.machine != admission.machine
            || !matches!(record.transition.status, MachineStatus::Suspended)
        {
            return Err(observed::boundary(io, crate::wire::ErrorCode::Conflict,
                "wake checkpoint differs from suspension"));
        }
        let command = record
            .transition
            .commands
            .iter()
            .find(|command| command.operation_id == waiting)
            .ok_or_else(|| observed::boundary(io, crate::wire::ErrorCode::Conflict,
                "suspended command is absent from outbox"))?;
        #[cfg(not(target_arch = "wasm32"))]
        let context = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wait context requires original observer",
            ))?;
            let owner = self.host.readonly_journal_owner_with_native_work(task, work).await?
                .into_native_shared(work)?;
            let mailbox = self.task_mailbox_with_native_work(owner, work)?;
            let (context, custody) = self.harness.durable_context_for_read_with_native_work(
                task, admission.operation_id, mailbox, work,
            ).await?;
            WorkerContext::Native(context.into_native_shared(custody, work)?)
        } else {
            WorkerContext::Ordinary(self.harness.durable_context(task, admission.operation_id).await?)
        };
        #[cfg(target_arch = "wasm32")]
        let context = WorkerContext::Ordinary(self.harness.durable_context(task, admission.operation_id).await?);
        #[cfg(not(target_arch = "wasm32"))]
        let payload = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wait payload requires original observer",
            ))?;
            let body = context.context().read_file_with_native_work(&command.payload, work).await?;
            io.decode_preparation::<Value>(&body)?.into_retained()
        } else {
            crate::contract::json_from_slice(&context.context().read_file(&command.payload).await?)
                .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?
        };
        #[cfg(target_arch = "wasm32")]
        let payload = crate::contract::json_from_slice(&context.context().read_file(&command.payload).await?)
            .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?;
        if !self.commands().wait_ready_observed(context.context(), command, payload, io).await?
        {
            return Ok(false);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native wake publication requires original observer",
            ))?;
            self.host.wake_command_with_native_work(task, slot.revision, waiting, work).await?;
        } else { self.host.wake_command(task, slot.revision, waiting).await?; }
        #[cfg(target_arch = "wasm32")]
        self.host.wake_command(task, slot.revision, waiting).await?;
        Ok(true)
    }

    async fn suspend_command(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        revision: u64,
        command: OperationId,
        guard_execution: bool,
        io: Option<&super::PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "native suspension requires original observer",
            ))?;
            return if guard_execution {
                self.execution_journal_with_native_work(task, fence, command, work).await?.0
                    .suspend_workflow_command_if_quiescent_with_native_work(revision, command, work).await
            } else {
                self.host.suspend_workflow_command_with_native_work(
                    task, observed::clone_fence(fence, Some(io))?, revision, Some(command), work,
                ).await
            };
        }
        if guard_execution {
            self.execution_journal(task, fence.clone(), command)
                .await?
                .suspend_workflow_command_if_quiescent(revision, command)
                .await
        } else {
            self.host
                .suspend_workflow_command(task, fence.clone(), revision, Some(command))
                .await
        }
    }

    /// Drives an already-claimed lease through the registered machine.
    pub async fn run_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
    ) -> Result<TaskWorkerOutcome> {
        let progress = self.run_task_observed(&lease, commands, maximum_transitions, None).await?;
        Ok(progress.with_lease(lease))
    }

    /// Drives the original admitted lease without cloning or replacing it.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn run_task_with_native_work(
        &self,
        lease: crate::distributed::WorkLease,
        recovery_custody: acyclic_fs::NativeAllocation,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskWorkerAttempt, acyclic_fs::NativeAllocation)> {
        if !work.shares_ledger(recovery_custody.work()) {
            return Ok((TaskWorkerAttempt::Unresolved {
                lease, error: Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "recovered lease custody differs from original work",
                },
            }, recovery_custody));
        }
        let io = super::PreparationIo::from_native_custody(recovery_custody);
        let value = self.resume_task_observed(lease, commands, maximum_transitions, Some(&io)).await;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok((value, custody))
    }

    async fn run_task_observed(
        &self,
        lease: &crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&super::PreparationIo>,
    ) -> Result<WorkerTurn> {
        if let Some(io) = io { io.check_cancellation()?; }
        if maximum_transitions == 0 {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid worker transition allowance",
                });
            }
            return Err(Error::Invalid("invalid worker transition allowance".into()));
        }
        let operation_id = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation_id.into_bytes());
        let admission = self.admission_observed(task, io).await?;
        let fence = observed::lease_fence(lease, io)?;
        deadline::run(
            &self.host, task, Some(&fence), admission.run_limits.deadline_epoch_ms,
            operation_id, self.run_task_owned(lease, &fence, commands, maximum_transitions, io), io, |_| false,
        ).await
    }

    async fn run_task_owned(
        &self,
        lease: &crate::distributed::WorkLease,
        fence: &LeaseFence,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&super::PreparationIo>,
    ) -> Result<WorkerTurn> {
        let operation_id = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation_id.into_bytes());
        #[cfg(not(target_arch = "wasm32"))]
        let context = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "worker context requires original native work",
            })?;
            WorkerContext::Native(self.task_context_with_native_work(task, fence, work).await?)
        } else {
            WorkerContext::Ordinary(self.harness.durable_context(task, operation_id).await?)
        };
        #[cfg(target_arch = "wasm32")]
        let context = WorkerContext::Ordinary(self.harness.durable_context(task, operation_id).await?);
        #[cfg(not(target_arch = "wasm32"))]
        let mut session = if let WorkerContext::Native(context) = &context {
            let work = io.and_then(|io| io.original_native_work()).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native worker lost its original observer",
            })?;
            self.host.start_task_with_native_work(lease, work).await?;
            self.open_task_with_native_work(task, fence, Arc::clone(context), work).await?
        } else {
            self.host.start_task(lease).await?;
            self.open_task(task, fence.clone()).await?
        };
        #[cfg(target_arch = "wasm32")]
        let mut session = {
            self.host.start_task(lease).await?;
            self.open_task(task, fence.clone()).await?
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut wake_custody;
        #[cfg(not(target_arch = "wasm32"))]
        let mut wake = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native wake input requires original observer",
            })?;
            let (wake, custody) = self.host.workflow_input_with_native_work(task, fence, work).await?;
            wake_custody = Some(custody);
            wake
        } else {
            wake_custody = None;
            self.host.workflow_input(task, fence).await?
        };
        #[cfg(target_arch = "wasm32")]
        let mut wake = self.host.workflow_input(task, fence).await?;
        let mut advanced = 0;
        loop {
            let revision = session.checkpoint().revision;
            #[cfg(not(target_arch = "wasm32"))]
            let _transition_custody;
            #[cfg(not(target_arch = "wasm32"))]
            let mut transition = if let Some(io) = io {
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native transition requires original observer",
                })?;
                let (value, custody) = session.latest_transition_with_native_work(work).await?;
                _transition_custody = Some(custody);
                value
            } else {
                _transition_custody = None;
                session.latest_transition().await?
            };
            #[cfg(target_arch = "wasm32")]
            let mut transition = session.latest_transition().await?;
            if advanced == maximum_transitions
                && transition.as_ref().is_none_or(|value| {
                    matches!(value.status, MachineStatus::Suspended) && !value.commands.is_empty()
                })
            {
                return Ok(WorkerTurn::Yielded);
            }
            let mut command_input = None;
            if let Some(transition) = &mut transition {
                if !transition.commands.is_empty() {
                    match self
                        .dispatch_commands(task, &context, fence, &transition.commands, commands, io)
                        .await?
                    {
                        ready @ CommandDispatch::Ready {..} => command_input = Some(ready),
                        CommandDispatch::Pending {
                            command,
                            guard_execution,
                        } => {
                            self.suspend_command(task, fence, revision, command, guard_execution, io)
                                .await?;
                            return Ok(WorkerTurn::Suspended { task, revision });
                        }
                        CommandDispatch::Indeterminate => {
                            self.settle_observed(task, fence, Outcome::Indeterminate { operation_id }, io).await?;
                            return Ok(WorkerTurn::Reconciling);
                        }
                    }
                }
                let outcome = match &mut transition.status {
                    MachineStatus::Completed { value } => Some(Outcome::Succeeded(std::mem::take(value))),
                    MachineStatus::Failed { message } => Some(Outcome::Failed {
                        message: std::mem::take(message),
                    }),
                    MachineStatus::Suspended => None,
                };
                if let Some(outcome) = outcome {
                    self.settle_observed(task, fence, outcome, io).await?;
                    return Ok(WorkerTurn::Completed { task });
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            let mut wake_input_used = false;
            let input = if transition.is_none() {
                Value::Null
            } else if let Some(CommandDispatch::Ready {value,..}) = command_input.as_mut() {
                std::mem::take(value)
            } else if let Some((wake_revision, value)) = &mut wake {
                if *wake_revision == revision {
                    #[cfg(not(target_arch = "wasm32"))]
                    { wake_input_used = true; }
                    std::mem::take(value)
                } else {
                    self.suspend_observed(task, fence, revision, io).await?;
                    return Ok(WorkerTurn::Suspended { task, revision });
                }
            } else {
                self.suspend_observed(task, fence, revision, io).await?;
                return Ok(WorkerTurn::Suspended { task, revision });
            };
            if advanced == maximum_transitions {
                return Ok(WorkerTurn::Yielded);
            }
            if let Some(io) = io {
                let hashed = acyclic_fs::WorkCounters {
                    bytes_hashed: b"harness/v2/task-worker-step\0".len() as u64 + 24,
                    ..Default::default()
                };
                io.preflight(hashed)?;
                io.charge(hashed)?;
            }
            let mut digest = blake3::Hasher::new();
            digest.update(b"harness/v2/task-worker-step\0");
            digest.update(&task.into_bytes());
            digest.update(&revision.to_be_bytes());
            let mut bytes = [0; 16];
            bytes.copy_from_slice(&digest.finalize().as_bytes()[..16]);
            #[cfg(not(target_arch = "wasm32"))]
            let native_input = if let Some(CommandDispatch::Ready {
                custody: Some(primary), additional_custody, ..
            }) = command_input.as_mut() {
                for incoming in additional_custody {
                    primary.absorb(incoming).map_err(Error::from)?;
                }
                let key = native_step_key_with_custody(revision, primary)?;
                Some(key)
            } else {
                None
            };
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(key) = native_input {
                let Some(CommandDispatch::Ready { custody, .. }) = command_input.as_mut() else {
                    unreachable!("native key requires retained command custody");
                };
                let custody = custody.take().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native input has no original command custody",
                })?;
                session.step_with_native_work(
                    OperationId::from_bytes(bytes), key, input, custody,
                ).await?;
            } else if let Some(io) = io {
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native step requires original observer",
                })?;
                let mut custody = if wake_input_used {
                    wake_custody.take().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "native wake input has no original custody",
                    })?
                } else {
                    io.take_native_custody(0)?
                };
                if !work.shares_ledger(custody.work()) {
                    drop(input);
                    return Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                        custody: Some(custody),
                    });
                }
                let key = match native_step_key_with_custody(revision, &mut custody) {
                    Ok(key) => key,
                    Err(error) => { drop(input); return Err(error); }
                };
                session.step_with_native_work(
                    OperationId::from_bytes(bytes), key, input, custody,
                ).await?;
            } else {
                session.step(
                    OperationId::from_bytes(bytes),
                    IdempotencyKey::new(format!("task-step:{revision}"))?,
                    input,
                ).await?;
            }
            #[cfg(target_arch = "wasm32")]
            session.step(
                OperationId::from_bytes(bytes),
                IdempotencyKey::new(format!("task-step:{revision}"))?,
                input,
            ).await?;
            advanced += 1;
        }
    }

    async fn dispatch_commands(
        &self,
        task: TaskId,
        context: &WorkerContext,
        fence: &LeaseFence,
        outbox: &[WorkflowCommand],
        commands: &dyn TaskCommandHost,
        io: Option<&super::PreparationIo>,
    ) -> Result<CommandDispatch> {
        if outbox.len() != 1 && outbox.iter().any(|command|
            command.kind == super::MAIL_RECEIVE_TASK_COMMAND_KIND)
        {
            return Err(Error::Invalid(
                "mail receive must checkpoint alone before the next command".into(),
            ));
        }
        let mut results = CommandDispatch::empty_results();
        for command in outbox {
            self.owner_observed(task, fence, false, io).await?;
            if command.payload.descriptor().byte_length() > self.maximum_payload_bytes {
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "command exceeds payload limit",
                    });
                }
                return Err(Error::Invalid("command exceeds payload limit".into()));
            }
            #[cfg(not(target_arch = "wasm32"))]
            let progress = if let WorkerContext::Native(shared) = context {
                let io = io.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native dispatch lost its original observer",
                })?;
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native dispatch requires original work",
                })?;
                if !shared.context().original_task_binding_matches(task, fence) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "native command context differs from original task lease",
                    });
                }
                let body = shared.context().read_file_with_native_work(&command.payload, work).await?;
                let payload = io.decode_preparation::<Value>(&body)?.into_retained();
                drop(body);
                commands.execute_with_native_work(
                    Arc::clone(shared), observed::clone_fence(fence, Some(io))?,
                    observed::clone_command(command, Some(io))?, payload, work,
                )?.await
            } else {
                let context = context.context();
                let payload = crate::contract::json_from_slice(&context.read_file(&command.payload).await?)
                    .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
                commands.execute(context.clone(), fence.clone(), command.clone(), payload).await
            };
            #[cfg(target_arch = "wasm32")]
            let progress = {
                let context = context.context();
                let payload = crate::contract::json_from_slice(&context.read_file(&command.payload).await?)
                    .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
                commands.execute(context.clone(), fence.clone(), command.clone(), payload).await
            };
            #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
            let progress = match progress {
                Err(error) if error.is_native_failure() => return Err(error),
                progress => progress,
            };
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                match &progress {
                    Ok(TaskCommandProgress::Ready(_)) => {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "native command returned an unobserved ordinary ready value",
                        });
                    }
                    Ok(TaskCommandProgress::ReadyWithNativeCustody { custody, .. }) => {
                        let work = io.original_native_work().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "native result lost its original observer",
                        })?;
                        if !work.shares_ledger(custody.work()) {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "native command result differs from original work",
                            });
                        }
                    }
                    _ => {}
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Ok(TaskCommandProgress::ReadyWithNativeCustody { custody, .. }) = &progress {
                self.host.verify_native_command_result(task, fence, custody.work()).await?;
            } else {
                self.owner_observed(task, fence, false, io).await?;
            }
            #[cfg(target_arch = "wasm32")]
            self.host.verify_dispatch_owner(task, fence.clone()).await?;
            match progress {
                Ok(progress @ TaskCommandProgress::Ready(_)) => {
                    results.push_result(command.operation_id, progress)?;
                    if !results.input_within_limit(context.context().scope().limits().file_bytes)? {
                        return Ok(CommandDispatch::Indeterminate);
                    }
                }
                #[cfg(not(target_arch = "wasm32"))]
                Ok(progress @ TaskCommandProgress::ReadyWithNativeCustody { .. }) => {
                    results.push_result(command.operation_id, progress)?;
                    if !results.input_within_limit(context.context().scope().limits().file_bytes)? {
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
        results.finish_results()?;
        Ok(results)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn stock_executor_with_native_work(
        &self, task: TaskId, fence: &LeaseFence, operation: OperationId,
        context: &Arc<crate::runtime::NativeTaskContext>, model: &Model,
        provider: Arc<dyn ModelProvider>, pipeline: &ContextPipeline,
        journal: &Arc<FilesystemExecutionJournal<P, A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::executor::NativeStockExecutor> {
        if !context.context().original_task_binding_matches(task, fence) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "stock model differs from original task binding",
            });
        }
        let executor = crate::executor::StockExecutor::new_with_native_work(
            model, provider, pipeline, &self.tools, work,
        )?.with_limits(context.context().scope().limits())
            .with_task_context_with_native_work(
                Arc::clone(context), operation, self.host.clone(), task, fence, work,
            )?.with_tool_authority_with_native_work(
                context.context().scope(), self.policy.clone(), work,
            )?;
        let executor = match self.native_journal_interactions(task, Arc::clone(journal), work)? {
            Some(router) => executor.with_native_interactions(router, work)?,
            None => executor,
        };
        Ok(executor)
    }

    /// Constructs stock execution with mandatory shared accounting, retained task scope and a journal fenced by that same host.
    pub async fn stock_execution(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
    ) -> Result<FilesystemTaskExecution<P, A, O>> {
        self.stock_execution_with_output(task, fence, turn, model, provider, context, None).await
    }

    pub(super) async fn stock_execution_with_output(
        &self, task: TaskId, fence: LeaseFence, turn: OperationId,
        model: Model, provider: Arc<dyn ModelProvider>, context: ContextPipeline,
        output: Option<&VolumeRef>,
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
        let task_context = match output {
            Some(volume) => task_context.with_output_volume(volume)?,
            None => task_context,
        };
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

#[cfg(not(target_arch = "wasm32"))]
fn native_step_key_with_custody(
    revision: u64, input: &mut acyclic_fs::NativeAllocation,
) -> Result<IdempotencyKey> {
    let (key, mut guard) = match native_step_key(revision, input.work()) {
        Ok(value) => value,
        Err(Error::NativeWork { cause, custody }) => {
            let mut held = match input.split_off(input.bytes()) {
                Ok(held) => held,
                Err(_) => return Err(Error::NativeWork { cause, custody }),
            };
            if let Some(mut incoming) = custody {
                if !held.work().shares_ledger(incoming.work())
                    || held.absorb(&mut incoming).is_err()
                {
                    return Err(Error::NativeWork { cause, custody: Some(incoming) });
                }
            }
            return Err(Error::NativeWork { cause, custody: Some(held) });
        }
        Err(error) => return Err(error),
    };
    input.absorb(&mut guard).map_err(|cause| Error::NativeWork {
        cause, custody: Some(guard),
    })?;
    Ok(key)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_step_key(
    revision: u64, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(IdempotencyKey, acyclic_fs::NativeAllocation)> {
    use std::fmt::Write as _;
    let io = super::PreparationIo::from_native_work(work)?;
    let hold = io.reserve_decode(32)?;
    io.allocation_attempt(32)?;
    let mut key = String::new();
    key.try_reserve_exact(32).map_err(Error::Allocation)?;
    if key.capacity() > 32 {
        io.retain_owned((key.capacity() - 32) as u64)?;
    }
    write!(&mut key, "task-step:{revision}").map_err(|_| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "native task step identity formatting failed",
    })?;
    let retained = key.capacity() as u64;
    let key = IdempotencyKey::new(key)?;
    hold.into_retained();
    Ok((key, io.into_native_custody(retained)?))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn execution_operation_with_native_work(
    task: TaskId, turn: OperationId, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<OperationId> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: b"harness/v2/task-execution\0".len() as u64 + 32,
        ..Default::default()
    }).map_err(Error::from)?;
    Ok(execution_operation(task, turn))
}

pub fn execution_operation(task: TaskId, turn: OperationId) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-execution\0");
    hasher.update(&task.into_bytes());
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

/// A stock executor and its mandatory task-owned journal.
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
            self.executor.execute_progress(input, &self.journal), None, |_| false)
        .await
    }

    /// Observes retained model/tool attempts without starting another step.
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

#[cfg(not(target_arch = "wasm32"))]
async fn verify_native_model_files(
    context: &TaskContext, content: &crate::model::ModelContent,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    use crate::model::{ModelContentPart, ModelDataPart, ToolResultContent};
    for part in content.parts() {
        match part {
            ModelContentPart::File { file, policy } => verify_native_model_file(context, file, policy, work).await?,
            ModelContentPart::ToolResult { content: ToolResultContent::Parts { parts }, .. } => {
                for part in parts {
                    if let ModelDataPart::File { file, policy } = part {
                        verify_native_model_file(context, file, policy, work).await?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn verify_native_model_file(
    context: &TaskContext, file: &crate::conversation::FileRef,
    policy: &crate::model::FileProjectionPolicy, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let body = context.read_file_with_native_work(file, work).await?;
    drop(body);
    if let crate::model::FileProjectionPolicy::Native(policy) = policy
        && let Some(binding) = &policy.configuration
    {
        let body = context.read_file_with_native_work(&binding.configuration.content, work).await?;
        drop(body);
    }
    Ok(())
}
