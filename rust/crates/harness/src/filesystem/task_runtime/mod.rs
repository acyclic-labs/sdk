//! Optional persistent task composition using the existing runtime authorities.

use super::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemInteractionHost, FilesystemWorkflowJournal,
};
use crate::runtime::TaskStateProvider;
#[cfg(not(target_arch = "wasm32"))]
use crate::runtime::{InteractionRouter, OwnedTaskMailbox};
#[cfg(not(target_arch = "wasm32"))]
use crate::store::HistoryIo;
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::VolumeRef,
    core::{AuthorityVerifier, SchemaRegistry, Scope},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    durable_tool::ResumableToolRegistry,
    executor::{ExecutionProgress, StockExecutor, TurnInput, TurnOutput},
    interaction::Interaction,
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, ContentBindings, ResumableTaskSession, RuntimeScope, TaskAdmissionRecord,
        TaskContext, TaskRegistry, TaskRunLimits, ToolPolicy,
    },
    scheduler::{LeaseFence, SessionLimits},
    tool::{ToolDefinition, ToolRegistry},
    workflow::{MachineRegistry, MachineStatus, WorkflowCommand},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{StreamProvider, UnixMillisClock};
use serde_json::Value;
use std::sync::Arc;

mod deadline;
mod inspection;
mod reconciliation;
pub use inspection::TaskSourceInspection;
mod command_input;
pub(super) mod observed;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Trusted command adapter.
pub trait TaskCommandHost: acyclic_stream::ProviderPlatform {
    /// Cancellation of this invocation, independent of its retained task lease.
    fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        None
    }

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
        _context: Arc<crate::runtime::TaskContext>,
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
        _context: Arc<crate::runtime::TaskContext>,
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

pub(super) enum WorkerContext {
    Ordinary(Arc<TaskContext>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<crate::runtime::TaskContext>),
}

impl WorkerContext {
    pub(super) fn context(&self) -> &TaskContext {
        match self {
            Self::Ordinary(context) => context,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(context) => context,
        }
    }
}

/// Composes caller-owned persistent providers without creating another scheduler or registry.
pub struct FilesystemTaskRuntime<P, A, O> {
    stream: Arc<P>,
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
    recursive_source: Option<Arc<super::ConfiguredRecursiveToolSource<P, A, O>>>,
    task_files: Option<Arc<crate::task_volume::TaskFiles<P>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
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

#[cfg(not(target_arch = "wasm32"))]
impl TaskContext {
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    pub(crate) fn ordinary_journal_owner<P: StreamProvider + 'static>(
        &self,
    ) -> Result<Arc<crate::durable_host::TaskJournalOwner<P>>> {
        if self.preparation_io()?.is_some() {
            return Err(Error::Unsupported(
                "ordinary task files require ordinary ownership".into(),
            ));
        }
        let mailbox = self
            .owned_mailbox
            .as_ref()
            .and_then(|mailbox| {
                (mailbox.as_ref() as &dyn std::any::Any).downcast_ref::<TaskJournalMailbox<P>>()
            })
            .ok_or_else(|| {
                Error::Unauthorized("task files require the original journal owner".into())
            })?;
        mailbox.owner.require_task_context_binding(self, None)?;
        Ok(Arc::clone(&mailbox.owner))
    }

    /// Reuses the original journal owner under its issued ledger and current fence.
    pub fn journal_owner<P: StreamProvider + 'static>(
        &self,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::durable_host::TaskJournalOwner<P>>> {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let (_, original_fence) = self.owned_task_binding(Some(&io))?;
        let mailbox = self.owned_mailbox.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native task context has no original mailbox owner",
        })?;
        let mailbox = (mailbox.as_ref() as &dyn std::any::Any)
            .downcast_ref::<TaskJournalMailbox<P>>()
            .filter(|mailbox| {
                original_fence == fence && mailbox.owner.native_shared_work_matches(work)
            })
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native task owner differs from its original runtime, fence or ledger",
            })?;
        Ok(Arc::clone(&mailbox.owner))
    }
}

impl<P: StreamProvider + 'static> crate::runtime::OwnedTaskMailbox for TaskJournalMailbox<P> {
    fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.owner.invocation_cancellation()
    }

    fn task_id(&self) -> TaskId {
        self.owner.task_id()
    }
    fn validate_scope(
        &self,
        scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work())
            && !self
                .native_custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "mailbox metadata differs from original work",
            });
        }
        self.owner.as_ref().require_mail_scope(scope, io)
    }
    fn task_binding(&self) -> (TaskId, &LeaseFence) {
        self.owner.as_ref().task_binding()
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn admission(&self) -> (&TaskAdmissionRecord, &crate::workflow::WorkflowAdmission) {
        (self.owner.admission(), self.owner.workflow_admission())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn claim_model_dispatch<'a>(
        &'a self,
        context: &'a TaskContext,
        dispatch: crate::model::ModelDispatch,
        fence: &'a LeaseFence,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(self.host.claim_model_from_context(context, dispatch, fence))
    }
    fn verify_owner<'a>(
        &'a self,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let mut history = io.map(crate::executor::PreparationIo::history_io);
            self.owner
                .verify(
                    false,
                    &mut history
                        .as_mut()
                        .map(|io| io as &mut dyn crate::store::HistoryIo),
                )
                .await
        })
    }
    fn file_publication_condition(
        &self,
    ) -> BoxFuture<'_, Result<(acyclic_stream::CommitCondition, Option<u64>)>> {
        Box::pin(async move {
            #[cfg(not(target_arch = "wasm32"))]
            if self.native_custody.is_some() {
                return Err(Error::Unsupported(
                    "task file publication requires ordinary execution".into(),
                ));
            }
            let condition = self
                .owner
                .condition(crate::distributed::JournalWrite::Fresh, &mut None)
                .await?;
            Ok((
                condition,
                self.owner.admission().run_limits.deadline_epoch_ms,
            ))
        })
    }
    fn send<'a>(
        &'a self,
        recipient: TaskId,
        operation: OperationId,
        payload: crate::conversation::FileRef,
    ) -> BoxFuture<'a, Result<crate::conversation::FileRef>> {
        Box::pin(async move {
            let (task, fence) = self.owner.as_ref().task_binding();
            self.host
                .send_mail(task, fence, recipient, operation, payload, None)
                .await
        })
    }
    fn inbox<'a>(
        &'a self,
        after: u64,
        maximum: usize,
    ) -> BoxFuture<'a, Result<Vec<crate::scheduler::InboxItem>>> {
        Box::pin(async move {
            self.owner.as_ref().verify(true, &mut None).await?;
            let (task, _) = self.owner.as_ref().task_binding();
            let items = self.host.inbox(task, after, maximum).await?;
            self.owner.as_ref().verify(true, &mut None).await?;
            Ok(items)
        })
    }
    fn read_received<'a>(
        &'a self,
        receive_operation: OperationId,
        item: &'a crate::scheduler::InboxItem,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            work.charge(acyclic_fs::WorkCounters::default())?;
            if !self
                .native_custody
                .as_ref()
                .is_some_and(|guard| guard.work().shares_allocation_context(work))
                || !self.owner.native_shared_work_matches(work)
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "mailbox differs from its original SDK metadata and owner ledger",
                });
            }
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.host
                .read_received_mail_owned(self.owner.as_ref(), receive_operation, item, io)
                .await
        })
    }
}

mod content;
mod context;
mod dispatch;
mod execution;
mod interactions;
mod journals;
mod open;
mod run;
mod wake;
mod worker;
pub use execution::FilesystemTaskExecution;
#[cfg(not(target_arch = "wasm32"))]
mod model_files;
#[cfg(not(target_arch = "wasm32"))]
use model_files::verify_native_model_files;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
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

    pub(super) fn tool_definition(
        &self,
        name: &str,
        revision: &str,
        io: Option<&super::PreparationIo>,
    ) -> Result<&ToolDefinition> {
        self.tools
            .get(name, Some(revision), io)?
            .map(|tool| &tool.definition)
            .ok_or_else(|| {
                super::interaction_host::interaction_boundary(
                    io,
                    crate::wire::ErrorCode::NotFound,
                    "pinned tool revision is absent",
                )
            })
    }

    pub(crate) fn tool_registry(&self) -> &ToolRegistry {
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
        self.task_context_with_cancellation(task, fence, None).await
    }

    pub(crate) async fn task_context_with_cancellation(
        &self,
        task: TaskId,
        fence: LeaseFence,
        cancellation: Option<&acyclic_fs::CancellationToken>,
    ) -> Result<TaskContext> {
        let (owner, scope) = self
            .host
            .ordinary_journal_owner_with_scope(task, &fence)
            .await?;
        let scope = scope.with_replayed_extensions(owner.admission().extensions.clone())?;
        let owner = match cancellation {
            Some(cancellation) => owner.with_invocation_cancellation(cancellation),
            None => owner,
        };
        let owner = Arc::new(owner);
        let context = self
            .harness
            .open_durable_context(
                task,
                OperationId::from_bytes(task.into_bytes()),
                None,
                crate::runtime::IssuedContextPurpose::Read,
                None,
                Some(scope),
            )
            .await?;
        let mut context =
            Arc::try_unwrap(context).unwrap_or_else(|_| unreachable!("fresh context is unshared"));
        context.owned_mailbox = Some(Arc::new(TaskJournalMailbox {
            host: Arc::clone(&self.host),
            owner: Arc::clone(&owner),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        }));
        context.bind_ordinary_created_owner(owner)?;
        if context.scope().resource_proofs().is_empty()
            && context.scope().grants().contains(
                &self
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            )
        {
            let context = context
                .into_shared()?
                .with_output_bindings(ContentBindings {
                    reader: FilesystemContentVerifier::new(
                        self.filesystem.clone(),
                        &self.verifier,
                        &self.signed,
                        self.maximum_payload_bytes,
                        None,
                    )?,
                    writer: Some(FilesystemContentPublisher::new(
                        self.filesystem.clone(),
                        &self.volume,
                        &self.verifier,
                        &self.signed,
                        self.maximum_payload_bytes,
                        None,
                    )?),
                })
                .await?;
            return Ok(Arc::try_unwrap(context)
                .unwrap_or_else(|_| unreachable!("fresh context is unshared")));
        }
        Ok(context)
    }

    /// Issues the stock runtime's genuine current-owner native task context.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn task_context_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::runtime::TaskContext>> {
        self.task_context_for_original_owner_with_native_work(task, fence, false, work)
            .await
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_step_key_with_custody(
    revision: u64,
    input: &mut acyclic_fs::NativeAllocation,
) -> Result<IdempotencyKey> {
    let (key, mut guard) = match native_step_key(revision, input.work()) {
        Ok(value) => value,
        Err(Error::NativeWork { cause, custody }) => {
            let Ok(mut held) = input.split_off(input.bytes()) else {
                return Err(Error::NativeWork { cause, custody });
            };
            if let Some(mut incoming) = custody
                && (!held.work().shares_ledger(incoming.work())
                    || held.absorb(&mut incoming).is_err())
            {
                return Err(Error::NativeWork {
                    cause,
                    custody: Some(incoming),
                });
            }
            return Err(Error::NativeWork {
                cause,
                custody: Some(held),
            });
        }
        Err(error) => return Err(error),
    };
    input
        .absorb(&mut guard)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: Some(guard),
        })?;
    Ok(key)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_step_key(
    revision: u64,
    work: &acyclic_fs::NativeMaterializationWork,
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
    hold.into_retained()?;
    Ok((key, io.into_native_custody(retained)?))
}

pub(super) fn execution_operation(
    task: TaskId,
    turn: OperationId,
    io: Option<&super::PreparationIo>,
) -> Result<OperationId> {
    if let Some(io) = io {
        let hashed = acyclic_fs::WorkCounters {
            bytes_hashed: b"harness/v2/task-execution\0".len() as u64 + 32,
            ..Default::default()
        };
        io.preflight(hashed)?;
        io.charge(hashed)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-execution\0");
    hasher.update(&task.into_bytes());
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}
