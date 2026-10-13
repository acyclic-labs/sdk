//! Optional persistent task composition using the existing runtime authorities.

use super::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemInteractionHost, FilesystemWorkflowJournal,
};
use crate::runtime::TaskStateProvider;
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::VolumeRef,
    core::{AuthorityVerifier, SchemaRegistry, Scope},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    durable_tool::ResumableToolRegistry,
    executor::{ExecutionJournal, StockExecutor, StockTurnProgress, TurnInput, TurnOutput},
    interaction::Interaction,
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, ContentBindings, DurableTaskHost, InteractionRouter, OwnedTaskMailbox,
        ResumableTaskSession, RuntimeScope, TaskAdmissionRecord, TaskContext, TaskRegistry,
        TaskRunLimits, ToolPolicy,
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
#[cfg(not(target_arch = "wasm32"))]
mod native_mailbox;
pub(super) mod observed;
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
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    Pending(usize),
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
    Ordinary(TaskContext),
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<crate::runtime::TaskContext>),
}

impl WorkerContext {
    pub(super) fn context(&self) -> &TaskContext {
        match self {
            Self::Ordinary(context) => context,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(context) => context.as_ref(),
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

mod approval;
mod configuration;
mod dispatch;
mod identity;
mod journals;
mod mailbox;
mod service;
mod stock;
mod wake;
mod worker;

pub(super) use identity::execution_operation;
use identity::*;
