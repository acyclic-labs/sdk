//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::contract::capability;
use crate::{
    Admission, BatchId, EffectId, Error, IdempotencyKey, InteractionId, OperationId, Outcome,
    Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{DistributedCoordinator, SchedulerPayloadStore},
    interaction::{Interaction, InteractionOutcome},
    registry::ComponentIdentity,
    runtime::{
        BatchCancellationReport, BatchCancellationStatus, DurableBatchRequest,
        DurableEffectObserver, DurableTaskHost, InputKey, RuntimeScope, TaskAdmissionRecord,
        TaskChildrenPage, TaskRegistry, TaskStateProvider, ToolPolicy, check_tool_approval,
        read_granted, require_descendant_grant, validate_policy_identity, validate_task_schemas,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    tool::ToolDefinition,
    workflow::MachineRegistry,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::OwnedBytes;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamError, StreamProvider, UnixMillisClock,
};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MailEvent {
    sender: TaskId,
    recipient: TaskId,
    message_id: OperationId,
    payload: FileRef,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerEvent {
    task_id: TaskId,
    operation_id: OperationId,
    deadline_unix_ms: u64,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchCancellation {
    batch_id: BatchId,
    group_id: crate::GroupId,
}

/// Derives the stable task interaction identity on the supplied IO context.
pub fn task_interaction_id(
    task_id: TaskId,
    operation_id: OperationId,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<InteractionId> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.hashed_bytes((b"harness/v2/task-interaction\0".len() + 32) as u64)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-interaction\0");
    hasher.update(&task_id.into_bytes());
    hasher.update(&operation_id.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(InteractionId::from_bytes(bytes))
}

fn worker_key(
    kind: &str,
    operation: OperationId,
    fence: &crate::scheduler::LeaseFence,
    revision: u64,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<IdempotencyKey> {
    let digest = crate::executor::json::canonical_json_digest(
        fence,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    IdempotencyKey::new(crate::executor::preparation_format(
        format_args!(
            "task-worker:{kind}:{operation}:{}:{revision}",
            blake3::Hash::from_bytes(digest).to_hex()
        ),
        work,
    )?)
}

/// Stream-backed admission and observation for one durable owner. More
/// specialized wait, mail, interaction, and effect providers can be composed
/// around this boundary without giving callers a coordinator handle.
pub struct CoordinatorTaskHost<P> {
    coordinator: Mutex<DistributedCoordinator<P>>,
    stream: Arc<P>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    owner: Authority,
    owner_scope: Scope,
    verifier: AuthorityVerifier,
    root_scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    clock: Arc<dyn UnixMillisClock>,
    policy: Option<Arc<dyn ToolPolicy>>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
}

/// One worker's exact task lease, bound to the coordinator's own Stream provider.
/// Journal constructors use this binding to atomically fence their publications.
#[cfg(feature = "filesystem")]
pub struct TaskJournalOwner<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    fence: crate::scheduler::LeaseFence,
    workflow: crate::workflow::WorkflowAdmission,
    admission: Arc<TaskAdmissionRecord>,
    input_agent: Option<crate::AgentId>,
    invocation_cancellation: Option<acyclic_fs::CancellationToken>,
    root_binding: std::sync::OnceLock<
        Arc<(
            crate::store::VerifiedRootBinding,
            Option<crate::runtime::NativeCustody>,
        )>,
    >,
    root_choice: std::sync::OnceLock<(OperationId, [u8; 32], [u8; 32])>,
    root_gate: Mutex<()>,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

mod admission;
mod admission_read;
#[cfg(feature = "filesystem")]
mod authorization;
mod batch;
mod control;
mod dispatch;
mod host;
mod journal;
mod lifecycle;
mod mail;
mod observations;
#[cfg(feature = "filesystem")]
mod owner_access;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod readonly;
#[cfg(feature = "filesystem")]
mod release;
mod timer;
mod volumes;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use readonly::TaskAdmissionReadonlyWitness;
mod workflow;
pub use volumes::TaskVolumeSnapshot;

#[cfg(test)]
pub(crate) mod tests;
