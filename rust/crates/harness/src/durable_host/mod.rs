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

#[cfg(feature = "filesystem")]
pub(crate) struct WorkflowWaitPage {
    pub(crate) next_revision: u64,
    pub(crate) through_revision: u64,
    pub(crate) events_read: u32,
    pub(crate) tasks: Vec<TaskId>,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchCancellation {
    batch_id: BatchId,
    group_id: crate::GroupId,
}

pub(crate) fn task_interaction_id(task_id: TaskId, operation_id: OperationId) -> InteractionId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-interaction\0");
    hasher.update(&task_id.into_bytes());
    hasher.update(&operation_id.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
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
    IdempotencyKey::new(crate::executor::preparation_format(format_args!(
        "task-worker:{kind}:{operation}:{}:{revision}",
        blake3::Hash::from_bytes(digest).to_hex()
    ), work)?)
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
    pub(crate) machines: MachineRegistry,
    pub(crate) clock: Arc<dyn UnixMillisClock>,
    policy: Option<Arc<dyn ToolPolicy>>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
    creation_providers: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Immutable volume selection authenticated by an allocation receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct TaskVolumeSnapshot {
    /// Authenticated volume identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: crate::conversation::VolumeRef,
    /// Original allocation generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
    pub generation: crate::resources::GenerationRef,
}

/// Canonical root binding and its single publication choice.
#[cfg(feature = "filesystem")]
pub struct RootBootstrapBindingWitness {
    proof: std::sync::OnceLock<(crate::store::VerifiedRootBinding, crate::NativeCustody)>,
    gate: Mutex<()>,
    pending: std::sync::OnceLock<(OperationId, [u8; 32], [u8; 32])>,
}

/// One worker's exact task lease, bound to the coordinator's own Stream provider.
/// Journal constructors use this binding to atomically fence their publications.
#[cfg(feature = "filesystem")]
pub struct TaskJournalOwner<P> {
    pub(crate) host: Arc<CoordinatorTaskHost<P>>,
    fence: crate::scheduler::LeaseFence,
    workflow: crate::workflow::WorkflowAdmission,
    workflow_output: Option<Value>,
    root_binding: Option<Arc<RootBootstrapBindingWitness>>,
    created_access: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    pub(crate) admission: Arc<TaskAdmissionRecord>,
    pub(crate) invocation_stop: Option<acyclic_fs::CancellationToken>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<acyclic_fs::NativeAllocation>,
}

mod admission;
mod batch;
mod dispatch;
mod deadline;
mod host;
mod journal;
mod lifecycle;
mod mail;
pub(crate) use mail::received_copy_operation;
mod timer;
mod workflow;
#[cfg(feature = "filesystem")]
mod tool_workflow;
#[cfg(feature = "filesystem")]
mod root_binding;

#[cfg(test)]
pub(crate) mod tests;

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for TimerEvent {}
