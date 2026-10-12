//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::contract::capability;
#[cfg(feature = "filesystem")]
use crate::scheduler::LeaseFence;
use crate::store::guarded_boundary;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::store::history::HistoryIo as _;
use crate::{
    Admission, BatchId, Capabilities, EffectId, Error, IdempotencyKey, InteractionId, OperationId,
    Outcome, Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{DistributedCoordinator, SchedulerPayloadStore},
    registry::ComponentIdentity,
    runtime::{
        BatchCancellationReport, BatchCancellationStatus, DurableBatchRequest,
        DurableEffectObserver, DurableTaskHost, InputKey, RuntimeScope, TaskAdmissionRecord,
        TaskChildrenPage, TaskRegistry, TaskStateProvider, ToolPolicy, read_granted,
        require_descendant_grant, validate_task_schemas,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    workflow::MachineRegistry,
};
#[cfg(test)]
use crate::{executor::ExecutionJournal, interaction::Interaction};
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
    schema_revision: u32,
    route_revision: u32,
    payload: FileRef,
}

const MAIL_SCHEMA_REVISION: u32 = 1;
const MAIL_ROUTE_REVISION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MailLocation {
    sender: TaskId,
    message_id: OperationId,
    recipient: TaskId,
    schema_revision: u32,
    route_revision: u32,
    sequence: u64,
    value_digest: [u8; 32],
}

const MAX_MAIL_LOCATION_BYTES: usize = 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerEvent {
    task_id: TaskId,
    operation_id: OperationId,
    deadline_unix_ms: u64,
}

/// Derives the canonical interaction identity for one task's original operation.
#[must_use]
pub fn task_interaction_id(task_id: TaskId, operation_id: OperationId) -> InteractionId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-interaction\0");
    hasher.update(&task_id.into_bytes());
    hasher.update(&operation_id.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
}

/// Derives the same original ticket identity after funding the actual SDK hash.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub fn task_interaction_id_with_native_work(
    task_id: TaskId,
    operation_id: OperationId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<InteractionId> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: b"harness/v2/task-interaction\0".len() as u64 + 32,
        ..Default::default()
    })?;
    Ok(task_interaction_id(task_id, operation_id))
}

fn worker_key_with_work(
    kind: &str,
    operation: OperationId,
    fence: &crate::scheduler::LeaseFence,
    revision: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<IdempotencyKey> {
    let digest = crate::contract::canonical_json_digest_with_work(
        fence,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    let mut key = String::new();
    if let Some(io) = work.as_deref_mut() {
        let digits = revision.checked_ilog10().map_or(1, |digits| digits + 1) as usize;
        let length = 115 + kind.len() + digits;
        io.admit_copy(length as u64)?;
        io.reserve_text(&mut key, length)?;
        io.copy_bytes(length as u64)?;
    }
    use std::fmt::Write as _;
    write!(
        &mut key,
        "task-worker:{kind}:{operation}:{}:{revision}",
        blake3::Hash::from_bytes(digest).to_hex()
    )
    .expect("writing into String");
    IdempotencyKey::new(key)
}

/// Stream-backed admission and observation for one durable owner.
pub struct CoordinatorTaskHost<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    stream: std::sync::Arc<P>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    owner_scope: Scope,
    verifier: AuthorityVerifier,
    root_scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    clock: Arc<dyn UnixMillisClock>,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_binding: Option<ComponentIdentity>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
    resource_creation: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
}

struct CreatedTaskPublicationLease<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    scope: Scope,
    verifier: AuthorityVerifier,
    operation: OperationId,
    fence: Option<crate::scheduler::LeaseFence>,
    clock: Arc<dyn UnixMillisClock>,
    deadline_epoch_ms: Option<u64>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Original root binding cache, shared by its issued read witnesses.
#[cfg(feature = "filesystem")]
pub struct RootBootstrapBindingWitness {
    proof: std::sync::OnceLock<(
        crate::store::VerifiedRootBinding,
        Option<crate::runtime::NativeCustody>,
    )>,
    gate: tokio::sync::Mutex<()>,
    pending: std::sync::OnceLock<(OperationId, [u8; 32], [u8; 32])>,
}

/// Canonical admission reader for protected preparation bootstrap.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct TaskAdmissionReadonlyWitness<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    admission: TaskAdmissionRecord,
    scratch: Option<crate::resource_preparation::ChildScratchCreationProof>,
    _native_custody: acyclic_fs::NativeAllocation,
}

/// One worker's exact task lease, bound to the coordinator's own Stream provider.
#[cfg(feature = "filesystem")]
pub struct TaskJournalOwner<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    stream: std::sync::Arc<P>,
    task_id: TaskId,
    fence: crate::scheduler::LeaseFence,
    original_admission_digest: [u8; 32],
    workflow: crate::workflow::WorkflowAdmission,
    output_schema: Value,
    input_grants: crate::Capabilities,
    input_created: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    input_agent: Option<crate::AgentId>,
    root_binding: Option<Arc<RootBootstrapBindingWitness>>,
    deadline_epoch_ms: Option<u64>,
    input_limits: crate::conversation::Limits,
    extensions: Option<crate::core::ExtensionAdmission>,
    invocation_cancellation: Option<acyclic_fs::CancellationToken>,
    #[cfg(not(target_arch = "wasm32"))]
    readonly_original: Option<OperationSpec>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_shared: bool,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

mod admission;
mod batch;
mod bootstrap;
mod configuration;
mod control;
mod headless;
mod interfaces;
mod journal;
mod mail;
mod metadata;
mod models;
mod owner;
mod owner_construction;
mod preparation;
mod readonly;
mod received_mail;
mod recovery;
mod resources;
#[cfg(feature = "filesystem")]
mod source_authorization;
#[cfg(feature = "filesystem")]
mod source_selection;
mod spawner;
mod state_provider;
mod timer;
mod workflow;
pub use headless::TaskVolumeSnapshot;
pub(crate) use mail::received_copy_operation;

#[cfg(test)]
pub(crate) mod tests;
