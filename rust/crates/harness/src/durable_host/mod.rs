//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::contract::capability;
use crate::{
    Admission, BatchId, Error, IdempotencyKey, InteractionId, OperationId, Outcome, Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{DistributedCoordinator, SchedulerPayloadStore},
    registry::ComponentIdentity,
    runtime::{
        BatchCancellationReport, BatchCancellationStatus, DurableBatchRequest,
        DurableEffectObserver, DurableTaskHost, InputKey, RuntimeScope, TaskAdmissionRecord,
        TaskChildrenPage, TaskRegistry, TaskStateProvider, ToolPolicy, read_granted,
        require_descendant_grant, validate_policy_identity, validate_task_schemas,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    workflow::MachineRegistry,
};
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

type TimerEvent = acyclic_actors::timers::TimerRecord<TaskId, OperationId>;

#[test]
fn sdk_and_native_timer_bytes_match() -> Result<()> {
    let record = acyclic_actors::timers::TimerRecord::new(
        "01010101-0101-0101-0101-010101010101".into(),
        "02020202-0202-0202-0202-020202020202".into(),
        u64::MAX,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let bytes = record
        .encode()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let event: TimerEvent = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(crate::contract::canonical_json_bytes(&event)?, bytes);
    Ok(())
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

fn canonical_payload<T: Serialize>(
    value: &T,
    maximum: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<OwnedBytes> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return io.encode(value, maximum, true);
    }
    let _ = io;
    let bytes = crate::contract::canonical_json_bytes(value)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid(
            "durable payload exceeds its byte limit".into(),
        ));
    }
    Ok(OwnedBytes::from(bytes))
}

fn operation_io(
    work: &Option<&mut dyn crate::store::HistoryIo>,
) -> Result<Option<crate::executor::PreparationIo>> {
    if work.is_none() {
        return Ok(None);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        return crate::executor::PreparationIo::from_native_work(native).map(Some);
    }
    Err(Error::Unsupported(
        "operation requires its original IO context".into(),
    ))
}

/// Stream-backed admission and observation for one durable owner. More
/// specialized wait, mail, interaction, and effect providers can be composed
/// around this boundary without giving callers a coordinator handle.
pub struct CoordinatorTaskHost<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    stream: Arc<P>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    owner: Authority,
    owner_scope: Scope,
    verifier: AuthorityVerifier,
    root_scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    pub(crate) clock: Arc<dyn UnixMillisClock>,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_binding: Option<ComponentIdentity>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
    #[cfg(feature = "filesystem")]
    resource_creation: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
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
    created: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    read_proof: Option<OperationSpec>,
    tool_output_schema: Option<Value>,
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
#[cfg(feature = "filesystem")]
mod created;
#[cfg(feature = "filesystem")]
mod created_consume;
#[cfg(feature = "filesystem")]
mod created_scope;
#[cfg(feature = "filesystem")]
use created_consume::consume_receipts;
mod dispatch;
mod host;
mod journal;
mod lifecycle;
mod mail;
mod observations;
#[cfg(feature = "filesystem")]
mod owner_access;
#[cfg(feature = "filesystem")]
mod preparation;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod readonly;
mod received_mail;
#[cfg(feature = "filesystem")]
mod release;
#[cfg(feature = "filesystem")]
mod source_graph;
#[cfg(feature = "filesystem")]
mod sources;
mod timer;
mod volumes;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use readonly::TaskAdmissionReadonlyWitness;
mod workflow;
pub use volumes::TaskVolumeSnapshot;

#[cfg(all(test, feature = "filesystem"))]
pub(crate) mod tests;
