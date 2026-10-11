//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::contract::capability;
use crate::{
    Admission, BatchId, Capabilities, EffectId, Error, IdempotencyKey, InteractionId, OperationId, Outcome,
    Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{ChildOperationPageRequest, DistributedCoordinator, SchedulerPayloadStore},
    durable_tool::DurableToolRunner,
    executor::ExecutionJournal,
    interaction::{Interaction, InteractionOutcome},
    registry::ComponentIdentity,
    runtime::{
        BatchCancellationReport, BatchCancellationStatus, DurableBatchRequest,
        DurableEffectObserver, DurableTaskHost, InputKey, RuntimeScope, TaskAdmissionRecord,
        TaskChild, TaskChildrenPage, TaskRegistry, ToolPolicy, check_tool_approval, read_granted,
        require_descendant_grant, validate_policy_identity, validate_task_schemas,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    tool::ToolDefinition,
    workflow::MachineRegistry,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
    UnixMillisClock,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;

mod headless;
pub use headless::TaskVolumeSnapshot;
#[cfg(feature = "filesystem")]
mod source_authorization;

/// Exact canonical send acknowledgement, authenticated by the atomic mail receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct MailSendAck {
    /// Retained sender admission.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub sender: TaskId,
    /// Exact retained recipient.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub recipient: TaskId,
    /// Exact idempotent send identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Exact donor-authorized payload accepted under the retained route and byte limits.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub payload: FileRef,
}

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

impl MailEvent {
    fn encode(&self, maximum_bytes: u64) -> Result<Vec<u8>> {
        // Count the whole typed envelope, including destination and revisions,
        // before allocating canonical bytes or deriving publication digests.
        crate::contract::validate_json_byte_bound(self, maximum_bytes)?;
        crate::contract::canonical_json_bytes(self)
    }

    fn decode(bytes: &[u8], recipient: TaskId) -> Result<Self> {
        let value: Value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&value)?.as_slice() != bytes {
            return Err(Error::Storage("mail event is not canonical JSON".into()));
        }
        let event: Self = serde_json::from_value(value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if event.recipient != recipient
            || event.schema_revision != MAIL_SCHEMA_REVISION
            || event.route_revision != MAIL_ROUTE_REVISION
        {
            return Err(Error::Storage(
                "mail route or schema revision differs".into(),
            ));
        }
        Ok(event)
    }
}

// Derived receipt only: the original mailbox bytes and shared commit remain
// authoritative. Its fixed metadata permits a bounded two-record lookup.
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

fn worker_key(
    kind: &str,
    operation: OperationId,
    fence: &crate::scheduler::LeaseFence,
    revision: u64,
) -> Result<IdempotencyKey> {
    let digest = crate::contract::canonical_json_digest(fence)?;
    IdempotencyKey::new(format!(
        "task-worker:{kind}:{operation}:{}:{revision}",
        blake3::Hash::from_bytes(digest).to_hex()
    ))
}

/// Stream-backed admission and observation for one durable owner. More
/// specialized wait, mail, interaction, and effect providers can be composed
/// around this boundary without giving callers a coordinator handle.
pub struct CoordinatorTaskHost<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    stream: StreamClient<P>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    owner: Authority,
    owner_scope: Scope,
    verifier: AuthorityVerifier,
    root_scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    clock: Arc<dyn UnixMillisClock>,
    tools: Option<Arc<DurableToolRunner>>,
    policy: Option<Arc<dyn ToolPolicy>>,
    interactions: Option<Arc<dyn ExecutionJournal>>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
    resource_creation: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
}

struct CreatedTaskPublicationLease<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    owner: Authority,
    scope: Scope,
    verifier: AuthorityVerifier,
    operation: OperationId,
    fence: Option<crate::scheduler::LeaseFence>,
    clock: Arc<dyn UnixMillisClock>,
    deadline_epoch_ms: Option<u64>,
}

impl<P: StreamProvider> crate::resource_preparation::CreatedTaskLease
    for CreatedTaskPublicationLease<P>
{
    fn verify(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh().await?;
            let operation = coordinator.observe_operation(
                &self.owner, &self.scope, &self.verifier, self.operation,
            )?;
            let fence = self.fence.as_ref().ok_or_else(|| {
                Error::Conflict("created resource task has no current execution owner".into())
            })?;
            crate::scheduler::require_execution_owner(&operation, fence, false)?;
            if self.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                coordinator.cancel_operation(
                    &self.owner, &self.scope, &self.verifier, self.operation,
                    IdempotencyKey::new(format!("task-cancel:{}", self.operation))?, true,
                ).await?;
                return Err(Error::Indeterminate(self.operation));
            }
            Ok(())
        })
    }
    fn fence(&self) -> Result<crate::scheduler::LeaseFence> {
        self.fence.clone().ok_or_else(|| {
            Error::Conflict("created resource task has no execution reservation".into())
        })
    }
}

/// Preserves an actual native quota/cancellation failure separately from an
/// original authenticated Harness-owner rejection, without formatting either.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Debug, thiserror::Error)]
pub enum NativeRootVerificationError {
    #[error("{0}")]
    Native(#[from] acyclic_fs::NativeTreeMaterializationError),
    #[error("{0}")]
    Owner(#[from] Error),
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeRootVerificationError {
    /// Only internal observer markers may recover a captured native failure.
    /// A later cleanup failure must not replace an original owner error.
    pub(crate) fn finish<T>(
        result: std::result::Result<T,Self>,
        native: Option<acyclic_fs::NativeTreeMaterializationError>,
    ) -> std::result::Result<T,Self> {
        let marker = result.as_ref().err().is_some_and(|error| match error {
            Self::Owner(Error::Indeterminate(operation)) => operation.into_bytes() == [0;16],
            Self::Owner(Error::Storage(message)) => message.is_empty(),
            _ => false,
        });
        if marker {
            if let Some(error) = native { return Err(Self::Native(error)); }
        }
        result
    }
}

#[cfg(feature = "filesystem")]
#[derive(Default)]
struct TaskRootBinding {
    proof: std::sync::OnceLock<crate::store::VerifiedRootBinding>,
    gate: tokio::sync::Mutex<()>,
    pending: std::sync::OnceLock<(OperationId,[u8;32],[u8;32])>,
}

/// One worker's exact task lease, bound to the coordinator's own Stream provider.
/// Journal constructors use this binding to atomically fence their publications.
#[cfg(feature = "filesystem")]
pub struct TaskJournalOwner<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    stream: StreamClient<P>,
    task_id: TaskId,
    fence: crate::scheduler::LeaseFence,
    original_admission_digest: [u8; 32],
    invocation_cancellation: Option<acyclic_fs::CancellationToken>,
    workflow: crate::workflow::WorkflowAdmission,
    maximum_payload_bytes: u64,
    output_schema: Value,
    input_grants: crate::Capabilities,
    input_created: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    input_agent: Option<crate::AgentId>,
    input_is_root: bool,
    root_binding: Option<Arc<TaskRootBinding>>,
    deadline_epoch_ms: Option<u64>,
    input_limits: crate::conversation::Limits,
    extensions: Option<crate::core::ExtensionAdmission>,
    extension_schemas: Option<crate::core::SchemaRegistry>,
    extension_runtime: Option<Arc<crate::extension::ExtensionRuntime>>,
}

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Returns the original task identity without exposing its lease or signer.
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Checks the original task's current authority before a fresh dispatch.
    pub async fn verify_dispatch(&self) -> Result<()> {
        self.verify(false).await
    }

    /// Checks retained settlement authority, including cancellation recovery.
    pub async fn verify_settlement(&self) -> Result<()> {
        self.verify(true).await
    }

    pub(crate) fn require_mail_scope(&self, scope: &RuntimeScope) -> Result<()> {
        if scope.grants() != &self.input_grants || scope.limits() != self.input_limits {
            return Err(Error::Unauthorized("mailbox scope differs from retained admission".into()));
        }
        Ok(())
    }

    pub(crate) const fn input_limits(&self) -> crate::conversation::Limits {
        self.input_limits
    }

    pub(crate) fn validate_input_file(&self, file: &FileRef) -> Result<()> {
        self.input_limits.validate_file(file)?;
        if !read_granted(&self.input_grants, file)? {
            return Err(Error::Unauthorized(
                "task cannot read the execution input file".into(),
            ));
        }
        Ok(())
    }

    /// Verifies only a kernel-authenticated created-volume input. The ordinary
    /// journal input resolver remains authoritative for every other volume.
    pub(crate) async fn verify_created_input_with_io(
        &self, file: &FileRef, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        match io {
            Some(io) => self.validate_input_file_with_io(file, io)?,
            None => self.validate_input_file(file)?,
        }
        let mut reader = None;
        if let Some(access) = &self.input_created {
            for binding in &access.references {
                if let Some(io) = io {
                    let step = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                    io.preflight(step)?;
                    io.charge(step)?;
                }
                if &binding.file == file {
                    reader = Some(&binding.reader);
                    break;
                }
            }
            if reader.is_none() {
                for binding in &access.content {
                    if let Some(io) = io {
                        let step = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                        io.preflight(step)?;
                        io.charge(step)?;
                    }
                    if &binding.volume == file.volume() {
                        reader = Some(&binding.binding.reader);
                        break;
                    }
                }
            }
        }
        let Some(reader) = reader else { return Ok(false); };
        let bytes_hold;
        let bytes = match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                self.verify_with_work(true, &mut Some(&mut borrowed)).await?;
                let bytes = crate::store::history::HistoryIo::read_content(
                    &mut borrowed, reader.as_ref(), file).await?;
                bytes_hold = Some(io.guard_owned(bytes.capacity() as u64)?);
                crate::store::history::HistoryIo::hashed_bytes(&mut borrowed, bytes.len() as u64)?;
                bytes
            }
            None => {
                bytes_hold = None;
                self.verify(true).await?;
                reader.read(file).await?
            }
        };
        file.descriptor().verify(&bytes)?;
        match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                self.verify_with_work(true, &mut Some(&mut borrowed)).await?;
            }
            None => self.verify(true).await?,
        }
        drop(bytes);
        drop(bytes_hold);
        Ok(true)
    }

    pub(crate) fn validate_input_file_with_io(
        &self, file: &FileRef, io: &crate::filesystem::PreparationIo,
    ) -> Result<()> {
        self.input_limits.validate_file(file)?;
        if !crate::runtime::read_granted_measured(&self.input_grants, file, io)? {
            return Err(Error::Unauthorized("task cannot read the execution input file".into()));
        }
        Ok(())
    }

    pub(crate) fn validate_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
    ) -> Result<(&crate::core::SchemaRegistry, [u8; 32])> {
        let (schemas, expected) = binding.verify_original(
            self.extensions.as_ref(),
            self.extension_schemas.as_ref(),
            self.extension_runtime.as_deref(),
        )?;
        let file = &binding.configuration.content;
        self.validate_input_file(file)?;
        if file.descriptor().byte_length() > self.input_limits.render_bytes {
            return Err(Error::Invalid(
                "native options exceed render byte limit".into(),
            ));
        }
        Ok((schemas, expected))
    }

    /// Verifies options against the original authenticated admission and linked code.
    /// This is read authority, so it remains valid during original-fence settlement.
    pub(crate) async fn verify_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
    ) -> Result<()> {
        let (schemas, expected) = self.validate_native_configuration(binding)?;
        let bytes = self
            .host
            .reader
            .read(&binding.configuration.content)
            .await?;
        schemas.verify_configuration_binding(
            &binding.configuration,
            expected,
            &bytes,
            self.input_limits,
        )
    }

    pub(crate) const fn input_grants(&self) -> &crate::Capabilities {
        &self.input_grants
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn validate_native_configuration_with_io(
        &self, binding: &crate::model::NativeConfigurationBinding,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<(&crate::core::SchemaRegistry, [u8; 32])> {
        let (schemas, expected) = binding.verify_original_with_io(
            self.extensions.as_ref(), self.extension_schemas.as_ref(),
            self.extension_runtime.as_deref(), io)?;
        let file = &binding.configuration.content;
        self.validate_input_file_with_io(file, io)?;
        if file.descriptor().byte_length() > self.input_limits.render_bytes {
            return Err(Error::Invalid("native options exceed render byte limit".into()));
        }
        Ok((schemas, expected))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_native_configuration_with_io(
        &self, binding: &crate::model::NativeConfigurationBinding,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<()> {
        let (schemas, expected) = self.validate_native_configuration_with_io(binding, io)?;
        let bytes_hold;
        let mut borrowed = io.history_io();
        let bytes = crate::store::history::HistoryIo::read_content(
            &mut borrowed, self.host.reader.as_ref(), &binding.configuration.content).await?;
        bytes_hold = io.guard_owned(bytes.capacity() as u64)?;
        let result = schemas.verify_configuration_binding_with_io(
            &binding.configuration, expected, &bytes, self.input_limits, io);
        drop(bytes);
        drop(bytes_hold);
        result
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) fn require_volume_grant(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
    ) -> Result<()> {
        let grant = volume.capability(operation)?;
        if !self.input_grants.contains(&grant) {
            return Err(Error::Unauthorized(format!("task scope lacks {grant}")));
        }
        Ok(())
    }

    /// Checks the original cached grant with bounded canonical identity work.
    /// No Stream or filesystem reads are claimed by this pure authorization.
    pub fn require_volume_grant_measured(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
        let result = work.preflight().and_then(|()|
            self.require_volume_grant_with_work(volume, operation, &mut work));
        work.finish(result)
    }

    fn require_volume_grant_with_work(
        &self, volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        io: &mut dyn crate::store::history::HistoryIo,
    ) -> Result<()> {
            volume.validate()?;
            let remaining = io.remaining()?;
            let encoded = crate::contract::canonical_json_bytes_bounded(volume,
                crate::contract::JsonBounds {
                    maximum_bytes: remaining.bytes_encoded,
                    maximum_items: remaining.bytes_encoded,
                    maximum_depth: 256,
                }, &mut |charge| io.codec_charge(charge),
            )?;
            io.retain_owned((encoded.capacity() - encoded.len()) as u64)?;
            if io.remaining()?.bytes_hashed < encoded.len() as u64 {
                return Err(Error::Invalid("volume grant hash exceeds admitted work".into()));
            }
            let digest = blake3::hash(&encoded);
            io.receive_work(acyclic_fs::WorkCounters {
                bytes_hashed: encoded.len() as u64, ..acyclic_fs::WorkCounters::default()
            })?;
            let retained = encoded.capacity() as u64;
            drop(encoded);
            io.release_owned(retained);
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes:64, items:0 })?;
            let hex = digest.to_hex();
            let prefix: &[u8] = match operation {
                crate::conversation::VolumeOperation::Read => b"volume:read:",
                crate::conversation::VolumeOperation::Write => b"volume:write:",
            };
            io.codec_charge(crate::contract::JsonCodecCharge::Copy { bytes:(prefix.len() + hex.len()) as u64 })?;
            let mut grant = [0; 77];
            grant[..prefix.len()].copy_from_slice(prefix);
            grant[prefix.len()..prefix.len()+hex.len()].copy_from_slice(hex.as_bytes());
            let grant = std::str::from_utf8(&grant[..prefix.len()+hex.len()])
                .map_err(|_| Error::Invalid("volume grant identity is not UTF-8".into()))?;
            if !self.input_grants.contains(grant) {
                return Err(Error::Unauthorized("task scope lacks required volume grant".into()));
            }
            Ok(())
    }

    /// Obtains only the original currently selected scratch allocation. This is
    /// not a result-volume proof, root allocation fallback or native leaf receipt.
    pub fn child_scratch_creation_proof(&self) -> Result<crate::resource_preparation::ChildScratchCreationProof> {
        let binding = self.input_created.as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no created scratch allocation".into()))?
            .current_content()?
            .ok_or_else(|| Error::Unauthorized("task has no current scratch allocation".into()))?;
        let proof = binding.scratch_creation.as_ref()
            .ok_or_else(|| Error::Unauthorized("current scratch is not a verified filesystem allocation".into()))?;
        if proof.original_task() != self.task_id || Some(proof.agent()) != self.input_agent
            || proof.volume() != &binding.volume
        {
            return Err(Error::Unauthorized("scratch allocation belongs to another original task".into()));
        }
        Ok(proof.clone())
    }

    pub(crate) async fn authorize_created_scratch_settlement_measured(
        &self, proof: &crate::resource_preparation::ChildScratchCreationProof,
        budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let mut work = crate::store::history::HistoryWork::new(budget,cancellation);
        let result = async {
            work.preflight()?;
            if !self.child_scratch_creation_proof()?.same_identity(proof) {
                return Err(Error::Unauthorized("scratch settlement substituted allocation evidence".into()));
            }
            self.require_volume_grant_with_work(proof.volume(), crate::conversation::VolumeOperation::Write, &mut work)?;
            self.condition_with_work(crate::distributed::JournalWrite::Settlement, &mut Some(&mut work)).await?;
            Ok(())
        }.await;
        work.finish(result)
    }

    pub(crate) fn require_interaction_grant(&self) -> Result<()> {
        if !self.input_grants.contains(capability::INTERACTION_ROUTE) {
            return Err(Error::Unauthorized(
                "task scope lacks interaction:route".into(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn require_interaction_grant_measured(
        &self, budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = (|| {
            let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
            io.preflight(work)?;
            let verified = self.require_interaction_grant();
            io.charge(work)?;
            verified
        })();
        match result {
            Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    pub(crate) fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence) {
        (self.task_id, &self.fence)
    }

    pub(crate) async fn suspend_quiescent_execution(
        &self,
        execution: OperationId,
        tail: u64,
        revision: u64,
        command: OperationId,
    ) -> Result<()> {
        self.host
            .suspend_workflow_command_guarded(
                self.task_id,
                self.fence.clone(),
                revision,
                Some(command),
                Some((execution, tail)),
            )
            .await
    }
    /// Consumes only the original journal's sealed current Pending witness and
    /// settled execution tail. Uncertain work keeps its original reservation.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn release_quiescent_execution_with_native_work(
        &self, execution: OperationId, tail: u64,
        pending: &crate::filesystem::WorkflowReleaseWitness<'_,P>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> std::result::Result<bool,NativeRootVerificationError> {
        let mut io = crate::store::history::native_work_io(work)?;
        let result: Result<bool> = async {
            if pending.task_binding() != (self.task_id,&self.fence) {
                return Err(Error::Unauthorized("pending workflow belongs to another task or fence".into()));
            }
            let mut coordinator = self.host.coordinator.lock().await;
            coordinator.refresh_with_work(&mut Some(&mut io)).await?;
            let operation = coordinator.observe_operation_ref_with_work(
                &self.host.owner,&self.host.owner_scope,&self.host.verifier,
                self.operation_id(),&mut Some(&mut io),
            )?;
            crate::scheduler::require_execution_owner(operation,&self.fence,true)?;
            if operation.cancellation_requested {
                return Err(Error::Conflict("read-only close cannot settle task cancellation".into()));
            }
            if operation.phase == crate::scheduler::OperationPhase::Reconciling {
                return Ok(false);
            }
            let revision = operation.revision;
            // The canonical fence bytes are the original worker-key hash
            // input, not a serialization performed solely to count cloning.
            let remaining = crate::store::history::HistoryIo::remaining(&io)?;
            let encoded = crate::contract::canonical_json_bytes_bounded(
                &self.fence,crate::contract::JsonBounds {
                    maximum_bytes:remaining.bytes_encoded,maximum_items:remaining.items_examined,
                    maximum_depth:256,
                },&mut |charge| crate::store::history::HistoryIo::codec_charge(&mut io,charge),
            )?;
            crate::store::history::HistoryIo::hashed_bytes(&mut io,encoded.len() as u64)?;
            let digest = blake3::hash(&encoded);
            let encoded_length = encoded.len() as u64;
            drop(encoded);
            crate::store::history::HistoryIo::release_owned(&mut io,encoded_length)?;
            let mut key = String::new();
            let digits = if revision == 0 {1} else {revision.ilog10() as usize+1};
            crate::store::history::HistoryIo::reserve_text(&mut io,&mut key,122+digits)?;
            use std::fmt::Write as _;
            write!(&mut key,"task-worker:release:{}:{}:{revision}",self.operation_id(),digest.to_hex())
                .map_err(|_| Error::Invalid("lease release key formatting failed".into()))?;
            let fence = crate::scheduler::LeaseFence {
                reservation_id:crate::store::history::HistoryIo::clone_text(&mut io,&self.fence.reservation_id)?,
                placement:crate::store::history::HistoryIo::clone_text(&mut io,&self.fence.placement)?,
            };
            coordinator.apply_if_execution_quiescent_with_work(
                IdempotencyKey::new(key)?,
                crate::scheduler::SchedulerEvent::LeaseReleased {operation_id:self.operation_id(),fence},
                execution,tail,Some((pending.path(),pending.tail())),&mut Some(&mut io),
            ).await?;
            Ok(true)
        }.await;
        NativeRootVerificationError::finish(result.map_err(Into::into),io.take_native_failure())
    }

    pub(crate) fn validate_storage(
        &self,
        verifier: &AuthorityVerifier,
        maximum_payload_bytes: u64,
    ) -> Result<()> {
        if verifier.audience() != &self.host.owner
            || maximum_payload_bytes > self.maximum_payload_bytes
        {
            return Err(Error::Unauthorized(
                "journal storage exceeds admitted task authority or limits".into(),
            ));
        }
        Ok(())
    }

    /// Returns the initial workflow identity derived from the retained task
    /// admission and its registered machine, rather than caller-selected state.
    #[must_use]
    pub const fn workflow_admission(&self) -> &crate::workflow::WorkflowAdmission {
        &self.workflow
    }

    pub(crate) fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation.as_ref()
    }

    /// Binds this already-sealed owner to the caller's actual live operation
    /// token. This neither grants authority nor turns remote task intent into
    /// a push cancellation signal; original journal guards remain authoritative.
    pub fn with_invocation_cancellation(mut self, cancellation: &acyclic_fs::CancellationToken) -> Self {
        self.invocation_cancellation = Some(cancellation.clone());
        self
    }

    /// Derives a tool-effect workflow from this same original task reservation.
    /// The independently registered tool may not adopt a foreign task or lease.
    pub(crate) async fn for_tool_workflow(
        mut self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<Self> {
        self.invocation_cancellation = Some(cancellation.clone());
        if context.task().durable_task_id() != Some(self.task_id)
            || context.task().owned_task_fence()? != self.fence
            || invocation.operation_id == self.operation_id()
        {
            return Err(Error::Unauthorized("tool workflow differs from original task owner".into()));
        }
        self.require_mail_scope(context.task().scope())?;
        let admission = self.host.observe_admission(self.task_id).await?;
        if context.task().scope().run_limits() != admission.run_limits
            || context.task().scope().extensions() != admission.extensions.as_ref()
        {
            return Err(Error::Unauthorized("tool workflow changed original admission policy".into()));
        }
        self.verify(false).await?;
        self.workflow = tool.workflow_admission(context, invocation)?;
        self.output_schema = tool.definition.output_schema.clone();
        Ok(self)
    }

    pub(crate) fn validate_workflow_record(
        &self,
        record: &crate::workflow::WorkflowRecord,
    ) -> Result<()> {
        if record.prior.machine != self.workflow.initial.machine {
            return Err(Error::Conflict(
                "workflow implementation differs from task admission".into(),
            ));
        }
        let (next, transition) = self.host.machines.step(&record.prior, &record.input)?;
        if next != record.next || transition != record.transition {
            return Err(Error::Conflict(
                "workflow transition differs from pinned machine".into(),
            ));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &transition.status {
            crate::tool::validate_value(&self.output_schema, value, "task output")?;
        }
        Ok(())
    }

    pub(crate) fn stream(&self) -> StreamClient<P> {
        self.stream.clone()
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id.into_bytes())
    }

    async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.condition_with_work(write, &mut None).await
    }

    async fn condition_with_work(
        &self,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self.deadline_epoch_ms.is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            if let Some(cancellation) = &self.invocation_cancellation {
                cancellation.cancel();
            }
            // The measured binding preflight rejects without pretending that
            // unobserved admission reads or durable cancellation were measured.
            if work.is_none() {
                self.host.expire_task_deadline(self.task_id, Some(self.fence.clone())).await?;
            }
            return Err(Error::Indeterminate(self.operation_id()));
        }
        let result = self.host
            .coordinator
            .lock()
            .await
            .journal_condition_with_work(
                &self.host.owner,
                &self.host.owner_scope,
                &self.host.verifier,
                self.operation_id(),
                &self.fence,
                write,
                work,
            )
            .await;
        if result.is_err() {
            if let Some(cancellation) = &self.invocation_cancellation {
                cancellation.cancel();
            }
        }
        result
    }

    pub(crate) async fn verify_dispatch_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.verify_with_work(false, work).await
    }

    pub(crate) async fn verify_with_work(
        &self, allow_settlement: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let write = if allow_settlement { crate::distributed::JournalWrite::Settlement }
            else { crate::distributed::JournalWrite::Fresh };
        self.condition_with_work(write, work).await.map(|_| ())
    }

    /// Checks the original owner fence on the caller's same native ledger.
    /// Fresh dispatch additionally requires current created scratch WRITE.
    /// Settlement grants neither namespace writes nor a new effect.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn verify_with_native_work(
        &self, allow_settlement: bool, work: &acyclic_fs::NativeMaterializationWork,
    ) -> std::result::Result<(),NativeRootVerificationError> {
        let mut io = crate::store::history::native_work_io(work)?;
        let result: Result<()> = async {
            if !allow_settlement && let Some(binding) = self.input_created.as_ref()
                .map(|access| access.current_content()).transpose()?.flatten()
            {
                if binding.binding.writer.is_none() {
                    return Err(Error::Unauthorized("current created scratch has no WRITE custody".into()));
                }
                self.require_volume_grant_with_work(&binding.volume, crate::conversation::VolumeOperation::Write, &mut io)?;
            }
            self.verify_with_work(allow_settlement, &mut Some(&mut io)).await
        }.await;
        NativeRootVerificationError::finish(result.map_err(Into::into),io.take_native_failure())
    }

    /// Authorizes exact retained volume custody on that same native-work ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn require_volume_grant_with_native_work(
        &self, volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> std::result::Result<(),NativeRootVerificationError> {
        let mut io = crate::store::history::native_work_io(work)?;
        let result: Result<()> = (|| {
            if operation == crate::conversation::VolumeOperation::Write {
                if let Some(binding) = self.input_created.as_ref()
                    .and_then(|access| access.content.iter().find(|binding| &binding.volume == volume))
                {
                    if binding.binding.writer.is_none() {
                        return Err(Error::Unauthorized("created volume has no retained WRITE custody".into()));
                    }
                }
            }
            self.require_volume_grant_with_work(volume, operation, &mut io)
        })();
        NativeRootVerificationError::finish(result.map_err(Into::into),io.take_native_failure())
    }

    /// Reads the original owned task's actual terminal result on the same
    /// native ledger. This read grants no execution/publication lease: terminal
    /// tasks no longer have one. Nonterminal or uncertain work returns `None`.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn outcome_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> std::result::Result<(Option<crate::Outcome<FileRef>>,acyclic_fs::NativeAllocation),NativeRootVerificationError> {
        let mut io = crate::store::history::native_work_io(work)?;
        let result = async {
            let mut coordinator = self.host.coordinator.lock().await;
            coordinator.refresh_with_work(&mut Some(&mut io)).await?;
            let operation = coordinator.observe_operation_ref_with_work(
                &self.host.owner, &self.host.owner_scope, &self.host.verifier,
                self.operation_id(), &mut Some(&mut io),
            )?;
            if operation.phase != crate::scheduler::OperationPhase::Terminal {
                return Ok((None,work.retain_owned(0)?));
            }
            let Some(outcome) = &operation.outcome else {
                return Err(Error::Invalid("terminal original task lacks its outcome".into()).into());
            };
            let output = match outcome {
                crate::Outcome::Succeeded(reference) => {
                    let bytes = crate::filesystem::UploadRecord::logical_owned_bytes(reference)?;
                    let custody = work.retain_owned(bytes)?;
                    crate::store::history::HistoryIo::codec_charge(&mut io,
                        crate::contract::JsonCodecCharge::Copy {bytes})?;
                    (crate::Outcome::Succeeded(reference.clone()),custody)
                },
                crate::Outcome::Failed {message} => {
                    work.charge(acyclic_fs::WorkCounters {
                        bytes_copied:message.len() as u64,
                        allocation_operations:u64::from(!message.is_empty()),..Default::default()
                    })?;
                    let custody = work.retain_owned(message.len() as u64)?;
                    (crate::Outcome::Failed {message:message.clone()},custody)
                },
                crate::Outcome::Cancelled => (crate::Outcome::Cancelled,work.retain_owned(0)?),
                crate::Outcome::Indeterminate {..} => {
                    return Err(Error::Invalid("uncertain task outcome cannot be terminal".into()).into());
                },
            };
            Ok((Some(output.0),output.1))
        }.await;
        NativeRootVerificationError::finish(result,io.take_native_failure())
    }

    /// Verifies the original task's current fresh-dispatch fence using the
    /// existing SDK-call ledger, not inferred provider-internal OS work.
    pub async fn verify_dispatch_measured(
        &self,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        self.verify_write_measured(crate::distributed::JournalWrite::Fresh, budget, cancellation).await
    }

    /// Retained effects may settle under their original fence after cancellation.
    pub async fn verify_settlement_measured(
        &self,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        self.verify_write_measured(crate::distributed::JournalWrite::Settlement, budget, cancellation).await
    }

    async fn verify_write_measured(
        &self,
        write: crate::distributed::JournalWrite,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.condition_with_work(write, &mut Some(&mut work)).await.map(|_| ())
        }.await;
        work.finish(result)
    }

    /// Authenticates one immutable resource request under this ordinary task's
    /// current uncancelled lease. The returned token grants no resource access.
    pub async fn resource_preparation(
        self: &Arc<Self>,
        request: crate::resource_preparation::ResourcePreparationRequest,
    ) -> Result<crate::resource_preparation::TaskResourcePreparation> {
        crate::contract::validate_json_byte_bound(&request, self.maximum_payload_bytes)?;
        request.validate()?;
        if let Some(checkpoint) = &request.source_checkpoint {
            self.validate_input_file(checkpoint)?;
        }
        self.verify(false).await?;
        // Allocation must consume an explicitly configured retained session,
        // independently of per-model or per-worker invocation limits.
        self.host
            .coordinator
            .lock()
            .await
            .scheduler()
            .session_limits(self.operation_id())?
            .validate()?;
        let admission = self.host.admission(self.operation_id()).await?;
        let input =
            serde_json::to_value(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        let parent_task = admission.parent.ok_or_else(|| {
            Error::Unauthorized("resource preparation requires an admitted parent".into())
        })?;
        if admission.input != input
            || request.child.parent != Some(parent_task)
            || request.child.operation_id == admission.operation_id
            || request.child.resource_proofs.iter().any(|proof| {
                request.resources.contains(&proof.resource) && proof.preparation != self.task_id
            })
            || !self.input_grants.contains(&request.creation_grant)
        {
            return Err(Error::Unauthorized(
                "resource request differs from its admitted input, parent or creation authority"
                    .into(),
            ));
        }
        // Source authority becomes newly retained provenance here. Count the
        // borrowed original grants with the request before owning their copy.
        crate::contract::validate_json_byte_bound(
            &(&request, &self.input_grants), request.maximum_bytes,
        )?;
        let project_source = self.host.seal_prepared_project_source(&request, parent_task).await?;
        let origin = crate::resource_preparation::ResourceCreationOrigin {
            owner: self.host.owner.clone(),
            parent_task,
            preparation_task: self.task_id,
            intended_child: request.child.operation_id,
            admission_digest: self.original_admission_digest,
            request_digest: crate::contract::canonical_json_digest(&request)?,
            creation_fence: self.fence.clone(),
            source_grants: self.input_grants.clone(),
        };
        Ok(crate::resource_preparation::TaskResourcePreparation {
            owner: self.clone(),
            request,
            origin,
            project_source,
        })
    }

    pub(crate) async fn verify(&self, settlement: bool) -> Result<()> {
        use crate::distributed::JournalWrite;
        self.condition(if settlement {
            JournalWrite::Settlement
        } else {
            JournalWrite::Fresh
        })
        .await
        .map(|_| ())
    }

    fn execution_write(
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
    ) -> crate::distributed::JournalWrite {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        match event {
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => JournalWrite::Model {
                attempt_id: purpose.attempt_operation(operation),
                step: *step,
                request_digest: *request_digest,
            },
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ContextPrepared { .. }
            | ExecutionEvent::ContextCompacted { .. }
            | ExecutionEvent::ToolStarted { .. } => JournalWrite::Fresh,
            ExecutionEvent::Model { .. }
            | ExecutionEvent::ToolCompleted { .. }
            | ExecutionEvent::ToolFailed { .. } => JournalWrite::Settlement,
        }
    }

    pub(crate) async fn verify_execution(
        &self,
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
    ) -> Result<()> {
        self.condition(Self::execution_write(operation, event))
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_model_history(
        &self,
        operation: OperationId,
        records: &[crate::executor::ExecutionRecord],
    ) -> Result<()> {
        self.verify_model_history_with_work(operation,records.iter(),&mut None).await
    }

    pub(crate) async fn verify_model_history_with_work<'a>(
        &self, operation: OperationId,
        records: impl Iterator<Item=&'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.host.verify_model_history_with_work(self.task_id,operation,records,work).await
    }

    pub(crate) fn require_effect_grants(&self, provider: &str, planning: bool) -> Result<()> {
        for grant in [
            capability::EFFECT_RUN.to_owned(),
            capability::effect_provider(provider),
        ] {
            if !self.input_grants.contains(&grant) {
                return Err(Error::Unauthorized(format!("task scope lacks {grant}")));
            }
        }
        if planning && !self.input_grants.contains(capability::EFFECT_PLAN) {
            return Err(Error::Unauthorized("task scope lacks effect:plan".into()));
        }
        Ok(())
    }

    /// Returns the authenticated executing agent from current sealed creation
    /// provenance, or the verified matching proofless-root declaration. Never
    /// trusts a preparer's requested agent or the declaring parent as a child.
    pub fn authenticated_binding_agent(&self) -> Result<Option<crate::AgentId>> {
        if !self.input_grants.contains(capability::CONVERSATION_BIND) {
            return Err(Error::Unauthorized("task scope lacks conversation:bind".into()));
        }
        Ok(self.input_agent)
    }

    pub(crate) fn authenticated_fork_agent(&self) -> Result<crate::AgentId> {
        if !self.input_grants.contains(capability::FORK_PUBLISH) {
            return Err(Error::Unauthorized("task scope lacks fork:publish".into()));
        }
        self.input_agent.ok_or_else(|| Error::Unauthorized("task has no authenticated current agent".into()))
    }

    pub(crate) fn binding_origin(&self) -> (TaskId, [u8;32], &crate::scheduler::LeaseFence) {
        (self.task_id, self.workflow.request_digest, &self.fence)
    }

    /// Serializes root bind replay/commit/cache on this same owner handle only.
    pub(crate) async fn lock_root_binding(&self) -> Option<tokio::sync::MutexGuard<'_,()>> {
        Some(self.root_binding.as_ref()?.gate.lock().await)
    }

    pub(crate) fn check_root_binding(
        &self, audience: &Authority, operation: OperationId, intent: [u8;32],
    ) -> Result<()> {
        if let Some(proof) = self.root_binding.as_ref().and_then(|state| state.proof.get()) {
            if proof.audience() != audience || proof.bind_operation() != operation || proof.intent_digest() != intent {
                return Err(Error::Conflict("root binding audience, operation or intent changed".into()));
            }
        }
        Ok(())
    }

    /// Pins a denial-only choice after protected scope authentication and
    /// measured audience hashing. It survives lost acknowledgment; it never
    /// supplies publication authority in place of a verified root bind.
    pub(crate) fn pin_root_binding_choice(
        &self, operation: OperationId, intent: [u8;32], audience_digest: [u8;32],
    ) -> Result<()> {
        let Some(state) = &self.root_binding else { return Ok(()); };
        let choice = (operation,intent,audience_digest);
        if let Err(choice) = state.pending.set(choice) {
            if state.pending.get() != Some(&choice) {
                return Err(Error::Conflict("root binding publication choice changed".into()));
            }
        }
        Ok(())
    }

    pub(crate) fn retain_root_binding(&self, proof: crate::store::VerifiedRootBinding) -> Result<()> {
        if !self.input_is_root || proof.original_task() != self.task_id
            || proof.admission_digest() != self.workflow.request_digest
            || proof.fence() != &self.fence
            || self.authenticated_binding_agent()? != Some(proof.agent())
        {
            return Err(Error::Unauthorized("root binding proof belongs to another owner or lease".into()));
        }
        let state = self.root_binding.as_ref().ok_or_else(|| Error::Unauthorized("owner has no root binding cache".into()))?;
        if let Err(proof) = state.proof.set(proof) {
            let retained = state.proof.get().ok_or_else(|| Error::Conflict("root binding cache is unavailable".into()))?;
            if retained.audience() != proof.audience() || retained.bind_operation() != proof.bind_operation()
                || retained.intent_digest() != proof.intent_digest() {
                return Err(Error::Conflict("root binding audience, operation or intent changed".into()));
            }
        }
        Ok(())
    }

    pub(crate) fn authenticate_fork_parent(&self, parent: &Authority) -> Result<()> {
        self.authenticated_fork_agent()?;
        let audience = match &self.input_created {
            Some(access) => access.current_content()?.and_then(|binding| binding.authority.as_ref()),
            None if self.input_is_root => self.root_binding.as_ref().and_then(|state| state.proof.get()).map(|proof| proof.audience()),
            None => None,
        }.ok_or_else(|| Error::Unauthorized("current caller has no authenticated publication audience".into()))?;
        if audience != parent {
            return Err(Error::Unauthorized("fork parent differs from the current caller audience".into()));
        }
        Ok(())
    }

    pub(crate) fn authenticate_binding_scope(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let agent = self.authenticated_binding_agent()?.ok_or_else(|| Error::Unauthorized("binding owner has no authenticated agent".into()))?;
        if scope.agent() != Some(agent) || !scope.capabilities().contains(capability::CONVERSATION_BIND) {
            return Err(Error::Unauthorized("binding scope differs from the original task actor or grant".into()));
        }
        if !self.input_is_root {
            let binding = match &self.input_created { Some(access) => access.current_content()?, None => None };
            if binding.and_then(|binding| binding.authority.as_ref()) != Some(audience) {
                return Err(Error::Unauthorized("binding audience differs from the current sealed child".into()));
            }
        }
        self.host.verifier.verify_scope_for_with_work(audience, scope, work)
    }

    pub(crate) async fn append_conversation(
        &self,
        publication: &crate::store::operations::IndexedPublication,
        key: &StreamKey,
        write: crate::distributed::JournalWrite,
    ) -> Result<bool> {
        self.append_conversation_with_work(publication, key, write, &mut None).await
    }

    pub(crate) async fn append_conversation_with_work(
        &self,
        publication: &crate::store::operations::IndexedPublication,
        key: &StreamKey,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let fresh = !matches!(write, crate::distributed::JournalWrite::Settlement);
        let mut request = self
            .prepare_append_with_work(
                &publication.path,
                publication.expected_tail,
                key,
                &publication.bytes,
                write,
                work,
            )
            .await?;
        publication.add_index(&mut request);
        if fresh && self.deadline_epoch_ms.is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline) {
            return Err(Error::Indeterminate(self.operation_id()));
        }
        match crate::distributed::commit_keyed_with_work(
            &self.stream, request, publication.operation_id, work,
        ).await? {
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
            acyclic_stream::CommitOutcome::Committed(envelope) => {
                publication
                    .verify(&envelope)
                    .map_err(|_| Error::Indeterminate(publication.operation_id))?;
                Ok(true)
            }
        }
    }

    pub(crate) async fn append_execution(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &StreamKey,
        bytes: Bytes,
        event: &crate::executor::ExecutionEvent,
    ) -> Result<bool> {
        let path = crate::distributed::execution_path(operation)?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            Self::execution_write(operation, event),
        )
        .await
    }

    pub(crate) async fn append_workflow(
        &self,
        admission: bool,
        expected_tail: u64,
        key: &StreamKey,
        bytes: Bytes,
    ) -> Result<bool> {
        let class = if admission {
            "workflow-admissions"
        } else {
            "workflows"
        };
        let path =
            acyclic_stream::StreamPath::new(format!("harness/v2/{class}/{}", self.workflow.operation_id))?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
        )
        .await
    }

    async fn prepare_append(
        &self,
        path: &acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: &Bytes,
        write: crate::distributed::JournalWrite,
    ) -> Result<acyclic_stream::CommitRequest> {
        self.prepare_append_with_work(path, expected_tail, key, bytes, write, &mut None).await
    }

    async fn prepare_append_with_work(
        &self,
        path: &acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: &Bytes,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::CommitRequest> {
        use acyclic_stream::{CommitCondition, CommitMutation};
        let owner = self.condition_with_work(write, work).await?;
        if let Some(io) = work.as_deref_mut() { io.begin_call()?; }
        let target = match self.stream.bounds(path.as_str()).await {
            Ok(_) => CommitCondition::Tail {
                path: path.clone(),
                expected: expected_tail,
            },
            Err(StreamError::NotFound) if expected_tail == 0 => {
                CommitCondition::Absent { path: path.clone() }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        // Conflicting transactions are retained by Stream too. A refreshed owner
        // revision gets its own recovery key; the journal's CAS and retry index
        // still prevent duplicate logical records.
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness/v2/owned-journal\0");
        hash.update(key.as_bytes());
        hash.update(&self.task_id.into_bytes());
        let encoded_fence = if let Some(io) = work.as_deref_mut() {
            let remaining = io.remaining()?;
            let bounds = crate::contract::JsonBounds {
                maximum_bytes: remaining.bytes_encoded,
                maximum_items: remaining.bytes_encoded,
                maximum_depth: 256,
            };
            let encoded = crate::contract::canonical_json_bytes_bounded(
                &self.fence, bounds, &mut |charge| io.codec_charge(charge),
            )?;
            // The codec admitted logical output before allocation. Account for
            // additional exposed Vec capacity without charging output twice.
            io.retain_owned((encoded.capacity() - encoded.len()) as u64)?;
            encoded
        } else {
            crate::contract::canonical_json_bytes(&self.fence)?
        };
        hash.update(&encoded_fence);
        let retained_fence = encoded_fence.capacity() as u64;
        drop(encoded_fence);
        if let Some(io) = work.as_deref_mut() { io.release_owned(retained_fence); }
        let CommitCondition::Tail { expected, .. } = &owner else {
            return Err(Error::Invalid("coordinator tail condition required".into()));
        };
        hash.update(&expected.to_le_bytes());
        hash.update(path.as_str().as_bytes());
        hash.update(&expected_tail.to_le_bytes());
        hash.update(&[u8::from(matches!(target, CommitCondition::Absent { .. }))]);
        hash.update(blake3::hash(bytes).as_bytes());
        let transaction_key = StreamKey::new(Bytes::copy_from_slice(hash.finalize().as_bytes()))?;
        Ok(acyclic_stream::CommitRequest {
            conditions: vec![owner, target],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![bytes.clone()],
            }],
            idempotency_key: transaction_key,
        })
    }

    pub(crate) async fn append(
        &self,
        path: acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: Bytes,
        write: crate::distributed::JournalWrite,
    ) -> Result<bool> {
        use acyclic_stream::{CommitOutcome, CommittedMutation};
        let request = self
            .prepare_append(&path, expected_tail, key, &bytes, write)
            .await?;
        let outcome =
            crate::distributed::commit_keyed(&self.stream, request, self.operation_id()).await?;
        match outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                    return Err(Error::Storage("invalid owned journal envelope".into()));
                };
                if append.path != path
                    || append.start != expected_tail
                    || append.end != expected_tail + 1
                    || append.tail != append.end
                    || append.records.len() != 1
                    || append.records.first().is_none_or(|record| {
                        record.sequence != expected_tail
                            || record.value != bytes
                            || record.commit_id != envelope.commit_id
                    })
                {
                    return Err(Error::Storage("invalid owned journal append".into()));
                }
                Ok(true)
            }
        }
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Authenticates operation controls against this task owner's trust root.
    /// Actual observation/cancellation also verifies the retained operation owner.
    pub async fn authorize_operation_control(
        &self,
        request: &crate::wire_api::OperationControlRequest,
    ) -> Result<()> {
        request.authenticate(&self.verifier)?;
        if request.owner != self.owner {
            return Err(Error::Unauthorized("operation control belongs to another owner".into()));
        }
        if !request.scope.capabilities().contains(capability::OPERATION_OBSERVE)
            && !request.scope.capabilities().contains(capability::OPERATION_CANCEL)
        {
            return Err(Error::Unauthorized("operation control capability is missing".into()));
        }
        Ok(())
    }

    /// Observes one exact operation through the existing coordinator.
    pub async fn observe_control(
        &self,
        request: &crate::wire_api::OperationControlRequest,
    ) -> Result<crate::scheduler::OperationState> {
        self.authorize_operation_control(request).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        coordinator.observe_operation(
            &request.owner,
            &request.scope,
            &self.verifier,
            request.operation_id,
        )
    }

    /// Cancels one operation with the caller's retained retry identity.
    pub async fn cancel_control(
        &self,
        request: &crate::wire_api::CancelControlRequest,
    ) -> Result<crate::scheduler::OperationState> {
        self.authorize_operation_control(&request.control).await?;
        let mut coordinator = self.coordinator.lock().await;
        let (_, state) = coordinator.cancel_operation(
            &request.control.owner,
            &request.control.scope,
            &self.verifier,
            request.control.operation_id,
            request.idempotency_key.clone(),
            request.recursive,
        ).await?;
        Ok(state)
    }

    /// Authenticates retained model claims without requiring an execution lease.
    /// Passive readers gain no publication or provider-dispatch authority.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_model_history(
        &self,
        task: TaskId,
        operation: OperationId,
        records: &[crate::executor::ExecutionRecord],
    ) -> Result<()> {
        self.verify_model_history_with_work(task,operation,records.iter(),&mut None).await
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_model_history_with_work<'a>(
        &self, task: TaskId, operation: OperationId,
        records: impl Iterator<Item=&'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !records.clone().any(|record| {
            matches!(
                record.event,
                crate::executor::ExecutionEvent::ModelStarted { .. }
            )
        }) {
            return Ok(());
        }
        let task_operation = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh_with_work(work).await?;
        coordinator.observe_operation_ref_with_work(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            task_operation, work,
        )?;
        for record in records {
            if let crate::executor::ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } = &record.event
            {
                coordinator.scheduler().require_model_claim(
                    task_operation,
                    purpose.attempt_operation(operation),
                    *step,
                    request_digest,
                )?;
            }
        }
        Ok(())
    }

    /// Claims through the same coordinator, restricted to this signed owner.
    pub async fn pull_work(
        &self,
        worker: &crate::distributed::Worker,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator
            .lock()
            .await
            .pull_owned(&self.owner, &self.owner_scope, &self.verifier, worker)
            .await
    }

    /// Claims this original operation without owner-wide task selection.
    pub async fn pull_operation(
        &self,
        worker: &crate::distributed::Worker,
        operation_id: OperationId,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator
            .lock()
            .await
            .pull_owned_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                worker,
                operation_id,
            )
            .await
    }

    /// Recovers the coordinator's original reservation after a lost pull response.
    /// This performs no claim, release, or publication. The returned reference is
    /// not execution authority: start/resume must still verify its exact fence.
    /// A retained cancelled or nonexecutable reservation remains unresolved.
    pub async fn recover_work(&self, task: TaskId) -> Result<crate::distributed::WorkPull> {
        use crate::distributed::{WorkLease, WorkPull};
        use crate::scheduler::OperationPhase;

        let operation_id = OperationId::from_bytes(task.into_bytes());
        self.admission(operation_id).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
        )?;
        let Some(reservation) = operation.reservation else {
            return Ok(WorkPull::Idle);
        };
        let lease = WorkLease {
            operation: operation.spec,
            reservation,
            checkpoint: operation.checkpoint,
            operation_revision: operation.revision,
        };
        if operation.cancellation_requested
            || !matches!(
                operation.phase,
                OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling
            )
        {
            return Ok(WorkPull::Unresolved {
                lease,
                error: Error::Conflict("retained task reservation requires reconciliation".into()),
            });
        }
        Ok(WorkPull::Claimed(lease))
    }

    /// Durably retains one timer identity without holding a waiting future.
    /// The existing owner-authenticated workflow wake supplies later admission.
    #[cfg(feature = "filesystem")]
    pub async fn poll_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<bool> {
        if deadline_unix_ms == 0 {
            return Err(Error::Invalid("timer deadline is invalid".into()));
        }
        let owner = self.journal_owner(task, fence.clone()).await?;
        if !owner.input_grants.contains("timer:wait") {
            return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
        }
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/timers/{task}"))?;
        let key = Self::event_key("timers", task, operation)?;
        let bytes = Bytes::from(crate::contract::canonical_json_bytes(&TimerEvent {
            task_id: task,
            operation_id: operation,
            deadline_unix_ms,
        })?);
        // Only the selected timer is retained. Paging bounds resident records;
        // no session-wide history ceiling is imposed.
        loop {
            self.verify_owner(task, &fence, false).await?;
            let (tail, found) = self.timer_state(task, operation, deadline_unix_ms).await?;
            if found {
                self.verify_owner(task, &fence, false).await?;
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
            if owner
                .append(
                    path.clone(),
                    tail,
                    &key,
                    bytes.clone(),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?
            {
                self.verify_owner(task, &fence, false).await?;
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
        }
    }

    /// Publishes ref-only mail under the sender's exact uncancelled lease.
    #[cfg(feature = "filesystem")]
    pub async fn send_owned(
        self: &Arc<Self>,
        sender: TaskId,
        fence: crate::scheduler::LeaseFence,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<MailSendAck> {
        // Validate the original task/lease before retaining or observing mail.
        self.journal_owner(sender, fence.clone()).await?;
        let ack = MailSendAck { sender, recipient, operation_id: message_id, payload };
        let bytes = Bytes::from(
            self.mail_bytes(sender, recipient, message_id, ack.payload.clone())
                .await?,
        );
        self.publish_mail(sender, recipient, message_id, bytes, Some(fence))
            .await?;
        Ok(ack)
    }

    /// Resolves one exact retained parcel under its receiver's original lease
    /// and sole receive command. Donor content authority remains donor-owned;
    /// these returned bytes do not grant access to the donor's volume.
    #[cfg(feature = "filesystem")]
    pub async fn read_received_mail_owned(
        self: &Arc<Self>,
        receiver: TaskId,
        fence: crate::scheduler::LeaseFence,
        receive_operation: OperationId,
        item: &InboxItem,
    ) -> Result<Vec<u8>> {
        let owner = self.journal_owner(receiver, fence.clone()).await?;
        owner.verify(true).await?;
        if item.task_id != receiver || item.sequence == 0 {
            return Err(Error::Unauthorized("parcel recipient or sequence differs".into()));
        }
        let path = format!("harness/v2/workflows/{}", owner.workflow_admission().operation_id);
        let workflow = owner.stream().stream(&path)?;
        let tail = workflow.bounds().await
            .map_err(|error| Error::Unauthorized(format!("parcel receive checkpoint is absent: {error}")))?.tail;
        let index = tail.checked_sub(1)
            .ok_or_else(|| Error::Unauthorized("parcel receive checkpoint is absent".into()))?;
        let latest = self.read_mail_workflow_record(&owner, &workflow, index).await?;
        let pending = latest.transition.commands.as_slice();
        let receive = if let [command] = pending
            && command.operation_id == receive_operation
            && command.kind == crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
        {
            command.clone()
        } else {
            let results = latest.input["commands"].as_array()
                .ok_or_else(|| Error::Unauthorized("parcel receive result is not committed".into()))?;
            let [result] = results.as_slice() else {
                return Err(Error::Unauthorized("parcel must consume one receive result".into()));
            };
            let exact = serde_json::to_value(Outcome::Succeeded(item.clone()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if result["operation_id"] != serde_json::to_value(receive_operation)
                .map_err(|error| Error::Invalid(error.to_string()))?
                || result["value"] != exact
            {
                return Err(Error::Unauthorized("parcel differs from the committed receive result".into()));
            }
            let prior_index = latest.prior.revision.checked_sub(1)
                .ok_or_else(|| Error::Unauthorized("parcel receive outbox is absent".into()))?;
            let prior = self.read_mail_workflow_record(&owner, &workflow, prior_index).await?;
            let [command] = prior.transition.commands.as_slice() else {
                return Err(Error::Unauthorized("parcel requires a sole receive outbox".into()));
            };
            if command.operation_id != receive_operation
                || command.kind != crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
                || prior.next != latest.prior
            {
                return Err(Error::Unauthorized("parcel receive operation differs".into()));
            }
            command.clone()
        };
        let receiver_admission = self.admission(owner.workflow_admission().operation_id).await?;
        let selection: crate::filesystem::MailReceiveTaskCommand = crate::contract::json_from_slice(
            &self.read_mail_source(&receiver_admission, &receive.payload).await?,
        ).map_err(|error| Error::Unauthorized(format!("invalid retained parcel selection: {error}")))?;
        if selection.after.checked_add(1) != Some(item.sequence) {
            return Err(Error::Unauthorized("parcel cursor differs from the retained receive".into()));
        }
        let records = self.mailbox(receiver)?.read(item.sequence - 1, 1).await?
            .try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Unauthorized("parcel source is absent".into()));
        };
        let event: MailEvent = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if record.sequence.checked_add(1) != Some(item.sequence)
            || event.message_id.to_string() != item.message_id || event.payload != item.payload
            || crate::contract::canonical_json_bytes(&event)?.as_slice() != record.value.as_ref()
        {
            return Err(Error::Unauthorized("parcel differs from its retained mailbox item".into()));
        }
        let sender_admission = self.admission(OperationId::from_bytes(event.sender.into_bytes())).await?;
        let direct = Self::authorize_mail_route(
            event.sender, &sender_admission, receiver, &receiver_admission,
        )?;
        receiver_admission.limits.validate_file(&event.payload)?;
        if !direct && !read_granted(&receiver_admission.grants, &event.payload)? {
            return Err(Error::Unauthorized("custom parcel requires recipient file authority".into()));
        }
        if !self.observe_mail_location(
            event.sender, receiver, event.message_id, &record.value, Some(record.sequence),
        ).await? {
            return Err(Error::Unauthorized("parcel has no original atomic receipt".into()));
        }
        let bytes = self.read_mail_source(&sender_admission, &event.payload).await?;
        owner.verify(true).await?;
        Ok(bytes)
    }

    #[cfg(feature = "filesystem")]
    async fn read_mail_workflow_record(
        &self,
        owner: &TaskJournalOwner<P>,
        workflow: &acyclic_stream::Stream<P>,
        index: u64,
    ) -> Result<crate::workflow::WorkflowRecord> {
        let records = workflow.read(index, 1).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Unauthorized("parcel workflow record is absent".into()));
        };
        let reference: FileRef = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        owner.input_limits().validate_file(&reference)?;
        if record.sequence != index
            || crate::contract::canonical_json_bytes(&reference)?.as_slice() != record.value.as_ref()
        {
            return Err(Error::Storage("parcel workflow reference differs".into()));
        }
        let value = self.read_json(&reference).await?;
        let envelope: crate::workflow::WorkflowRecord = serde_json::from_value(value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        envelope.validate()?;
        if envelope.prior.revision != index || envelope.next.revision != index + 1 {
            return Err(Error::Storage("parcel workflow revision differs".into()));
        }
        owner.validate_workflow_record(&envelope)?;
        Ok(envelope)
    }

    /// Requests a direct child's entire subtree to stop under the caller's
    /// retained lease. The returned scheduler state proves intent, not provider stop.
    #[cfg(feature = "filesystem")]
    pub async fn cancel_child_owned(
        &self,
        parent: TaskId,
        fence: crate::scheduler::LeaseFence,
        child: TaskId,
        operation: OperationId,
    ) -> Result<crate::scheduler::OperationState> {
        self.verify_owner(parent, &fence, false).await?;
        let admitted = self.admission(OperationId::from_bytes(parent.into_bytes())).await?;
        if !admitted.grants.contains(capability::OPERATION_CANCEL)
            || self.admission(OperationId::from_bytes(child.into_bytes())).await?.parent != Some(parent)
        {
            return Err(Error::Unauthorized("cancellation requires an authorized direct child".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let parent_state = coordinator.observe_operation(
            &self.owner, &self.owner_scope, &self.verifier,
            OperationId::from_bytes(parent.into_bytes()),
        )?;
        crate::scheduler::require_execution_owner(&parent_state, &fence, false)?;
        coordinator.cancel_child_operation(
            &self.owner, &self.owner_scope, &self.verifier,
            OperationId::from_bytes(child.into_bytes()),
            IdempotencyKey::new(format!("task-child-cancel:{parent}:{operation}"))?,
            OperationId::from_bytes(parent.into_bytes()), fence,
        ).await
    }


    /// Starts or reattaches the exact already-admitted lease. This does not
    /// acquire another reservation or infer that an uncertain provider stopped.
    pub async fn start_task(&self, lease: &crate::distributed::WorkLease) -> Result<()> {
        let operation_id = lease.operation.operation_id;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        self.admission(operation_id).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
        )?;
        if operation.spec != lease.operation
            || operation.reservation.as_ref() != Some(&lease.reservation)
        {
            return Err(Error::Conflict(
                "worker lease differs from retained admission".into(),
            ));
        }
        if operation.phase == crate::scheduler::OperationPhase::Running {
            return crate::scheduler::require_execution_owner(&operation, &fence, false);
        }
        let event = if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            crate::scheduler::SchedulerEvent::ReconciliationResumed {
                operation_id,
                fence: fence.clone(),
            }
        } else {
            crate::scheduler::SchedulerEvent::Started {
                operation_id,
                fence: fence.clone(),
            }
        };
        coordinator
            .apply(
                operation_id,
                worker_key("start", operation_id, &fence, operation.revision)?,
                event,
            )
            .await?;
        Ok(())
    }

    /// Releases an owned slot after the worker durably retains its checkpoint
    /// and knows there is no uncertain active dispatch. No passive future is kept.
    pub async fn suspend_workflow(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
    ) -> Result<()> {
        self.suspend_workflow_command(task, fence, revision, None)
            .await
    }

    /// Retains the exact passive command in the existing suspension slot.
    pub async fn suspend_workflow_command(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
        waiting_command: Option<OperationId>,
    ) -> Result<()> {
        self.suspend_workflow_command_guarded(task, fence, revision, waiting_command, None)
            .await
    }

    /// Releases a passive command's slot only while its existing execution
    /// journal is empty, atomically with the scheduler publication. The caller
    /// supplies the exact execution identity; this is not an effects settlement.
    pub async fn suspend_workflow_command_if_execution_idle(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
        command: OperationId,
        execution: OperationId,
    ) -> Result<()> {
        self.suspend_workflow_command_guarded(
            task,
            fence,
            revision,
            Some(command),
            Some((execution, 0)),
        )
        .await
    }

    async fn suspend_workflow_command_guarded(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
        )?;
        let key = worker_key("suspend", operation_id, &fence, revision)?;
        let event = crate::scheduler::SchedulerEvent::WorkflowSuspended {
            operation_id,
            fence,
            workflow_revision: revision,
            waiting_command,
        };
        if let Some((execution, tail)) = idle_execution {
            coordinator
                .apply_if_execution_quiescent(key, event, execution, tail)
                .await?;
        } else {
            coordinator.apply(operation_id, key, event).await?;
        }
        let current = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
        )?;
        if current.phase != crate::scheduler::OperationPhase::Suspended
            || current.workflow.as_ref().is_none_or(|slot| {
                slot.revision != revision || slot.waiting_command != waiting_command
            })
        {
            return Err(Error::Conflict(
                "workflow suspension receipt is no longer current".into(),
            ));
        }
        Ok(())
    }

    /// One bounded source page, filtered by the current signed owner projection.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn workflow_wait_page(
        &self,
        after_revision: u64,
        through_revision: Option<u64>,
        limit: u32,
    ) -> Result<WorkflowWaitPage> {
        if limit == 0 {
            return Err(Error::Invalid("wake page limit is out of bounds".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        self.verifier.verify_audience(&self.owner)?;
        self.verifier.verify(&self.owner_scope)?;
        if !self
            .owner_scope
            .capabilities()
            .contains("operation:observe")
        {
            return Err(Error::Unauthorized(
                "wake discovery requires operation:observe".into(),
            ));
        }
        let through_revision = through_revision.unwrap_or(coordinator.revision());
        if after_revision > through_revision || through_revision > coordinator.revision() {
            return Err(Error::Invalid(
                "wake cursor exceeds its coordinator snapshot".into(),
            ));
        }
        let count = u32::try_from(u64::from(limit.min(64)).min(through_revision - after_revision))
            .map_err(|_| Error::Storage("wake page count exceeds its allowance".into()))?;
        let events = if count == 0 {
            Vec::new()
        } else {
            crate::distributed::read_coordinator_event_page(&self.stream, after_revision, count)
                .await?
        };
        if events.len() != count as usize {
            return Err(Error::Storage("wake snapshot has missing events".into()));
        }
        let mut tasks = BTreeSet::new();
        for event in events {
            if !matches!(
                event.event,
                crate::scheduler::SchedulerEvent::WorkflowSuspended {
                    waiting_command: Some(_),
                    ..
                }
            ) {
                continue;
            }
            let operation = coordinator
                .scheduler()
                .operation(event.operation_id)
                .ok_or_else(|| Error::Storage("wake event operation is absent".into()))?;
            if operation.spec.owner.authority() != &self.owner {
                continue;
            }
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                event.operation_id,
            )?;
            if operation.phase == crate::scheduler::OperationPhase::Suspended
                && !operation.cancellation_requested
                && operation
                    .workflow
                    .as_ref()
                    .is_some_and(|slot| slot.waiting_command.is_some())
            {
                tasks.insert(crate::runtime::task_id_for_admission(event.operation_id));
            }
        }
        Ok(WorkflowWaitPage {
            next_revision: after_revision + u64::from(count),
            through_revision,
            events_read: count,
            tasks: tasks.into_iter().collect(),
        })
    }

    /// Authenticated read of a passive slot; never claims execution ownership.
    #[cfg(feature = "filesystem")]
    pub async fn workflow_suspension(
        &self,
        task: TaskId,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
        )?;
        if operation.phase != crate::scheduler::OperationPhase::Suspended
            || operation.cancellation_requested
        {
            return Ok(None);
        }
        Ok(operation.workflow)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn workflow_stream(&self) -> StreamClient<P> {
        self.stream.clone()
    }

    /// Readiness requires the exact timer already retained by owned dispatch.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn timer_ready(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline: u64,
    ) -> Result<bool> {
        let (_, retained) = self.timer_state(task, operation, deadline).await?;
        Ok(retained && self.clock.now_unix_millis() >= deadline)
    }

    /// Wakes the exact retained waiting command using the existing owner-
    /// authenticated revision CAS. This does not dispatch or replace that command.
    #[cfg(feature = "filesystem")]
    pub async fn wake_command(
        &self,
        task: TaskId,
        revision: u64,
        waiting: OperationId,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let key = format!("task-command-wake:{task}:{revision}:{waiting}");
        let input = self.payloads.stage(operation, &key, b"null").await?;
        let admission = self.admission(operation).await?;
        admission.limits.validate_file(&input)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its command wake".into(),
            ));
        }
        self.read_json(&input).await?;
        self.coordinator
            .lock()
            .await
            .resume_command(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                revision,
                input,
                IdempotencyKey::new(key)?,
                waiting,
            )
            .await?;
        Ok(())
    }

    /// Owner-authorized delivery into one exact durable workflow resume slot.
    pub async fn resume_workflow(
        &self,
        task: TaskId,
        revision: u64,
        input: FileRef,
        key: IdempotencyKey,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        admission.limits.validate_file(&input)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its workflow resume input".into(),
            ));
        }
        self.read_json(&input).await?;
        self.coordinator
            .lock()
            .await
            .resume_workflow(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                revision,
                input,
                key,
            )
            .await?;
        Ok(())
    }

    /// Reads the retained input under the current lease. Its workflow revision
    /// lets recovery skip an input already consumed by a committed step.
    pub async fn workflow_input(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> Result<Option<(u64, Value)>> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        let slot = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh().await?;
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            )?;
            crate::scheduler::require_execution_owner(&operation, fence, false)?;
            operation.workflow
        };
        let Some(slot) = slot else { return Ok(None) };
        let Some(input) = slot.input else {
            return Ok(None);
        };
        admission.limits.validate_file(&input)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its retained workflow input".into(),
            ));
        }
        let value = self.read_json(&input).await?;
        self.verify_owner(task, fence, false).await?;
        Ok(Some((slot.revision, value)))
    }

    /// Publishes a schema-checked result or retains uncertainty with this exact
    /// lease. A staged result can survive a publication conflict without making
    /// that conflict a successful task completion.
    pub async fn settle_task(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        outcome: Outcome<Value>,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        let settlement = matches!(&outcome, Outcome::Cancelled | Outcome::Indeterminate { .. });
        self.verify_owner(task, &fence, settlement).await?;
        let digest = crate::contract::canonical_json_digest(&outcome)?;
        let key = IdempotencyKey::new(format!(
            "task-settle:{operation_id}:{}:{}",
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(&fence)?).to_hex(),
            blake3::Hash::from_bytes(digest).to_hex()
        ))?;
        let outcome = match outcome {
            Outcome::Succeeded(value) => {
                crate::tool::validate_value(&admission.output_schema, &value, "task output")?;
                let bytes = crate::contract::canonical_json_bytes(&value)?;
                if bytes.len() as u64 > admission.limits.file_bytes {
                    return Err(Error::Invalid(
                        "task result exceeds admitted file limit".into(),
                    ));
                }
                Outcome::Succeeded(
                    self.payloads
                        .stage(operation_id, key.as_str(), &bytes)
                        .await?,
                )
            }
            Outcome::Failed { message } => Outcome::Failed { message },
            Outcome::Cancelled => Outcome::Cancelled,
            Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
        };
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        if !settlement
            && admission.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            coordinator.cancel_operation(
                &self.owner, &self.owner_scope, &self.verifier, operation_id,
                IdempotencyKey::new(format!("task-cancel:{operation_id}"))?, true,
            ).await?;
            return Err(Error::Indeterminate(operation_id));
        }
        let current = coordinator.observe_operation(
            &self.owner, &self.owner_scope, &self.verifier, operation_id,
        )?;
        crate::scheduler::require_execution_owner(&current, &fence, settlement)?;
        coordinator.apply(
            operation_id, key,
            crate::scheduler::SchedulerEvent::Completed {
                operation_id, outcome, fence: Some(fence), execution_duration_ns: None,
            },
        ).await?;
        Ok(())
    }

    async fn verify_owner(
        &self,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        settlement: bool,
    ) -> Result<()> {
        if !settlement {
            let operation = OperationId::from_bytes(task_id.into_bytes());
            let admitted = self.read_retained_admission(operation).await?;
            if admitted.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                self.expire_task_deadline(task_id, Some(fence.clone())).await?;
                return Err(Error::Indeterminate(operation));
            }
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task_id.into_bytes()),
        )?;
        crate::scheduler::require_execution_owner(&operation, fence, settlement)
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<TaskJournalOwner<P>> {
        let declaration = self.retained_declaration(OperationId::from_bytes(task_id.into_bytes()))
            .await?.ok_or_else(|| Error::NotFound("task admission".into()))?;
        let admission = self.read_declared_admission(&declaration).await?;
        let scope = self.authenticated_scope(&admission).await?;
        let input_agent = match scope.current_created_agent()? {
            Some(agent) => Some(agent),
            None if admission.parent.is_none() && admission.resource_proofs.is_empty() => {
                let agent = declaration.owner_scope.agent();
                if agent != self.owner_scope.agent() {
                    return Err(Error::Unauthorized("root current actor differs from original declaration".into()));
                }
                agent
            }
            None => None,
        };
        let machine = self
            .machines
            .resolve(&admission.machine)
            .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?;
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: admission.operation_id,
            request_digest: crate::contract::canonical_json_digest(&admission)?,
            initial: crate::workflow::MachineCheckpoint {
                machine: admission.machine.clone(),
                revision: 0,
                state: machine.initialize(&admission.input)?,
            },
        };
        self.machines.validate_checkpoint(&workflow.initial)?;
        let stream = self.coordinator.lock().await.journal_client();
        let input_is_root = admission.parent.is_none() && admission.resource_proofs.is_empty();
        let owner = TaskJournalOwner {
            host: self.clone(),
            stream,
            task_id,
            fence,
            original_admission_digest: workflow.request_digest,
            invocation_cancellation: None,
            workflow,
            maximum_payload_bytes: admission.limits.file_bytes,
            output_schema: admission.output_schema,
            input_grants: admission.grants,
            input_created: scope.created_access(),
            input_agent,
            input_is_root,
            root_binding: input_is_root.then(|| Arc::new(TaskRootBinding::default())),
            input_limits: admission.limits,
            deadline_epoch_ms: admission.run_limits.deadline_epoch_ms,
            extensions: admission.extensions,
            extension_schemas: self.root_scope.extension_schema_registry(),
            extension_runtime: self.root_scope.extension_runtime(),
        };
        owner.verify(true).await?;
        Ok(owner)
    }
    /// Binds a trusted owner, its exact task/machine registries, and immutable
    /// admission payload provider. The host validates definitions even when a
    /// caller bypasses the high-level typed runtime.
    #[allow(
        clippy::too_many_arguments,
        reason = "each provider boundary is explicit at construction"
    )]
    pub fn new(
        coordinator: DistributedCoordinator<P>,
        stream: StreamClient<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: Authority,
        owner_scope: Scope,
        verifier: AuthorityVerifier,
        root_scope: RuntimeScope,
        tasks: TaskRegistry,
        machines: MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self> {
        verifier.verify_audience(&owner)?;
        verifier.verify(&owner_scope)?;
        for capability in [
            capability::OPERATION_DECLARE,
            capability::OPERATION_OBSERVE,
            capability::OPERATION_CANCEL,
        ] {
            if !owner_scope.capabilities().contains(capability) {
                return Err(Error::Unauthorized(format!(
                    "owner scope lacks {capability}"
                )));
            }
        }
        if !root_scope.grants().is_subset_of(owner_scope.capabilities()) {
            return Err(Error::Unauthorized(
                "runtime grants exceed the signed owner scope".into(),
            ));
        }
        Ok(Self {
            coordinator: Arc::new(Mutex::new(coordinator)),
            stream,
            payloads,
            reader,
            owner,
            owner_scope,
            verifier,
            root_scope,
            tasks,
            machines,
            clock,
            tools: None,
            policy: None,
            interactions: None,
            effects: None,
            session_limits: None,
            resource_creation: Vec::new(),
        })
    }

    /// Current trusted wall clock shared by admission, workers and effect gates.
    #[must_use]
    pub fn now_unix_millis(&self) -> u64 {
        self.clock.now_unix_millis()
    }

    /// Reads the configured owner's exact current operation without granting
    /// dispatch, publication or another reservation.
    pub async fn observe_task(&self, task: TaskId) -> Result<crate::scheduler::OperationState> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        coordinator.observe_operation(
            &self.owner, &self.owner_scope, &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
        )
    }

    /// Cancels only when the original admitted absolute deadline has elapsed.
    /// A supplied fence must still own the exact reservation; uncertain effects
    /// and their original reservation remain available for reconciliation.
    pub async fn expire_task_deadline(
        &self,
        task: TaskId,
        fence: Option<crate::scheduler::LeaseFence>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let admission = self.read_retained_admission(operation).await?;
        let deadline = admission.run_limits.deadline_epoch_ms.ok_or_else(|| {
            Error::Invalid("task has no admitted deadline".into())
        })?;
        if self.clock.now_unix_millis() < deadline {
            return Err(Error::Conflict("task deadline has not elapsed".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let state = coordinator.observe_operation(
            &self.owner, &self.owner_scope, &self.verifier, operation,
        )?;
        if state.outcome.as_ref().is_some_and(|outcome| !matches!(outcome, Outcome::Indeterminate { .. })) {
            return Ok(());
        }
        if let Some(fence) = fence {
            if state.reservation.as_ref().map(crate::scheduler::LeaseFence::from).as_ref() != Some(&fence) {
                return Err(Error::Conflict("deadline observer does not own the current reservation".into()));
            }
        }
        if state.cancellation_requested {
            return Ok(());
        }
        coordinator.cancel_operation(
            &self.owner, &self.owner_scope, &self.verifier, operation,
            IdempotencyKey::new(format!("task-cancel:{operation}"))?, true,
        ).await?;
        Ok(())
    }

    /// Registers one explicitly trusted, revision-pinned creation verifier.
    /// Raw proof references never install authority or provider bindings.
    pub fn with_resource_creation_provider(
        mut self,
        provider: Arc<dyn crate::resource_preparation::ResourceCreationProvider>,
    ) -> Result<Self> {
        provider.provider().validate()?;
        if self.resource_creation.iter().any(|bound| bound.provider() == provider.provider()) {
            return Err(Error::Invalid("resource creation provider is already registered".into()));
        }
        self.resource_creation.push(provider);
        Ok(self)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn with_resource_creation_providers(
        mut self,
        providers: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
    ) -> Result<Self> {
        if !self.resource_creation.is_empty() {
            return Err(Error::Conflict("resource creation registry is already configured".into()));
        }
        for (index, provider) in providers.iter().enumerate() {
            provider.provider().validate()?;
            if providers[..index].iter().any(|bound| bound.provider() == provider.provider()) {
                return Err(Error::Invalid("resource creation provider is registered twice".into()));
            }
        }
        self.resource_creation = providers;
        Ok(self)
    }

    /// Requires these immutable shared ceilings for every root admitted by this
    /// host. Root admission publishes its declaration and configuration together.
    pub fn with_session_limits(mut self, limits: crate::scheduler::SessionLimits) -> Result<Self> {
        limits.validate()?;
        self.session_limits = Some(limits);
        Ok(self)
    }

    /// Admits through the existing task path while retaining the parent's lease.
    pub async fn admit_owned(
        &self,
        admission: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<Admission<TaskId>> {
        let parent = admission
            .parent
            .ok_or_else(|| Error::Invalid("owned child admission has no parent".into()))?;
        self.verify_owner(parent, &fence, false).await?;
        self.admit_record(admission, Some(fence)).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one complete immutable task admission validation path"
    )]
    async fn admit_record(
        &self,
        admission: TaskAdmissionRecord,
        parent_fence: Option<crate::scheduler::LeaseFence>,
    ) -> Result<Admission<TaskId>> {
        admission.validate()?;
        if !admission.resource_proofs.is_empty()
            && parent_fence.is_none()
            && self.retained_declaration(admission.operation_id).await?.is_none()
        {
            return Err(Error::Unauthorized(
                "created resource admission requires the current parent execution fence".into(),
            ));
        }
        let operation_id = admission.operation_id;
        let identity = admission.task.clone();
        let machine = admission.machine.clone();
        let output_schema = admission.output_schema.clone();
        let parent = admission.parent;
        let scope = self.authenticated_scope(&admission).await?;
        self.validate_parent_scope(parent, &scope).await?;
        if parent.is_some() {
            require_descendant_grant(scope.grants(), &identity)?;
        }
        if let Some(extensions) = scope.extensions() {
            extensions.validate()?;
        }
        if scope.extensions() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "task extensions differ from owner bindings".into(),
            ));
        }
        self.validate_local_admission_policy(&admission)?;
        if machine.name != identity.name
            || machine.version != identity.version
            || machine.digest == [0; 32]
            || self.machines.resolve(&machine).is_none()
        {
            return Err(Error::Invalid(
                "task and machine identities disagree".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &identity,
            &machine,
            Some(&admission.input_schema),
            &output_schema,
            Some(&admission.input),
        )?;
        admission.validate()?;
        let canonical = admission.canonical_value();
        let bytes = crate::contract::canonical_json_bytes(&canonical)?;
        if bytes.len() as u64 > scope.limits().file_bytes {
            return Err(Error::Invalid(
                "durable task admission exceeds its file limit".into(),
            ));
        }
        let key = format!("task-admission:{operation_id}");
        let (state, owner_scope) =
            if let Some(retained) = self.retained_declaration(operation_id).await? {
                if self.read_admission_json(&retained.state).await? != canonical {
                    return Err(Error::Conflict("task admission changed on retry".into()));
                }
                (retained.state, retained.owner_scope)
            } else {
                let state = self.payloads.stage(operation_id, &key, &bytes).await?;
                state.descriptor().verify(&bytes)?;
                if self.read_json(&state).await? != canonical {
                    return Err(Error::Storage(
                        "staged task admission changed before publication".into(),
                    ));
                }
                (state, self.owner_scope.clone())
            };
        let spec = OperationSpec {
            operation_id,
            owner_scope,
            parent: parent.map(|parent| ParentLink {
                operation_id: OperationId::from_bytes(parent.into_bytes()),
                slot: operation_id.to_string(),
            }),
            owner: DurableOwner::Attached {
                authority: self.owner.clone(),
            },
            entrypoint: EntrypointRef {
                name: identity.name,
                version: identity.version,
                digest: identity.digest,
                result_schema: output_schema,
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: BTreeMap::new(),
            orchestration: Orchestration::Leaf,
            state,
        };
        let outcome = if let Some(fence) = parent_fence {
            self.coordinator
                .lock()
                .await
                .declare_operation_owned(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    IdempotencyKey::new(key)?,
                    fence,
                )
                .await
        } else {
            self.declare_task(spec, IdempotencyKey::new(key)?).await
        };
        match outcome {
            Ok(_) => Ok(Admission::Accepted(TaskId::from_bytes(
                operation_id.into_bytes(),
            ))),
            Err(Error::Indeterminate(_)) => Ok(Admission::Indeterminate { operation_id }),
            Err(error) => Err(error),
        }
    }

    async fn declare_task(
        &self,
        spec: OperationSpec,
        key: IdempotencyKey,
    ) -> Result<crate::distributed::CoordinatorApply> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(limits) = self.session_limits.filter(|_| spec.parent.is_none()) {
            coordinator
                .declare_session(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    limits,
                    key,
                )
                .await
        } else {
            coordinator
                .declare_operation(&self.owner, &self.owner_scope, &self.verifier, spec, key)
                .await
        }
    }

    /// Installs a separately replaceable ref-only durable tool runner.
    #[must_use]
    pub fn with_tools(mut self, tools: Arc<DurableToolRunner>) -> Self {
        self.tools = Some(tools);
        self
    }

    /// Pins the policy enforced by this owner, independently of caller contexts.
    pub fn with_policy(mut self, policy: Arc<dyn ToolPolicy>) -> Result<Self> {
        validate_policy_identity(&policy.identity())?;
        self.policy = Some(policy);
        Ok(self)
    }

    /// Binds the owner journal used to admit and reconcile addressable interactions.
    #[must_use]
    pub fn with_interactions(mut self, journal: Arc<dyn ExecutionJournal>) -> Self {
        self.interactions = Some(journal);
        self
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }

    async fn authenticated_scope(&self, admission: &TaskAdmissionRecord) -> Result<RuntimeScope> {
        let mut visited = BTreeSet::new();
        let access = self.verify_created_access(admission, &mut visited).await?;
        self.root_scope.with_created_access(admission, access)
    }

    fn verify_created_access<'a>(
        &'a self,
        admission: &'a TaskAdmissionRecord,
        visited: &'a mut BTreeSet<OperationId>,
    ) -> BoxFuture<'a, Result<Option<crate::resource_preparation::CreatedResourceAccess>>> {
        Box::pin(async move {
            if admission.resource_proofs.is_empty() {
                return Ok(None);
            }
            if !visited.insert(admission.operation_id) {
                return Err(Error::Unauthorized("resource preparation proof is cyclic".into()));
            }
            let parent = admission.parent.ok_or_else(|| {
                Error::Unauthorized("created resource admission has no parent".into())
            })?;
            let parent_admission = self.read_retained_admission(
                OperationId::from_bytes(parent.into_bytes()),
            ).await?;
            let mut receipts = Vec::with_capacity(admission.resource_proofs.len());
            let mut current_default: Option<(usize, OperationId)> = None;
            for proof in &admission.resource_proofs {
                let provider = self.resource_creation.iter().find(|provider| {
                    provider.provider() == &proof.provider
                }).ok_or_else(|| Error::Unsupported("resource proof verifier is not bound".into()))?;
                let preparation = self.read_retained_admission(
                    OperationId::from_bytes(proof.preparation.into_bytes()),
                ).await?;
                let request: crate::resource_preparation::ResourcePreparationRequest =
                    serde_json::from_value(preparation.input.clone())
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                request.validate()?;
                if preparation.parent != request.child.parent
                    || preparation.operation_id.into_bytes() != proof.preparation.into_bytes()
                    || request.provider != proof.provider
                    || !request.resources.contains(&proof.resource)
                    || !request.child.resource_proofs.contains(proof)
                    || !preparation.grants.contains(&request.creation_grant)
                {
                    return Err(Error::Unauthorized("resource proof differs from admitted preparation".into()));
                }
                let preparation_access = self.verify_created_access(&preparation, visited).await?;
                self.root_scope.with_created_access(&preparation, preparation_access)?;
                let preparation_parent = preparation.parent.ok_or_else(|| {
                    Error::Unauthorized("resource preparation has no original parent".into())
                })?;
                let original_parent = self.read_retained_admission(
                    OperationId::from_bytes(preparation_parent.into_bytes()),
                ).await?;
                if !original_parent.grants.contains(&request.creation_grant)
                    || !preparation.grants.is_subset_of(&original_parent.grants)
                {
                    return Err(Error::Unauthorized("creation authority was not narrowed from the parent".into()));
                }
                let inherited = request.child != *admission;
                let mut origin_distance = 0;
                if inherited {
                    if !parent_admission.resource_proofs.contains(proof) {
                        return Err(Error::Unauthorized("created resource proof is not inherited from the parent".into()));
                    }
                    let original_child = self.read_retained_admission(request.child.operation_id).await?;
                    if original_child != request.child {
                        return Err(Error::Unauthorized("created resource origin admission changed".into()));
                    }
                }
                {
                    let mut coordinator = self.coordinator.lock().await;
                    coordinator.refresh().await?;
                    let prepared = coordinator.observe_operation(
                        &self.owner, &self.owner_scope, &self.verifier, preparation.operation_id,
                    )?;
                    if prepared.cancellation_requested
                        || !matches!(prepared.outcome, Some(Outcome::Succeeded(_)))
                    {
                        return Err(Error::Unauthorized("resource preparation did not succeed".into()));
                    }
                    if inherited {
                        let mut cursor = Some(OperationId::from_bytes(parent.into_bytes()));
                        let mut lineage = BTreeSet::new();
                        let mut found = false;
                        while let Some(operation) = cursor {
                            if operation == request.child.operation_id {
                                origin_distance = lineage.len() + 1;
                                found = true;
                                break;
                            }
                            if !lineage.insert(operation) {
                                return Err(Error::Conflict("indexed operation lineage is cyclic".into()));
                            }
                            let state = coordinator.observe_operation(
                                &self.owner, &self.owner_scope, &self.verifier, operation,
                            )?;
                            cursor = state.spec.parent.map(|link| link.operation_id);
                        }
                        if !found {
                            return Err(Error::Unauthorized("resource proof belongs to another child lineage".into()));
                        }
                    }
                }
                let receipt = provider.verify(&request, proof).await?;
                if receipt.origin.owner != self.owner
                    || receipt.origin.parent_task != preparation_parent
                    || receipt.origin.preparation_task != proof.preparation
                    || receipt.origin.intended_child != request.child.operation_id
                    || receipt.origin.admission_digest != crate::contract::canonical_json_digest(&preparation)?
                    || receipt.origin.request_digest != crate::contract::canonical_json_digest(&request)?
                    || receipt.origin.source_grants != preparation.grants
                {
                    return Err(Error::Unauthorized("resource allocation provenance changed".into()));
                }
                if receipt.content.iter().any(|binding| binding.default_writer)
                    && current_default.is_none_or(|(distance, _)| origin_distance < distance)
                {
                    current_default = Some((origin_distance, receipt.origin.intended_child));
                }
                receipts.push(receipt);
            }
            visited.remove(&admission.operation_id);
            crate::resource_preparation::CreatedResourceAccess::consume(
                admission, receipts, current_default.map(|(_, origin)| origin),
            ).map(Some)
        })
    }

    async fn read_json(&self, reference: &FileRef) -> Result<Value> {
        reference.validate()?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("durable task payload is not JSON".into()));
        }
        let bytes = self.reader.read(reference).await?;
        reference.descriptor().verify(&bytes)?;
        let value: Value = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&value)? != bytes {
            return Err(Error::Invalid(
                "durable task payload is not canonical JSON".into(),
            ));
        }
        Ok(value)
    }

    async fn retained_declaration(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OperationSpec>> {
        let declaration_key = IdempotencyKey::new(format!("task-admission:{operation_id}"))?;
        let indexed = crate::distributed::observe_declaration(
            &self.stream,
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &declaration_key,
        )
        .await?;
        if indexed.is_some() {
            return Ok(indexed);
        }
        {
            // Missing/legacy locations are not evidence of missing admission.
            // Bootstrap through the existing authoritative reducer, then use
            // its verified declaration. This fallback remains a mutable read.
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh().await?;
            match coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            ) {
                Ok(operation) => Ok(Some(operation.spec)),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    async fn read_admission_json(&self, reference: &FileRef) -> Result<Value> {
        self.read_json(reference)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed task admission content is missing".into())
                }
                other => other,
            })
    }

    async fn read_retained_admission(&self, operation_id: OperationId) -> Result<TaskAdmissionRecord> {
        let spec = self
            .retained_declaration(operation_id)
            .await?
            .ok_or_else(|| Error::NotFound("task admission".into()))?;
        self.read_declared_admission(&spec).await
    }

    async fn read_declared_admission(&self, spec: &OperationSpec) -> Result<TaskAdmissionRecord> {
        let operation_id = spec.operation_id;
        if let Some(limits) = self.session_limits {
            let mut coordinator = self.coordinator.lock().await;
            // Session configuration and parent links are immutable. A cached
            // hierarchy can check these pinned ceilings without refreshing
            // unrelated lifecycle events; unknown descendants still bootstrap.
            let retained = match coordinator.scheduler().session_limits(operation_id) {
                Err(Error::NotFound(_)) => {
                    coordinator.refresh().await?;
                    coordinator.scheduler().session_limits(operation_id)?
                }
                other => other?,
            };
            if retained != limits {
                return Err(Error::Conflict(
                    "retained session ceilings differ from owner bindings".into(),
                ));
            }
        }
        let value = self.read_admission_json(&spec.state).await?;
        let admission = TaskAdmissionRecord::from_canonical_value(value)?;
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "durable task policy differs from its admission".into(),
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate()?;
        }
        if admission.extensions.as_ref() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "retained task extensions differ from owner bindings".into(),
            ));
        }
        if admission.operation_id != operation_id
            || admission.task.name != spec.entrypoint.name
            || admission.task.version != spec.entrypoint.version
            || admission.task.digest != spec.entrypoint.digest
            || admission.output_schema != spec.entrypoint.result_schema
            || spec.parent
                != admission.parent.map(|parent| ParentLink {
                    operation_id: OperationId::from_bytes(parent.into_bytes()),
                    slot: operation_id.to_string(),
                })
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || self.machines.resolve(&admission.machine).is_none()
        {
            return Err(Error::Conflict(
                "durable task admission disagrees with its declaration".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input),
        )?;
        Ok(admission)
    }
    async fn admission(&self, operation_id: OperationId) -> Result<TaskAdmissionRecord> {
        let admission = self.read_retained_admission(operation_id).await?;
        self.authenticated_scope(&admission).await?;
        Ok(admission)
    }


    async fn validate_parent_scope(
        &self,
        parent: Option<TaskId>,
        scope: &RuntimeScope,
    ) -> Result<()> {
        if let Some(parent) = parent {
            let admitted = self
                .read_retained_admission(OperationId::from_bytes(parent.into_bytes()))
                .await?;
            let allowed = Capabilities::new(admitted.grants.iter().chain(
                scope.created_grants().into_iter().flat_map(Capabilities::iter),
            ));
            RuntimeScope::new(allowed, admitted.limits)?
                .with_run_limits(admitted.run_limits)?
                .narrow(scope.grants().clone(), scope.limits())?
                .with_run_limits(scope.run_limits())?;
        }
        Ok(())
    }

    fn validate_local_admission_policy(&self, admission: &TaskAdmissionRecord) -> Result<()> {
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "task admission policy differs from owner".into(),
            ));
        }
        if admission.execution.is_some() {
            return Err(Error::Unsupported(
                "local durable host cannot admit a remote execution route".into(),
            ));
        }
        Ok(())
    }

    fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v3/mail/by-recipient/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn batch_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/batches/{batch_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn batch_cancellation_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/batch-cancellations/{batch_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn retained_batch_cancellation(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchCancellation>> {
        let stream = self.batch_cancellation_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch cancellation history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage(
                "batch cancellation record is missing".into(),
            ));
        };
        let declaration: BatchCancellation = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if declaration.batch_id != batch_id
            || crate::contract::canonical_json_bytes(&declaration)? != record.value.as_ref()
        {
            return Err(Error::Storage(
                "batch cancellation record is not canonical".into(),
            ));
        }
        Ok(Some(declaration))
    }

    async fn declare_batch_cancellation(&self, request: &DurableBatchRequest) -> Result<()> {
        let declaration = BatchCancellation {
            batch_id: request.batch_id,
            group_id: request.group_id,
        };
        let bytes = crate::contract::canonical_json_bytes(&declaration)?;
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        self.publish_control(
            &self.batch_cancellation_stream(request.batch_id)?,
            "batch-cancel",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &bytes,
        )
        .await?;
        if self.retained_batch_cancellation(request.batch_id).await? != Some(declaration) {
            return Err(Error::Conflict(
                "batch cancellation belongs to another group".into(),
            ));
        }
        Ok(())
    }

    async fn retained_batch(&self, batch_id: BatchId) -> Result<Option<Value>> {
        let stream = self.batch_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch manifest history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage("batch manifest record is missing".into()));
        };
        let reference: FileRef = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&reference)? != record.value.as_ref() {
            return Err(Error::Storage("batch manifest ref is not canonical".into()));
        }
        self.read_json(&reference)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed batch manifest content is missing".into())
                }
                other => other,
            })
            .map(Some)
    }

    async fn commit_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        let canonical = request.canonical_value();
        let bytes = crate::contract::canonical_json_bytes(&canonical)?;
        if bytes.len() as u64 > request.scope.limits().file_bytes {
            return Err(Error::Invalid("batch manifest exceeds file limit".into()));
        }
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        let key = format!("batch-request:{}", request.batch_id);
        let reference = self.payloads.stage(operation_id, &key, &bytes).await?;
        reference.descriptor().verify(&bytes)?;
        if self.read_json(&reference).await? != canonical {
            return Err(Error::Storage("staged batch request changed".into()));
        }
        let published = crate::contract::canonical_json_bytes(&reference)?;
        self.publish_control(
            &self.batch_stream(request.batch_id)?,
            "batch",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &published,
        )
        .await?;
        if self.retained_batch(request.batch_id).await? != Some(canonical) {
            return Err(Error::Conflict(
                "batch identity belongs to another request".into(),
            ));
        }
        Ok(())
    }

    async fn validate_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        request.validate()?;
        if request.execution.is_some()
            || request.policy != self.policy.as_ref().map(|policy| policy.identity())
            || request.machine.name != request.task.name
            || request.machine.version != request.task.version
            || request.machine.digest == [0; 32]
            || self.machines.resolve(&request.machine).is_none()
        {
            return Err(Error::Invalid(
                "batch implementation or policy is invalid".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &request.task,
            &request.machine,
            Some(&request.input_schema),
            &request.output_schema,
            None,
        )?;
        if request.scope.resource_proofs().is_empty() {
            self.root_scope.narrow(request.scope.grants().clone(), request.scope.limits())?
                .with_run_limits(request.scope.run_limits())?;
        } else if request.inputs.is_empty() {
            let parent = request.parent.ok_or_else(|| Error::Unauthorized("created batch has no parent".into()))?;
            let admitted = self.read_retained_admission(OperationId::from_bytes(parent.into_bytes())).await?;
            if request.scope.resource_proofs().iter().any(|proof| !admitted.resource_proofs.contains(proof)) {
                return Err(Error::Unauthorized("empty batch resource proofs differ from parent".into()));
            }
            self.authenticated_scope(&admitted).await?
                .narrow(request.scope.grants().clone(), request.scope.limits())?
                .with_run_limits(request.scope.run_limits())?;
        } else {
            let first = request.member_admission(0)?;
            self.authenticated_scope(&first).await?;
        }
        if request.parent.is_some() {
            require_descendant_grant(request.scope.grants(), &request.task)?;
        }
        if request.scope.extensions() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "batch extensions differ from owner bindings".into(),
            ));
        }
        validate_task_schemas(&request.input_schema, &request.output_schema, true)?;
        let input = crate::contract::compile_json_schema(&request.input_schema, "batch input")?;
        for value in &request.inputs {
            input.validate(value).map_err(|error| {
                Error::Invalid(format!("batch input failed validation: {error}"))
            })?;
        }
        Ok(())
    }

    fn event_key(kind: &str, task_id: TaskId, operation_id: OperationId) -> Result<StreamKey> {
        let identity = format!("harness/v2/{kind}/{task_id}/{operation_id}");
        StreamKey::new(Bytes::copy_from_slice(
            blake3::hash(identity.as_bytes()).as_bytes(),
        ))
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn timer_stream(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/timers/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn authorize_mail_route(
        sender: TaskId,
        sender_admission: &TaskAdmissionRecord,
        recipient: TaskId,
        recipient_admission: &TaskAdmissionRecord,
    ) -> Result<bool> {
        let direct = sender_admission.parent == Some(recipient)
            || recipient_admission.parent == Some(sender);
        if !direct
            && (!sender_admission.grants.contains(capability::MAIL_SEND)
                || !recipient_admission.grants.contains(capability::MAIL_READ))
        {
            return Err(Error::Unauthorized(
                "custom mail route requires sender mail:send and recipient mail:read".into(),
            ));
        }
        Ok(direct)
    }

    /// Resolves only the authenticated donor's exact immutable parcel bytes.
    /// This does not install a donor reader or grant in the recipient scope.
    async fn read_mail_source(
        &self, admission: &TaskAdmissionRecord, payload: &FileRef,
    ) -> Result<Vec<u8>> {
        payload.validate()?;
        admission.limits.validate_file(payload)?;
        if !read_granted(&admission.grants, payload)? {
            return Err(Error::Unauthorized("mail donor cannot read its payload".into()));
        }
        let scope = self.authenticated_scope(admission).await?;
        let bytes = if let Some(reader) = scope.created_reference(payload) {
            reader.read(payload).await?
        } else if let Some(binding) = scope.created_content(Some(payload.volume())) {
            binding.reader.read(payload).await?
        } else {
            self.reader.read(payload).await?
        };
        payload.descriptor().verify(&bytes)?;
        Ok(bytes)
    }

    async fn mail_bytes(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<Vec<u8>> {
        payload.validate()?;
        let sender_admission = self
            .admission(OperationId::from_bytes(sender.into_bytes()))
            .await?;
        let recipient_admission = self
            .admission(OperationId::from_bytes(recipient.into_bytes()))
            .await?;
        let direct = Self::authorize_mail_route(sender, &sender_admission, recipient, &recipient_admission)?;
        recipient_admission.limits.validate_file(&payload)?;
        if !direct && !read_granted(&recipient_admission.grants, &payload)? {
            return Err(Error::Unauthorized(
                "recipient cannot read the mailed file".into(),
            ));
        }
        sender_admission.limits.validate_file(&payload)?;
        if !read_granted(&sender_admission.grants, &payload)? {
            return Err(Error::Unauthorized("mail donor cannot read its payload".into()));
        }
        let event = MailEvent {
            sender,
            recipient,
            message_id,
            schema_revision: MAIL_SCHEMA_REVISION,
            route_revision: MAIL_ROUTE_REVISION,
            payload,
        };
        event.encode(acyclic_stream::MAX_RECORD_BYTES as u64)
    }

    /// Observes the original atomic mail receipt; never publishes another send.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn reconcile_sent_mail(
        &self,
        sender: TaskId,
        fence: crate::scheduler::LeaseFence,
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
    ) -> Result<bool> {
        self.verify_owner(sender, &fence, true).await?;
        let bytes = self.mail_bytes(sender, recipient, message, payload).await?;
        let retained = self.observe_mail_location(sender, recipient, message, &bytes, None).await?;
        self.verify_owner(sender, &fence, true).await?;
        Ok(retained)
    }

    fn mail_location_path(
        sender: TaskId,
        message: OperationId,
    ) -> Result<acyclic_stream::StreamPath> {
        Ok(acyclic_stream::StreamPath::new(format!(
            "harness/v3/mail/by-intent/{sender}/{message}"
        ))?)
    }

    async fn observe_mail_location(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        bytes: &[u8],
        expected_sequence: Option<u64>,
    ) -> Result<bool> {
        let location = self
            .stream
            .stream(Self::mail_location_path(sender, message)?.as_str())?;
        let records = match location.read(0, 2).await {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let [record] = records.as_slice() else {
            return Err(Error::Storage(
                "mail location must contain one record".into(),
            ));
        };
        if record.sequence != 0 || record.value.len() > MAX_MAIL_LOCATION_BYTES {
            return Err(Error::Storage(
                "mail location exceeds its bounded format".into(),
            ));
        }
        let retained: MailLocation = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if retained.sender != sender
            || retained.message_id != message
            || retained.schema_revision != MAIL_SCHEMA_REVISION
            || retained.route_revision != MAIL_ROUTE_REVISION
            || crate::contract::canonical_json_bytes(&retained)?.as_slice() != record.value.as_ref()
        {
            return Err(Error::Storage("mail location identity differs".into()));
        }
        if expected_sequence.is_some_and(|sequence| sequence != retained.sequence) {
            return Err(Error::Storage(
                "inbox record differs from its atomic location".into(),
            ));
        }
        let mailbox = self.mailbox(retained.recipient)?;
        let original = mailbox
            .read(retained.sequence, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let [original] = original.as_slice() else {
            return Err(Error::Storage("mail location source is absent".into()));
        };
        if original.sequence != retained.sequence
            || original.commit_id != record.commit_id
            || blake3::hash(&original.value).as_bytes() != &retained.value_digest
        {
            return Err(Error::Storage(
                "mail location differs from its atomic source".into(),
            ));
        }
        // Equality with the already validated canonical event checks the whole
        // payload, including its immutable generation, without decoding a body
        // from an untrusted location. No receipt grants publication authority.
        if retained.recipient != recipient || original.value.as_ref() != bytes {
            return Err(Error::Conflict("mail identity reused".into()));
        }
        Ok(true)
    }

    fn mail_location(
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        sequence: u64,
        bytes: &[u8],
    ) -> Result<(MailLocation, Bytes)> {
        let location = MailLocation {
            sender,
            message_id: message,
            recipient,
            schema_revision: MAIL_SCHEMA_REVISION,
            route_revision: MAIL_ROUTE_REVISION,
            sequence,
            value_digest: *blake3::hash(bytes).as_bytes(),
        };
        crate::contract::validate_json_byte_bound(&location, MAX_MAIL_LOCATION_BYTES as u64)?;
        let encoded = Bytes::from(crate::contract::canonical_json_bytes(&location)?);
        Ok((location, encoded))
    }

    fn mail_commit_key(
        location: &MailLocation,
        fence: &Option<crate::scheduler::LeaseFence>,
        conditions: &[acyclic_stream::CommitCondition],
    ) -> Result<StreamKey> {
        use acyclic_stream::CommitCondition;
        // Conflict receipts are retained too. The physical key includes every
        // exact CAS; the immutable location fences the logical intent.
        let physical = crate::contract::canonical_json_bytes(&(
            location,
            fence,
            conditions
                .iter()
                .map(|condition| match condition {
                    CommitCondition::Tail { path, expected } => (path.as_str(), Some(*expected)),
                    CommitCondition::Absent { path } => (path.as_str(), None),
                })
                .collect::<Vec<_>>(),
        ))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic.harness.mail-publication.v2\0");
        hash.update(&physical);
        Ok(StreamKey::new(Bytes::copy_from_slice(
            hash.finalize().as_bytes(),
        ))?)
    }

    async fn publish_mail(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        bytes: Bytes,
        fence: Option<crate::scheduler::LeaseFence>,
    ) -> Result<()> {
        use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest};
        let mailbox = self.mailbox(recipient)?;
        let location_path = Self::mail_location_path(sender, message)?;
        let mut source_verified = false;
        loop {
            if let Some(fence) = &fence {
                self.verify_owner(sender, fence, false).await?;
            }
            if self
                .observe_mail_location(sender, recipient, message, &bytes, None)
                .await?
            {
                if let Some(fence) = &fence {
                    self.verify_owner(sender, fence, false).await?;
                }
                return Ok(());
            }
            if !source_verified {
                let event: MailEvent = crate::contract::json_from_slice(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                if event.sender != sender || event.message_id != message {
                    return Err(Error::Storage("mail publication identity differs".into()));
                }
                let admission = self.admission(OperationId::from_bytes(sender.into_bytes())).await?;
                self.read_mail_source(&admission, &event.payload).await?;
                source_verified = true;
            }
            let (tail, target) = match mailbox.bounds().await {
                Ok(bounds) => (
                    bounds.tail,
                    CommitCondition::Tail {
                        path: mailbox.path().clone(),
                        expected: bounds.tail,
                    },
                ),
                Err(StreamError::NotFound) => (
                    0,
                    CommitCondition::Absent {
                        path: mailbox.path().clone(),
                    },
                ),
                Err(error) => return Err(error.into()),
            };
            let (location, location_bytes) =
                Self::mail_location(sender, recipient, message, tail, &bytes)?;
            let conditions = vec![
                target,
                CommitCondition::Absent {
                    path: location_path.clone(),
                },
            ];
            #[cfg(feature = "filesystem")]
            let conditions = {
                let mut conditions = conditions;
                if let Some(fence) = &fence {
                    conditions.push(
                        self.coordinator
                            .lock()
                            .await
                            .journal_condition(
                                &self.owner,
                                &self.owner_scope,
                                &self.verifier,
                                OperationId::from_bytes(sender.into_bytes()),
                                fence,
                                crate::distributed::JournalWrite::Fresh,
                            )
                            .await?,
                    );
                }
                conditions
            };
            #[cfg(not(feature = "filesystem"))]
            if fence.is_some() {
                return Err(Error::Unsupported(
                    "owned mail publication is not bound".into(),
                ));
            }
            let key = Self::mail_commit_key(&location, &fence, &conditions)?;
            let request = CommitRequest {
                conditions,
                mutations: vec![
                    CommitMutation::Append {
                        path: mailbox.path().clone(),
                        records: vec![bytes.clone()],
                    },
                    CommitMutation::Append {
                        path: location_path.clone(),
                        records: vec![location_bytes],
                    },
                ],
                idempotency_key: key,
            };
            match crate::distributed::commit_keyed(&self.stream, request, message).await? {
                CommitOutcome::Conflict(_) => continue,
                CommitOutcome::Committed(_) => {
                    if !self
                        .observe_mail_location(sender, recipient, message, &bytes, None)
                        .await?
                    {
                        return Err(Error::Storage("committed mail receipt is absent".into()));
                    }
                    if let Some(fence) = &fence {
                        self.verify_owner(sender, fence, false).await?;
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn timer_state(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<(u64, bool)> {
        let stream = self.timer_stream(task)?;
        let tail = match stream.bounds().await {
            Ok(bounds) => bounds.tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(error.into()),
        };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            let page = stream
                .read(after, (tail - after).min(64) as u32)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            if page.is_empty() || page.len() > 64 {
                return Err(Error::Storage("invalid timer replay page".into()));
            }
            for record in page {
                if record.sequence != after || after >= tail {
                    return Err(Error::Storage("timer replay sequence differs".into()));
                }
                let event: TimerEvent = crate::contract::json_from_slice(&record.value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if event.task_id != task || event.deadline_unix_ms == 0 {
                    return Err(Error::Conflict(
                        "timer history belongs to another task".into(),
                    ));
                }
                if event.operation_id == operation {
                    if found || event.deadline_unix_ms != deadline_unix_ms {
                        return Err(Error::Conflict("timer identity reused".into()));
                    }
                    found = true;
                }
                after += 1;
            }
        }
        Ok((tail, found))
    }

    async fn publish_control(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
    ) -> Result<()> {
        if self
            .publish_control_at(stream, kind, task_id, operation_id, bytes, None)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict(
                "unconditional control append conflicted".into(),
            ))
        }
    }

    async fn publish_control_at(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
        expected_tail: Option<u64>,
    ) -> Result<bool> {
        let key = Self::event_key(kind, task_id, operation_id)?;
        let key = if let Some(tail) = expected_tail {
            let mut digest = blake3::Hasher::new();
            digest.update(b"harness/v2/control-tail\0");
            digest.update(key.as_bytes());
            digest.update(&tail.to_le_bytes());
            StreamKey::new(Bytes::copy_from_slice(digest.finalize().as_bytes()))?
        } else {
            key
        };
        let outcome = crate::distributed::append_keyed(
            (stream, &self.stream),
            Bytes::copy_from_slice(bytes),
            expected_tail,
            key,
            operation_id,
            "control identity",
        )
        .await?;
        match outcome {
            AppendOutcome::Committed(receipt) if receipt.end == receipt.start + 1 => {
                let records = stream
                    .read(receipt.start, 1)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                let [record] = records.as_slice() else {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                };
                if record.sequence != receipt.start
                    || record.commit_id != receipt.commit_id
                    || record.value.as_ref() != bytes
                {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                }
                Ok(true)
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid control append receipt".into()))
            }
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }
}

impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    fn admit_owned<'a>(
        &'a self,
        request: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move { CoordinatorTaskHost::admit_owned(self, request, fence).await })
    }

    fn verify_dispatch_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, false).await })
    }

    fn verify_execution_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, true).await })
    }

    fn claim_model_dispatch<'a>(
        &'a self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            let admission = self.admission(operation_id).await?;
            if !admission.grants.contains("model:generate") {
                return Err(Error::Unauthorized(
                    "task scope lacks model:generate".into(),
                ));
            }
            let scope = RuntimeScope::new(admission.grants, admission.limits)?
                .with_run_limits(admission.run_limits)?;
            self.validate_parent_scope(admission.parent, &scope).await?;
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh().await?;
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            )?;
            crate::scheduler::require_model_owner(&operation, &fence)?;
            let session = coordinator.scheduler().session_limits(operation_id)?;
            let ceiling = (scope.limits().model_steps as u64)
                .min(
                    scope
                        .run_limits()
                        .max_steps
                        .map_or(u64::MAX, |value| value as u64),
                )
                .min(session.model_steps);
            let lease_digest = blake3::hash(&crate::contract::canonical_json_bytes(&fence)?);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new(format!(
                        "model-dispatch:{attempt_id}:{step}:{lease_digest}"
                    ))?,
                    crate::scheduler::SchedulerEvent::ModelDispatchClaimed {
                        operation_id,
                        attempt_id,
                        step,
                        request_digest,
                        fence,
                        ceiling,
                    },
                )
                .await?;
            Ok(())
        })
    }

    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }

    fn supports_batch_cancellation(&self) -> bool {
        true
    }

    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.admission(operation_id).await
        })
    }

    fn children<'a>(
        &'a self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async move {
            let mut coordinator = self.coordinator.lock().await;
            let page = coordinator
                .observe_children(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    OperationId::from_bytes(parent.into_bytes()),
                    ChildOperationPageRequest {
                        expected_revision,
                        after_slot: after_slot.as_deref(),
                        maximum,
                    },
                )
                .await?;
            Ok(TaskChildrenPage {
                revision: page.revision,
                entries: page
                    .entries
                    .into_iter()
                    .map(|entry| TaskChild {
                        slot: entry.slot,
                        task_id: crate::runtime::task_id_for_admission(entry.operation_id),
                    })
                    .collect(),
                next_after: page.next_after,
            })
        })
    }

    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async move {
            match self.admission(operation_id).await {
                Ok(admission) => Ok(Some((
                    admission.task_id(),
                    admission,
                ))),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async move {
            self.validate_batch(&request).await?;
            let Some(retained) = self.retained_batch(request.batch_id).await? else {
                return Ok(None);
            };
            if retained != request.canonical_value() {
                return Err(Error::Conflict(
                    "batch identity belongs to another request".into(),
                ));
            }
            let cancelled = self.retained_batch_cancellation(request.batch_id).await?;
            if cancelled
                .as_ref()
                .is_some_and(|declaration| declaration.group_id != request.group_id)
            {
                return Err(Error::Conflict(
                    "batch cancellation belongs to another group".into(),
                ));
            }
            let mut entries = Vec::with_capacity(request.inputs.len());
            for index in 0..request.inputs.len() {
                let operation_id = request.operation_id(index);
                let expected = request.member_admission(index)?;
                match self.admission(operation_id).await {
                    Ok(admission) if admission == expected => entries.push(Admission::Accepted(
                        admission.task_id(),
                    )),
                    Ok(_) => {
                        return Err(Error::Conflict(
                            "batch member belongs to another admission".into(),
                        ));
                    }
                    Err(Error::NotFound(_)) if cancelled.is_some() => {
                        entries.push(Admission::Rejected {
                            reason: "batch cancellation preceded member admission".into(),
                        });
                    }
                    Err(Error::NotFound(_)) => {
                        entries.push(Admission::Indeterminate { operation_id });
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(Some(entries))
        })
    }

    fn load_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async move {
            let Some(value) = self.retained_batch(batch_id).await? else {
                return Ok(None);
            };
            let request = DurableBatchRequest::from_canonical_value(value)?;
            if request.batch_id != batch_id {
                return Err(Error::Conflict(
                    "batch manifest has another identity".into(),
                ));
            }
            self.validate_batch(&request).await?;
            Ok(Some(request))
        })
    }

    fn admit_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async move {
            let parent_fence = if request.scope.resource_proofs().is_empty() {
                None
            } else {
                let parent = request.parent.ok_or_else(|| Error::Unauthorized("created batch has no parent".into()))?;
                Some(request.scope.current_creation_parent_fence(parent).await?)
            };
            self.validate_batch(&request).await?;
            self.validate_parent_scope(request.parent, &request.scope)
                .await?;
            self.commit_batch(&request).await?;
            let observed = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| Error::Storage("committed batch disappeared".into()))?;
            let mut entries = Vec::with_capacity(observed.len());
            for (index, admission) in observed.into_iter().enumerate() {
                match admission {
                    Admission::Indeterminate { operation_id } => {
                        let member = request.member_admission(index)?;
                        if member.operation_id != operation_id {
                            return Err(Error::Conflict("batch member operation changed".into()));
                        }
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                        {
                            entries.push(Admission::Rejected {
                                reason: "batch cancellation preceded member admission".into(),
                            });
                            continue;
                        }
                        let admitted = self.admit_record(member, parent_fence.clone()).await?;
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                            && let Admission::Accepted(task_id) = &admitted
                        {
                            self.cancel(*task_id).await?;
                            entries.push(admitted);
                            continue;
                        }
                        entries.push(admitted);
                    }
                    other => entries.push(other),
                }
            }
            Ok(entries)
        })
    }

    fn cancel_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async move {
            let Some(request) = self.load_batch(batch_id).await? else {
                return Ok(None);
            };
            self.declare_batch_cancellation(&request).await?;
            let admissions = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| {
                    Error::Storage("retained batch disappeared during cancellation".into())
                })?;
            let mut entries = Vec::with_capacity(admissions.len());
            for (index, admission) in admissions.into_iter().enumerate() {
                let status = match admission {
                    Admission::Accepted(task_id) => match self.cancel(task_id).await {
                        Ok(()) => BatchCancellationStatus::Requested,
                        Err(Error::Indeterminate(operation_id)) => {
                            BatchCancellationStatus::Indeterminate { operation_id }
                        }
                        Err(error) => BatchCancellationStatus::Unresolved {
                            message: error.to_string(),
                        },
                    },
                    Admission::Rejected { reason } => {
                        BatchCancellationStatus::NotAdmitted { reason }
                    }
                    Admission::Indeterminate { operation_id } => {
                        BatchCancellationStatus::Indeterminate { operation_id }
                    }
                };
                entries.push((InputKey { batch_id, index }, status));
            }
            Ok(Some(BatchCancellationReport {
                group_id: request.group_id,
                batch_id,
                entries,
            }))
        })
    }

    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move {
            if task_id.into_bytes() != operation_id.into_bytes() {
                return Err(Error::Unauthorized(
                    "task identity does not match its admission".into(),
                ));
            }
            let admission = self.read_retained_admission(operation_id).await?;
            let scope = self.authenticated_scope(&admission).await?;
            if admission.resource_proofs.is_empty() {
                return Ok(scope);
            }
            let fence = {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh().await?;
                let operation = coordinator.observe_operation(
                    &self.owner, &self.owner_scope, &self.verifier, operation_id,
                )?;
                operation.reservation.as_ref().map(crate::scheduler::LeaseFence::from)
            };
            Ok(scope.with_created_publication_lease(Arc::new(CreatedTaskPublicationLease {
                coordinator: self.coordinator.clone(),
                owner: self.owner.clone(),
                scope: self.owner_scope.clone(),
                verifier: self.verifier.clone(),
                operation: operation_id,
                fence,
                clock: self.clock.clone(),
                deadline_epoch_ms: admission.run_limits.deadline_epoch_ms,
            })))
        })
    }

    fn admit<'a>(
        &'a self,
        admission: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move { self.admit_record(admission, None).await })
    }

    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            let observed = {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh().await?;
                coordinator.observe_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                )?
            };
            match observed.outcome {
                Some(Outcome::Succeeded(reference)) => {
                    Ok(Some(Outcome::Succeeded(self.read_json(&reference).await?)))
                }
                Some(Outcome::Failed { message }) => Ok(Some(Outcome::Failed { message })),
                Some(Outcome::Cancelled) => Ok(Some(Outcome::Cancelled)),
                Some(Outcome::Indeterminate { operation_id }) => {
                    Ok(Some(Outcome::Indeterminate { operation_id }))
                }
                None => Ok(None),
            }
        })
    }

    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.coordinator
                .lock()
                .await
                .cancel_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                    IdempotencyKey::new(format!("task-cancel:{operation_id}"))?,
                    true,
                )
                .await?;
            Ok(())
        })
    }

    fn interact<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        Box::pin(async move {
            interaction.validate()?;
            if operation_id.into_bytes() == task_id.into_bytes() {
                return Err(Error::Invalid(
                    "interaction operation must differ from task admission".into(),
                ));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !admission.grants.contains(capability::INTERACTION_ROUTE) {
                return Err(Error::Unauthorized(
                    "task scope lacks interaction:route".into(),
                ));
            }
            let journal = self.interactions.as_ref().ok_or_else(|| {
                Error::Unsupported("durable interaction journal is not bound".into())
            })?;
            // A caller-selected operation ID is meaningful only within its
            // admitted task. Namespace the journal ticket by both identities
            // so another task cannot observe or resolve its interaction.
            let id = task_interaction_id(task_id, operation_id);
            journal.open_interaction(id, interaction).await?;
            Ok(journal
                .interaction_outcome(id)
                .await?
                .unwrap_or(InteractionOutcome::Indeterminate { operation_id }))
        })
    }

    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // The explicit owner-level custom route is not the task-context
            // default. Only a retained sender lease enables implicit topology.
            let recipient_scope = self.admission(OperationId::from_bytes(recipient.into_bytes())).await?;
            if !self.admission(OperationId::from_bytes(sender.into_bytes())).await?
                .grants.contains(capability::MAIL_SEND)
                || !recipient_scope.grants.contains(capability::MAIL_READ)
                || !read_granted(&recipient_scope.grants, &payload)?
            {
                return Err(Error::Unauthorized("unfenced custom mail requires explicit mail and file grants".into()));
            }
            let bytes = Bytes::from(
                self.mail_bytes(sender, recipient, message_id, payload)
                    .await?,
            );
            self.publish_mail(sender, recipient, message_id, bytes, None)
                .await
        })
    }

    fn inbox<'a>(
        &'a self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(async move {
            if limit == 0 {
                return Err(Error::Invalid("inbox page bound is invalid".into()));
            }
            let recipient_admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            let mailbox = self.mailbox(task_id)?;
            let bounds = match mailbox.bounds().await {
                Ok(bounds) => bounds,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            if after > bounds.tail {
                return Err(Error::Invalid("inbox cursor is beyond the tail".into()));
            }
            let page_limit = u32::try_from(limit.min(acyclic_stream::MAX_ITEMS))
                .map_err(|_| Error::Invalid("inbox page bound is invalid".into()))?;
            let page = match mailbox.read(after, page_limit).await {
                Ok(records) => records.try_collect::<Vec<_>>().await?,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            if page.len() > limit {
                return Err(Error::Storage("inbox page exceeds requested limit".into()));
            }
            let mut expected = after;
            let mut items = Vec::with_capacity(page.len());
            for record in page {
                if record.sequence != expected {
                    return Err(Error::Storage("inbox replay sequence differs".into()));
                }
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| Error::Storage("inbox sequence exhausted".into()))?;

                let event = MailEvent::decode(&record.value, task_id)?;
                event.payload.validate()?;
                recipient_admission.limits.validate_file(&event.payload)?;
                let sender_admission = self
                    .admission(OperationId::from_bytes(event.sender.into_bytes()))
                    .await?;
                let direct = Self::authorize_mail_route(
                    event.sender, &sender_admission, task_id, &recipient_admission,
                )?;
                if !direct && !read_granted(&recipient_admission.grants, &event.payload)? {
                    return Err(Error::Unauthorized("custom mail history contains an unreadable payload".into()));
                }
                sender_admission.limits.validate_file(&event.payload)?;
                if !read_granted(&sender_admission.grants, &event.payload)? {
                    return Err(Error::Unauthorized("mail source cannot read its payload".into()));
                }
                if !self
                    .observe_mail_location(
                        event.sender,
                        task_id,
                        event.message_id,
                        &record.value,
                        Some(record.sequence),
                    )
                    .await?
                {
                    return Err(Error::Unsupported(
                        "unindexed mail format is not supported".into(),
                    ));
                }
                items.push(InboxItem {
                    task_id,
                    sequence: record
                        .sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("inbox sequence exhausted".into()))?,
                    message_id: event.message_id.to_string(),
                    payload: event.payload,
                });
            }
            Ok(items)
        })
    }

    fn wait_until<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if deadline_unix_ms == 0 {
                return Err(Error::Invalid("timer deadline is invalid".into()));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !admission.grants.contains(capability::TIMER_WAIT) {
                return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
            }
            let event = TimerEvent {
                task_id,
                operation_id,
                deadline_unix_ms,
            };
            let bytes = crate::contract::canonical_json_bytes(&event)?;
            let stream = self.timer_stream(task_id)?;
            loop {
                let (tail, found) = self
                    .timer_state(task_id, operation_id, deadline_unix_ms)
                    .await?;
                if found {
                    break;
                }
                if self
                    .publish_control_at(
                        &stream,
                        "timers",
                        task_id,
                        operation_id,
                        &bytes,
                        Some(tail),
                    )
                    .await?
                {
                    break;
                }
            }
            loop {
                let now = self.clock.now_unix_millis();
                if now >= deadline_unix_ms {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(
                    deadline_unix_ms.saturating_sub(now).min(60_000),
                ))
                .await;
            }
        })
    }

    fn execute_tool<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
        scope: RuntimeScope,
        context: crate::runtime::ToolContext,
    ) -> BoxFuture<'a, Result<Outcome<Value>>> {
        Box::pin(async move {
            if operation_id.into_bytes() == task_id.into_bytes() {
                return Err(Error::Invalid(
                    "tool operation must differ from task admission".into(),
                ));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            self.root_scope
                .narrow(admission.grants, admission.limits)?
                .narrow(scope.grants().clone(), scope.limits())?;
            if context.task().durable_task_id() != Some(task_id)
                || context.task().scope().grants() != scope.grants()
                || context.task().scope().limits() != scope.limits()
                || context.operation_id() != operation_id
                || context.call_id() != operation_id.to_string()
            {
                return Err(Error::Unauthorized(
                    "tool context does not match the admitted task".into(),
                ));
            }
            let capability = capability::tool_call(&definition.name);
            if !scope.grants().contains(&capability) {
                return Err(Error::Unauthorized(format!(
                    "task scope lacks {capability}"
                )));
            }
            let tools = self
                .tools
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable tool runner is not bound".into()))?;
            if let Some(policy) = &self.policy {
                if admission.policy.as_ref() != Some(&policy.identity()) {
                    return Err(Error::Conflict(
                        "durable policy changed before tool dispatch".into(),
                    ));
                }
                if let Some((approval_id, request)) = context
                    .task()
                    .policy_approval(
                        policy.as_ref(),
                        &definition,
                        &crate::tool::ToolInvocation {
                            operation_id,
                            call_id: operation_id.to_string(),
                            name: definition.name.clone(),
                            arguments: arguments.clone(),
                        },
                    )
                    .await?
                {
                    check_tool_approval(self.interact(task_id, approval_id, request).await?)?;
                }
                if admission.policy.as_ref() != Some(&policy.identity()) {
                    return Err(Error::Conflict(
                        "durable policy changed before tool execution".into(),
                    ));
                }
            }
            tools
                .run_with_context(task_id, operation_id, definition, arguments, context)
                .await
        })
    }

    fn reconcile_effect<'a>(
        &'a self,
        task_id: TaskId,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        Box::pin(async move {
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !admission.grants.contains(capability::EFFECT_RUN) {
                return Err(Error::Unauthorized("task scope lacks effect:run".into()));
            }
            let effects = self
                .effects
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable effect observer is not bound".into()))?;
            effects.reconcile(effect_id).await
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, GroupId,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        core::{AggregateKind, AuthorityIssuer},
        resources::ProviderRef,
        runtime::TaskDefinition,
        workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine},
    };
    use acyclic_stream::{MemoryStream, SystemUnixMillisClock};

    #[cfg(feature = "filesystem")]
    mod resource_preparation;
    #[cfg(feature = "filesystem")]
    pub(crate) mod created_resource_access;

    struct MemoryPayloads {
        volume: VolumeRef,
        files: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    impl MemoryPayloads {
        fn new() -> Result<Self> {
            Ok(Self {
                volume: VolumeRef::new(
                    ProviderRef::new("batch-test", "filesystem", "2")?,
                    "private",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                )?,
                files: Mutex::new(BTreeMap::new()),
            })
        }
    }

    // Finite byte-identity model: two choices for each of the six envelope
    // fields. Unsupported revisions are identity probes, not accepted mail.
    #[tokio::test]
    async fn bounded_mail_envelope_identity_and_missing_field_controls() -> Result<()> {
        let payloads = MemoryPayloads::new()?;
        let operation = OperationId::from_bytes([8; 16]);
        let first = payloads.stage(operation, "first", b"7").await?;
        let second = payloads.stage(operation, "second", b"8").await?;
        let mut cases = Vec::new();
        let mut identities = BTreeSet::new();
        for mask in 0..64_u8 {
            let event = MailEvent {
                sender: TaskId::from_bytes([1 + (mask & 1); 16]),
                recipient: TaskId::from_bytes([3 + ((mask >> 1) & 1); 16]),
                message_id: OperationId::from_bytes([5 + ((mask >> 2) & 1); 16]),
                schema_revision: 1 + u32::from((mask >> 3) & 1),
                route_revision: 1 + u32::from((mask >> 4) & 1),
                payload: if mask & 32 == 0 { first.clone() } else { second.clone() },
            };
            let bytes = crate::contract::canonical_json_bytes(&event)?;
            assert!(identities.insert(bytes.clone()), "different envelopes aliased");
            if mask & 24 == 0 {
                let restored = MailEvent::decode(&bytes, event.recipient)?;
                assert_eq!(crate::contract::canonical_json_bytes(&restored)?, bytes);
            } else {
                assert!(matches!(
                    MailEvent::decode(&bytes, event.recipient),
                    Err(Error::Storage(_))
                ));
            }
            cases.push(event);
        }
        assert_eq!(identities.len(), 64);
        for omitted in [
            "sender", "recipient", "message_id", "schema_revision", "route_revision", "payload",
        ] {
            let mut broken_identities = BTreeSet::new();
            for event in &cases {
                let mut fields = serde_json::to_value(event)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert!(fields.as_object_mut().expect("mail object").remove(omitted).is_some());
                let bytes = crate::contract::canonical_json_bytes(&fields)?;
                assert!(matches!(
                    MailEvent::decode(&bytes, event.recipient),
                    Err(Error::Storage(_))
                ), "missing field accepted: {omitted}");
                broken_identities.insert(bytes);
            }
            assert_eq!(broken_identities.len(), 32, "field-removal probe: {omitted}");
        }
        let event = &cases[0];
        let encoded = crate::contract::canonical_json_bytes(event)?;
        assert_eq!(event.encode(encoded.len() as u64)?, encoded);
        assert!(matches!(
            event.encode(encoded.len() as u64 - 1),
            Err(Error::Invalid(_))
        ));
        let mut previous = serde_json::to_value(event)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        for field in ["recipient", "schema_revision", "route_revision"] {
            previous.as_object_mut().expect("mail object").remove(field);
        }
        let previous_size = crate::contract::canonical_json_bytes(&previous)?.len() as u64;
        assert!(matches!(event.encode(previous_size), Err(Error::Invalid(_))));
        Ok(())
    }

    #[tokio::test]
    async fn atomic_mail_envelope_reuse_and_hostile_inbox_controls() -> Result<()> {
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let payloads = Arc::new(MemoryPayloads::new()?);
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "mail-envelope-owner".into(),
        };
        let issuer = AuthorityIssuer::new("mail-envelope", [7; 32], authority.clone());
        let grants = Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:test.mail@1".to_owned(),
            "mail:send".to_owned(),
            "mail:read".to_owned(),
            payloads.volume.capability(crate::conversation::VolumeOperation::Read)?,
        ]);
        let signed = issuer.root("owner", grants.clone());
        let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
        let implementation = Arc::new(BatchMachine {
            identity: MachineIdentity {
                name: "test.mail".into(),
                version: "1".into(),
                digest: [3; 32],
            },
            schema: serde_json::json!({"type":"integer"}),
        });
        let definition = TaskDefinition::<Value, Value>::resumable(
            implementation.clone(),
            serde_json::json!({"type":"integer"}),
            serde_json::json!({"type":"integer"}),
        )?;
        let task_identity = definition.identity().clone();
        let machine = implementation.identity.clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation)?;
        let host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            authority,
            signed,
            issuer.verifier(),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?;
        for index in 1..=3_u8 {
            host.admit(TaskAdmissionRecord {
                resource_proofs: Vec::new(),
                operation_id: OperationId::from_bytes([index; 16]),
                task: task_identity.clone(),
                machine: machine.clone(),
                input: serde_json::json!(0),
                input_schema: serde_json::json!({"type":"integer"}),
                output_schema: serde_json::json!({"type":"integer"}),
                parent: None,
                grants: runtime_scope.grants().clone(),
                limits: runtime_scope.limits(),
                run_limits: runtime_scope.run_limits(),
                policy: None,
                extensions: None,
                execution: None,
            }).await?;
        }
        let sender = TaskId::from_bytes([1; 16]);
        let recipient = TaskId::from_bytes([2; 16]);
        let other = TaskId::from_bytes([3; 16]);
        let message = OperationId::from_bytes([8; 16]);
        let payload = payloads.stage(message, "body", b"7").await?;
        let changed = payloads.stage(message, "changed", b"8").await?;
        host.send(sender, recipient, message, payload.clone()).await?;
        host.send(sender, recipient, message, payload.clone()).await?;
        let retained = host.mailbox(recipient)?.read(0, 1).await?
            .try_collect::<Vec<_>>().await?;
        let event = MailEvent::decode(&retained[0].value, recipient)?;
        assert_eq!(event.sender, sender);
        assert_eq!(event.recipient, recipient);
        assert_eq!(event.message_id, message);
        assert_eq!(event.schema_revision, MAIL_SCHEMA_REVISION);
        assert_eq!(event.route_revision, MAIL_ROUTE_REVISION);
        assert_eq!(host.inbox(recipient, 0, 4).await?.len(), 1);
        for (destination, body) in [(other, payload.clone()), (recipient, changed)] {
            assert!(matches!(
                host.send(sender, destination, message, body).await,
                Err(Error::Conflict(_))
            ));
        }
        for (schema_revision, route_revision) in [(2, 1), (1, 2)] {
            let altered = MailEvent {
                schema_revision, route_revision,
                ..MailEvent::decode(&retained[0].value, recipient)?
            };
            let bytes = Bytes::from(crate::contract::canonical_json_bytes(&altered)?);
            assert!(matches!(
                host.publish_mail(sender, recipient, message, bytes, None).await,
                Err(Error::Conflict(_))
            ));
        }
        assert_eq!(host.mailbox(recipient)?.bounds().await?.tail, 1);
        assert!(host.inbox(other, 0, 4).await?.is_empty());
        assert!(host.outcome(recipient).await?.is_none());

        let original = serde_json::to_value(&event)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut hostile = Vec::new();
        let mut wrong_recipient = original.clone();
        wrong_recipient["recipient"] = serde_json::json!(other);
        hostile.push(("recipient".to_owned(), wrong_recipient));
        for field in ["schema_revision", "route_revision"] {
            for revision in [0_u32, 2, u32::MAX] {
                let mut value = original.clone();
                value[field] = serde_json::json!(revision);
                hostile.push((format!("{field}:{revision}"), value));
            }
        }
        for field in [
            "sender", "recipient", "message_id", "schema_revision", "route_revision", "payload",
        ] {
            let mut value = original.clone();
            assert!(value.as_object_mut().expect("mail object").remove(field).is_some());
            hostile.push((format!("missing:{field}"), value));
        }
        let mut unknown = original.clone();
        unknown["unexpected"] = serde_json::json!(true);
        hostile.push(("unknown-field".into(), unknown));
        for (index, (label, mut value)) in hostile.into_iter().enumerate() {
            let operation = OperationId::from_bytes([100 + index as u8; 16]);
            if value.get("message_id").is_some() {
                value["message_id"] = serde_json::json!(operation);
            }
            let bytes = Bytes::from(crate::contract::canonical_json_bytes(&value)?);
            // Retain hostile bytes through the real atomic engine, including a
            // genuine shared-commit location. Rejection must come from the
            // production envelope reader, not a missing receipt or mock.
            host.publish_mail(sender, recipient, operation, bytes, None).await?;
            assert!(matches!(
                host.inbox(recipient, 1 + index as u64, 1).await,
                Err(Error::Storage(_))
            ), "hostile envelope exposed: {label}");
        }
        for fault in 0..7_u8 {
            use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest};
            let operation = OperationId::from_bytes([150 + fault; 16]);
            let event = MailEvent {
                sender, recipient, message_id: operation,
                schema_revision: MAIL_SCHEMA_REVISION,
                route_revision: MAIL_ROUTE_REVISION,
                payload: payload.clone(),
            };
            let bytes = Bytes::from(event.encode(acyclic_stream::MAX_RECORD_BYTES as u64)?);
            let mailbox = host.mailbox(recipient)?;
            let sequence = mailbox.bounds().await?.tail;
            let (location, _) = CoordinatorTaskHost::<MemoryStream>::mail_location(
                sender, recipient, operation, sequence, &bytes,
            )?;
            let mut receipt = serde_json::to_value(location)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match fault {
                0 => receipt["schema_revision"] = serde_json::json!(0),
                1 => receipt["schema_revision"] = serde_json::json!(2),
                2 => receipt["route_revision"] = serde_json::json!(0),
                3 => receipt["route_revision"] = serde_json::json!(2),
                4 => { receipt.as_object_mut().expect("location object").remove("schema_revision"); }
                5 => { receipt.as_object_mut().expect("location object").remove("route_revision"); }
                _ => receipt["unexpected"] = serde_json::json!(true),
            }
            let location_path = CoordinatorTaskHost::<MemoryStream>::mail_location_path(sender, operation)?;
            let committed = stream.commit(CommitRequest {
                conditions: vec![
                    CommitCondition::Tail { path: mailbox.path().clone(), expected: sequence },
                    CommitCondition::Absent { path: location_path.clone() },
                ],
                mutations: vec![
                    CommitMutation::Append { path: mailbox.path().clone(), records: vec![bytes] },
                    CommitMutation::Append {
                        path: location_path,
                        records: vec![Bytes::from(crate::contract::canonical_json_bytes(&receipt)?)],
                    },
                ],
                idempotency_key: StreamKey::new(Bytes::from(vec![200 + fault]))?,
            }).await?;
            assert!(matches!(committed, CommitOutcome::Committed(_)));
            assert!(matches!(
                host.inbox(recipient, sequence, 1).await,
                Err(Error::Storage(_))
            ), "malformed atomic location exposed: {fault}");
        }
        Ok(())
    }


    impl SchedulerPayloadStore for MemoryPayloads {
        fn stage<'a>(
            &'a self,
            operation_id: OperationId,
            idempotency_key: &'a str,
            bytes: &'a [u8],
        ) -> BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move {
                let path = format!(
                    "scheduler/{operation_id}/{}.json",
                    blake3::hash(idempotency_key.as_bytes()).to_hex(),
                );
                let mut files = self.files.lock().await;
                if let Some(existing) = files.get(&path) {
                    if existing != bytes {
                        return Err(Error::Conflict("staged identity changed bytes".into()));
                    }
                } else {
                    files.insert(path.clone(), bytes.to_vec());
                }
                FileRef::new(
                    self.volume.clone(),
                    path,
                    "immutable-1",
                    FileDescriptor::from_bytes(bytes, "application/json")?,
                    "payload.json",
                )
            })
        }
    }

    impl ContentResidencyVerifier for MemoryPayloads {
        fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.read(reference).await?;
                Ok(())
            })
        }

        fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            Box::pin(async move {
                if reference.volume() != &self.volume {
                    return Err(Error::Unauthorized(
                        "payload belongs to another owner".into(),
                    ));
                }
                let bytes = self
                    .files
                    .lock()
                    .await
                    .get(reference.path())
                    .cloned()
                    .ok_or_else(|| Error::NotFound("payload".into()))?;
                reference.descriptor().verify(&bytes)?;
                Ok(bytes)
            })
        }
    }

    struct BatchMachine {
        identity: MachineIdentity,
        schema: Value,
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn wake_discovery_pages_filter_owners_and_recheck_current_cancellation() -> Result<()> {
        use crate::scheduler::{LeaseFence, Reservation, SchedulerEvent};
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "discovery-owner".into(),
        };
        let foreign = Authority {
            kind: AggregateKind::Task,
            id: "foreign-owner".into(),
        };
        let issuer = AuthorityIssuer::new("discovery", [7; 32], owner.clone());
        let foreign_issuer = AuthorityIssuer::new("discovery", [8; 32], foreign.clone());
        let scope = issuer.root(
            "owner",
            Capabilities::new(["operation:declare", "operation:observe", "operation:cancel"]),
        );
        let foreign_scope = foreign_issuer.root("owner", scope.capabilities().clone());
        let mut coordinator = DistributedCoordinator::open(&stream, payloads.clone()).await?;
        for index in 1..=18u8 {
            let operation = OperationId::from_bytes([index; 16]);
            let state = payloads.stage(operation, "state", b"null").await?;
            let fence = LeaseFence {
                reservation_id: format!("lease-{index}"),
                placement: "worker".into(),
            };
            let events = [
                SchedulerEvent::Declared {
                    spec: Box::new(OperationSpec {
                        operation_id: operation,
                        owner_scope: if index % 2 == 0 {
                            foreign_scope.clone()
                        } else {
                            scope.clone()
                        },
                        parent: None,
                        owner: DurableOwner::Attached {
                            authority: if index % 2 == 0 {
                                foreign.clone()
                            } else {
                                owner.clone()
                            },
                        },
                        entrypoint: EntrypointRef {
                            name: "test.discovery".into(),
                            version: "1".into(),
                            digest: [2; 32],
                            result_schema: serde_json::json!({}),
                        },
                        dependencies: BTreeSet::new(),
                        resources: ResourceRequest::default(),
                        placement: BTreeMap::new(),
                        orchestration: Orchestration::Leaf,
                        state,
                    }),
                },
                SchedulerEvent::WaitingForCapacity {
                    operation_id: operation,
                },
                SchedulerEvent::Admitted {
                    operation_id: operation,
                    reservation: Reservation {
                        id: fence.reservation_id.clone(),
                        placement: fence.placement.clone(),
                        admitted: ResourceRequest::default(),
                    },
                },
                SchedulerEvent::Started {
                    operation_id: operation,
                    fence: fence.clone(),
                },
                SchedulerEvent::WorkflowSuspended {
                    operation_id: operation,
                    fence,
                    workflow_revision: 1,
                    waiting_command: (index != 1).then_some(OperationId::from_bytes([99; 16])),
                },
            ];
            for (step, event) in events.into_iter().enumerate() {
                let key = IdempotencyKey::new(format!("discovery-{index}-{step}"))?;
                if let SchedulerEvent::Declared { spec } = event {
                    let (authority, signed, verifier) = if index % 2 == 0 {
                        (&foreign, &foreign_scope, foreign_issuer.verifier())
                    } else {
                        (&owner, &scope, issuer.verifier())
                    };
                    coordinator
                        .declare_operation(authority, signed, &verifier, *spec, key)
                        .await?;
                } else {
                    coordinator.apply(operation, key, event).await?;
                }
            }
        }
        let cancel = |index| SchedulerEvent::CancellationRequested {
            operation_id: OperationId::from_bytes([index; 16]),
            recursive: false,
        };
        coordinator
            .apply(
                OperationId::from_bytes([3; 16]),
                IdempotencyKey::new("discovery-cancel-3")?,
                cancel(3),
            )
            .await?;
        let host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads.clone(),
            payloads,
            owner,
            scope.clone(),
            issuer.verifier(),
            RuntimeScope::new(scope.capabilities().clone(), Limits::default())?,
            TaskRegistry::default(),
            MachineRegistry::default(),
            Arc::new(SystemUnixMillisClock),
        )?;
        let first = host.workflow_wait_page(0, None, 64).await?;
        assert_eq!(first.events_read, 64);
        assert_eq!(first.next_revision, 64);
        assert_eq!(first.through_revision, 91);
        assert_eq!(
            first.tasks,
            [5, 7, 9, 11].map(|index| TaskId::from_bytes([index; 16]))
        );
        host.coordinator
            .lock()
            .await
            .apply(
                OperationId::from_bytes([17; 16]),
                IdempotencyKey::new("discovery-cancel-17")?,
                cancel(17),
            )
            .await?;
        let second = host
            .workflow_wait_page(first.next_revision, Some(first.through_revision), 64)
            .await?;
        assert_eq!(second.events_read, 27);
        assert_eq!(second.next_revision, 91);
        assert_eq!(second.through_revision, 91);
        assert_eq!(
            second.tasks,
            [13, 15].map(|index| TaskId::from_bytes([index; 16]))
        );
        let finished = host.workflow_wait_page(91, Some(91), 64).await?;
        assert_eq!(finished.events_read, 0);
        assert!(finished.tasks.is_empty());
        let retry = host.workflow_wait_page(64, Some(91), 64).await?;
        assert_eq!(retry.tasks, second.tasks);
        let new_snapshot = host.workflow_wait_page(0, None, 1).await?;
        assert_eq!(new_snapshot.through_revision, 92);
        Ok(())
    }

    impl ResumableMachine for BatchMachine {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }
        fn state_schema(&self) -> &Value {
            &self.schema
        }
        fn initialize(&self, input: &Value) -> Result<Value> {
            Ok(input.clone())
        }
        fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
            Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            })
        }
    }

    #[cfg(feature = "filesystem")]
    struct EffectTaskFixture<P> {
        host: Arc<CoordinatorTaskHost<P>>,
        binding: TaskJournalOwner<P>,
        payloads: Arc<MemoryPayloads>,
        runtime_scope: RuntimeScope,
    }

    #[cfg(feature = "filesystem")]
    async fn effect_task_fixture<P: StreamProvider>(
        stream: StreamClient<P>,
        provider: &str,
        additional: Vec<String>,
    ) -> Result<EffectTaskFixture<P>> {
        let payloads = Arc::new(MemoryPayloads::new()?);
        let (host, binding, runtime_scope) = effect_task_fixture_with_storage(
            stream, provider, additional, payloads.clone(), payloads.clone(), &payloads.volume,
        ).await?;
        Ok(EffectTaskFixture { host, binding, payloads, runtime_scope })
    }

    #[cfg(feature = "filesystem")]
    #[allow(clippy::type_complexity)]
    async fn effect_task_fixture_with_storage<P: StreamProvider>(
        stream: StreamClient<P>,
        provider: &str,
        additional: Vec<String>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        payload_volume: &VolumeRef,
    ) -> Result<(Arc<CoordinatorTaskHost<P>>, TaskJournalOwner<P>, RuntimeScope)> {
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "effect-task-owner".into(),
        };
        let issuer = AuthorityIssuer::new("effect-task", [51; 32], authority.clone());
        let mut grants = vec![
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:test.effect@1".into(),
            "effect:run".into(),
            "effect:plan".into(),
            capability::effect_provider(provider),
            payload_volume.capability(crate::conversation::VolumeOperation::Read)?,
        ];
        grants.extend(additional);
        let grants = Capabilities::new(grants);
        let signed = issuer.root("owner", grants.clone());
        let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
        let implementation = Arc::new(BatchMachine {
            identity: MachineIdentity {
                name: "test.effect".into(),
                version: "1".into(),
                digest: [52; 32],
            },
            schema: serde_json::json!({"type":"integer"}),
        });
        let definition = TaskDefinition::<Value, Value>::resumable(
            implementation.clone(),
            serde_json::json!({"type":"integer"}),
            serde_json::json!({"type":"integer"}),
        )?;
        let task_identity = definition.identity().clone();
        let machine = implementation.identity.clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation)?;
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, reader.clone()).await?,
            stream.clone(),
            payloads.clone(),
            reader,
            authority,
            signed,
            issuer.verifier(),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?);
        let operation = OperationId::from_bytes([53; 16]);
        let task = TaskId::from_bytes(operation.into_bytes());
        host.admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: operation,
            task: task_identity,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
        let crate::distributed::WorkPull::Claimed(lease) = host
            .pull_work(&crate::distributed::Worker {
                id: "effect-worker".into(),
                available: Default::default(),
                labels: BTreeMap::new(),
            })
            .await?
        else {
            return Err(Error::NotFound("effect lease".into()));
        };
        host.start_task(&lease).await?;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        let binding = host.journal_owner(task, fence.clone()).await?;
        Ok((host, binding, runtime_scope))
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn recovery_after_pull_response_loss_keeps_original_reservation_without_writes()
    -> Result<()> {
        use crate::distributed::{WorkLease, WorkPull, Worker};
        use std::sync::atomic::Ordering;

        for mode in 0..3 {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let stream = StreamClient::new(provider.clone());
            let fixture = effect_task_fixture(stream.clone(), "test.recovery", Vec::new()).await?;
            let host = fixture.host;
            let (task, _) = fixture.binding.task_binding();
            let WorkPull::Claimed(previous) = host.recover_work(task).await? else {
                return Err(Error::NotFound("original running reservation".into()));
            };
            host.coordinator
                .lock()
                .await
                .release_lease(&previous, IdempotencyKey::new("recover-fixture-release")?)
                .await?;
            if mode == 1 {
                provider.lose_ack.store(true, Ordering::SeqCst);
                provider.hide_receipt.store(true, Ordering::SeqCst);
            } else if mode == 2 {
                provider.location_fault.store(4, Ordering::SeqCst);
            }
            let response = host
                .pull_operation(
                    &Worker {
                        id: "reopened-worker".into(),
                        available: Default::default(),
                        labels: BTreeMap::new(),
                    },
                    OperationId::from_bytes(task.into_bytes()),
                )
                .await?;
            let expected = match response {
                WorkPull::Claimed(lease) if mode == 0 => lease,
                WorkPull::Unresolved { lease, .. } if mode != 0 => lease,
                _ => return Err(Error::Conflict("pull fault did not execute".into())),
            };
            // No route or client lease is retained. Rebuild the actual controller
            // from its original providers, scope and registered implementations.
            let reopened = Arc::new(CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, fixture.payloads.clone()).await?,
                stream.clone(),
                host.payloads.clone(),
                host.reader.clone(),
                host.owner.clone(),
                host.owner_scope.clone(),
                host.verifier.clone(),
                host.root_scope.clone(),
                host.tasks.clone(),
                host.machines.clone(),
                host.clock.clone(),
            )?);
            drop(fixture.binding);
            drop(host);
            let journal = stream.stream("harness/v3/coordinator/events")?;
            let tail = journal.tail().await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let recovered = reopened.recover_work(task).await?;
            assert_eq!(journal.tail().await?, tail);
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            assert!(
                reopened
                    .recover_work(TaskId::from_bytes([201; 16]))
                    .await
                    .is_err()
            );
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            if mode == 2 {
                assert!(matches!(recovered, WorkPull::Idle));
                continue;
            }
            let WorkPull::Claimed(recovered) = recovered else {
                return Err(Error::NotFound("committed original reservation".into()));
            };
            assert_eq!(recovered, expected);
            assert_ne!(recovered.reservation, previous.reservation);
            provider.forbid_writes.store(false, Ordering::SeqCst);
            reopened.start_task(&recovered).await?;
            assert!(reopened.start_task(&previous).await.is_err());
            reopened.cancel(task).await?;
            let tail = journal.tail().await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let WorkPull::Unresolved { lease, .. } = reopened.recover_work(task).await? else {
                return Err(Error::Conflict(
                    "cancelled reservation was made ready".into(),
                ));
            };
            let WorkLease {
                reservation,
                operation,
                ..
            } = lease;
            assert_eq!(reservation, recovered.reservation);
            assert_eq!(operation, recovered.operation);
            assert_eq!(journal.tail().await?, tail);
            assert!(reopened.start_task(&recovered).await.is_err());
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn task_effect_dispatch_race_and_cancelled_settlement_use_one_owner() -> Result<()> {
        use crate::{
            core::{EffectGuarantee, EffectStatus, SchemaRegistry},
            effect_host::{ConversationEffectHost, TaskEffectPlan},
            effects::{EffectDispatch, EffectObservation, EffectProvider, EffectRegistry},
        };
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct ReceiptProvider {
            calls: AtomicUsize,
            known: AtomicBool,
            request: Mutex<Option<EffectDispatch>>,
        }
        impl EffectProvider for ReceiptProvider {
            fn id(&self) -> &str {
                "test.receipt"
            }
            fn guarantees(&self, _: &str) -> BTreeSet<EffectGuarantee> {
                BTreeSet::from([EffectGuarantee::AtMostOnce])
            }
            fn linearizable_reconciliation(&self) -> bool {
                false
            }
            fn dispatch<'a>(
                &'a self,
                request: EffectDispatch,
            ) -> BoxFuture<'a, Result<EffectObservation>> {
                Box::pin(async move {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    *self.request.lock().await = Some(request);
                    Err(Error::Storage("lost provider reply".into()))
                })
            }
            fn reconcile<'a>(
                &'a self,
                attempt: crate::EffectAttemptId,
            ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
                Box::pin(async move {
                    if !self.known.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    let request = self
                        .request
                        .lock()
                        .await
                        .clone()
                        .ok_or_else(|| Error::NotFound("receipt".into()))?;
                    assert_eq!(request.attempt_id, attempt);
                    Ok(Some(EffectObservation {
                        provider: request.provider,
                        effect_id: request.effect_id,
                        attempt_id: attempt,
                        request_digest: request.request_digest,
                        guarantee: request.guarantee,
                        status: EffectStatus::Failed {
                            message: "observed exit".into(),
                        },
                    }))
                })
            }
        }

        let EffectTaskFixture {
            host,
            binding,
            payloads,
            runtime_scope,
        } = effect_task_fixture(
            StreamClient::new(Arc::new(MemoryStream::default())),
            "test.receipt",
            Vec::new(),
        )
        .await?;
        let stream = binding.stream();
        let operation = binding.operation_id();
        let (task, fence) = binding.task_binding();
        let request = payloads.stage(operation, "request", b"{}").await?;
        let schema = FileRef::new(
            request.volume().clone(),
            request.path(),
            request.version(),
            FileDescriptor::from_bytes(b"{}", "application/schema+json")?,
            "schema.json",
        )?;
        let plan = TaskEffectPlan {
            provider: "test.receipt".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            effect_kind: "test.exit.v1".into(),
            request,
            result_schema: schema,
        };
        let receipt = Arc::new(ReceiptProvider {
            calls: AtomicUsize::new(0),
            known: AtomicBool::new(false),
            request: Mutex::new(None),
        });
        let mut providers = EffectRegistry::default().with_result_resolver(payloads.clone());
        providers.register(receipt.clone())?;
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: "effect-conversation".into(),
        };
        let conversation_issuer =
            AuthorityIssuer::new("effect-conversation", [54; 32], conversation.clone());
        let conversation_scope = conversation_issuer.root("owner", runtime_scope.grants().clone());
        // Deliberately use another client: task reads and conditional writes must
        // still use the exact coordinator provider retained by the binding.
        let effects = ConversationEffectHost::new(
            StreamClient::new(Arc::new(MemoryStream::default())),
            conversation.clone(),
            conversation_issuer.clone(),
            conversation_scope,
            SchemaRegistry::new(),
            payloads.clone(),
            providers,
        )?;
        let command = OperationId::from_bytes([55; 16]);
        let (first, second) = tokio::join!(
            effects.run_task_effect(&binding, command, plan.clone()),
            effects.run_task_effect(&binding, command, plan.clone())
        );
        assert!(matches!(
            (&first, &second),
            (Err(_), Ok(EffectStatus::Indeterminate)) | (Ok(EffectStatus::Indeterminate), Err(_))
        ));
        assert_eq!(receipt.calls.load(Ordering::SeqCst), 1);
        let mut mutated = plan.clone();
        mutated.effect_kind = "changed.v1".into();
        assert!(matches!(
            effects
                .run_task_effect(&binding, command, mutated.clone())
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            effects
                .reconcile_task_effect(&binding, command, &mutated)
                .await,
            Err(Error::Conflict(_))
        ));
        host.cancel(task).await?;
        assert!(
            effects
                .run_task_effect(&binding, command, plan.clone())
                .await
                .is_err()
        );
        assert_eq!(
            effects
                .reconcile_task_effect(&binding, command, &plan)
                .await?,
            EffectStatus::Indeterminate
        );
        receipt.known.store(true, Ordering::SeqCst);
        assert_eq!(
            effects
                .reconcile_task_effect(&binding, command, &plan)
                .await?,
            EffectStatus::Failed {
                message: "observed exit".into()
            }
        );
        let aggregate = crate::store::StreamAggregate::open(
            &stream,
            conversation,
            conversation_issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?;
        assert_eq!(aggregate.reducer().revision(), 3);
        assert_eq!(receipt.calls.load(Ordering::SeqCst), 1);
        host.settle_task(task, fence.clone(), Outcome::Cancelled).await?;
        assert!(
            effects
                .reconcile_task_effect(&binding, command, &plan)
                .await
                .is_err()
        );
        Ok(())
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    mod native_process_tests;

    #[tokio::test]
    async fn model_claims_use_pinned_ceiling_and_current_owner() -> Result<()> {
        model_claim_host(StreamClient::new(Arc::new(MemoryStream::default())))
            .await
            .map(|_| ())
    }

    #[cfg(feature = "filesystem-local")]
    #[tokio::test]
    async fn owned_task_journal_guards_survive_local_stream_reopen() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let payloads = model_claim_host(StreamClient::new(Arc::new(provider))).await?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let stream = StreamClient::new(Arc::new(provider));
        assert_eq!(
            stream
                .stream(format!(
                    "harness/v2/workflows/{}",
                    OperationId::from_bytes([77; 16])
                ))?
                .tail()
                .await?,
            1
        );
        assert_eq!(
            stream
                .stream(format!(
                    "harness/v2/execution/{}",
                    OperationId::from_bytes([78; 16])
                ))?
                .tail()
                .await?,
            75
        );
        // The coordinator journal is durable independently of the in-memory
        // Filesystem payloads used by this focused guard qualification.
        let coordinator = DistributedCoordinator::open(&stream, payloads).await?;
        let operation = coordinator
            .scheduler()
            .operation(OperationId::from_bytes([77; 16]))
            .ok_or_else(|| Error::NotFound("reopened owned task".into()))?;
        assert!(operation.cancellation_requested);
        assert!(operation.reservation.is_some());
        Ok(())
    }

    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn owned_task_journal_native_release_requires_original_settlement_and_binding()
    -> Result<()> {
        use crate::executor::{ExecutionEvent, ExecutionJournal as _, ToolFailureKind};
        use crate::filesystem::{
            FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
            FilesystemSchedulerPayloadStore, FilesystemWorkflowJournal,
        };
        use crate::workflow::WorkflowJournal as _;

        // Keep the older custom-provider fixture unchanged. This qualification
        // uses the stock pinned Filesystem reader, not a fake measured adapter.
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await.map_err(|error| Error::Storage(error.to_string()))?;
        let stream = StreamClient::new(Arc::new(provider));
        let filesystem_provider = ProviderRef::new("native-owned-release", "filesystem", "2")?;
        let filesystem = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::memory(), filesystem_provider.clone(),
        )?);
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            filesystem_provider.clone(), "native-owned", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        filesystem.create_volume(&volume).await?;
        let authority = Authority { kind: AggregateKind::Task, id: "effect-task-owner".into() };
        let issuer = AuthorityIssuer::new("effect-task", [51; 32], authority);
        let volume_grants = vec![
            volume.capability(crate::conversation::VolumeOperation::Read)?,
            volume.capability(crate::conversation::VolumeOperation::Write)?,
        ];
        let content_scope = issuer.root_for_agent(
            agent, "native-owned-content", Capabilities::new(volume_grants.clone()),
        );
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(), volume.clone(), &issuer.verifier(), &content_scope, 65_536,
        )?);
        let reader = Arc::new(FilesystemContentVerifier::new(
            filesystem.clone(), issuer.verifier(), content_scope.clone(), 65_536,
        )?);
        let (host, owner, runtime_scope) = effect_task_fixture_with_storage(
            stream.clone(), "test.native-release", volume_grants,
            payloads, reader, &volume,
        ).await?;
        let (task, fence) = owner.task_binding();
        let fence = fence.clone();
        let operation = OperationId::from_bytes(task.into_bytes());
        let attempt = OperationId::from_bytes([54; 16]);
        let workflow = Arc::new(FilesystemWorkflowJournal::for_task(
            owner.clone(), filesystem.clone(), volume.clone(),
            issuer.verifier(), content_scope.clone(), 65_536,
        )?);
        let journal = FilesystemExecutionJournal::for_task(
            owner.clone(), attempt, filesystem.clone(), volume.clone(),
            issuer.verifier(), content_scope.clone(), 65_536,
        )?;
        let units = 100_000;
        let bytes = 100_000_000;
        let budget = acyclic_fs::WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes, bytes_hashed: bytes,
            bytes_copied: bytes, bytes_encoded: bytes, source_bytes_read: bytes,
            source_path_components: units, source_entries_visited: units, output_bytes: bytes,
            items_examined: units, items_returned: units, allocation_operations: units,
            peak_allocation_bytes: bytes, materializations: units,
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        workflow.admit(owner.workflow_admission().clone()).await?;
        assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        journal.append(attempt, "started".into(), ExecutionEvent::Started).await?;
        let invocation = crate::tool::ToolInvocation::for_model_call(
            attempt, 0, "native-owned-tool".into(), "test".into(), serde_json::json!({}),
        );
        let invocation = crate::executor::stage_json(
            &journal, attempt, "tool-invocation", &invocation,
        ).await?;
        journal.append(attempt, "tool-started".into(), ExecutionEvent::ToolStarted {
            step: 0, call_id: "native-owned-tool".into(), invocation,
        }).await?;
        let machine_identity = owner.workflow_admission().initial.machine.clone();
        let mut workflow_host = crate::workflow::DurableWorkflowHost::open(
            host.machines.clone(), owner.workflow_admission().initial.clone(), workflow.clone(),
        ).await?;
        workflow_host.step(
            OperationId::from_bytes([57; 16]), IdempotencyKey::new("native-suspended-step")?,
            serde_json::json!(0),
        ).await?;
        // Cold selected-state reads keep the actual audited decode allowance
        // on the original operation ledger until the returned DTO drops.
        {
            let baseline = work.live_allocation_bytes();
            let observed = workflow.observe_transition_with_native_work(1, &work).await?;
            assert_eq!(observed.0.next.revision, 1);
            assert!(matches!(observed.0.transition.status, crate::workflow::MachineStatus::Suspended));
            assert!(observed.1.bytes() > 0);
            assert_eq!(work.live_allocation_bytes(), baseline + observed.1.bytes());
            drop(observed);
            assert_eq!(work.live_allocation_bytes(), baseline);
            let admitted = workflow.admission_with_native_work(&work).await?;
            assert_eq!(admitted.0.as_ref(), Some(owner.workflow_admission()));
            assert!(admitted.1.bytes() > 0);
            assert_eq!(work.live_allocation_bytes(), baseline + admitted.1.bytes());
            drop(admitted);
            assert_eq!(work.live_allocation_bytes(), baseline);
            let denied_work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget { backend_read_operations: 0, ..budget }, &cancellation,
            ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let denied_live = denied_work.live_allocation_bytes();
            assert!(matches!(workflow.observe_transition_with_native_work(1, &denied_work).await,
                Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(_), .. })));
            assert_eq!(denied_work.receipt().backend_read_operations, 0);
            assert_eq!(denied_work.live_allocation_bytes(), denied_live);
            let cancelled = acyclic_fs::CancellationToken::new();
            let cancelled_work = acyclic_fs::NativeMaterializationWork::new(budget, &cancelled)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let prefix = cancelled_work.receipt();
            cancelled.cancel();
            assert!(matches!(workflow.admission_with_native_work(&cancelled_work).await,
                Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
            assert_eq!(cancelled_work.receipt(), prefix);
        }
        assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        {
            let coordinator = host.coordinator.lock().await;
            let current = coordinator.scheduler().operation(operation)
                .ok_or_else(|| Error::NotFound("original active native release task".into()))?;
            assert_eq!(current.phase, crate::scheduler::OperationPhase::Running);
            assert!(current.reservation.is_some());
        }
        journal.append(attempt, "tool-failed".into(), ExecutionEvent::ToolFailed {
            step: 0, call_id: "native-owned-tool".into(), reason: ToolFailureKind::ExecutorRejected,
        }).await?;

        // Both refusal paths preserve the typed native failure and the lease;
        // cancellation does not perform any work beyond its constructor prefix.
        let cancelled = acyclic_fs::CancellationToken::new();
        let cancelled_work = acyclic_fs::NativeMaterializationWork::new(budget, &cancelled)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let cancelled_prefix = cancelled_work.receipt();
        cancelled.cancel();
        assert!(matches!(
            journal.release_if_quiescent_with_native_work(&cancelled_work, &workflow).await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None
            })
        ));
        assert_eq!(cancelled_work.receipt(), cancelled_prefix);
        let quota_work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget { backend_read_operations: 0, ..budget }, &cancellation,
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let quota_live = quota_work.live_allocation_bytes();
        assert!(matches!(
            journal.release_if_quiescent_with_native_work(&quota_work, &workflow).await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }
                ), custody: None
            })
        ));
        assert_eq!(quota_work.receipt().backend_read_operations, 0);
        assert_eq!(quota_work.live_allocation_bytes(), quota_live);
        assert!(host.coordinator.lock().await.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("quota-refused original task".into()))?
            .reservation.is_some());

        // These witnesses come from the actual authenticated source observer.
        // Independently advancing either source tail must fail the original
        // three-tail publication, without consuming the still-current lease.
        {
            let proof_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
            let pending = workflow.prove_release_with_io(&proof_io).await?
                .ok_or_else(|| Error::NotFound("original Suspended source proof".into()))?;
            let execution_tail = stream.stream(format!("harness/v2/execution/{attempt}"))?
                .tail().await?;
            workflow_host.step(
                OperationId::from_bytes([58; 16]), IdempotencyKey::new("native-next-suspended-step")?,
                serde_json::json!(0),
            ).await?;
            assert!(matches!(
                owner.release_quiescent_execution_with_native_work(
                    attempt, execution_tail, &pending, &work,
                ).await,
                Err(Error::Conflict(_))
            ));
            let coordinator = host.coordinator.lock().await;
            let reservation = coordinator.scheduler().operation(operation)
                .and_then(|current| current.reservation.as_ref())
                .ok_or_else(|| Error::NotFound("workflow-raced original lease".into()))?;
            assert_eq!(crate::scheduler::LeaseFence::from(reservation), fence);
        }
        {
            let proof_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
            let pending = workflow.prove_release_with_io(&proof_io).await?
                .ok_or_else(|| Error::NotFound("current Suspended source proof".into()))?;
            let execution_tail = stream.stream(format!("harness/v2/execution/{attempt}"))?
                .tail().await?;
            let invocation = crate::tool::ToolInvocation::for_model_call(
                attempt, 1, "interleaved-native-tool".into(), "test".into(), serde_json::json!({}),
            );
            let invocation = crate::executor::stage_json(
                &journal, attempt, "interleaved-tool-invocation", &invocation,
            ).await?;
            journal.append(attempt, "interleaved-tool-started".into(), ExecutionEvent::ToolStarted {
                step: 1, call_id: "interleaved-native-tool".into(), invocation,
            }).await?;
            assert!(matches!(
                owner.release_quiescent_execution_with_native_work(
                    attempt, execution_tail, &pending, &work,
                ).await,
                Err(Error::Conflict(_))
            ));
            assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
            let coordinator = host.coordinator.lock().await;
            let reservation = coordinator.scheduler().operation(operation)
                .and_then(|current| current.reservation.as_ref())
                .ok_or_else(|| Error::NotFound("execution-raced original lease".into()))?;
            assert_eq!(crate::scheduler::LeaseFence::from(reservation), fence);
        }
        journal.append(attempt, "interleaved-tool-failed".into(), ExecutionEvent::ToolFailed {
            step: 1, call_id: "interleaved-native-tool".into(), reason: ToolFailureKind::ExecutorRejected,
        }).await?;

        let workflow_path = format!("harness/v2/workflows/{operation}");
        let execution_path = format!("harness/v2/execution/{attempt}");
        let workflow_tail = stream.stream(&workflow_path)?.tail().await?;
        let execution_tail = stream.stream(&execution_path)?.tail().await?;
        assert_eq!(workflow_tail, 2);
        assert_eq!(execution_tail, 5);
        assert!(journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        assert!(host.coordinator.lock().await.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("settled original task".into()))?
            .reservation.is_none());
        assert_eq!(stream.stream(&workflow_path)?.tail().await?, workflow_tail);
        assert_eq!(stream.stream(&execution_path)?.tail().await?, execution_tail);
        assert!(work.receipt().authority_records_read > 0);
        assert!(work.receipt().authority_records_appended > 0);
        assert!(work.receipt().object_bytes_read > 0);
        assert!(work.receipt().bytes_hashed > 0);

        // A real replacement reservation supplies the mismatched fence; no
        // sealed proof or witness is fabricated to exercise this negative gate.
        let worker = crate::distributed::Worker {
            id: "replacement-native-worker".into(), available: Default::default(),
            labels: BTreeMap::new(),
        };
        let crate::distributed::WorkPull::Claimed(replacement) =
            host.pull_operation(&worker, operation).await?
        else { return Err(Error::NotFound("replacement native lease".into()).into()); };
        host.start_task(&replacement).await?;
        let replacement_owner = host.journal_owner(
            task, crate::scheduler::LeaseFence::from(&replacement.reservation),
        ).await?;
        assert_ne!(replacement_owner.task_binding().1, &fence);
        let replacement_workflow = FilesystemWorkflowJournal::for_task(
            replacement_owner.clone(), filesystem.clone(), volume.clone(),
            issuer.verifier(), content_scope.clone(), 65_536,
        )?;
        let denied_prefix = work.receipt();
        assert!(matches!(
            journal.release_if_quiescent_with_native_work(&work, &replacement_workflow).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);
        let replacement_journal = FilesystemExecutionJournal::for_task(
            replacement_owner.clone(), OperationId::from_bytes([55; 16]),
            filesystem.clone(), volume.clone(), issuer.verifier(), content_scope.clone(), 65_536,
        )?;
        let other_volume = VolumeRef::new(
            filesystem_provider, "other-native-owned", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        filesystem.create_volume(&other_volume).await?;
        let other_scope = issuer.root_for_agent(agent, "other-native-content", Capabilities::new(vec![
            other_volume.capability(crate::conversation::VolumeOperation::Read)?,
            other_volume.capability(crate::conversation::VolumeOperation::Write)?,
        ]));
        let other_volume_workflow = FilesystemWorkflowJournal::for_task(
            replacement_owner, filesystem.clone(), other_volume,
            issuer.verifier(), other_scope, 65_536,
        )?;
        assert!(matches!(
            replacement_journal.release_if_quiescent_with_native_work(&work, &other_volume_workflow).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);
        let other_issuer = AuthorityIssuer::new("other-native-owner", [91; 32], Authority {
            kind: AggregateKind::Task, id: "other-native-owner".into(),
        });
        assert!(matches!(
            FilesystemWorkflowJournal::for_task(
                owner, filesystem.clone(), volume.clone(), other_issuer.verifier(),
                content_scope.clone(), 65_536,
            ),
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);

        let other_operation = OperationId::from_bytes([56; 16]);
        host.admit(TaskAdmissionRecord {
            operation_id: other_operation,
            task: host.tasks.get::<Value, Value>("test.effect")?.identity().clone(),
            machine: machine_identity, input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"integer"}), parent: None,
            grants: runtime_scope.grants().clone(), limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(), policy: None, extensions: None, execution: None,
        }).await?;
        let crate::distributed::WorkPull::Claimed(other_lease) =
            host.pull_operation(&worker, other_operation).await?
        else { return Err(Error::NotFound("other real native lease".into()).into()); };
        host.start_task(&other_lease).await?;
        let other_owner = host.journal_owner(
            TaskId::from_bytes(other_operation.into_bytes()),
            crate::scheduler::LeaseFence::from(&other_lease.reservation),
        ).await?;
        let other_workflow = FilesystemWorkflowJournal::for_task(
            other_owner, filesystem, volume, issuer.verifier(), content_scope, 65_536,
        )?;
        assert!(matches!(
            replacement_journal.release_if_quiescent_with_native_work(&work, &other_workflow).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);
        assert!(host.coordinator.lock().await.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("binding-refused original lease".into()))?
            .reservation.is_some());
        Ok(())
    }

    async fn model_claim_host<P: StreamProvider>(
        stream: StreamClient<P>,
    ) -> Result<Arc<MemoryPayloads>> {
        let prepared_request = crate::model::PreparedModelRequest::prepare(
            crate::model::ModelRequest {
                model: crate::model::Model::new(
                    "synthetic",
                    "journal-test",
                    "1",
                    serde_json::json!({}),
                )?,
                messages: vec![crate::model::ModelMessage {
                    role: crate::model::ModelRole::User,
                    content: crate::model::ModelContent::Text("fixture request".into()),
                }],
                tools: Vec::new(),
                max_output_tokens: Some(32),
            },
            Limits::default(),
        )?;
        let model_digest = prepared_request.manifest().request_digest;
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "model-owner".into(),
        };
        let issuer = AuthorityIssuer::new("model-test", [7; 32], owner.clone());
        let owner_scope = issuer.root(
            "owner",
            Capabilities::new([
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.model@1",
                "model:generate",
                "interaction:route",
            ]),
        );
        let scope = RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.model".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        let definition = TaskDefinition::<Value, i64>::resumable(
            implementation.clone(),
            serde_json::json!({"type": "integer"}),
            serde_json::json!({"type": "integer"}),
        )?;
        let task = definition.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation)?;
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner.clone(),
            owner_scope.clone(),
            issuer.verifier(),
            scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?);
        let operation_id = OperationId::from_bytes([77; 16]);
        let task_id = TaskId::from_bytes(operation_id.into_bytes());
        host.admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id,
            task,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: crate::runtime::TaskRunLimits {
                max_steps: Some(1),
                ..scope.run_limits()
            },
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
        let fence = {
            let mut coordinator = host.coordinator.lock().await;
            coordinator
                .configure_session(
                    &owner,
                    &owner_scope,
                    &issuer.verifier(),
                    operation_id,
                    crate::scheduler::SessionLimits {
                        active_tasks: 1,
                        total_tasks: 1,
                        depth: 0,
                        model_steps: 2,
                    },
                    IdempotencyKey::new("model-session")?,
                )
                .await?;
            let lease = coordinator
                .pull(&crate::distributed::Worker {
                    id: "model-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("model lease".into()))?;
            let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-start")?,
                    crate::scheduler::SchedulerEvent::Started {
                        operation_id,
                        fence: fence.clone(),
                    },
                )
                .await?;
            fence
        };
        host.verify_execution_owner(task_id, fence.clone()).await?;
        let attempt = OperationId::from_bytes([78; 16]);
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
            .await?;
        #[cfg(feature = "filesystem")]
        let (workflow, filesystem, volume, content_scope, original_owner, original_pending) = {
            use crate::workflow::{DurableWorkflowHost, WorkflowJournal};
            let provider = ProviderRef::new("owned-workflow", "filesystem", "2")?;
            let filesystem = Arc::new(crate::filesystem::FilesystemHost::new(
                acyclic_fs::Fs::memory(),
                provider.clone(),
            )?);
            let volume = VolumeRef::new(
                provider,
                "owned-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            )?;
            filesystem.create_volume(&volume).await?;
            let content_scope = issuer.root_for_agent(
                AgentId::from_bytes([1; 16]),
                "workflow-content",
                Capabilities::new([
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            );
            let original_owner = host.journal_owner(task_id, fence.clone()).await?;
            assert!(
                original_owner
                    .validate_storage(&issuer.verifier(), scope.limits().file_bytes + 1)
                    .is_err()
            );
            let foreign = AuthorityIssuer::new(
                "foreign-owner",
                [8; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "foreign".into(),
                },
            );
            assert!(
                original_owner
                    .validate_storage(&foreign.verifier(), 65_536)
                    .is_err()
            );
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let admission = binding.workflow_admission().clone();
            let journal = Arc::new(crate::filesystem::FilesystemWorkflowJournal::for_task(
                binding,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?);
            let release_budget = acyclic_fs::WorkBudget {
                authority_records_read: 100_000, authority_records_appended: 100_000,
                authority_bytes_read: 100_000_000, authority_bytes_written: 100_000_000,
                object_probes: 100_000, backend_read_operations: 100_000,
                backend_write_operations: 100_000, durability_operations: 100_000,
                page_reads: 100_000, page_writes: 100_000,
                object_bytes_read: 100_000_000, object_bytes_written: 100_000_000,
                bytes_hashed: 100_000_000, bytes_copied: 100_000_000, bytes_encoded: 100_000_000,
                source_bytes_read: 100_000_000, source_path_components: 100_000,
                source_entries_visited: 100_000, output_bytes: 100_000_000,
                items_examined: 100_000, items_returned: 100_000,
                allocation_operations: 100_000, peak_allocation_bytes: 100_000_000,
                materializations: 100_000,
            };
            let release_cancellation = acyclic_fs::CancellationToken::new();
            let before_admission = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            assert!(journal.prove_release_with_io(&before_admission).await?.is_none());
            let mut wrong = admission.clone();
            wrong.initial.state = serde_json::json!(1);
            assert!(journal.admit(wrong).await.is_err());
            assert_eq!(journal.admit(admission.clone()).await?, admission);
            let admission_only = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            assert!(journal.prove_release_with_io(&admission_only).await?.is_none());
            let mut workflow = DurableWorkflowHost::open(
                host.machines.clone(),
                admission.initial,
                journal.clone(),
            )
            .await?;
            workflow
                .step(
                    OperationId::from_bytes([85; 16]),
                    IdempotencyKey::new("owned-step")?,
                    serde_json::json!(0),
                )
                .await?;
            let release_io = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            let witness = journal.prove_release_with_io(&release_io).await?
                .ok_or_else(|| Error::Invalid("committed Suspended workflow needs its original witness".into()))?;
            assert_eq!(witness.task_binding(), (task_id, &fence));
            assert_eq!(witness.path(), format!("harness/v2/workflows/{}", OperationId::from_bytes(task_id.into_bytes())));
            assert_eq!(witness.tail(), 1);
            assert!(release_io.receipt().authority_records_read > 0);
            assert!(release_io.receipt().object_bytes_read > 0);
            assert!(release_io.receipt().bytes_hashed > 0);
            let denied_budget = acyclic_fs::WorkBudget { peak_allocation_bytes: 0, ..release_budget };
            let denied = crate::filesystem::PreparationIo::from_budget(denied_budget, &release_cancellation)?;
            assert!(journal.prove_release_with_io(&denied).await.is_err());
            denied.receipt().verify(denied_budget).map_err(|error| Error::Invalid(error.to_string()))?;
            let cancelled = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            release_cancellation.cancel();
            assert!(journal.prove_release_with_io(&cancelled).await.is_err());
            assert_eq!(cancelled.receipt(), acyclic_fs::WorkCounters::default());
            let records = journal.replay(0, 1).await?;
            let [record] = records.as_slice() else {
                return Err(Error::Invalid("one owned transition required".into()));
            };
            assert!(matches!(
                journal
                    .commit(0, record.idempotency_key.clone(), record.clone())
                    .await?,
                crate::workflow::WorkflowCommitOutcome::Replayed(_)
            ));
            let mut forged = record.clone();
            forged.next.state = serde_json::json!(1);
            forged.transition.state = serde_json::json!(1);
            assert!(
                journal
                    .commit(0, forged.idempotency_key.clone(), forged)
                    .await
                    .is_err()
            );
            let path =
                acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
            let key = StreamKey::new(Bytes::from_static(b"owned-before-replacement"))?;
            let original_pending = original_owner
                .prepare_append(
                    &path,
                    1,
                    &key,
                    &Bytes::from_static(b"never published"),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            (
                journal,
                filesystem,
                volume,
                content_scope,
                original_owner,
                original_pending,
            )
        };
        #[cfg(feature = "filesystem")]
        let (execution, execution_pending, model_request) = {
            use crate::executor::{ExecutionEvent, ExecutionJournal};
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            // Use a retained prepared request so owner/claim checks receive valid input.
            let model_request = journal
                .stage(
                    attempt,
                    "model-request".into(),
                    prepared_request.bytes().to_vec(),
                    "application/json",
                )
                .await?;
            let pending_operation = OperationId::from_bytes([80; 16]);
            let digest = blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))?;
            let bytes = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "operation_id": pending_operation,
                    "turn_operation": pending_operation,
                    "version": 2,
                    "retry_digest": digest.to_hex().to_string(),
                    "event": ExecutionEvent::Started { request_digest: [9; 32] },
                }))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            let path = acyclic_stream::StreamPath::new(format!(
                "harness/v2/execution/{pending_operation}"
            ))?;
            let pending = original_owner
                .prepare_append(
                    &path,
                    0,
                    &key,
                    &bytes,
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            journal
                .append(
                    attempt,
                    "request".into(),
                    ExecutionEvent::Started {
                        request_digest: [9; 32],
                    },
                )
                .await?;
            assert!(
                journal
                    .append(
                        attempt,
                        "wrong-claim".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: [4; 32],
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err()
            );
            assert!(
                journal
                    .append(
                        attempt,
                        "absent-claim".into(),
                        ExecutionEvent::ModelStarted {
                            step: 1,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err()
            );
            let start = ExecutionEvent::ModelStarted {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                request_digest: model_digest,
                request: model_request.clone(),
            };
            assert!(
                journal
                    .append_if_tail(attempt, 1, "model".into(), start.clone())
                    .await?
            );
            assert!(
                journal
                    .append_if_tail(attempt, 1, "model".into(), start)
                    .await?
            );
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
            assert!(
                journal
                    .append_if_tail(
                        attempt,
                        2,
                        "uncharged-summary".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Summary,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        },
                    )
                    .await
                    .is_err(),
                "a response claim must not authorize a summary admission"
            );
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
            assert!(matches!(
                journal.suspend_workflow_command_if_quiescent(1, attempt).await,
                Err(Error::Indeterminate(id)) if id == attempt
            ));
            let invocation = crate::tool::ToolInvocation::for_model_call(
                attempt,
                0,
                "owned-tool".into(),
                "test".into(),
                serde_json::json!({}),
            );
            let invocation =
                crate::executor::stage_json(&journal, attempt, "tool-invocation", &invocation)
                    .await?;
            journal
                .append(
                    attempt,
                    "tool".into(),
                    ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: "owned-tool".into(),
                        invocation,
                    },
                )
                .await?;
            assert!(matches!(
                journal.suspend_workflow_command_if_quiescent(1, attempt).await,
                Err(Error::Indeterminate(id)) if id == attempt
            ));
            assert!(journal.replay(operation_id, 0, 1).await.is_err());
            assert!(matches!(
                journal
                    .interaction_outcome(crate::InteractionId::from_bytes([91; 16]))
                    .await,
                Err(Error::Unsupported(_))
            ));
            let uncharged = OperationId::from_bytes([79; 16]);
            let unbound = crate::filesystem::FilesystemExecutionJournal::new(
                stream.clone(),
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            unbound
                .append(
                    uncharged,
                    "request".into(),
                    ExecutionEvent::Started {
                        request_digest: [9; 32],
                    },
                )
                .await?;
            unbound
                .append(
                    uncharged,
                    "model".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        request_digest: model_digest,
                        request: model_request.clone(),
                    },
                )
                .await?;
            let adopted = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                uncharged,
                uncharged,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            assert!(
                adopted.replay(uncharged, 0, 64).await.is_err(),
                "cold replay must reject a model start lacking its retained charge"
            );
            (journal, pending, model_request)
        };
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
            .await?;
        assert!(
            host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
                .await
                .is_err(),
            "the task ceiling is one despite the larger session ceiling"
        );
        assert!(
            host.claim_model_dispatch(
                task_id,
                crate::executor::ModelPurpose::Summary.attempt_operation(attempt),
                0,
                [5; 32],
                fence.clone(),
            )
            .await
            .is_err(),
            "summary admissions must consume the same exhausted task ceiling"
        );
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, [4; 32], fence.clone())
                .await
                .is_err()
        );
        #[cfg(feature = "filesystem")]
        let (
            approval_journal,
            conversation_issuer,
            approval_id,
            approval_request,
            approval_pending,
        ) = {
            use crate::executor::ExecutionJournal;
            let conversation = Authority {
                kind: AggregateKind::Conversation,
                id: "owned-approval".into(),
            };
            let conversation_issuer =
                AuthorityIssuer::new("owned-approval", [54; 32], conversation.clone());
            let conversation_scope = conversation_issuer.root_for_agent(
                AgentId::from_bytes([1; 16]),
                "owner",
                Capabilities::new([
                    "conversation:bind".to_owned(),
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            );
            let mut aggregate = crate::store::StreamAggregate::open(
                &stream,
                conversation.clone(),
                conversation_issuer.verifier(),
                crate::core::SchemaRegistry::new(),
            )
            .await?;
            aggregate
                .execute(crate::core::Command {
                    operation_id: OperationId::from_bytes([90; 16]),
                    idempotency_key: IdempotencyKey::new("owned-approval-bind")?,
                    expected_revision: 0,
                    scope: conversation_scope.clone(),
                    causal_parent: None,
                    action: crate::core::Action::BindConversation {
                        agent: AgentId::from_bytes([1; 16]),
                    },
                })
                .await?;
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?
            .with_interaction_owner(
                conversation_issuer.verifier(),
                crate::core::SchemaRegistry::new(),
                conversation_scope,
                volume.clone(),
            )?;
            let unbound = crate::filesystem::FilesystemExecutionJournal::new(
                stream.clone(),
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            let response_scope = conversation_issuer.root_for_agent(
                AgentId::from_bytes([1; 16]),
                "owner",
                Capabilities::new([
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            );
            assert!(
                matches!(
                    unbound.with_interaction_owner(
                        conversation_issuer.verifier(),
                        crate::core::SchemaRegistry::new(),
                        response_scope,
                        volume.clone(),
                    ),
                    Err(Error::Unauthorized(_))
                ),
                "a task storage owner is not a retained task lease"
            );
            let id = crate::InteractionId::from_bytes([91; 16]);
            let request = Interaction::approval("approve exact action", attempt, [5; 32])?;
            journal.open_interaction(id, request.clone()).await?;
            journal.open_interaction(id, request.clone()).await?;
            assert_eq!(journal.interaction_outcome(id).await?, None);
            assert_eq!(stream.stream(conversation.stream_path()?)?.tail().await?, 2);
            assert!(matches!(
                journal
                    .open_interaction(id, Interaction::approval("changed", attempt, [6; 32])?)
                    .await,
                Err(Error::Conflict(_))
            ));
            let pending = original_owner
                .prepare_append(
                    &acyclic_stream::StreamPath::new(conversation.stream_path()?)?,
                    2,
                    &StreamKey::new(Bytes::from_static(b"approval-before-replacement"))?,
                    &Bytes::from_static(b"must not publish"),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            (journal, conversation_issuer, id, request, pending)
        };
        let mut stale = fence.clone();
        stale.reservation_id.push_str("-stale");
        assert!(
            host.verify_execution_owner(task_id, stale.clone())
                .await
                .is_err()
        );
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, model_digest, stale)
                .await
                .is_err()
        );
        let fence = {
            let mut coordinator = host.coordinator.lock().await;
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-release")?,
                    crate::scheduler::SchedulerEvent::LeaseReleased {
                        operation_id,
                        fence: fence.clone(),
                    },
                )
                .await?;
            let lease = coordinator
                .pull(&crate::distributed::Worker {
                    id: "replacement-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("replacement model lease".into()))?;
            let replacement = crate::scheduler::LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-restart")?,
                    crate::scheduler::SchedulerEvent::Started {
                        operation_id,
                        fence: replacement.clone(),
                    },
                )
                .await?;
            replacement
        };
        #[cfg(feature = "filesystem")]
        let (replacement_approval, cancellation_approval_pending) = {
            use crate::executor::ExecutionJournal;
            assert!(matches!(
                stream.commit(approval_pending).await?,
                acyclic_stream::CommitOutcome::Conflict(_)
            ));
            assert!(
                approval_journal
                    .open_interaction(approval_id, approval_request.clone())
                    .await
                    .is_err()
            );
            assert!(
                approval_journal
                    .interaction_outcome(approval_id)
                    .await
                    .is_err()
            );
            let conversation_scope = conversation_issuer.root_for_agent(
                AgentId::from_bytes([1; 16]),
                "owner",
                Capabilities::new([
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            );
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let pending = binding
                .prepare_append(
                    &acyclic_stream::StreamPath::new(
                        conversation_issuer.verifier().audience().stream_path()?,
                    )?,
                    2,
                    &StreamKey::new(Bytes::from_static(b"approval-before-cancellation"))?,
                    &Bytes::from_static(b"must not publish"),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                binding,
                attempt,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?
            .with_interaction_owner(
                conversation_issuer.verifier(),
                crate::core::SchemaRegistry::new(),
                conversation_scope,
                volume.clone(),
            )?;
            journal
                .open_interaction(approval_id, approval_request.clone())
                .await?;
            assert_eq!(journal.interaction_outcome(approval_id).await?, None);
            assert_eq!(
                stream
                    .stream(conversation_issuer.verifier().audience().stream_path()?)?
                    .tail()
                    .await?,
                2
            );
            (journal, pending)
        };
        // An admitted attempt whose execution-journal claim was not yet written
        // can continue under a new owner without spending another unit.
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
            .await?;
        assert!(
            host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
                .await
                .is_err()
        );
        #[cfg(feature = "filesystem")]
        let (replacement_workflow, pending) = {
            use crate::workflow::WorkflowJournal;
            assert!(
                matches!(
                    stream.commit(original_pending).await?,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ),
                "lease replacement after preflight must atomically prevent publication"
            );
            assert_eq!(
                stream
                    .stream(format!("harness/v2/workflows/{operation_id}"))?
                    .tail()
                    .await?,
                1
            );
            let record = workflow
                .replay(0, 1)
                .await?
                .pop()
                .ok_or_else(|| Error::NotFound("owned workflow record".into()))?;
            assert!(
                workflow
                    .commit(0, record.idempotency_key.clone(), record.clone())
                    .await
                    .is_err(),
                "the previous lease cannot replay a checkpoint mutation"
            );
            assert!(original_owner.verify(false).await.is_err());
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let path =
                acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
            let key = StreamKey::new(Bytes::from_static(b"owned-pending"))?;
            let pending = binding
                .prepare_append(
                    &path,
                    1,
                    &key,
                    &Bytes::from_static(b"never published"),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            let journal = crate::filesystem::FilesystemWorkflowJournal::for_task(
                binding,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            assert!(matches!(
                journal
                    .commit(0, record.idempotency_key.clone(), record)
                    .await?,
                crate::workflow::WorkflowCommitOutcome::Replayed(_)
            ));
            (journal, pending)
        };
        #[cfg(feature = "filesystem")]
        let execution_pending =
            {
                assert!(
                    matches!(
                        stream.commit(execution_pending).await?,
                        acyclic_stream::CommitOutcome::Conflict(_)
                    ),
                    "lease replacement must atomically fence execution publication"
                );
                let pending_operation = OperationId::from_bytes([80; 16]);
                let path = acyclic_stream::StreamPath::new(format!(
                    "harness/v2/execution/{pending_operation}"
                ))?;
                assert!(matches!(
                    stream.bounds(path.as_str()).await,
                    Err(StreamError::NotFound)
                ));
                let digest =
                    blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
                let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))?;
                let bytes = Bytes::from(serde_json::to_vec(&serde_json::json!({
                "operation_id": pending_operation,
                "turn_operation": pending_operation,
                "version": 2,
                "retry_digest": digest.to_hex().to_string(),
                "event": crate::executor::ExecutionEvent::Started { request_digest: [9; 32] },
            })).map_err(|error| Error::Invalid(error.to_string()))?);
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .prepare_append(
                        &path,
                        0,
                        &key,
                        &bytes,
                        crate::distributed::JournalWrite::Fresh,
                    )
                    .await?
            };
        host.coordinator
            .lock()
            .await
            .cancel_operation(
                &owner,
                &owner_scope,
                &issuer.verifier(),
                operation_id,
                IdempotencyKey::new("model-cancel")?,
                false,
            )
            .await?;
        // Cancellation retains this owner only to settle an already dispatched attempt.
        host.verify_execution_owner(task_id, fence.clone()).await?;
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::{ExecutionEvent, ExecutionJournal};
            assert!(
                matches!(
                    stream.commit(execution_pending).await?,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ),
                "cancellation must atomically fence execution publication"
            );
            assert!(matches!(
                stream
                    .bounds(&format!(
                        "harness/v2/execution/{}",
                        OperationId::from_bytes([80; 16])
                    ))
                    .await,
                Err(StreamError::NotFound)
            ));
            assert!(
                execution
                    .append(
                        attempt,
                        "model".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err(),
                "stale lease must reject even an exact retry"
            );
            assert!(
                execution
                    .append(
                        attempt,
                        "stale-tool-settlement".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "owned-tool".into(),
                            reason: crate::executor::ToolFailureKind::ExecutorRejected
                        }
                    )
                    .await
                    .is_err(),
                "a replaced lease cannot settle the retained dispatch"
            );
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            assert!(
                journal
                    .append(
                        attempt,
                        "model".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err(),
                "cancellation forbids new or retried starts"
            );
            let bytes = crate::contract::canonical_json_bytes(
                &serde_json::to_value(crate::model::ModelEvent::Content {
                    delta: "settled".into(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            )?;
            let reference = journal
                .stage(
                    attempt,
                    "model-observation".into(),
                    bytes,
                    "application/json",
                )
                .await?;
            let observation = ExecutionEvent::Model {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                event: reference.clone(),
            };
            assert!(
                journal
                    .append(
                        attempt,
                        "missing-tool".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "missing".into(),
                            reason: crate::executor::ToolFailureKind::ExecutorRejected
                        }
                    )
                    .await
                    .is_err()
            );
            let tool_settlement = ExecutionEvent::ToolFailed {
                step: 0,
                call_id: "owned-tool".into(),
                reason: crate::executor::ToolFailureKind::ExecutorRejected,
            };
            journal
                .append(attempt, "tool-settlement".into(), tool_settlement.clone())
                .await?;
            journal
                .append(attempt, "tool-settlement".into(), tool_settlement.clone())
                .await?;
            assert!(
                journal
                    .append(attempt, "another-tool-settlement".into(), tool_settlement)
                    .await
                    .is_err()
            );
            for index in 0..70 {
                journal
                    .append(attempt, format!("settlement-{index}"), observation.clone())
                    .await?;
            }
            journal
                .append(attempt, "settlement-69".into(), observation.clone())
                .await?;
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 64);
            assert_eq!(journal.replay(attempt, 64, 64).await?.len(), 10);
            let reopened = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            reopened
                .append(attempt, "settlement-69".into(), observation.clone())
                .await?;
            assert!(
                reopened
                    .append(
                        attempt,
                        "settlement-69".into(),
                        ExecutionEvent::Model {
                            step: 1,
                            purpose: crate::executor::ModelPurpose::Response,
                            event: reference,
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 10);
            // Tool settlement and model content alone cannot prove quiescence,
            // including after rebuilding the projection across replay pages.
            assert!(matches!(
                reopened.suspend_workflow_command_if_quiescent(1, attempt).await,
                Err(Error::Indeterminate(id)) if id == attempt
            ));
            let completed = crate::executor::stage_json(
                &reopened,
                attempt,
                "model-completed",
                &crate::model::ModelEvent::Completed {
                    metadata: Value::Null,
                },
            )
            .await?;
            reopened
                .append(
                    attempt,
                    "model-completed".into(),
                    ExecutionEvent::Model {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        event: completed,
                    },
                )
                .await?;
            assert!(
                reopened
                    .append(attempt, "after-completed".into(), observation.clone(),)
                    .await
                    .is_err()
            );
            // A settled journal still cannot release a cancelled task's owner.
            assert!(
                reopened
                    .suspend_workflow_command_if_quiescent(1, attempt)
                    .await
                    .is_err()
            );
            // Exact old observations remain replayable after dispatch settlement.
            reopened
                .append(attempt, "settlement-69".into(), observation)
                .await?;
            assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 11);
        }
        #[cfg(feature = "filesystem")]
        {
            use crate::workflow::WorkflowJournal;
            assert!(
                matches!(
                    stream.commit(pending).await?,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ),
                "cancellation after preflight must atomically prevent publication"
            );
            assert_eq!(
                stream
                    .stream(format!("harness/v2/workflows/{operation_id}"))?
                    .tail()
                    .await?,
                1
            );
            let record = replacement_workflow
                .replay(0, 1)
                .await?
                .pop()
                .ok_or_else(|| Error::NotFound("cancelled workflow record".into()))?;
            assert!(
                replacement_workflow
                    .commit(0, record.idempotency_key.clone(), record)
                    .await
                    .is_err(),
                "cancellation must reject checkpoint mutation retries"
            );
            assert!(
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .verify(false)
                    .await
                    .is_err()
            );
        }
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::ExecutionJournal;
            assert!(matches!(
                stream.commit(cancellation_approval_pending).await?,
                acyclic_stream::CommitOutcome::Conflict(_)
            ));
            assert!(
                replacement_approval
                    .open_interaction(approval_id, approval_request)
                    .await
                    .is_err()
            );
            assert_eq!(
                replacement_approval
                    .interaction_outcome(approval_id)
                    .await?,
                None
            );
            assert_eq!(
                stream
                    .stream(conversation_issuer.verifier().audience().stream_path()?)?
                    .tail()
                    .await?,
                2
            );
        }
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence)
                .await
                .is_err()
        );
        Ok(payloads)
    }

    #[tokio::test]
    async fn admission_retry_retains_original_owner_grant_after_renewal() -> Result<()> {
        for session in [false, true] {
            let stream = StreamClient::new(Arc::new(MemoryStream::default()));
            let payloads = Arc::new(MemoryPayloads::new()?);
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "batch-owner".into(),
            };
            let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
            let owner_scope = issuer.root(
                "owner",
                Capabilities::new([
                    "operation:declare",
                    "operation:observe",
                    "operation:cancel",
                    "task:spawn:test.batch@1",
                ]),
            );
            let runtime_scope =
                RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
            let machine = MachineIdentity {
                name: "test.batch".into(),
                version: "1".into(),
                digest: [2; 32],
            };
            let mut machines = MachineRegistry::default();
            let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
                identity: machine.clone(),
                schema: serde_json::json!({"type": "integer"}),
            });
            machines.register(implementation.clone())?;
            let definition = TaskDefinition::<Value, i64>::resumable(
                implementation,
                serde_json::json!({"type": "integer"}),
                serde_json::json!({"type": "integer"}),
            )?;
            let task = definition.identity().clone();
            let mut tasks = TaskRegistry::default();
            tasks.register(definition)?;
            let coordinator = DistributedCoordinator::open(&stream, payloads.clone()).await?;
            let host = CoordinatorTaskHost::new(
                coordinator,
                stream.clone(),
                payloads.clone(),
                payloads.clone(),
                owner.clone(),
                owner_scope.clone(),
                issuer.verifier(),
                runtime_scope.clone(),
                tasks.clone(),
                machines.clone(),
                Arc::new(SystemUnixMillisClock),
            )?;
            let limits = crate::scheduler::SessionLimits {
                active_tasks: 8,
                total_tasks: 16,
                depth: 4,
                model_steps: 100,
            };
            let host = if session {
                host.with_session_limits(limits)?
            } else {
                host
            };
            let root_admission = |operation_id, input| TaskAdmissionRecord {
                operation_id,
                resource_proofs: Vec::new(),
                task: task.clone(),
                machine: machine.clone(),
                input,
                input_schema: serde_json::json!({"type": "integer"}),
                output_schema: serde_json::json!({"type": "integer"}),
                parent: None,
                grants: runtime_scope.grants().clone(),
                limits: runtime_scope.limits(),
                run_limits: runtime_scope.run_limits(),
                policy: None,
                extensions: runtime_scope.extensions().cloned(),
                execution: None,
            };

            let operation_id = OperationId::from_bytes([88; 16]);
            let admission = root_admission(operation_id, serde_json::json!(0));
            assert!(matches!(
                host.admit(admission.clone()).await?,
                Admission::Accepted(_)
            ));
            let original = host
                .retained_declaration(operation_id)
                .await?
                .ok_or_else(|| Error::NotFound("original admission".into()))?;
            let lease = host
                .coordinator
                .lock()
                .await
                .pull(&crate::distributed::Worker {
                    id: "renewal-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("parent lease".into()))?;
            host.start_task(&lease).await?;
            let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
            let mut child = root_admission(OperationId::from_bytes([89; 16]), serde_json::json!(0));
            child.parent = Some(TaskId::from_bytes(operation_id.into_bytes()));
            assert!(matches!(
                host.admit_owned(child.clone(), fence.clone()).await?,
                Admission::Accepted(_)
            ));
            let original_child = host
                .retained_declaration(child.operation_id)
                .await?
                .ok_or_else(|| Error::NotFound("original child admission".into()))?;
            let renewed = issuer.root("renewed-owner", owner_scope.capabilities().clone());
            let reopened = CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, payloads.clone()).await?,
                stream.clone(),
                payloads.clone(),
                payloads.clone(),
                owner.clone(),
                renewed,
                issuer.verifier(),
                runtime_scope.clone(),
                tasks.clone(),
                machines.clone(),
                Arc::new(SystemUnixMillisClock),
            )?;
            let reopened = if session {
                reopened.with_session_limits(limits)?
            } else {
                reopened
            };
            let revision = reopened.coordinator.lock().await.revision();
            assert!(matches!(
                reopened.admit(admission.clone()).await?,
                Admission::Accepted(_)
            ));
            assert_eq!(reopened.coordinator.lock().await.revision(), revision);
            assert_eq!(
                reopened.retained_declaration(operation_id).await?,
                Some(original.clone())
            );
            assert!(matches!(
                reopened.admit_owned(child.clone(), fence.clone()).await?,
                Admission::Accepted(_)
            ));
            assert_eq!(reopened.coordinator.lock().await.revision(), revision);
            assert_eq!(
                reopened.retained_declaration(child.operation_id).await?,
                Some(original_child.clone())
            );
            let mut stale = fence.clone();
            stale.reservation_id.push_str("-stale");
            assert!(matches!(
                reopened.admit_owned(child.clone(), stale).await,
                Err(Error::Unauthorized(_))
            ));
            assert_eq!(reopened.coordinator.lock().await.revision(), revision);
            let mut changed = admission.clone();
            changed.input = serde_json::json!(1);
            assert!(matches!(
                reopened.admit(changed).await,
                Err(Error::Conflict(_))
            ));
            let impostor = AuthorityIssuer::new("batch-test", [8; 32], owner.clone());
            let impostor_host = CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, payloads.clone()).await?,
                stream.clone(),
                payloads.clone(),
                payloads.clone(),
                owner.clone(),
                impostor.root("renewed-owner", owner_scope.capabilities().clone()),
                impostor.verifier(),
                runtime_scope.clone(),
                tasks.clone(),
                machines.clone(),
                Arc::new(SystemUnixMillisClock),
            )?;
            let impostor_host = if session {
                impostor_host.with_session_limits(limits)?
            } else {
                impostor_host
            };
            assert!(matches!(
                impostor_host.admit(admission).await,
                Err(Error::Unauthorized(_))
            ));
            assert_eq!(
                reopened.retained_declaration(operation_id).await?,
                Some(original)
            );
            reopened
                .coordinator
                .lock()
                .await
                .cancel_operation(
                    &owner,
                    &reopened.owner_scope,
                    &issuer.verifier(),
                    operation_id,
                    IdempotencyKey::new("renewal-parent-cancel")?,
                    false,
                )
                .await?;
            let cancelled_revision = reopened.coordinator.lock().await.revision();
            assert!(matches!(
                reopened.admit_owned(child.clone(), fence).await,
                Err(Error::Conflict(_))
            ));
            assert_eq!(
                reopened.coordinator.lock().await.revision(),
                cancelled_revision
            );
            assert_eq!(
                reopened.retained_declaration(child.operation_id).await?,
                Some(original_child)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn partially_admitted_batch_reopens_and_fills_only_missing_slots() -> Result<()> {
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "batch-owner".into(),
        };
        let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
        let owner_scope = issuer.root(
            "owner",
            Capabilities::new([
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.batch@1",
            ]),
        );
        let runtime_scope =
            RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.batch".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let mut machines = MachineRegistry::default();
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        machines.register(implementation.clone())?;
        let definition = TaskDefinition::<Value, i64>::resumable(
            implementation,
            serde_json::json!({"type": "integer"}),
            serde_json::json!({"type": "integer"}),
        )?;
        let task = definition.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let coordinator = DistributedCoordinator::open(&stream, payloads.clone()).await?;
        let host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner.clone(),
            owner_scope.clone(),
            issuer.verifier(),
            runtime_scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?;
        let root_admission = |operation_id, input| TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id,
            task: task.clone(),
            machine: machine.clone(),
            input,
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: runtime_scope.extensions().cloned(),
            execution: None,
        };
        assert!(matches!(
            host.admit(root_admission(
                OperationId::from_bytes([7; 16]),
                serde_json::json!("not an integer"),
            ))
            .await,
            Err(Error::Invalid(_))
        ));
        let parent_operation = OperationId::from_bytes([8; 16]);
        let parent = TaskId::from_bytes(parent_operation.into_bytes());
        assert!(matches!(host.admit(root_admission(
            parent_operation, serde_json::json!(0),
        )).await?, Admission::Accepted(id) if id == parent));
        let metadata_revision = host.coordinator.lock().await.revision();
        let unrelated_operation = OperationId::from_bytes([201; 16]);
        let mut unrelated = host
            .coordinator
            .lock()
            .await
            .scheduler()
            .operation(parent_operation)
            .ok_or_else(|| Error::NotFound("metadata parent".into()))?
            .spec
            .clone();
        unrelated.operation_id = unrelated_operation;
        let mut remote = DistributedCoordinator::open(&stream, payloads.clone()).await?;
        remote
            .declare_operation(
                &owner,
                &owner_scope,
                &issuer.verifier(),
                unrelated,
                IdempotencyKey::new("unrelated-metadata-observation")?,
            )
            .await?;
        drop(remote);
        assert_eq!(
            host.admission(parent_operation).await?.operation_id,
            parent_operation
        );
        let cached = host.coordinator.lock().await;
        assert_eq!(cached.revision(), metadata_revision);
        assert!(
            cached.scheduler().operation(unrelated_operation).is_none(),
            "immutable metadata reads must not hydrate unrelated lifecycle history"
        );
        drop(cached);
        let request = DurableBatchRequest {
            group_id: GroupId::from_bytes([9; 16]),
            batch_id: BatchId::from_bytes([10; 16]),
            group_policy: crate::runtime::BatchGroupPolicy::CollectAll,
            task: task.clone(),
            machine: machine.clone(),
            inputs: vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ],
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: Some(parent),
            scope: runtime_scope.clone(),
            policy: None,
            execution: None,
        };
        let bounded_parent_operation = OperationId::from_bytes([27; 16]);
        let bounded_parent = TaskId::from_bytes(bounded_parent_operation.into_bytes());
        let mut bounded = root_admission(bounded_parent_operation, serde_json::json!(0));
        bounded.grants = Capabilities::new(["task:spawn:test.batch@1"]);
        bounded.limits.model_steps = 2;
        bounded.run_limits = crate::runtime::TaskRunLimits {
            concurrency: Some(2),
            max_steps: Some(2),
            deadline_epoch_ms: None,
        };
        assert!(matches!(
            host.admit(bounded.clone()).await?,
            Admission::Accepted(_)
        ));
        for kind in 0..5 {
            let mut child = bounded.clone();
            child.operation_id = OperationId::new();
            child.parent = Some(bounded_parent);
            match kind {
                0 => child.grants = runtime_scope.grants().clone(),
                1 => child.limits.model_steps = 3,
                2 => child.run_limits.max_steps = Some(3),
                3 => child.run_limits.max_steps = None,
                _ => child.run_limits.concurrency = None,
            }
            let operation_id = child.operation_id;
            assert!(matches!(
                host.admit(child).await,
                Err(Error::Unauthorized(_))
            ));
            assert!(host.reconcile_admission(operation_id).await?.is_none());
        }
        let mut valid_child = bounded.clone();
        valid_child.operation_id = OperationId::new();
        valid_child.parent = Some(bounded_parent);
        valid_child.limits.model_steps = 1;
        valid_child.run_limits.max_steps = Some(1);
        valid_child.run_limits.concurrency = Some(1);
        assert!(matches!(
            host.admit(valid_child).await?,
            Admission::Accepted(_)
        ));
        let mut wide_batch = request.clone();
        wide_batch.batch_id = BatchId::from_bytes([28; 16]);
        wide_batch.parent = Some(bounded_parent);
        assert!(matches!(
            host.admit_batch(wide_batch.clone()).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(host.retained_batch(wide_batch.batch_id).await?.is_none());
        let mut forged = request.clone();
        forged.task.digest = [99; 32];
        assert!(matches!(
            host.admit_batch(forged).await,
            Err(Error::Conflict(_))
        ));
        let mut altered_schema = request.clone();
        altered_schema.output_schema = serde_json::json!({"type": "number"});
        assert!(matches!(
            host.admit_batch(altered_schema).await,
            Err(Error::Conflict(_))
        ));
        host.commit_batch(&request).await?;
        let first = request.operation_id(0);
        assert!(matches!(host.admit(request.member_admission(0)?).await?,
            Admission::Accepted(id) if id.into_bytes() == first.into_bytes()));
        let reopened = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner,
            owner_scope,
            issuer.verifier(),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?;
        let restored = reopened
            .load_batch(request.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("retained batch request".into()))?;
        assert_eq!(restored.canonical_value(), request.canonical_value());
        let observed = reopened
            .reconcile_batch(request.clone())
            .await?
            .ok_or_else(|| Error::NotFound("committed batch".into()))?;
        assert!(matches!(
            observed.as_slice(),
            [
                Admission::Accepted(_),
                Admission::Indeterminate { .. },
                Admission::Indeterminate { .. }
            ]
        ));
        let completed = reopened.admit_batch(request.clone()).await?;
        assert_eq!(completed.len(), 3);
        for (index, admission) in completed.iter().enumerate() {
            let Admission::Accepted(task_id) = admission else {
                return Err(Error::Conflict("batch slot was not admitted".into()));
            };
            assert_eq!(
                task_id.into_bytes(),
                request.operation_id(index).into_bytes()
            );
        }
        assert_eq!(reopened.admit_batch(request.clone()).await?, completed);
        let mut root_request = request.clone();
        root_request.batch_id = BatchId::from_bytes([15; 16]);
        root_request.parent = None;
        let root_members = reopened.admit_batch(root_request.clone()).await?;
        assert!(
            root_members
                .iter()
                .all(|entry| matches!(entry, Admission::Accepted(_)))
        );
        assert_eq!(
            reopened
                .load_batch(root_request.batch_id)
                .await?
                .ok_or_else(|| Error::NotFound("root batch request".into()))?
                .canonical_value(),
            root_request.canonical_value()
        );
        let manifest = reopened.batch_stream(request.batch_id)?;
        assert_eq!(manifest.bounds().await?.tail, 1);
        let records = manifest.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        assert_eq!(records.len(), 1);
        assert!(
            !std::str::from_utf8(&records[0].value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .contains("inputs")
        );
        let mut collision = request.clone();
        collision.batch_id = BatchId::from_bytes([11; 16]);
        let mut claimed_admission = collision.member_admission(0)?;
        claimed_admission.input = serde_json::json!(999);
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&collision).await?;
        assert!(matches!(
            reopened.reconcile_batch(collision).await,
            Err(Error::Conflict(_))
        ));
        let mut root_collision = request.clone();
        root_collision.batch_id = BatchId::from_bytes([12; 16]);
        let mut claimed_admission = root_collision.member_admission(0)?;
        claimed_admission.parent = None;
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&root_collision).await?;
        assert!(matches!(
            reopened.reconcile_batch(root_collision).await,
            Err(Error::Conflict(_))
        ));
        let other_parent_operation = OperationId::from_bytes([13; 16]);
        let other_parent = TaskId::from_bytes(other_parent_operation.into_bytes());
        assert!(matches!(
            reopened
                .admit(root_admission(other_parent_operation, serde_json::json!(0),))
                .await?,
            Admission::Accepted(_)
        ));
        let mut foreign_parent = request.clone();
        foreign_parent.batch_id = BatchId::from_bytes([14; 16]);
        let mut claimed_admission = foreign_parent.member_admission(0)?;
        claimed_admission.parent = Some(other_parent);
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&foreign_parent).await?;
        assert!(matches!(
            reopened.reconcile_batch(foreign_parent).await,
            Err(Error::Conflict(_))
        ));
        let mut cancelled = request.clone();
        cancelled.batch_id = BatchId::from_bytes([16; 16]);
        cancelled.group_policy = crate::runtime::BatchGroupPolicy::CancelOnFailure;
        cancelled.parent = None;
        reopened.commit_batch(&cancelled).await?;
        let first_cancelled = cancelled.member_admission(0)?;
        assert!(matches!(
            reopened.admit(first_cancelled).await?,
            Admission::Accepted(_)
        ));
        let cancellation = reopened
            .cancel_batch(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("retained cancellation".into()))?;
        assert_eq!(cancellation.group_id, cancelled.group_id);
        assert_eq!(cancellation.entries.len(), cancelled.inputs.len());
        assert!(matches!(
            &cancellation.entries[0].1,
            BatchCancellationStatus::Requested
        ));
        assert!(
            cancellation.entries[1..]
                .iter()
                .all(|(_, status)| matches!(status, BatchCancellationStatus::NotAdmitted { .. }))
        );
        assert_eq!(
            reopened
                .retained_batch_cancellation(cancelled.batch_id)
                .await?
                .ok_or_else(|| Error::NotFound("cancellation declaration".into()))?
                .group_id,
            cancelled.group_id
        );
        let after_cancel = reopened.admit_batch(cancelled.clone()).await?;
        assert!(matches!(
            after_cancel.as_slice(),
            [
                Admission::Accepted(_),
                Admission::Rejected { .. },
                Admission::Rejected { .. }
            ]
        ));
        assert_eq!(
            reopened.reconcile_batch(cancelled.clone()).await?,
            Some(after_cancel)
        );
        let repeated = reopened
            .cancel_batch(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("replayed cancellation".into()))?;
        assert_eq!(repeated.entries.len(), cancelled.inputs.len());
        assert!(matches!(
            &repeated.entries[0].1,
            BatchCancellationStatus::Requested
        ));
        assert_eq!(
            reopened
                .batch_cancellation_stream(cancelled.batch_id)?
                .bounds()
                .await?
                .tail,
            1
        );
        let mut changed = request.clone();
        changed.inputs.swap(0, 1);
        assert!(matches!(
            reopened.reconcile_batch(changed).await,
            Err(Error::Conflict(_))
        ));
        // A retained control path must still contain exactly one record.
        // Full Stream history remains readable even when that invariant fails.
        manifest
            .append_at(Bytes::from_static(b"unexpected-second-record"), 1)
            .await?;
        assert!(matches!(
            reopened.retained_batch(request.batch_id).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(
            manifest
                .read(0, 2)
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            2
        );
        Ok(())
    }
}
