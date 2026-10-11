//! First-party original-root allocation through the stock task effect lifecycle.
//!
//! Immutable attachment inputs precede creation. They describe the native
//! capability selected for that creation, not successful allocation. Only the
//! Filesystem's original creation operation and immutable output prove success.

use super::{
    LocalGeneration, LocalLazyWorkspace, PiScratchCreation, RootCreation,
    ServiceResources, SharedPhysicalRoot, SharedRootAdmission,
    pi_owner::PiOwnerConfig,
    pi_root::PiRootIntent,
    pi_root_allocation::PiRootAllocation,
    pi_root_inputs::{PiRootPreparation, PiRootRequest, ROOT_ALLOCATION_EFFECT,
        ROOT_ALLOCATION_PROVIDER, ROOT_INPUT_DIRECTORY},
};
use acyclic_fs::{
    CancellationToken, DemandSource, DistributedFs, LazyWorkspaceStore, Lifecycle, LocalAuthorityBackend,
    LocalObjectBackend, MeasuredResult as FilesystemMeasuredResult, NativeAllocation, NativeMaterializationWork,
    NativeTreeMaterializationError, OperationReceipt, SourceReference, VolumeConfig, WorkBudget, WorkCounters, WorkError,
};
use acyclic_harness::{
    EffectAttemptId, EffectId, Error, IdempotencyKey, JsonBounds, JsonCodecCharge, OperationId, Result, TaskId,
    canonical_json_bytes_bounded, canonical_json_bytes_bounded_native_trusted,
    json_bytes_bounded_trusted, json_bytes_bounded_native_trusted, json_from_slice_bounded_trusted,
    json_value_bounded_native_trusted, json_from_slice_bounded_native_trusted,
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::{EffectGuarantee, EffectState, EffectStatus},
    durable_host::TaskJournalOwner,
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    filesystem::FilesystemHost,
    resources::GenerationRef,
    runtime::TaskContext,
    store::{HistoryReadLimits, HistoryReader},
};
use acyclic_harness::wire::ErrorCode;
use acyclic_harness::{JsonValueSchema, json_value_decode_owned_bytes, json_typed_diagnostic_owned_bytes,
    sdk_json_diagnostic_bytes};
use acyclic_stream::{BoxProviderFuture, StreamClient, StreamProvider};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::{BTreeMap, BTreeSet}, path::Path, sync::Arc};
use tokio::sync::Mutex;

pub(crate) const OPERATOR_INPUT_DIRECTORY: &str = "pi-operator-inputs";

type Host = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

/// Inline failure prefix: constructor denial never allocates a counter carrier.
pub(crate) type PiMeasuredResult<T> = std::result::Result<T, (Error, WorkCounters)>;

/// Actual handles returned by dispatch, never reconstructed from route IDs.
pub(crate) struct PiRootHandle {
    pub(crate) workspace: Arc<LocalLazyWorkspace>,
    pub(crate) physical: Arc<SharedPhysicalRoot>,
    _custody: Option<NativeAllocation>,
}

impl PiRootHandle {
    /// Cold restoration transfers actual copied-key custody and admits the
    /// compiled entry payload before its caller grows the root index. This is
    /// logical map-entry ownership, not an opaque BTree node/RSS observation.
    pub(crate) fn from_observed_with_native_work(
        workspace: LocalLazyWorkspace, physical: Arc<SharedPhysicalRoot>,
        key: (String, NativeAllocation), work: &NativeMaterializationWork,
    ) -> Result<(String, Self)> {
        let mut key = NativeOwned { value: key.0, _custody: key.1 };
        if !key._custody.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "root index key custody differs from original source work" });
        }
        let workspace = workspace.into_arc_with_native_work(work)
            .map_err(|source| Error::NativeSource { source, custody: None })?;
        let mut entry = work.retain_owned(std::mem::size_of::<(String, Self)>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        key._custody.absorb(&mut entry)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok((key.value, Self { workspace, physical, _custody: Some(key._custody) }))
    }
}

/// Producer-authenticated original FS allocation evidence, NOT a native
/// deletion permit. No caller can construct, deserialize or retarget this.
/// Owned native preparation proof plus current fenced scratch WRITE and drained
/// effects are independent mandatory checks at the actual release boundary.
pub(crate) struct PiRootScratchAllocationProof {
    task: TaskId,
    agent: acyclic_harness::AgentId,
    volume: VolumeRef,
    operation: acyclic_fs::OperationId,
    workspace: [u8; 16],
    generation: GenerationRef,
    output: FileRef,
    _custody: NativeAllocation,
}
impl PiRootScratchAllocationProof {
    pub(crate) fn task(&self) -> TaskId { self.task }
    pub(crate) fn agent(&self) -> acyclic_harness::AgentId { self.agent }
    pub(crate) fn volume(&self) -> &VolumeRef { &self.volume }
    pub(crate) fn operation(&self) -> acyclic_fs::OperationId { self.operation }
    pub(crate) fn workspace_id(&self) -> [u8; 16] { self.workspace }
    pub(crate) fn generation(&self) -> &GenerationRef { &self.generation }
    pub(crate) fn output(&self) -> &FileRef { &self.output }

    /// Rechecks original cached WRITE and the fresh current fence BEFORE the
    /// task's terminal publication. Native disposal additionally requires the
    /// original owned preparation identity and drained effects.

    pub(crate) async fn authorize_settlement_with_native_work<P: StreamProvider + 'static>(
        &self, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if owner.task_id() != self.task
            || owner.authenticated_binding_agent_with_native_work(work)? != Some(self.agent)
            || self.volume.class() != acyclic_harness::conversation::VolumeClass::AgentPrivate
            || self.volume.owner() != &acyclic_harness::conversation::VolumeOwner::Agent(self.agent) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "scratch release differs from its original task and bound private agent" });
        }
        owner.require_volume_grant_with_native_work(&self.volume, VolumeOperation::Write, work)?;
        owner.verify_with_native_work(false, work).await
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttachmentInput {
    task: TaskId,
    command: OperationId,
    effect: EffectId,
    attempt: EffectAttemptId,
    request_digest: [u8; 32],
    provider: String,
    effect_kind: String,
    root: String,
    volume: VolumeRef,
    native_path: String,
    source: SourceReference,
    native_identity: [u8; 16],
    budget: WorkBudget,
}

/// Counts actual closed DTO labels plus serde's expected-list punctuation.
/// Inventories must include the concrete derives and custom STATIC labels;
/// arbitrary custom Deserialize/Display is outside this trusted boundary.
pub(crate) const fn pi_serde_diagnostic_labels_bytes(labels: &[&str]) -> u64 {
    let mut total = 0_u64;
    let mut index = 0;
    while index < labels.len() {
        total += labels[index].len() as u64 + 4;
        index += 1;
    }
    total
}

// Original Root request/allocation/attachment and their closed SDK leaves.
pub(crate) const ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES: u64 =
    acyclic_harness::preparation_serde_diagnostic_label_bytes()
    + pi_serde_diagnostic_labels_bytes(&[
    "PiRootRequest", "binding", "root", "PiTaskBinding", "native_session_id",
    "task_id", "agent_id", "project_volumes", "scratch", "results",
    "PiScratchBinding", "volume", "allocation", "PiScratchAllocation", "state",
    "creation", "preparing", "created", "PiScratchCreation", "workspace_id",
    "generation", "PiRootIntent", "kind", "scratch_only", "project",
    "context_id", "root_id", "paths", "PiRootAllocation", "roots", "RootCreation",
    "repository_workspace_id", "source_identity", "source_epoch", "native_root_identity",
    "AttachmentInput", "task", "command", "effect", "attempt", "digest", "provider",
    "effect_kind", "native_path", "source", "native_identity", "budget",
    "SourceReference", "identity", "epoch", "TaskEffectPlan", "guarantee",
    "request", "result_schema", "exactly_once", "at_least_once", "at_most_once",
    "ProviderRefWire", "namespace", "family", "version", "ResourceRefWire", "key",
    "workspace", "generation", "artifact", "sandbox", "checkpoint", "stream",
    "context", "run", "VolumeRefWire", "id", "class", "owner", "agent_private",
    "session_shared", "agent", "session", "FileRefWire", "path", "descriptor",
    "display_name", "FileDescriptorWire", "sha256", "byte_length", "media_type",
    "SHA-256 digest must contain exactly 32 bytes",
    "file byte length exceeds cross-language precision", "media type is invalid",
    // WorkBudget is the WorkCounters alias; its derive defaults missing fields
    // and ignores unknown ones, but actual wire/type labels remain inventoried.
    "WorkCounters", "authorityRecordsRead", "authorityRecordsAppended",
    "authorityBytesRead", "authorityBytesWritten", "objectProbes",
    "backendReadOperations", "backendWriteOperations", "durabilityOperations",
    "pageReads", "pageWrites", "objectBytesRead", "objectBytesWritten",
    "bytesHashed", "bytesCopied", "bytesEncoded", "sourceBytesRead",
    "sourcePathComponents", "sourceEntriesVisited", "outputBytes",
    "itemsExamined", "itemsReturned", "allocationOperations",
    "peakAllocationBytes", "materializations",
]);

/// Only audited first-party serde shapes enter this decoder. The concrete
/// envelope and SDK-issued source shape dominate known String/Vec/map storage;
/// neither is an application bytes-to-heap ratio or physical allocator claim.
trait RootArtifact: DeserializeOwned + Serialize {
    const VALUE_SCHEMA: JsonValueSchema = JsonValueSchema::None;
}
impl RootArtifact for PiRootRequest {}
impl RootArtifact for acyclic_harness::effect_host::TaskEffectPlan {}
impl RootArtifact for PiRootAllocation {}
impl RootArtifact for BTreeMap<String, AttachmentInput> {}
impl RootArtifact for serde_json::Value {
    const VALUE_SCHEMA: JsonValueSchema = JsonValueSchema::Root;
}

/// Finite allowance for native attachment plus first-party metadata processing.
/// Native entry/body allowances are unchanged; JSON token/custody work is
/// cumulative across the request, creator input, schema and result producers.
pub(crate) fn allocation_budget() -> WorkBudget {
    WorkBudget { items_examined: 131_072, items_returned: 65_536, allocation_operations: 65_536,
        ..super::native_root_source::budget() }
}

fn same_allocation(left: &PiRootAllocation, right: &PiRootAllocation) -> bool {
    left.scratch == right.scratch && left.roots.keys().eq(right.roots.keys())
        && left.roots.iter().all(|(key, root)| {
            let other = &right.roots[key];
            root.repository_workspace_id == other.repository_workspace_id
                && root.source_identity == other.source_identity
                && root.source_epoch == other.source_epoch
                && root.native_root_identity == other.native_root_identity
        })
}

struct ProviderState {
    dispatched: bool,
    handles: BTreeMap<String, PiRootHandle>,
}

pub(crate) struct PiRootProvider<P> {
    owner: Arc<PiOwnerConfig>,
    resources: ServiceResources,
    filesystem: Arc<Host>,
    stream: StreamClient<P>,
    preparation: PiRootPreparation,
    journal_owner: Option<Arc<TaskJournalOwner<P>>>,
    context: Arc<acyclic_harness::NativeTaskContext>,
    original: NativeMaterializationWork,
    state: Mutex<ProviderState>,
    creation_cancelled: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
    _custody: NativeAllocation,
}

/// Receives only the original protected owner and authenticated preparation.
/// The stock effect host remains the only durable dispatch winner/authority.
#[allow(clippy::too_many_arguments, reason = "explicit original owner and finite producer contract")]
pub(crate) fn root_provider<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: StreamClient<P>,
    preparation: PiRootPreparation,
    journal_owner: Arc<TaskJournalOwner<P>>,
    context: Arc<acyclic_harness::NativeTaskContext>,
    original: NativeMaterializationWork,
) -> Result<Arc<PiRootProvider<P>>> {
    root_provider_native_inner(owner, resources, filesystem, stream, preparation,
        Some(journal_owner), context, original)
}

/// Fresh creator's source gate. The custody-bearing preparation must already
/// have been loaded through the original admission; it is identity metadata,
/// not READ authority. The existing issued context and owner supply that READ.
/// Created-root restoration additionally requires the original successful
/// filesystem/allocation observer, never this precreation guard alone.
pub(crate) async fn guard_root_preparation_source_with_native_work<P: StreamProvider + 'static>(
    context: &TaskContext, current_owner: &TaskJournalOwner<P>,
    preparation: &PiRootPreparation, root_key: &str,
    expected: Option<acyclic_fs::NativeRootIdentity>, work: &NativeMaterializationWork,
) -> Result<acyclic_fs::NativeRootIdentity> {
    let request = preparation.request();
    let project = request.binding.project_volumes.get(root_key).ok_or(Error::NativeBoundary {
        code: ErrorCode::Unauthorized, reason: "root source is not selected by its original admission",
    })?;
    // This authenticates the actual SDK task/fence and scoped READ before any
    // observer await or physical source open. No path or source ID is a grant.
    current_owner.require_volume_grant_with_native_work(project, VolumeOperation::Read, work)?;
    context.verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    if current_owner.task_id() != request.binding.task_id
        || preparation.command().operation_id != super::pi_root_inputs::preparation_operation_with_native_work(request.binding.task_id, work)?
        || preparation.plan().provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan().effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan().guarantee != EffectGuarantee::AtMostOnce {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "root source differs from its original admitted creator operation" });
    }
    let PiRootIntent::Project { paths, .. } = &request.root else {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "scratch-only admission has no native project source" });
    };
    let path = paths.get(root_key).ok_or(Error::NativeBoundary {
        code: ErrorCode::Unauthorized, reason: "root source has no original configured binding",
    })?;
    current_owner.verify_with_native_work(false, work).await?;
    let identity = work.root_identity(Path::new(path))
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if let Some(expected) = expected {
        if identity != expected {
            return Err(Error::NativeWork { cause: NativeTreeMaterializationError::AllocationChanged {
                expected, actual: identity }, custody: None });
        }
    }
    context.verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    current_owner.verify_with_native_work(false, work).await?;
    Ok(identity)
}

/// Existing Created roots use the genuine SDK readonly owner/context pair.
/// No lazy snapshot is invented: the same immutable successful-allocation
/// observer authenticates original uploads, creator operations and lazy state.
#[allow(clippy::too_many_arguments, reason = "original creator and scoped readonly owner evidence remain explicit")]
pub(crate) async fn guard_created_root_source_with_native_work<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: StreamClient<P>, preparation: PiRootPreparation,
    current_read_owner: &TaskJournalOwner<P>, key: &str,
    expected_allocation: &PiRootAllocation, work: &NativeMaterializationWork,
) -> Result<acyclic_fs::NativeRootIdentity> {
    work.admit(WorkCounters::UNCHARGED)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let context = Arc::clone(preparation.native_context());
    let request = preparation.request();
    if current_read_owner.task_id() != request.binding.task_id
        || context.context().durable_task_id() != Some(request.binding.task_id)
    {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "created root source differs from the original readonly task" });
    }
    // The observer verifies all selected resources, so every native identity
    // probe is preceded by the original admission's sealed READ permission.
    for volume in request.binding.project_volumes.values() {
        context.context().verify_original_volume_read_with_native_work(
            current_read_owner, volume, work).await?;
    }
    if !request.binding.project_volumes.contains_key(key) {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "created source is not an originally selected root" });
    }
    expected_allocation.validate(&request.binding)?;
    let observer = root_provider_native_inner(owner, resources, filesystem, stream,
        preparation, None, Arc::clone(&context), work.clone())?;
    let effect = observer.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let (value, custody) = observer.observe_allocation_with_native_work(attempt, work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("created root lacks its original successful output"))?;
    let observed = NativeOwned { value, _custody: custody };
    if !same_allocation(expected_allocation, &observed.value.1) {
        return Err(PiRootProvider::<P>::native_failure("created root substitutes original allocation evidence"));
    }
    let PiRootIntent::Project { paths, .. } = &observer.preparation.request().root else {
        return Err(PiRootProvider::<P>::native_failure("scratch-only allocation has no created native source"));
    };
    let path = paths.get(key).ok_or_else(|| PiRootProvider::<P>::native_failure(
        "created source has no original native selection"))?;
    let identity = work.root_identity(Path::new(path))
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let expected = acyclic_fs::NativeRootIdentity::from_bytes(observed.value.1.roots[key].native_root_identity);
    if identity != expected {
        return Err(Error::NativeWork { cause: NativeTreeMaterializationError::AllocationChanged {
            expected, actual: identity }, custody: None });
    }
    context.context().verify_original_volume_read_with_native_work(current_read_owner,
        &observer.preparation.request().binding.project_volumes[key], work).await?;
    Ok(identity)
}

// Only these three ServiceResources fields own backing storage. The existing
// FS/store/root-registry clones are reference-counted handles with no growth.
pub(crate) fn service_resources_with_native_work(
    resources: &ServiceResources, work: &NativeMaterializationWork,
) -> Result<(ServiceResources, NativeAllocation)> {
    let native = |cause| Error::NativeWork { cause, custody: None };
    let (value, custody) = work.copy_path(&resources.data).map_err(native)?;
    let data = NativeOwned { value, _custody: custody };
    let (value, custody) = work.display(&resources.binary_identity).map_err(native)?;
    let mut binary = NativeOwned { value, _custody: custody };
    let (value, custody) = work.display(&resources.instance_id).map_err(native)?;
    let mut instance = NativeOwned { value, _custody: custody };
    let mut owned = NativeOwned {
        value: ServiceResources {
            data: data.value, fs: resources.fs.clone(), store: resources.store.clone(),
            shared_roots: resources.shared_roots.clone(), binary_identity: binary.value,
            instance_id: instance.value, private_state_root: resources.private_state_root.clone(),
        },
        _custody: data._custody,
    };
    owned._custody.absorb(&mut binary._custody).map_err(native)?;
    owned._custody.absorb(&mut instance._custody).map_err(native)?;
    Ok((owned.value, owned._custody))
}

#[allow(clippy::too_many_arguments, reason = "original protected owner and issued context remain explicit")]
fn root_provider_native_inner<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: StreamClient<P>, preparation: PiRootPreparation,
    journal_owner: Option<Arc<TaskJournalOwner<P>>>,
    context: Arc<acyclic_harness::NativeTaskContext>, original: NativeMaterializationWork,
) -> Result<Arc<PiRootProvider<P>>> {
    let native = |cause| Error::NativeWork { cause, custody: None };
    original.admit(WorkCounters::UNCHARGED).map_err(native)?;
    if !original.budget().has_finite_allowances() {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
            reason: "root allocation requires finite original work allowances" });
    }
    let request = preparation.request();
    if request.binding.task_id != owner.policy().root_task || request.binding.agent_id != owner.policy().agent
        || journal_owner.as_ref().is_some_and(|current|
            current.task_id() != request.binding.task_id
                || context.context().durable_task_id() != Some(request.binding.task_id))
        || !matches!(request.binding.scratch.allocation, super::PiScratchAllocation::Preparing)
        || preparation.plan().provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan().effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan().guarantee != EffectGuarantee::AtMostOnce {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "root allocator differs from its original admission and issued context" });
    }
    let (value, custody) = owner.authority_with_native_work(&original)?;
    let authority = NativeOwned { value, _custody: custody };
    for grant in ["effect:run", "conversation:bind"] {
        if !authority.value.scope.grants().contains(grant) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "original root allocation scope lacks a required capability" });
        }
    }
    let (value, custody) = original.display(&format_args!("effect:provider:{ROOT_ALLOCATION_PROVIDER}")).map_err(native)?;
    let provider_grant = NativeOwned { value, _custody: custody };
    if !authority.value.scope.grants().contains(&provider_grant.value) {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root allocation provider grant is absent" });
    }
    for volume in request.binding.project_volumes.values().chain(std::iter::once(&request.binding.scratch.volume)) {
        let (value, custody) = volume.capability_with_native_work(VolumeOperation::Read, &original)?;
        let grant = NativeOwned { value, _custody: custody };
        if volume.provider() != owner.policy().journal.provider()
            || !authority.value.scope.grants().contains(&grant.value) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "original root volume provider or read grant differs" });
        }
    }
    let (value, custody) = request.binding.scratch.volume.capability_with_native_work(VolumeOperation::Write, &original)?;
    let grant = NativeOwned { value, _custody: custody };
    if !authority.value.scope.grants().contains(&grant.value) {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root scratch write grant is absent" });
    }
    drop(grant);
    drop(authority);
    let mut custody = original.retain_owned(std::mem::size_of::<PiRootProvider<P>>() as u64).map_err(native)?;
    let (value, resource_custody) = service_resources_with_native_work(resources, &original)?;
    let mut resources = NativeOwned { value, _custody: resource_custody };
    custody.absorb(&mut resources._custody).map_err(native)?;
    original.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
    Ok(Arc::new(PiRootProvider {
        owner, resources: resources.value, filesystem, stream, preparation, journal_owner, context, original,
        state: Mutex::new(ProviderState { dispatched: false, handles: BTreeMap::new() }),
        creation_cancelled: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        fault: std::sync::atomic::AtomicU8::new(0),
        _custody: custody,
    }))
}

/// Reopens historical successful creation after the root released its lease.
/// This private construction cannot dispatch, stage a result or acquire a source.
#[allow(clippy::too_many_arguments, reason = "original archived owner, producers and finite observation")]
pub(crate) async fn observe_root_allocation<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: StreamClient<P>, preparation: PiRootPreparation,
    work: &NativeMaterializationWork,
) -> Result<Option<((FileRef, PiRootAllocation), NativeAllocation)>> {
    let context = Arc::clone(preparation.native_context());
    let observer = root_provider_native_inner(owner, resources, filesystem, stream, preparation,
        None, context, work.clone())?;
    let attempt = EffectAttemptId::from_bytes(observer.effect_id_with_native_work(work)?.into_bytes());
    observer.observe_allocation_with_native_work(attempt, work).await
}

#[allow(clippy::too_many_arguments, reason = "original immutable root creation observation, not disposal authority")]
pub(crate) async fn prove_root_scratch_allocation_with_native_work<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: StreamClient<P>, preparation: PiRootPreparation, work: &NativeMaterializationWork,
) -> Result<Option<PiRootScratchAllocationProof>> {
    let context = Arc::clone(preparation.native_context());
    let observer = root_provider_native_inner(owner, resources, filesystem, stream, preparation,
        None, context, work.clone())?;
    let effect = observer.effect_id_with_native_work(work)?;
    observer.prove_scratch_allocation_with_native_work(EffectAttemptId::from_bytes(effect.into_bytes()), work).await
}

/// Root output is its genuinely created private scratch, never the protected
/// bootstrap journal. Root has no fabricated standard child creation binding.
#[allow(clippy::too_many_arguments, reason = "actual caller, original protected creator and bounded producers")]
pub(crate) async fn bind_root_output_context_with_native_work<P: StreamProvider + 'static>(
    context: Arc<acyclic_harness::NativeTaskContext>, current_owner: &Arc<TaskJournalOwner<P>>,
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: StreamClient<P>, request: &PiRootRequest, work: &NativeMaterializationWork,
) -> Result<Arc<acyclic_harness::NativeTaskContext>> {
    work.admit(WorkCounters::UNCHARGED).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let invalid = || Error::NativeBoundary { code: ErrorCode::Unauthorized,
        reason: "root output context differs from its original admitted task scope" };
    if context.context().durable_task_id() != Some(current_owner.task_id())
        || current_owner.task_id() != owner.policy().root_task
        || context.context().scope().grants() != &owner.policy().grants
        || request.binding.task_id != owner.policy().root_task {
        return Err(invalid());
    }
    // Authorization is the original admitted WRITE, not app proof labels,
    // process-stop evidence, path identity or the protected bootstrap scope.
    current_owner.require_volume_grant_with_native_work(&request.binding.scratch.volume,
        VolumeOperation::Write, work)?;
    current_owner.verify_with_native_work(false, work).await?;
    let preparation = load_original_preparation_with_native_work(&owner, &filesystem, request,
        Arc::clone(&context), work).await?;
    let observer = root_provider_native_inner(owner, resources, Arc::clone(&filesystem), stream,
        preparation, None, Arc::clone(&context), work.clone())?;
    let effect = observer.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let proof = observer.prove_scratch_allocation_with_native_work(attempt, work).await?
        .ok_or_else(invalid)?;
    let (authority, custody) = observer.owner.conversation_authority_with_native_work(
        proof.task, proof.agent, &observer.owner.policy().grants, work)?;
    super::pi_conversation::bind_task_conversation_with_native_work(
        &observer.stream, authority, custody, current_owner,
        &proof.volume, proof.operation, observer.owner.policy().limits, work).await?;
    if current_owner.authenticated_binding_agent_with_native_work(work)? != Some(proof.agent) {
        return Err(invalid());
    }
    let generation = observer.creation_with_native_work(effect, attempt, "scratch",
        &proof.volume, VolumeConfig::portable(Lifecycle::Durable), work).await?.ok_or_else(invalid)?;
    let actual = observer.scratch_creation_with_native_work(&generation, work)?;
    if actual.value.workspace_id != proof.workspace || actual.value.generation != proof.generation {
        return Err(invalid());
    }
    let (value, custody) = observer.owner.conversation_authority_with_native_work(
        proof.task, proof.agent, &observer.owner.policy().grants, work)?;
    let authority = NativeOwned { value, _custody: custody };
    let (value, custody) = authority.value.1.verifier_with_native_work(work)?;
    let verifier = NativeOwned { value, _custody: custody };
    let source = filesystem.native_root_output_source_with_native_work(current_owner,
        &proof.volume, proof.operation, &generation, &verifier.value, &authority.value.2, work).await?;
    context.bind_root_output_context_with_native_work(source, work).await?;
    Ok(context)
}

/// Selected original-root source evidence under the caller's remaining quota.
/// The request comes from the caller's authenticated original root admission;
/// immutable canonical inputs and actual successful producer history pin it again.
#[allow(clippy::too_many_arguments, reason = "independent original source and bounded observer arguments")]
pub(crate) async fn prove_root_project_source_with_native_work<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>, stream: StreamClient<P>,
    request: &PiRootRequest, project: &VolumeRef, snapshot: acyclic_fs::LazySnapshotRef,
    context: Arc<acyclic_harness::NativeTaskContext>, current_owner: &TaskJournalOwner<P>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    // The current caller's sealed READ is mandatory even though creation was
    // authored by the original Root. Neither Root metadata nor path is a grant.
    current_owner.require_volume_grant_with_native_work(project, VolumeOperation::Read, work)?;
    context.context().verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    let preparation = load_original_preparation_with_native_work(&owner, &filesystem, request,
        Arc::clone(&context), work).await?;
    let provider = root_provider_native_inner(owner, resources, filesystem, stream,
        preparation, None, context, work.clone())?;
    let effect = provider.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let original = provider.authenticate_with_native_work(attempt, true, work).await?;
    let reference = provider.original_artifact_with_native_work(effect, attempt, "result", work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("root source has no canonical successful allocation output"))?;
    let recorded = provider.read_with_native_work::<PiRootAllocation>(&reference.value, work).await?;
    recorded.value.validate(&request.binding)?;
    let key = request.binding.project_volumes.iter().find_map(|(key, volume)| (volume == project).then_some(key))
        .ok_or_else(|| PiRootProvider::<P>::native_failure("project is not an originally selected root source"))?;
    let root = &recorded.value.roots[key];
    if snapshot.workspace_id.into_bytes() != root.repository_workspace_id
        || snapshot.source.identity != root.source_identity || snapshot.source.epoch < root.source_epoch {
        return Err(PiRootProvider::<P>::native_failure("snapshot substitutes original root workspace or source"));
    }
    let inputs_reference = provider.original_artifact_with_native_work(effect, attempt, "attachments", work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("root source has no original attachment input"))?;
    let inputs = provider.read_with_native_work::<BTreeMap<String, AttachmentInput>>(&inputs_reference.value, work).await?;
    let PiRootIntent::Project { paths, .. } = &request.root else {
        return Err(PiRootProvider::<P>::native_failure("scratch-only root has no native project source"));
    };
    if !inputs.value.keys().eq(paths.keys()) {
        return Err(PiRootProvider::<P>::native_failure("root source selection differs from original attachment"));
    }
    let input = &inputs.value[key];
    provider.validate_attachment_with_native_work(input, effect, attempt, original.value.request_digest,
        key, project, &paths[key])?;
    if input.source.identity != root.source_identity || input.source.epoch != root.source_epoch
        || input.native_identity != root.native_root_identity {
        return Err(PiRootProvider::<P>::native_failure("root source allocation differs from original attachment"));
    }
    let generation = provider.creation_with_native_work(effect, attempt, key, project,
        VolumeConfig::native(Lifecycle::Durable), work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("root source filesystem creation is absent"))?;
    if generation.workspace().id() != snapshot.workspace_id {
        return Err(PiRootProvider::<P>::native_failure("root source snapshot substitutes actual creation"));
    }
    let state = provider.resources.store.load_lazy_workspace_with_native_work(snapshot.workspace_id, work).await
        .map_err(|source| Error::NativeSource { source, custody: None })?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("root source lazy binding is absent"))?;
    state.as_state().validate_root_binding(snapshot.workspace_id, input.source)
        .map_err(|source| Error::NativeSource { source, custody: None })?;
    let identity = work.root_identity(Path::new(&paths[key])).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if identity.to_bytes() != root.native_root_identity
        || !provider.observe_conversation_with_native_work(effect, attempt, work).await? {
        return Err(PiRootProvider::<P>::native_failure("root source native identity or canonical binding differs"));
    }
    provider.context.context().verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    Ok(())
}

fn pi_input_grants(owner: &PiOwnerConfig) -> Result<(ContentGrant, ContentGrant)> {
    let authority = owner.authority()?;
    Ok((ContentGrant::verify_directory_read(&authority.verifier, &authority.signed,
            &owner.policy().journal, ROOT_INPUT_DIRECTORY)?,
        ContentGrant::verify(&authority.verifier, &authority.signed, &owner.policy().journal, VolumeOperation::Write)?))
}

fn pi_input_write_grant_with_native_work(
    owner: &PiOwnerConfig, work: &NativeMaterializationWork,
) -> Result<NativeOwned<ContentGrant>> {
    let authority = owner.authority_with_native_work(work)?;
    let authority = NativeOwned { value: authority.0, _custody: authority.1 };
    let grant = ContentGrant::verify_with_native_work(
        &authority.value.verifier, &authority.value.signed, &owner.policy().journal,
        VolumeOperation::Write, work)?;
    Ok(NativeOwned { value: grant.0, _custody: grant.1 })
}

fn pi_input_coordinates(owner: &PiOwnerConfig, operation: OperationId, label: &str,
    directory: &'static str, purpose: &'static str) -> Result<(String, String, IdempotencyKey)> {
    if label.is_empty() || label.len() > 64
        || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')) {
        return Err(Error::Invalid("Pi input label must be one bounded path component".into()));
    }
    let display_name = format!("{label}.json");
    let path = format!("{directory}/{operation}/{display_name}");
    if path.len() > owner.policy().limits.path_bytes {
        return Err(Error::Invalid("Pi input path exceeds its original owner limit".into()));
    }
    let key = IdempotencyKey::new(format!("acyclic.pi/{purpose}/{}/{operation}/{label}", owner.policy().root_task))?;
    Ok((path, display_name, key))
}

fn validate_pi_input(owner: &PiOwnerConfig, file: &FileRef, path: &str, display_name: &str) -> Result<()> {
    validate_pi_input_media(owner, file, path, display_name, "application/json")
}

fn validate_pi_input_media(
    owner: &PiOwnerConfig, file: &FileRef, path: &str, display_name: &str, media_type: &str,
) -> Result<()> {
    owner.policy().limits.validate_file(file)?;
    if file.volume() != &owner.policy().journal || file.path() != path || file.display_name() != display_name
        || file.descriptor().media_type() != media_type
        || file.descriptor().byte_length() > owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES) {
        return Err(Error::Invalid("Pi original input operation returned another immutable file".into()));
    }
    Ok(())
}

/// Trusted original-owner bootstrap only; this grants no task journal writer.
/// Changed retry bytes are rejected by the existing canonical upload engine.
#[allow(clippy::too_many_arguments, reason = "original owner, operation and bounded canonical producer")]
pub(crate) async fn stage_pi_input<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<FileRef>> {
    stage_pi_input_namespace(owner, filesystem, operation, label, value, budget, cancellation,
        ROOT_INPUT_DIRECTORY, "input").await
}

/// Same original upload key and protected bootstrap grant as stage_pi_input,
/// with the caller's original native ledger owning both codec and publication.
pub(crate) async fn stage_pi_input_with_native_work<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    stage_pi_input_namespace_with_native_work(owner, filesystem, operation, label, value, work,
        ROOT_INPUT_DIRECTORY, "input").await
}

pub(crate) async fn stage_pi_operator_input_with_native_work<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    stage_pi_input_namespace_with_native_work(owner, filesystem, operation, label, value, work,
        OPERATOR_INPUT_DIRECTORY, "operator-input").await
}

/// Readonly original operation metadata, never a mutable path or new upload.
pub(crate) async fn observe_pi_input_with_native_work(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let coordinates = pi_input_coordinates_with_native_work(
        owner, operation, label, ROOT_INPUT_DIRECTORY, "input", work)?;
    let grant = pi_input_write_grant_with_native_work(owner, work)?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let result = filesystem.content_for_operation_with_native_work(
        &owner.policy().journal, &grant.value, &coordinates.key.value, &coordinates.path.value, maximum, work).await;
    let result = match result {
        Ok(Some((reference, custody))) => {
            if let Err(error) = validate_pi_input(owner, &reference, &coordinates.path.value, &coordinates.display_name.value) {
                drop(reference);
                drop(custody);
                Err(error)
            } else { Ok(Some((reference, custody))) }
        }
        result => result,
    };
    drop(coordinates);
    result
}

#[allow(clippy::too_many_arguments, reason = "fixed trusted namespace shares the original publisher engine")]
async fn stage_pi_input_namespace_with_native_work<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    work: &NativeMaterializationWork, directory: &'static str, purpose: &'static str,
) -> Result<(FileRef, NativeAllocation)> {
    let coordinates = pi_input_coordinates_with_native_work(
        owner, operation, label, directory, purpose, work)?;
    publish_pi_input_with_native_coordinates(owner, filesystem, value, "application/json", coordinates, work).await
}

/// Fixed original root artifact coordinates, including the original schema
/// media type; this is not an operation/label input alias or a new upload key.
pub(crate) async fn stage_pi_root_input_with_native_work<A, O, T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, name: &str, value: &T,
    media_type: &str, work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let expected_media = root_input_media(name)?;
    if media_type != expected_media {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "original Pi root artifact media type differs" });
    }
    let coordinates = root_input_coordinates_with_native_work(owner, name, work)?;
    publish_pi_input_with_native_coordinates(owner, filesystem, value, media_type, coordinates, work).await
}

fn root_input_media(name: &str) -> Result<&'static str> {
    match name {
        "root" | "preparation" => Ok("application/json"),
        "allocation-schema" => Ok("application/schema+json"),
        _ => Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "unknown original Pi root artifact" }),
    }
}

fn root_input_coordinates_with_native_work(
    owner: &PiOwnerConfig, name: &str, work: &NativeMaterializationWork,
) -> Result<NativeInputCoordinates> {
    root_input_media(name)?;
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let (display_name, custody) = work.display(&format_args!("{name}.json")).map_err(native_error)?;
    let display_name = NativeOwned { value: display_name, _custody: custody };
    let (path, custody) = work.display(&format_args!("{ROOT_INPUT_DIRECTORY}/{}", display_name.value))
        .map_err(native_error)?;
    let path = NativeOwned { value: path, _custody: custody };
    if path.value.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "original Pi root artifact path exceeds owner limit" });
    }
    let (key, custody) = work.display(&format_args!(
        "acyclic.pi/{name}-input/{}", owner.policy().root_task)).map_err(native_error)?;
    let key = IdempotencyKey::new(key)?;
    let key = NativeOwned { value: key, _custody: custody };
    Ok(NativeInputCoordinates { path, display_name, key })
}

/// Same fixed canonical root upload operation as staging, without creating or
/// trusting the current file at its path. Returned metadata keeps native custody.
pub(crate) async fn observe_pi_root_input_with_native_work<A, O>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, name: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let media_type = root_input_media(name)?;
    let coordinates = root_input_coordinates_with_native_work(owner, name, work)?;
    let grant = pi_input_write_grant_with_native_work(owner, work)?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let result = filesystem.content_for_operation_with_native_work(
        &owner.policy().journal, &grant.value, &coordinates.key.value, &coordinates.path.value, maximum, work).await;
    match result {
        Ok(Some((reference, custody))) => {
            if let Err(error) = validate_pi_input_media(owner, &reference, &coordinates.path.value,
                &coordinates.display_name.value, media_type) {
                drop(reference);
                drop(custody);
                Err(error)
            } else {
                Ok(Some((reference, custody)))
            }
        }
        result => result,
    }
}

async fn publish_pi_input_with_native_coordinates<A, O, T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, value: &T, media_type: &str,
    coordinates: NativeInputCoordinates, work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let grant = pi_input_write_grant_with_native_work(owner, work)?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let (bytes, bytes_custody) = guarded_pi_bytes(value, maximum, work)?;
    let encoded = NativeOwned { value: bytes, _custody: bytes_custody };
    let length = encoded.value.len() as u64;
    let result = filesystem.put_content_with_native_work(&owner.policy().journal, &grant.value, &coordinates.path.value,
        &encoded.value, media_type, &coordinates.display_name.value, maximum, &coordinates.key.value, work).await;
    drop(encoded);
    let result = match result {
        Ok((reference, custody)) => {
            let validation = validate_pi_input_media(owner, &reference, &coordinates.path.value,
                &coordinates.display_name.value, media_type)
                .and_then(|()| if reference.descriptor().byte_length() == length { Ok(()) }
                    else { Err(Error::NativeBoundary { code: ErrorCode::Storage, reason: "Pi input original body length differs" }) });
            match validation {
                Ok(()) => Ok((reference, custody)),
                Err(error) => { drop(reference); drop(custody); Err(error) }
            }
        }
        result => result,
    };
    drop(coordinates);
    result
}

// Declaration order destroys every owned payload before its allocation guard,
// including an aborted async publication and any early validation/codec error.
struct NativeOwned<T> {
    value: T,
    _custody: NativeAllocation,
}

async fn read_root_artifact_with_native_work<T: RootArtifact>(
    owner: &PiOwnerConfig, context: &TaskContext, reference: &FileRef,
    work: &NativeMaterializationWork,
) -> Result<NativeOwned<T>> {
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    if reference.descriptor().byte_length() > maximum || reference.path().len() > owner.policy().limits.path_bytes
        || !matches!(reference.descriptor().media_type(), "application/json" | "application/schema+json") {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
            reason: "root artifact exceeds original header bounds or media type" });
    }
    let bytes = context.read_file_with_native_work(reference, work).await?;
    // Fixed closed typed envelope plus the SDK-issued selected source shape;
    // known nested String/Vec/map logical ownership is dominated by that SDK
    // inventory, not a Pi bytes-to-heap multiplier or claimed physical malloc.
    let maximum_owned = std::mem::size_of::<T>() as u64;
    let (value, custody) = guarded_pi_decode(bytes.as_ref(), maximum, maximum_owned,
        T::VALUE_SCHEMA, ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES, work)?;
    let decoded = NativeOwned { value, _custody: custody };
    let (value, custody) = guarded_pi_bytes(&decoded.value, maximum, work)?;
    let encoded = NativeOwned { value, _custody: custody };
    if encoded.value.as_slice() != bytes.as_ref() {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
            reason: "root artifact is not its immutable first-party encoding" });
    }
    Ok(decoded)
}

pub(crate) async fn load_original_preparation_with_native_work(
    owner: &PiOwnerConfig, filesystem: &Host, expected: &PiRootRequest,
    context: Arc<acyclic_harness::NativeTaskContext>, work: &NativeMaterializationWork,
) -> Result<PiRootPreparation> {
    let missing = || Error::NativeBoundary { code: ErrorCode::Unauthorized,
        reason: "original root preparation upload operation is absent" };
    let (value, custody) = observe_pi_root_input_with_native_work(owner, filesystem, "preparation", work).await?
        .ok_or_else(missing)?;
    let mut payload = NativeOwned { value, _custody: custody };
    let plan = read_root_artifact_with_native_work::<acyclic_harness::effect_host::TaskEffectPlan>(
        owner, context.context(), &payload.value, work).await?;
    let (value, custody) = observe_pi_root_input_with_native_work(owner, filesystem, "root", work).await?
        .ok_or_else(missing)?;
    let root = NativeOwned { value, _custody: custody };
    let (value, custody) = observe_pi_root_input_with_native_work(owner, filesystem, "allocation-schema", work).await?
        .ok_or_else(missing)?;
    let schema_reference = NativeOwned { value, _custody: custody };
    if plan.value.provider != ROOT_ALLOCATION_PROVIDER || plan.value.effect_kind != ROOT_ALLOCATION_EFFECT
        || plan.value.guarantee != EffectGuarantee::AtMostOnce
        || plan.value.request != root.value || plan.value.result_schema != schema_reference.value {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root plan differs from canonical input operations" });
    }
    let request = read_root_artifact_with_native_work::<PiRootRequest>(owner, context.context(), &root.value, work).await?;
    if &request.value != expected {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "root request differs from original canonical input" });
    }
    let schema = read_root_artifact_with_native_work::<serde_json::Value>(
        owner, context.context(), &schema_reference.value, work).await?;
    let (value, custody) = PiRootAllocation::schema_with_native_work(&request.value.binding, work)?;
    let expected_schema = NativeOwned { value, _custody: custody };
    if schema.value != expected_schema.value {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root allocation schema differs" });
    }
    let (value, custody) = work.display(acyclic_harness::filesystem::EFFECT_TASK_COMMAND_KIND)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut kind = NativeOwned { value, _custody: custody };
    payload._custody.absorb(&mut kind._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(PiRootPreparation::from_original_parts(acyclic_harness::workflow::WorkflowCommand {
        operation_id: super::pi_root_inputs::preparation_operation_with_native_work(owner.policy().root_task, work)?,
        kind: kind.value, payload: payload.value,
    }, plan.value, request.value,
        [Some(payload._custody), Some(plan._custody), Some(request._custody), None], context))
}

struct NativeInputCoordinates {
    path: NativeOwned<String>,
    display_name: NativeOwned<String>,
    key: NativeOwned<IdempotencyKey>,
}

fn pi_input_coordinates_with_native_work(
    owner: &PiOwnerConfig, operation: OperationId, label: &str,
    directory: &'static str, purpose: &'static str, work: &NativeMaterializationWork,
) -> Result<NativeInputCoordinates> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    work.cancellation().check().map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
    if label.is_empty() || label.len() > 64
        || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')) {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "Pi input label must be one bounded path component" });
    }
    let (display_name, custody) = work.display(&format_args!("{label}.json")).map_err(native_error)?;
    let display_name = NativeOwned { value: display_name, _custody: custody };
    let (path, custody) = work.display(&format_args!("{directory}/{operation}/{}", display_name.value))
        .map_err(native_error)?;
    let path = NativeOwned { value: path, _custody: custody };
    if path.value.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "Pi input path exceeds its original owner limit" });
    }
    let (key, custody) = work.display(&format_args!(
        "acyclic.pi/{purpose}/{}/{operation}/{label}", owner.policy().root_task)).map_err(native_error)?;
    let key = IdempotencyKey::new(key)?;
    let key = NativeOwned { value: key, _custody: custody };
    Ok(NativeInputCoordinates { path, display_name, key })
}

pub(crate) fn guarded_pi_bytes<T: Serialize>(
    value: &T, maximum_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let remaining = work.remaining().map_err(native_error)?;
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    let mut diagnostic = work.retain_owned(sdk_json_diagnostic_bytes()).map_err(native_error)?;
    let bounds = JsonBounds { maximum_bytes, maximum_items: remaining.items_examined, maximum_depth: 64 };
    let result = json_bytes_bounded_native_trusted(value, bounds, ErrorCode::Invalid,
        &mut |charge| native_buffer_codec_charge(work, &mut custody, charge));
    match result {
        Ok(bytes) => Ok((bytes, custody)),
        Err(Error::NativeEncode { code, source, custody: None }) => {
            if custody.bytes() != 0 { diagnostic.absorb(&mut custody).map_err(native_error)?; }
            Err(Error::NativeEncode { code, source, custody: Some(diagnostic) })
        },
        Err(error @ Error::NativeWork { custody: None, .. }) if custody.bytes() == 0 => {
            drop(custody);
            Err(error)
        }
        Err(Error::NativeWork { cause, custody: None }) =>
            Err(Error::NativeWork { cause, custody: Some(custody) }),
        Err(error) => Err(error),
    }
}

pub(crate) fn native_buffer_codec_charge(
    work: &NativeMaterializationWork, custody: &mut NativeAllocation, charge: JsonCodecCharge,
) -> Result<()> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    if let JsonCodecCharge::Release { bytes } = charge {
        return custody.release(bytes).map_err(native_error);
    }
    work.cancellation().check().map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
    match charge {
        JsonCodecCharge::Work { bytes, items } => work.charge(WorkCounters {
            bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
        }).map_err(native_error),
        JsonCodecCharge::CopyAdmission { bytes } => work.admit(WorkCounters {
            bytes_copied: bytes, ..WorkCounters::default()
        }).map_err(native_error),
        JsonCodecCharge::Copy { bytes } => work.charge(WorkCounters {
            bytes_copied: bytes, ..WorkCounters::default()
        }).map_err(native_error),
        JsonCodecCharge::Read { bytes } => work.charge(WorkCounters {
            source_bytes_read: bytes, ..WorkCounters::default()
        }).map_err(native_error),
        JsonCodecCharge::Inspect { items } => work.charge(WorkCounters {
            items_examined: items, ..WorkCounters::UNCHARGED
        }).map_err(native_error),
        JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
            allocation_operations: 1, ..WorkCounters::default()
        }).map_err(native_error),
        JsonCodecCharge::Admission { maximum_owned_bytes, .. } =>
            custody.resize_retained(maximum_owned_bytes).map_err(native_error),
        JsonCodecCharge::Retain { bytes } => {
            let retained = custody.bytes().checked_add(bytes)
                .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
            custody.resize_retained(retained).map_err(native_error)
        }
        JsonCodecCharge::Release { .. } => unreachable!(),
    }
}

/// Signed operator scopes are root-only input, never in the directory inherited
/// by recursive children. The canonical engine and protected writer are shared.
#[allow(clippy::too_many_arguments, reason = "original owner, operation and bounded canonical producer")]
pub(crate) async fn stage_pi_operator_input<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<FileRef>> {
    stage_pi_input_namespace(owner, filesystem, operation, label, value, budget, cancellation,
        OPERATOR_INPUT_DIRECTORY, "operator-input").await
}

#[allow(clippy::too_many_arguments, reason = "two fixed trusted namespaces share the original measured publisher")]
async fn stage_pi_input_namespace<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    budget: WorkBudget, cancellation: &CancellationToken, directory: &'static str, purpose: &'static str,
) -> PiMeasuredResult<OperationReceipt<FileRef>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() { return Err(work.failure("Pi input requires finite work allowances")); }
    cancellation.check().map_err(|error| work.failure(error))?;
    let (path, display_name, key) = pi_input_coordinates(owner, operation, label, directory, purpose).map_err(|error| work.failure(error))?;
    let (_, grant) = pi_input_grants(owner).map_err(|error| work.failure(error))?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let bytes = work.encode_cancellable(value, maximum, Some(cancellation))?;
    let result = work.accept(filesystem.put_content_measured(&owner.policy().journal, &grant, &path,
        &bytes, "application/json", &display_name, maximum, &key, work.remaining()?, cancellation).await);
    let length = bytes.len() as u64;
    drop(bytes);
    work.codec_charge(JsonCodecCharge::Release { bytes: length }).map_err(|error| work.failure(error))?;
    let reference = result?;
    validate_pi_input(owner, &reference, &path, &display_name).map_err(|error| work.failure(error))?;
    if reference.descriptor().byte_length() != length { return Err(work.failure("Pi input original body length differs")); }
    Ok(OperationReceipt { value: reference, work: work.spent })
}

/// Reads original upload metadata plus its immutable body, never the current
/// file at that path. Absence is not authority to stage or allocate anything.
pub(crate) async fn observe_pi_input(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<Option<FileRef>>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() { return Err(work.failure("Pi input requires finite work allowances")); }
    cancellation.check().map_err(|error| work.failure(error))?;
    let value = original_pi_input_body(owner, filesystem, operation, label, &mut work, cancellation).await?
        .map(|(reference, bytes)| { drop(bytes); reference });
    Ok(OperationReceipt { value, work: work.spent })
}

async fn original_pi_input_body(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    work: &mut Work, cancellation: &CancellationToken,
) -> PiMeasuredResult<Option<(FileRef, Vec<u8>)>> {
    let (path, display_name, key) = pi_input_coordinates(owner, operation, label, ROOT_INPUT_DIRECTORY, "input").map_err(|error| work.failure(error))?;
    let (read, write) = pi_input_grants(owner).map_err(|error| work.failure(error))?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let reference = work.accept(filesystem.content_for_operation_measured(&owner.policy().journal, &write,
        &key, &path, maximum, work.remaining()?, cancellation).await)?;
    let Some(reference) = reference else { return Ok(None); };
    validate_pi_input(owner, &reference, &path, &display_name).map_err(|error| work.failure(error))?;
    let bytes = work.accept(filesystem.read_content_measured(&reference, &read, maximum, work.remaining()?, cancellation).await)?;
    Ok(Some((reference, bytes)))
}

/// Reads and decodes the exact original upload operation, never its current
/// path. Raw producer bytes remain in custody throughout the trusted decode.
pub(crate) async fn read_pi_input<T: DeserializeOwned>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    maximum_owned_bytes: u64, budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<Option<T>>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() || maximum_owned_bytes == u64::MAX {
        return Err(work.failure("Pi input decoding requires finite work and audited ownership allowances"));
    }
    cancellation.check().map_err(|error| work.failure(error))?;
    let Some((_reference, bytes)) = original_pi_input_body(owner, filesystem, operation, label,
        &mut work, cancellation).await? else {
        return Ok(OperationReceipt { value: None, work: work.spent });
    };
    let length = bytes.len() as u64;
    // Adopt the existing producer allocation without charging it a second time.
    work.encoded_live = length;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let result = (|| {
        let remaining = work.remaining()?;
        let bounds = JsonBounds { maximum_bytes: maximum, maximum_items: remaining.items_examined, maximum_depth: 64 };
        json_from_slice_bounded_trusted(&bytes, bounds, maximum_owned_bytes,
            &mut |charge| work.cancellable_codec_charge(charge, Some(cancellation)))
            .map_err(|error| work.failure(error))
    })();
    drop(bytes);
    work.codec_charge(JsonCodecCharge::Release { bytes: length }).map_err(|error| work.failure(error))?;
    Ok(OperationReceipt { value: Some(result?), work: work.spent })
}

/// Reads only the original immutable upload. Body access remains the actual
/// caller context's authenticated directory READ, never bootstrap journal WRITE.
/// Both the raw body and its source FileRef retain their producer guards until
/// the original trusted decoder has finished; only the typed DTO survives.
#[allow(clippy::too_many_arguments, reason = "original immutable operation plus actual scoped content reader")]
pub(crate) async fn read_pi_input_with_native_work<T: DeserializeOwned>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    maximum_owned_bytes: u64, value_schema: JsonValueSchema, schema_label_bytes: u64,
    context: &TaskContext, work: &NativeMaterializationWork,
) -> Result<Option<(T, NativeAllocation)>> {
    let Some((reference, custody)) = observe_pi_input_with_native_work(
        owner, filesystem, operation, label, work).await? else {
        return Ok(None);
    };
    let reference = NativeOwned { value: reference, _custody: custody };
    let bytes = context.read_file_with_native_work(&reference.value, work).await?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let decoded = guarded_pi_decode_with_code(bytes.as_ref(), maximum, maximum_owned_bytes,
        ErrorCode::Storage, value_schema, schema_label_bytes, work)?;
    drop(bytes);
    drop(reference);
    Ok(Some(decoded))
}

/// Finite trusted codec boundary for audited Pi input/cursor DTOs. Returned
/// bytes retain their actual logical output custody in the original receipt.
pub(crate) fn encode_pi_value<T: Serialize>(
    value: &T, maximum_bytes: u64, budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<Vec<u8>>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() || maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(work.failure("Pi encoding requires finite positive byte and work allowances"));
    }
    cancellation.check().map_err(|error| work.failure(error))?;
    let value = work.encode_cancellable(value, maximum_bytes, Some(cancellation))?;
    Ok(OperationReceipt { value, work: work.spent })
}

/// Exact original canonical identity bytes, distinct from ordinary DTO
/// serialization order. Returned Vec custody remains in the measured receipt.
pub(crate) fn canonical_pi_value<T: Serialize>(
    value: &T, maximum_bytes: u64, budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<Vec<u8>>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() || maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(work.failure("Pi canonical encoding requires finite positive byte and work allowances"));
    }
    cancellation.check().map_err(|error| work.failure(error))?;
    let value = work.encode_canonical_cancellable(value, maximum_bytes, cancellation)?;
    Ok(OperationReceipt { value, work: work.spent })
}

/// Original SDK canonical bytes, with each actual producer callback using the
/// caller's existing native ledger and cancellation rather than a fresh meter.
pub(crate) fn canonical_pi_value_with_native_work<T: Serialize + ?Sized>(
    value: &T, maximum_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let remaining = work.remaining().map_err(native_error)?;
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    let mut diagnostic = work.retain_owned(sdk_json_diagnostic_bytes()).map_err(native_error)?;
    let mut parser_quota: Option<NativeAllocation> = None;
    let bounds = JsonBounds { maximum_bytes, maximum_items: remaining.items_examined, maximum_depth: 64 };
    let result = canonical_json_bytes_bounded_native_trusted(value, bounds, ErrorCode::Invalid, &mut |charge| {
        if !matches!(charge, JsonCodecCharge::Release { .. }) {
            work.cancellation().check().map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        }
        match charge {
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                let mut quota = work.retain_owned(0).map_err(native_error)?;
                let parser_owned = maximum_owned_bytes.checked_sub(sdk_json_diagnostic_bytes())
                    .ok_or(Error::NativeBoundary { code: ErrorCode::Invalid,
                        reason: "SDK raw parser quota omits its diagnostic inventory" })?;
                quota.resize_retained(parser_owned).map_err(native_error)?;
                parser_quota = Some(quota);
                Ok(())
            }
            JsonCodecCharge::Retain { bytes } if parser_quota.is_some() => {
                let observed = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                let mut quota = parser_quota.take().unwrap();
                custody.absorb(&mut quota).map_err(native_error)?;
                custody.substitute(observed).map_err(native_error)
            }
            charge => native_buffer_codec_charge(work, &mut custody, charge),
        }
    });
    match result {
        Ok(bytes) => {
            drop(parser_quota);
            drop(diagnostic);
            Ok((bytes, custody))
        },
        Err(Error::NativeEncode { code, source, custody: None }) => {
            if let Some(mut quota) = parser_quota { diagnostic.absorb(&mut quota).map_err(native_error)?; }
            if custody.bytes() != 0 { diagnostic.absorb(&mut custody).map_err(native_error)?; }
            Err(Error::NativeEncode { code, source, custody: Some(diagnostic) })
        },
        Err(error @ Error::NativeWork { custody: None, .. }) if custody.bytes() == 0 => {
            drop(custody);
            Err(error)
        }
        Err(Error::NativeWork { cause, custody: None }) =>
            Err(Error::NativeWork { cause, custody: Some(custody) }),
        Err(error) => Err(error),
    }
}

/// The exact existing BLAKE3 identity over those canonical bytes.
pub(crate) fn digest_pi_value_with_native_work<T: Serialize + ?Sized>(
    value: &T, work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let maximum = work.remaining().map_err(native_error)?.bytes_encoded;
    let (bytes, custody) = canonical_pi_value_with_native_work(value, maximum, work)?;
    let bytes = NativeOwned { value: bytes, _custody: custody };
    work.charge(WorkCounters { bytes_hashed: bytes.value.len() as u64, ..WorkCounters::default() })
        .map_err(native_error)?;
    let digest = blake3::hash(&bytes.value);
    drop(bytes);
    work.display(&format_args!("{}", digest.to_hex())).map_err(native_error)
}

/// The caller supplies its audited typed-deserializer ownership allowance.
/// Admission is conservative logical custody, never invented actual allocation
/// work; arbitrary custom deserializers are not an untrusted plugin boundary.
pub(crate) fn decode_pi_value<T: DeserializeOwned>(
    bytes: &[u8], maximum_bytes: u64, maximum_owned_bytes: u64,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<T>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() || maximum_bytes == 0 || maximum_bytes == u64::MAX
        || maximum_owned_bytes == u64::MAX {
        return Err(work.failure("Pi decoding requires finite byte, ownership and work allowances"));
    }
    cancellation.check().map_err(|error| work.failure(error))?;
    let remaining = work.remaining()?;
    let bounds = JsonBounds { maximum_bytes, maximum_items: remaining.items_examined, maximum_depth: 64 };
    let value = json_from_slice_bounded_trusted(bytes, bounds, maximum_owned_bytes,
        &mut |charge| work.cancellable_codec_charge(charge, Some(cancellation)))
        .map_err(|error| work.failure(error))?;
    Ok(OperationReceipt { value, work: work.spent })
}

/// Only audited first-party Deserialize implementations may use this boundary.
/// The caller's closed-DTO witness reserves logical ownership before serde
/// parses; it is not a generic physical-heap or arbitrary-deserializer proof.
/// Keep the input's original native guard alive throughout this call and retain
/// the returned guard through the decoded DTO's last consumer.
pub(crate) fn guarded_pi_decode<T: DeserializeOwned>(
    bytes: &[u8], maximum_bytes: u64, maximum_owned_bytes: u64,
    value_schema: JsonValueSchema, schema_label_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    guarded_pi_decode_with_code(bytes, maximum_bytes, maximum_owned_bytes, ErrorCode::Invalid,
        value_schema, schema_label_bytes, work)
}

pub(crate) fn guarded_pi_decode_with_code<T: DeserializeOwned>(
    bytes: &[u8], maximum_bytes: u64, maximum_owned_bytes: u64, code: ErrorCode,
    value_schema: JsonValueSchema, schema_label_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let remaining = work.remaining().map_err(native_error)?;
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    let mut retained_owned = maximum_owned_bytes;
    let bounds = JsonBounds {
        maximum_bytes,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = json_from_slice_bounded_native_trusted(bytes, bounds, maximum_owned_bytes, code, value_schema, &mut |charge| {
        if let JsonCodecCharge::Release { bytes } = charge {
            return custody.release(bytes).map_err(native_error);
        }
        work.cancellation().check()
            .map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        match charge {
            JsonCodecCharge::Work { bytes, items } => work.charge(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::CopyAdmission { bytes } => work.admit(WorkCounters {
                bytes_copied: bytes, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Copy { bytes } => work.charge(WorkCounters {
                bytes_copied: bytes, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Read { bytes } => work.charge(WorkCounters {
                source_bytes_read: bytes, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Inspect { items } => work.charge(WorkCounters {
                items_examined: items, ..WorkCounters::UNCHARGED
            }).map_err(native_error),
            JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                let diagnostic = json_typed_diagnostic_owned_bytes(value_shape, schema_label_bytes, Some(code))?;
                retained_owned = maximum_owned_bytes.checked_add(json_value_decode_owned_bytes(value_shape)?)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                let admitted = retained_owned.checked_add(diagnostic)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                custody.resize_retained(admitted).map_err(native_error)
            }
            JsonCodecCharge::Retain { bytes } => {
                let retained = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                custody.resize_retained(retained).map_err(native_error)
            }
            JsonCodecCharge::Release { .. } => unreachable!(),
        }
    });
    match result {
        Ok(value) => {
            // The SDK drops its decoder/scratch before returning the DTO.
            // Only the caller's opaque closed-DTO ownership witness survives.
            custody.substitute(retained_owned).map_err(native_error)?;
            Ok((value, custody))
        }
        Err(Error::NativeDecode { code, source, custody: None }) => {
            // The SDK's original serde diagnostic remains opaque-owned on its
            // preparse quota until the final native error consumer drops it.
            // Never stringify, copy, or release this surviving error payload.
            Err(Error::NativeDecode { code, source, custody: Some(custody) })
        }
        Err(error) => {
            // The SDK has already destroyed its partial DTO and parser here.
            custody.substitute(0).map_err(native_error)?;
            drop(custody);
            Err(error)
        }
    }
}

/// Produces the SDK encoder's actual Value on the caller's original native
/// ledger. The returned guard owns its logical payload/slots, not allocator-node
/// or RSS overhead, and must outlive the Value's final consumer. No local meter,
/// cancellation token, serde-to-value pass, or retrospective allocation is used.
pub(crate) fn guarded_pi_value<T: Serialize>(
    value: &T, maximum_owned_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let remaining = work.remaining().map_err(native_error)?;
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    // Closed first-party serializers use the SDK's actual static encode/raw
    // diagnostic inventory. Opaque custom Serialize diagnostics are outside
    // this trusted helper's contract and require their own audited admission.
    let mut diagnostic = work.retain_owned(sdk_json_diagnostic_bytes()).map_err(native_error)?;
    // RawValue's existing codec branch admits opaque parser/schema scratch.
    // This reservation is not a reported physical allocation; actual Value
    // payload growth is owned separately through Retain callbacks below.
    let mut parser_quota: Option<NativeAllocation> = None;
    let bounds = JsonBounds {
        maximum_bytes: remaining.bytes_encoded,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = json_value_bounded_native_trusted(value, bounds, ErrorCode::Invalid, &mut |charge| {
        // Release follows actual buffer destruction and is unconditional even
        // when cancellation or a work limit interrupted the producer.
        if let JsonCodecCharge::Release { bytes } = charge {
            return custody.release(bytes).map_err(native_error);
        }
        work.cancellation().check()
            .map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        match charge {
            JsonCodecCharge::Work { bytes, items } => work.charge(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::CopyAdmission { bytes } => work.admit(WorkCounters {
                bytes_copied: bytes, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Copy { bytes } => work.charge(WorkCounters {
                bytes_copied: bytes, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Read { bytes } => work.charge(WorkCounters {
                source_bytes_read: bytes, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Inspect { items } => work.charge(WorkCounters {
                items_examined: items, ..WorkCounters::UNCHARGED
            }).map_err(native_error),
            JsonCodecCharge::AllocationOperation { .. } => work.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }).map_err(native_error),
            JsonCodecCharge::Retain { bytes } => {
                let observed = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                if observed > maximum_owned_bytes {
                    return Err(native_error(NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                        counter: "pi_value_owned_bytes", observed, maximum: maximum_owned_bytes,
                    })));
                }
                if let Some(mut quota) = parser_quota.take() {
                    // This is the already parsed RawValue's observed ownership,
                    // not an observed allocator attempt. Transfer its admitted
                    // opaque quota atomically on the same original ledger,
                    // without an overlapping second claim or a live-byte gap.
                    custody.absorb(&mut quota).map_err(native_error)?;
                    custody.substitute(observed).map_err(native_error)?;
                    Ok(())
                } else {
                    // Retain is logical custody, not an allocator observation.
                    // The SDK reports genuine owned String/Vec attempts through
                    // its separate AllocationOperation callback.
                    custody.resize_retained(observed).map_err(native_error)
                }
            }
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                // The next Retain is the original parser's observed Value.
                // No actual allocator/node work is inferred from its quota.
                let mut quota = work.retain_owned(0).map_err(native_error)?;
                let parser_owned = maximum_owned_bytes.checked_sub(sdk_json_diagnostic_bytes())
                    .ok_or(Error::NativeBoundary { code: ErrorCode::Invalid,
                        reason: "SDK raw parser quota omits its diagnostic inventory" })?;
                quota.resize_retained(parser_owned).map_err(native_error)?;
                parser_quota = Some(quota);
                Ok(())
            }
            JsonCodecCharge::Release { .. } => unreachable!(),
        }
    });
    match result {
        Ok(value) => {
            drop(parser_quota);
            drop(diagnostic);
            Ok((value, custody))
        },
        Err(Error::NativeEncode { code, source, custody: None }) => {
            if let Some(mut quota) = parser_quota {
                diagnostic.absorb(&mut quota).map_err(native_error)?;
            }
            if custody.bytes() != 0 {
                diagnostic.absorb(&mut custody).map_err(native_error)?;
            }
            Err(Error::NativeEncode { code, source, custody: Some(diagnostic) })
        },
        Err(error @ Error::NativeWork { custody: None, .. }) if custody.bytes() == 0 => {
            drop(custody);
            Err(error)
        }
        Err(Error::NativeWork { cause, custody: None }) =>
            Err(Error::NativeWork { cause, custody: Some(custody) }),
        Err(error) => Err(error),
    }
}

/// The existing canonical BLAKE3 identity, with actual public-codec work and
/// hash bytes. This does not substitute SHA256 or ordinary struct-field order.
pub(crate) fn digest_pi_value<T: Serialize>(
    value: &T, budget: WorkBudget, cancellation: &CancellationToken,
) -> PiMeasuredResult<OperationReceipt<String>> {
    let mut work = Work::new(budget);
    if !budget.has_finite_allowances() { return Err(work.failure("Pi digest requires finite work allowances")); }
    cancellation.check().map_err(|error| work.failure(error))?;
    let remaining = work.remaining()?;
    let bytes = work.encode_canonical_cancellable(value, remaining.bytes_encoded, cancellation)?;
    let length = bytes.len() as u64;
    let result: PiMeasuredResult<String> = (|| {
        cancellation.check().map_err(|error| work.failure(error))?;
        work.charge(WorkCounters { bytes_hashed: length, ..WorkCounters::default() })?;
        let digest = blake3::hash(&bytes);
        work.cancellable_codec_charge(JsonCodecCharge::Retain { bytes: 64 }, Some(cancellation))
            .map_err(|error| work.failure(error))?;
        Ok(hex::encode(digest.as_bytes()))
    })();
    drop(bytes);
    work.codec_charge(JsonCodecCharge::Release { bytes: length }).map_err(|error| work.failure(error))?;
    Ok(OperationReceipt { value: result?, work: work.spent })
}

struct Work {
    spent: WorkCounters,
    budget: WorkBudget,
    encoded_live: u64,
    decode_admission: u64,
}

impl Work {
    fn new(budget: WorkBudget) -> Self {
        Self { spent: WorkCounters::default(), budget, encoded_live: 0, decode_admission: 0 }
    }
    fn failure(&self, error: impl std::fmt::Display) -> (Error, WorkCounters) {
        (Error::Storage(error.to_string()), self.spent)
    }
    fn remaining(&self) -> PiMeasuredResult<WorkBudget> {
        let mut remaining = self.spent.remaining(self.budget).map_err(|error| self.failure(error))?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
            .checked_sub(self.encoded_live).and_then(|bytes| bytes.checked_sub(self.decode_admission))
            .ok_or_else(|| self.failure("Pi JSON custody exceeds original allocation allowance"))?;
        Ok(remaining)
    }
    fn charge(&mut self, delta: WorkCounters) -> PiMeasuredResult<()> {
        let next = self.spent.checked_add(delta).map_err(|error| self.failure(error))?;
        next.verify_within(&self.budget).map_err(|error| self.failure(error))?;
        self.spent = next;
        Ok(())
    }
    fn accept<T, E: std::fmt::Display>(&mut self, result: FilesystemMeasuredResult<OperationReceipt<T>, E>) -> PiMeasuredResult<T> {
        match result {
            Ok(mut receipt) => {
                receipt.work.peak_allocation_bytes = receipt.work.peak_allocation_bytes
                    .checked_add(self.encoded_live).ok_or_else(|| self.failure("Pi producer custody overflow"))?;
                self.charge(receipt.work)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                let mut actual = *failure.work;
                actual.peak_allocation_bytes = actual.peak_allocation_bytes.checked_add(self.encoded_live)
                    .ok_or_else(|| self.failure("Pi failed producer custody overflow"))?;
                self.spent = self.spent.checked_add(actual).map_err(|error| self.failure(error))?;
                Err(self.failure(failure.error))
            }
        }
    }
    fn accept_inline<T>(&mut self, result: PiMeasuredResult<OperationReceipt<T>>) -> PiMeasuredResult<T> {
        match result {
            Ok(mut receipt) => {
                receipt.work.peak_allocation_bytes = receipt.work.peak_allocation_bytes
                    .checked_add(self.encoded_live).ok_or_else(|| self.failure("Pi producer custody overflow"))?;
                self.charge(receipt.work)?;
                Ok(receipt.value)
            }
            Err((error, mut actual)) => {
                actual.peak_allocation_bytes = actual.peak_allocation_bytes.checked_add(self.encoded_live)
                    .ok_or_else(|| self.failure("Pi failed producer custody overflow"))?;
                self.spent = self.spent.checked_add(actual).map_err(|error| self.failure(error))?;
                Err((error, self.spent))
            }
        }
    }
    fn codec_charge(&mut self, charge: JsonCodecCharge) -> Result<()> {
        match charge {
            JsonCodecCharge::Work { bytes, items } => self.charge(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
            }).map_err(|(error, _)| error),
            JsonCodecCharge::Read { bytes } => self.charge(WorkCounters {
                source_bytes_read: bytes, ..WorkCounters::default()
            }).map_err(|(error, _)| error),
            JsonCodecCharge::Inspect { items } => self.charge(WorkCounters {
                items_examined: items, ..WorkCounters::UNCHARGED
            }).map_err(|(error, _)| error),
            JsonCodecCharge::CopyAdmission { bytes } =>
                self.spent.admit(&WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }, &self.budget)
                    .map_err(|error| Error::Invalid(error.to_string())),
            JsonCodecCharge::Copy { bytes } => self.charge(WorkCounters {
                bytes_copied: bytes, ..WorkCounters::default()
            }).map_err(|(error, _)| error),
            JsonCodecCharge::AllocationOperation { .. } => self.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }).map_err(|(error, _)| error),
            JsonCodecCharge::Retain { bytes } => {
                let live = self.encoded_live.checked_add(bytes).ok_or_else(|| Error::Invalid("Pi JSON custody overflow".into()))?;
                if live.checked_add(self.decode_admission).is_none_or(|total| total > self.budget.peak_allocation_bytes) {
                    return Err(Error::Invalid("Pi JSON custody exceeds original allocation allowance".into()));
                }
                self.charge(WorkCounters { peak_allocation_bytes: live,
                    ..WorkCounters::default() }).map_err(|(error, _)| error)?;
                self.encoded_live = live;
                Ok(())
            }
            JsonCodecCharge::Release { bytes } => {
                self.encoded_live = self.encoded_live.checked_sub(bytes)
                    .ok_or_else(|| Error::Invalid("Pi JSON custody release differs".into()))?;
                Ok(())
            }
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                // This is a conservative typed-serde admission, not a fabricated
                // actual allocation receipt. Keep it reserved through this call.
                let admitted = self.decode_admission.checked_add(maximum_owned_bytes)
                    .ok_or_else(|| Error::Invalid("Pi typed JSON admission overflow".into()))?;
                if admitted.checked_add(self.encoded_live).is_none_or(|total| total > self.budget.peak_allocation_bytes) {
                    return Err(Error::Invalid("Pi typed JSON exceeds original ownership allowance".into()));
                }
                self.decode_admission = admitted;
                Ok(())
            }
        }
    }
    fn cancellable_codec_charge(&mut self, charge: JsonCodecCharge, cancellation: Option<&CancellationToken>) -> Result<()> {
        // Freed custody must be released even if cancellation interrupted work.
        if !matches!(charge, JsonCodecCharge::Release { .. }) {
            if let Some(cancellation) = cancellation {
                cancellation.check().map_err(|error| Error::Invalid(error.to_string()))?;
            }
        }
        self.codec_charge(charge)
    }

    fn encode(&mut self, value: &impl Serialize, maximum: u64) -> PiMeasuredResult<Vec<u8>> {
        self.encode_cancellable(value, maximum, None)
    }
    fn encode_cancellable(&mut self, value: &impl Serialize, maximum: u64,
        cancellation: Option<&CancellationToken>) -> PiMeasuredResult<Vec<u8>> {
        let remaining = self.remaining()?;
        let bounds = JsonBounds { maximum_bytes: maximum.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined, maximum_depth: 64 };
        json_bytes_bounded_trusted(value, bounds, &mut |charge| self.cancellable_codec_charge(charge, cancellation))
            .map_err(|error| self.failure(error))
    }
    fn encode_canonical_cancellable(&mut self, value: &impl Serialize, maximum: u64,
        cancellation: &CancellationToken) -> PiMeasuredResult<Vec<u8>> {
        let remaining = self.remaining()?;
        let bounds = JsonBounds { maximum_bytes: maximum.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined, maximum_depth: 64 };
        canonical_json_bytes_bounded(value, bounds,
            &mut |charge| self.cancellable_codec_charge(charge, Some(cancellation)))
            .map_err(|error| self.failure(error))
    }
    
}

impl<P: StreamProvider + 'static> PiRootProvider<P> {
    pub(crate) async fn observe_allocation_with_native_work(&self, attempt: EffectAttemptId,
        work: &NativeMaterializationWork) -> Result<Option<((FileRef, PiRootAllocation), NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let _state = self.state.lock().await;
        let original = self.authenticate_with_native_work(attempt, true, work).await?;
        let effect = self.effect_id_with_native_work(work)?;
        let Some(mut reference) = self.original_artifact_with_native_work(effect, attempt, "result", work).await?
            else { return Ok(None); };
        let mut recorded = self.read_with_native_work::<PiRootAllocation>(&reference.value, work).await?;
        let Some(actual) = self.observe_resources_with_native_work(effect, attempt, original.value.request_digest, work).await?
            else { return Ok(None); };
        if !same_allocation(&recorded.value, &actual.value)
            || !self.observe_conversation_with_native_work(effect, attempt, work).await? {
            return Err(Self::native_failure("successful root output differs from original resources or binding"));
        }
        reference._custody.absorb(&mut recorded._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Some(((reference.value, recorded.value), reference._custody)))
    }

    pub(crate) async fn prove_scratch_allocation_with_native_work(&self, attempt: EffectAttemptId,
        work: &NativeMaterializationWork) -> Result<Option<PiRootScratchAllocationProof>> {
        self.check_native_ledger(work)?;
        let _state = self.state.lock().await;
        self.authenticate_with_native_work(attempt, true, work).await?;
        let effect = self.effect_id_with_native_work(work)?;
        let Some(mut output) = self.original_artifact_with_native_work(effect, attempt, "result", work).await?
            else { return Ok(None); };
        let recorded = self.read_with_native_work::<PiRootAllocation>(&output.value, work).await?;
        let binding = &self.preparation.request().binding;
        recorded.value.validate(binding)?;
        if binding.scratch.volume.class() != acyclic_harness::conversation::VolumeClass::AgentPrivate
            || binding.scratch.volume.owner() != &acyclic_harness::conversation::VolumeOwner::Agent(binding.agent_id) {
            return Err(Self::native_failure("root scratch is not its original agent-private output resource"));
        }
        let Some(generation) = self.creation_with_native_work(effect, attempt, "scratch", &binding.scratch.volume,
            VolumeConfig::portable(Lifecycle::Durable), work).await? else { return Ok(None); };
        let mut scratch = self.scratch_creation_with_native_work(&generation, work)?;
        if recorded.value.scratch != scratch.value
            || !self.observe_conversation_with_native_work(effect, attempt, work).await? {
            return Err(Self::native_failure("root scratch output differs from original creation or binding"));
        }
        let operation = self.creation_operation_with_native_work(effect, attempt, "scratch", work)?;
        let (value, custody) = binding.scratch.volume.clone_with_native_work(work)?;
        let mut volume = NativeOwned { value, _custody: custody };
        output._custody.absorb(&mut scratch._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        output._custody.absorb(&mut volume._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Some(PiRootScratchAllocationProof {
            task: binding.task_id, agent: binding.agent_id, volume: volume.value, operation,
            workspace: scratch.value.workspace_id, generation: scratch.value.generation,
            output: output.value, _custody: output._custody,
        }))
    }

    async fn acquire_original_source_with_native_work(&self, key: &str, path: &str,
        admission: SharedRootAdmission, expected: Option<acyclic_fs::NativeRootIdentity>,
        work: &NativeMaterializationWork) -> Result<Arc<SharedPhysicalRoot>> {
        let owner = self.execution_owner()?;
        let pinned = std::sync::Mutex::new(expected);
        self.resources.shared_roots.acquire_with_native_work(Path::new(path), admission, work, || {
            let pinned = &pinned;
            async move {
                let previous = *pinned.lock().map_err(|_| Self::native_failure("root source identity guard is poisoned"))?;
                let identity = guard_root_preparation_source_with_native_work(self.context.context(),
                    owner, &self.preparation, key, previous, work).await?;
                let mut original = pinned.lock().map_err(|_| Self::native_failure("root source identity guard is poisoned"))?;
                if original.is_some_and(|previous| previous != identity) {
                    return Err(Self::native_failure("root identity changed between original source guards"));
                }
                *original = Some(identity);
                Ok(identity)
            }
        }).await
    }

    async fn capture_inputs_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        digest: [u8; 32], work: &NativeMaterializationWork)
        -> Result<NativeOwned<BTreeMap<String, AttachmentInput>>> {
        let mut inputs = NativeOwned { value: BTreeMap::new(),
            _custody: work.retain_owned(0).map_err(|cause| Error::NativeWork { cause, custody: None })? };
        let PiRootIntent::Project { paths, .. } = &self.preparation.request().root else { return Ok(inputs); };
        // No original creation may exist before authoritative source identities
        // were published. A transient capture before this upload is not an ID.
        for key in paths.keys().map(String::as_str).chain(std::iter::once("scratch")) {
            let (volume, config) = if key == "scratch" {
                (&self.preparation.request().binding.scratch.volume, VolumeConfig::portable(Lifecycle::Durable))
            } else {
                (&self.preparation.request().binding.project_volumes[key], VolumeConfig::native(Lifecycle::Durable))
            };
            if self.creation_with_native_work(effect, attempt, key, volume, config, work).await?.is_some() {
                return Err(Self::native_failure("original creation exists without immutable root attachment input"));
            }
        }
        for (key, path) in paths {
            self.execution_owner()?.verify_with_native_work(false, work).await?;
            self.require_native_creation()?;
            let physical = self.acquire_original_source_with_native_work(key, path,
                SharedRootAdmission::Fresh, None, work).await?;
            let native = |cause| Error::NativeWork { cause, custody: None };
            let (value, custody) = work.display(ROOT_ALLOCATION_PROVIDER).map_err(native)?;
            let mut provider = NativeOwned { value, _custody: custody };
            let (value, custody) = work.display(ROOT_ALLOCATION_EFFECT).map_err(native)?;
            let mut kind = NativeOwned { value, _custody: custody };
            let (value, custody) = work.display(key).map_err(native)?;
            let mut root = NativeOwned { value, _custody: custody };
            let (value, custody) = work.display(path).map_err(native)?;
            let mut native_path = NativeOwned { value, _custody: custody };
            let (value, custody) = self.preparation.request().binding.project_volumes[key].clone_with_native_work(work)?;
            let mut volume = NativeOwned { value, _custody: custody };
            volume._custody.absorb(&mut provider._custody).map_err(native)?;
            volume._custody.absorb(&mut kind._custody).map_err(native)?;
            volume._custody.absorb(&mut root._custody).map_err(native)?;
            volume._custody.absorb(&mut native_path._custody).map_err(native)?;
            let mut record = NativeOwned { value: AttachmentInput {
                task: self.owner.policy().root_task, command: self.preparation.command().operation_id,
                effect, attempt, request_digest: digest, provider: provider.value,
                effect_kind: kind.value, root: root.value, volume: volume.value, native_path: native_path.value,
                source: physical.source.reference(),
                native_identity: physical.source.inner().root_identity().to_bytes(), budget: allocation_budget(),
            }, _custody: volume._custody };
            let (value, custody) = work.display(key).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let mut owned_key = NativeOwned { value, _custody: custody };
            let mut slot = work.retain_owned(std::mem::size_of::<(String, AttachmentInput)>() as u64)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs._custody.absorb(&mut slot).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs._custody.absorb(&mut record._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs._custody.absorb(&mut owned_key._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs.value.insert(owned_key.value, record.value);
        }
        #[cfg(test)]
        self.test_fault_native("before-attachments-stage")?;
        let published = self.stage_with_native_work(effect, attempt, "attachments", &inputs.value, work).await?;
        drop(published);
        Ok(inputs)
    }

    async fn allocate_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        original: &EffectState, state: &mut ProviderState, work: &NativeMaterializationWork)
        -> Result<(EffectObservation, NativeAllocation)> {
        self.check_native_ledger(work)?;
        self.require_native_creation()?;
        let owner = self.execution_owner()?;
        owner.verify_with_native_work(false, work).await?;
        self.bind_conversation_with_native_work(effect, attempt, work).await?;
        #[cfg(test)]
        self.test_fault_native("after-conversation-bind")?;
        let distributed = DistributedFs::new(self.resources.fs.clone(), self.resources.store.clone());
        if let PiRootIntent::Project { paths, .. } = &self.preparation.request().root {
            let inputs = match self.original_artifact_with_native_work(effect, attempt, "attachments", work).await? {
                Some(reference) => self.read_with_native_work::<BTreeMap<String, AttachmentInput>>(&reference.value, work).await?,
                None => self.capture_inputs_with_native_work(effect, attempt, original.request_digest, work).await?,
            };
            if !inputs.value.keys().eq(paths.keys()) {
                return Err(Self::native_failure("root attachment selection differs from original creator"));
            }
            for (key, path) in paths {
                let volume = &self.preparation.request().binding.project_volumes[key];
                let input = &inputs.value[key];
                self.validate_attachment_with_native_work(input, effect, attempt, original.request_digest,
                    key, volume, path)?;
                owner.verify_with_native_work(false, work).await?;
                self.require_native_creation()?;
                let identity = guard_root_preparation_source_with_native_work(self.context.context(),
                    owner, &self.preparation, key, None, work).await?;
                if identity.to_bytes() != input.native_identity {
                    return Err(Self::native_failure("original root native identity was replaced"));
                }
                let generation = self.creation_with_native_work(effect, attempt, key, volume,
                    VolumeConfig::native(Lifecycle::Durable), work).await?;
                let mut persisted_source = input.source;
                let mut existing_workspace = None;
                if let Some(generation) = generation {
                    if let Some(binding) = self.resources.store.load_lazy_workspace_with_native_work(
                        generation.workspace().id(), work).await
                        .map_err(|source| Error::NativeSource { source, custody: None })? {
                        binding.as_state().validate_root_binding(generation.workspace().id(), input.source)
                            .map_err(|source| Error::NativeSource { source, custody: None })?;
                        persisted_source = binding.as_state().source;
                        existing_workspace = Some(generation.workspace());
                    }
                    if state.handles.contains_key(key) { continue; }
                }
                let physical = self.acquire_original_source_with_native_work(key, path,
                    SharedRootAdmission::Resume(persisted_source), Some(identity), work).await?;
                if physical.source.reference().identity != input.source.identity
                    || physical.source.inner().root_identity().to_bytes() != input.native_identity {
                    return Err(Self::native_failure("resumed root source differs from original allocation"));
                }
                owner.verify_with_native_work(false, work).await?;
                self.require_native_creation()?;
                let workspace = match existing_workspace {
                    Some(workspace) => LocalLazyWorkspace::resume_root_observation_with_native_work(
                        workspace, Arc::clone(&physical.source), self.resources.store.clone(), input.source, work).await
                        .map_err(|(source, custody)| Error::NativeSource { source, custody })?,
                    None => {
                        let operation = self.creation_operation_with_native_work(effect, attempt, key, work)?;
                        let (value, custody) = volume.storage_name_with_native_work(work)?;
                        let name = NativeOwned { value, _custody: custody };
                        distributed.attach_lazy_with_operation_with_native_work(&name.value,
                            Arc::clone(&physical.source), VolumeConfig::native(Lifecycle::Durable),
                            operation, work, work.cancellation()).await
                            .map_err(|(source, custody)| Error::NativeSource { source, custody })?.0
                    }
                };
                let (value, custody) = work.display(key).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut owned_key = NativeOwned { value, _custody: custody };
                let mut slot = work.retain_owned(std::mem::size_of::<(String, PiRootHandle)>() as u64)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                slot.absorb(&mut owned_key._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let workspace = workspace.into_arc_with_native_work(work)
                    .map_err(|source| Error::NativeSource { source, custody: None })?;
                state.handles.insert(owned_key.value, PiRootHandle { workspace, physical, _custody: Some(slot) });
                #[cfg(test)]
                self.test_fault_native("after-root-creation")?;
            }
        }
        owner.verify_with_native_work(false, work).await?;
        let volume = &self.preparation.request().binding.scratch.volume;
        owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
        if self.creation_with_native_work(effect, attempt, "scratch", volume,
            VolumeConfig::portable(Lifecycle::Durable), work).await?.is_none() {
            owner.verify_with_native_work(false, work).await?;
            self.require_native_creation()?;
            let operation = self.creation_operation_with_native_work(effect, attempt, "scratch", work)?;
            let (value, custody) = volume.storage_name_with_native_work(work)?;
            let name = NativeOwned { value, _custody: custody };
            self.resources.fs.create_workspace_with_operation_with_native_work(
                &name.value, VolumeConfig::portable(Lifecycle::Durable), operation, work, work.cancellation()).await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        }
        #[cfg(test)]
        self.test_fault_native("before-result-stage")?;
        owner.verify_with_native_work(false, work).await?;
        let observation = self.observe_result_with_native_work(effect, attempt, original, true, work).await?
            .ok_or(Self::native_failure("root creator lacks authentic original resource acknowledgement"))?;
        #[cfg(test)]
        self.test_fault_native("after-result-stage")?;
        Ok(observation)
    }

    pub(crate) async fn dispatch_native(&self, request: EffectDispatch,
        work: &NativeMaterializationWork) -> Result<(EffectObservation, NativeAllocation)> {
        self.check_native_ledger(work)?;
        self.require_native_creation()?;
        let mut state = self.state.lock().await;
        let original = self.authenticate_with_native_work(request.attempt_id, false, work).await?;
        let effect = self.effect_id_with_native_work(work)?;
        if request.effect_id != effect || request.provider != original.value.provider
            || request.effect_kind != original.value.effect_kind || request.request != original.value.request
            || request.guarantee != original.value.guarantee || request.request_digest != original.value.request_digest {
            return Err(Self::native_failure("root dispatch differs from original durable attempt"));
        }
        self.execution_owner()?.verify_with_native_work(false, work).await?;
        if let Some(observation) = self.observe_result_with_native_work(effect, request.attempt_id,
            &original.value, false, work).await? {
            state.dispatched = true;
            return Ok(observation);
        }
        if state.dispatched { return Err(Self::native_failure("original root dispatch remains indeterminate")); }
        state.dispatched = true;
        self.allocate_with_native_work(effect, request.attempt_id, &original.value, &mut state, work).await
    }

    pub(crate) async fn reconcile_native(&self, attempt: EffectAttemptId,
        work: &NativeMaterializationWork) -> Result<Option<(EffectObservation, NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let mut state = self.state.lock().await;
        self.execution_owner()?.verify_with_native_work(true, work).await?;
        let original = self.authenticate_with_native_work(attempt, false, work).await?;
        let effect = self.effect_id_with_native_work(work)?;
        if let Some(observation) = self.observe_result_with_native_work(effect, attempt, &original.value, true, work).await? {
            return Ok(Some(observation));
        }
        if self.creation_cancelled.load(std::sync::atomic::Ordering::SeqCst) { return Ok(None); }
        match self.execution_owner()?.verify_with_native_work(false, work).await {
            Ok(()) => {
                state.dispatched = true;
                self.allocate_with_native_work(effect, attempt, &original.value, &mut state, work).await.map(Some)
            }
            Err(Error::Conflict(_)
                | Error::NativeBoundary { code: ErrorCode::Conflict, .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn test_fault_native(&self, phase: &'static str) -> Result<()> {
        let code = match phase {
            "after-root-creation" => 1, "before-result-stage" => 2,
            "before-attachments-stage" => 3, "after-result-stage" => 4,
            "after-conversation-bind" => 5, _ => unreachable!(),
        };
        if self.fault.compare_exchange(code, 0, std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst).is_ok() {
            return Err(Error::NativeBoundary { code: ErrorCode::Storage, reason: phase });
        }
        Ok(())
    }

    fn native_failure(reason: &'static str) -> Error {
        Error::NativeBoundary { code: ErrorCode::Unauthorized, reason }
    }

    fn require_native_creation(&self) -> Result<()> {
        if self.creation_cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Self::native_failure("original root creator is cancelled"));
        }
        Ok(())
    }

    fn validate_attachment_with_native_work(&self, input: &AttachmentInput,
        effect: EffectId, attempt: EffectAttemptId, digest: [u8; 32],
        key: &str, volume: &VolumeRef, path: &str) -> Result<()> {
        if input.task != self.owner.policy().root_task
            || input.command != self.preparation.command().operation_id
            || input.effect != effect || input.attempt != attempt || input.request_digest != digest
            || input.provider != ROOT_ALLOCATION_PROVIDER || input.effect_kind != ROOT_ALLOCATION_EFFECT
            || input.root != key || &input.volume != volume || input.native_path != path
            || input.source.identity == [0; 16] || input.native_identity == [0; 16]
            || input.budget != allocation_budget() {
            return Err(Self::native_failure("root attachment differs from its original admitted creator input"));
        }
        Ok(())
    }

    async fn creation_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        resource: &str, volume: &VolumeRef, config: VolumeConfig,
        work: &NativeMaterializationWork) -> Result<Option<LocalGeneration>> {
        let operation = self.creation_operation_with_native_work(effect, attempt, resource, work)?;
        let (value, custody) = volume.storage_name_with_native_work(work)?;
        let name = NativeOwned { value, _custody: custody };
        self.resources.fs.workspace_for_creation_operation_with_native_work(&name.value,
            config, operation, work, work.cancellation()).await
            .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })
    }

    fn scratch_creation_with_native_work(&self, generation: &LocalGeneration,
        work: &NativeMaterializationWork) -> Result<NativeOwned<PiScratchCreation>> {
        let digest = generation.id().digest().into_bytes();
        let volume = &self.preparation.request().binding.scratch.volume;
        let (reference, custody) = GenerationRef::new_with_native_work(
            volume.provider(), &digest, None, work)?;
        let scratch = NativeOwned { value: PiScratchCreation {
            workspace_id: generation.workspace().id().into_bytes(), generation: reference,
        }, _custody: custody };
        scratch.value.validate(volume)?;
        Ok(scratch)
    }

    async fn observe_resources_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        digest: [u8; 32], work: &NativeMaterializationWork)
        -> Result<Option<NativeOwned<PiRootAllocation>>> {
        let mut roots = NativeOwned { value: BTreeMap::new(),
            _custody: work.retain_owned(0).map_err(|cause| Error::NativeWork { cause, custody: None })? };
        if let PiRootIntent::Project { paths, .. } = &self.preparation.request().root {
            let Some(reference) = self.original_artifact_with_native_work(effect, attempt, "attachments", work).await?
                else { return Ok(None); };
            let inputs = self.read_with_native_work::<BTreeMap<String, AttachmentInput>>(&reference.value, work).await?;
            if !inputs.value.keys().eq(paths.keys()) {
                return Err(Self::native_failure("root attachment selection differs from original admission"));
            }
            for (key, path) in paths {
                let volume = &self.preparation.request().binding.project_volumes[key];
                let input = &inputs.value[key];
                self.validate_attachment_with_native_work(input, effect, attempt, digest, key, volume, path)?;
                let Some(generation) = self.creation_with_native_work(effect, attempt, key,
                    volume, VolumeConfig::native(Lifecycle::Durable), work).await?
                    else { return Ok(None); };
                let Some(state) = self.resources.store.load_lazy_workspace_with_native_work(
                    generation.workspace().id(), work).await
                    .map_err(|source| Error::NativeSource { source, custody: None })?
                    else { return Ok(None); };
                state.as_state().validate_root_binding(generation.workspace().id(), input.source)
                    .map_err(|source| Error::NativeSource { source, custody: None })?;
                let observed = work.root_identity(Path::new(path))
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                if observed.to_bytes() != input.native_identity {
                    return Err(Self::native_failure("root native identity was substituted"));
                }
                let (value, custody) = work.display(key).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let owned_key = NativeOwned { value, _custody: custody };
                let mut slot = work.retain_owned(std::mem::size_of::<(String, RootCreation)>() as u64)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                roots._custody.absorb(&mut slot).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let NativeOwned { value, mut _custody } = owned_key;
                roots._custody.absorb(&mut _custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                roots.value.insert(value, RootCreation {
                    repository_workspace_id: generation.workspace().id().into_bytes(),
                    source_identity: input.source.identity, source_epoch: input.source.epoch,
                    native_root_identity: input.native_identity,
                });
            }
        }
        let Some(generation) = self.creation_with_native_work(effect, attempt, "scratch",
            &self.preparation.request().binding.scratch.volume,
            VolumeConfig::portable(Lifecycle::Durable), work).await? else { return Ok(None); };
        let scratch = self.scratch_creation_with_native_work(&generation, work)?;
        let NativeOwned { value: scratch, mut _custody } = scratch;
        roots._custody.absorb(&mut _custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let allocation = NativeOwned { value: PiRootAllocation { roots: roots.value, scratch },
            _custody: roots._custody };
        allocation.value.validate(&self.preparation.request().binding)?;
        Ok(Some(allocation))
    }

    async fn observe_result_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        state: &EffectState, stage_missing: bool, work: &NativeMaterializationWork)
        -> Result<Option<(EffectObservation, NativeAllocation)>> {
        let Some(actual) = self.observe_resources_with_native_work(effect, attempt, state.request_digest, work).await?
            else { return Ok(None); };
        if !self.observe_conversation_with_native_work(effect, attempt, work).await? { return Ok(None); }
        let result = match self.original_artifact_with_native_work(effect, attempt, "result", work).await? {
            Some(reference) => {
                let recorded = self.read_with_native_work::<PiRootAllocation>(&reference.value, work).await?;
                if !same_allocation(&recorded.value, &actual.value) {
                    return Err(Self::native_failure("root output substitutes original filesystem resources"));
                }
                reference
            }
            None if stage_missing => self.stage_with_native_work(effect, attempt, "result", &actual.value, work).await?,
            None => return Ok(None),
        };
        let (value, custody) = work.display(ROOT_ALLOCATION_PROVIDER)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut provider = NativeOwned { value, _custody: custody };
        let mut result = result;
        result._custody.absorb(&mut provider._custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Some((EffectObservation { provider: provider.value, effect_id: effect, attempt_id: attempt,
            request_digest: state.request_digest, guarantee: state.guarantee,
            status: EffectStatus::Succeeded { result: result.value } }, result._custody)))
    }

    fn check_native_ledger(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.original.shares_ledger(work) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "root allocation must use its original request ledger" });
        }
        work.admit(WorkCounters::UNCHARGED)
            .map_err(|cause| Error::NativeWork { cause, custody: None })
    }

    fn artifact_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        name: &str, work: &NativeMaterializationWork) -> Result<NativeInputCoordinates> {
        let native = |cause| Error::NativeWork { cause, custody: None };
        let (value, custody) = work.display(&format_args!("{name}.json")).map_err(native)?;
        let display_name = NativeOwned { value, _custody: custody };
        let (value, custody) = work.display(&format_args!(
            "{ROOT_INPUT_DIRECTORY}/allocation/{effect}/{attempt}/{}", display_name.value)).map_err(native)?;
        let path = NativeOwned { value, _custody: custody };
        if path.value.len() > self.owner.policy().limits.path_bytes {
            return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
                reason: "root allocation artifact exceeds original path limit" });
        }
        let (value, custody) = work.display(&format_args!(
            "acyclic.pi/root-allocation/{}/{}/{effect}/{attempt}/{name}",
            self.owner.policy().root_task, self.preparation.command().operation_id)).map_err(native)?;
        let key = NativeOwned { value: IdempotencyKey::new(value)?, _custody: custody };
        Ok(NativeInputCoordinates { path, display_name, key })
    }

    async fn original_artifact_with_native_work(&self, effect: EffectId,
        attempt: EffectAttemptId, name: &str, work: &NativeMaterializationWork)
        -> Result<Option<NativeOwned<FileRef>>> {
        let coordinates = self.artifact_with_native_work(effect, attempt, name, work)?;
        let grant = pi_input_write_grant_with_native_work(&self.owner, work)?;
        let result = self.filesystem.content_for_operation_with_native_work(
            &self.owner.policy().journal, &grant.value, &coordinates.key.value,
            &coordinates.path.value, self.maximum(), work).await?;
        match result {
            Some((value, custody)) => {
                let reference = NativeOwned { value, _custody: custody };
                validate_pi_input_media(&self.owner, &reference.value, &coordinates.path.value,
                    &coordinates.display_name.value, "application/json")?;
                Ok(Some(reference))
            }
            None => Ok(None),
        }
    }

    async fn stage_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        name: &str, value: &impl Serialize, work: &NativeMaterializationWork)
        -> Result<NativeOwned<FileRef>> {
        let coordinates = self.artifact_with_native_work(effect, attempt, name, work)?;
        let (value, custody) = publish_pi_input_with_native_coordinates(&self.owner,
            &*self.filesystem, value, "application/json", coordinates, work).await?;
        Ok(NativeOwned { value, _custody: custody })
    }

    async fn read_with_native_work<T: RootArtifact>(&self, reference: &FileRef,
        work: &NativeMaterializationWork) -> Result<NativeOwned<T>> {
        read_root_artifact_with_native_work(&self.owner, self.context.context(), reference, work).await
    }

    async fn authenticate_with_native_work(&self, attempt: EffectAttemptId,
        require_success: bool, work: &NativeMaterializationWork) -> Result<NativeOwned<EffectState>> {
        self.check_native_ledger(work)?;
        let effect = self.effect_id_with_native_work(work)?;
        let (value, custody) = self.owner.effect_verifier_with_native_work(work)?;
        let verifier = NativeOwned { value, _custody: custody };
        let (value, custody) = HistoryReader::new_with_native_work(
            &self.stream, verifier.value.audience(), &verifier.value, work)?;
        let reader = NativeOwned { value, _custody: custody };
        let (value, custody) = reader.value.pin_with_native_work(0, work).await?;
        let cursor = NativeOwned { value, _custody: custody };
        let (value, custody) = reader.value.effect_with_native_work(&cursor.value, effect,
            HistoryReadLimits { maximum_events: self.owner.policy().limits.context_messages,
                maximum_bytes: self.maximum() }, work).await?;
        let state = NativeOwned { value: value.ok_or(Error::NativeBoundary {
            code: ErrorCode::Unauthorized, reason: "original root effect is absent" })?, _custody: custody };
        let plan = self.preparation.plan();
        if state.value.provider != ROOT_ALLOCATION_PROVIDER
            || state.value.effect_kind != ROOT_ALLOCATION_EFFECT
            || state.value.guarantee != EffectGuarantee::AtMostOnce
            || state.value.request != plan.request || state.value.result_schema != plan.result_schema
            || state.value.attempts.as_slice() != [attempt]
            || !matches!(state.value.status, EffectStatus::Dispatched | EffectStatus::Indeterminate | EffectStatus::Succeeded { .. })
            || (require_success && !matches!(state.value.status, EffectStatus::Succeeded { .. })) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "root effect differs from its original request, schema, provider or attempt" });
        }
        let retained_plan = self.read_with_native_work::<acyclic_harness::effect_host::TaskEffectPlan>(
            &self.preparation.command().payload, work).await?;
        let request = self.read_with_native_work::<PiRootRequest>(&plan.request, work).await?;
        let schema = self.read_with_native_work::<serde_json::Value>(&plan.result_schema, work).await?;
        let (value, custody) = PiRootAllocation::schema_with_native_work(&request.value.binding, work)?;
        let expected_schema = NativeOwned { value, _custody: custody };
        if retained_plan.value != *plan || request.value != *self.preparation.request()
            || schema.value != expected_schema.value {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "original immutable root preparation differs" });
        }
        if let EffectStatus::Succeeded { result } = &state.value.status {
            let canonical = self.original_artifact_with_native_work(effect, attempt, "result", work).await?;
            if canonical.as_ref().map(|reference| &reference.value) != Some(result) {
                return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                    reason: "root success differs from its original immutable output operation" });
            }
        }
        Ok(state)
    }

    pub(crate) fn cancel(&self) { self.creation_cancelled.store(true, std::sync::atomic::Ordering::SeqCst); }

    fn execution_owner(&self) -> Result<&TaskJournalOwner<P>> {
        self.journal_owner.as_deref().ok_or(Error::NativeBoundary {
            code: ErrorCode::Unauthorized, reason: "archived root observer cannot dispatch or resume creation" })
    }

    pub(crate) async fn take_created_handles(&self) -> BTreeMap<String, PiRootHandle> {
        std::mem::take(&mut self.state.lock().await.handles)
    }

    fn effect_id_with_native_work(&self, work: &NativeMaterializationWork) -> Result<EffectId> {
        acyclic_harness::effect_host::task_effect_id_with_native_work(
            self.owner.policy().root_task, self.preparation.command().operation_id, work)
    }

    fn maximum(&self) -> u64 { self.owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES) }

    fn creation_operation_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        resource: &str, original: &NativeMaterializationWork) -> Result<acyclic_fs::OperationId> {
        let (bytes, custody) = guarded_pi_bytes(&("acyclic.pi/root-resource/v1",
            self.owner.policy().root_task, self.preparation.command().operation_id,
            effect, attempt, resource), self.maximum(), original)?;
        let bytes = NativeOwned { value: bytes, _custody: custody };
        original.charge(WorkCounters { bytes_hashed: bytes.value.len() as u64, ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut id = [0; 16];
        id.copy_from_slice(&blake3::hash(&bytes.value).as_bytes()[..16]);
        drop(bytes);
        Ok(acyclic_fs::OperationId::from_bytes(id))
    }

    async fn bind_conversation_with_native_work(
        &self, effect: EffectId, attempt: EffectAttemptId, original: &NativeMaterializationWork,
    ) -> Result<()> {
        let authority = self.owner.conversation_authority_with_native_work(
            self.preparation.request().binding.task_id,
            self.preparation.request().binding.agent_id, &self.owner.policy().grants, original)?;
        let original_creation = self.creation_operation_with_native_work(effect, attempt, "scratch", original)?;
        super::pi_conversation::bind_task_conversation_with_native_work(
            &self.stream, authority.0, authority.1, self.execution_owner()?,
            &self.preparation.request().binding.scratch.volume, original_creation,
            self.owner.policy().limits, original).await
    }

    async fn observe_conversation_with_native_work(
        &self, effect: EffectId, attempt: EffectAttemptId, original: &NativeMaterializationWork,
    ) -> Result<bool> {
        let authority = self.owner.conversation_authority_with_native_work(
            self.preparation.request().binding.task_id,
            self.preparation.request().binding.agent_id, &self.owner.policy().grants, original)?;
        let original_creation = self.creation_operation_with_native_work(effect, attempt, "scratch", original)?;
        super::pi_conversation::observe_task_conversation_with_native_work(
            &self.stream, authority.0, authority.1,
            &self.preparation.request().binding.scratch.volume, original_creation, self.maximum(), original).await
    }

}

impl<P: StreamProvider + 'static> EffectProvider for PiRootProvider<P> {
    fn id(&self) -> &str { ROOT_ALLOCATION_PROVIDER }
    fn guarantees(&self, kind: &str) -> BTreeSet<EffectGuarantee> {
        if kind == ROOT_ALLOCATION_EFFECT { [EffectGuarantee::AtMostOnce].into() } else { BTreeSet::new() }
    }
    fn linearizable_reconciliation(&self) -> bool { false }
    fn dispatch<'a>(&'a self, _request: EffectDispatch) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(async { Err(Error::NativeBoundary { code: ErrorCode::Unsupported,
            reason: "root allocation requires the funded native effect dispatch boundary" }) })
    }
    fn reconcile<'a>(&'a self, _attempt: EffectAttemptId) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(async { Err(Error::NativeBoundary { code: ErrorCode::Unsupported,
            reason: "root allocation requires the funded native effect reconciliation boundary" }) })
    }
    fn dispatch_with_native_work<'a>(&'a self, request: EffectDispatch,
        work: &'a NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, (EffectObservation, NativeAllocation)>> {
        self.check_native_ledger(work)?;
        acyclic_harness::NativeFutureBundle::new(work, || async move { self.dispatch_native(request, work).await })
    }
    fn reconcile_with_native_work<'a>(&'a self, attempt: EffectAttemptId,
        work: &'a NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, Option<(EffectObservation, NativeAllocation)>>> {
        self.check_native_ledger(work)?;
        acyclic_harness::NativeFutureBundle::new(work, || async move { self.reconcile_native(attempt, work).await })
    }
    fn supports_guarantee_with_native_work(&self, kind: &str, guarantee: EffectGuarantee,
        work: &NativeMaterializationWork) -> Result<bool> {
        self.check_native_ledger(work)?;
        Ok(kind == ROOT_ALLOCATION_EFFECT && guarantee == EffectGuarantee::AtMostOnce)
    }
}

#[cfg(test)]
#[path = "pi_root_provider_tests.rs"]
mod tests;
