//! First-party original-root allocation through the stock task effect lifecycle.

use acyclic_fs::demand::{DemandSource, SourceReference};
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
    CancellationToken, LazyWorkspaceStore, Lifecycle, LocalAuthorityBackend,
    LocalObjectBackend, NativeAllocation, NativeMaterializationWork,
    NativeTreeMaterializationError, VolumeConfig, WorkBudget, WorkCounters, WorkError,
};
use acyclic_harness::{
    EffectAttemptId, EffectId, Error, IdempotencyKey, JsonBounds, JsonCodecCharge, OperationId, Result, TaskId,
    canonical_json_bytes_bounded,
    json_bytes_bounded,
    json_value_bounded, json_from_slice_bounded,
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
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::{BTreeMap, BTreeSet}, path::Path, sync::Arc};
use tokio::sync::Mutex;

pub(crate) const OPERATOR_INPUT_DIRECTORY: &str = "pi-operator-inputs";

type Host = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

/// Inline failure prefix: constructor denial never allocates a counter carrier.

/// Actual handles returned by dispatch, never reconstructed from route IDs.
pub(crate) struct PiRootHandle {
    pub(crate) workspace: Arc<LocalLazyWorkspace>,
    pub(crate) physical: Arc<SharedPhysicalRoot>,
    _custody: NativeAllocation,
}

impl PiRootHandle {
    /// Cold restoration transfers actual copied-key custody and admits the compiled entry payload before its caller grows the root index.
    pub(crate) fn from_observed_with_native_work(
        workspace: LocalLazyWorkspace, physical: Arc<SharedPhysicalRoot>,
        key: (String, NativeAllocation), work: &NativeMaterializationWork,
    ) -> Result<(String, Self)> {
        let mut key = (key.0, key.1);
        if !key.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "root index key custody differs from original source work" });
        }
        let workspace = workspace.into_arc_with_native_work(work)
            .map_err(|source| Error::NativeSource { source, custody: None })?;
        let mut entry = work.claim(std::mem::size_of::<(String, Self)>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        key.1.absorb(&mut entry)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok((key.0, Self { workspace, physical, _custody: key.1 }))
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
    "WorkCounters", "authorityRecordsRead", "authorityRecordsAppended",
    "authorityBytesRead", "authorityBytesWritten", "objectProbes",
    "backendReadOperations", "backendWriteOperations", "durabilityOperations",
    "pageReads", "pageWrites", "objectBytesRead", "objectBytesWritten",
    "bytesHashed", "bytesCopied", "bytesEncoded", "sourceBytesRead",
    "sourcePathComponents", "sourceEntriesVisited", "outputBytes",
    "itemsExamined", "itemsReturned", "allocationOperations",
    "peakAllocationBytes", "materializations",
]);

/// Finite allowance for native attachment plus first-party metadata processing.
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
    stream: std::sync::Arc<P>,
    preparation: PiRootPreparation,
    journal_owner: Option<Arc<TaskJournalOwner<P>>>,
    original: NativeMaterializationWork,
    state: Mutex<ProviderState>,
    creation_cancelled: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
    _custody: NativeAllocation,
}


/// Fresh creator's source gate.
pub(crate) async fn guard_root_preparation_source_with_native_work<P: StreamProvider + 'static>(
    context: &TaskContext, current_owner: &TaskJournalOwner<P>,
    preparation: &PiRootPreparation, root_key: &str,
    expected: Option<acyclic_fs::NativeRootIdentity>, work: &NativeMaterializationWork,
) -> Result<acyclic_fs::NativeRootIdentity> {
    let request = &preparation.request;
    let project = request.binding.project_volumes.get(root_key).ok_or(Error::NativeBoundary {
        code: ErrorCode::Unauthorized, reason: "root source is not selected by its original admission",
    })?;
    current_owner.require_volume_grant(project, VolumeOperation::Read, &mut Some(&mut acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?))?;
    context.verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    if current_owner.task_id() != request.binding.task_id
        || preparation.command.operation_id != super::pi_root_inputs::preparation_operation(request.binding.task_id, work)?
        || preparation.plan.provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan.effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan.guarantee != EffectGuarantee::AtMostOnce {
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
#[allow(clippy::too_many_arguments, reason = "original creator and scoped readonly owner evidence remain explicit")]
pub(crate) async fn guard_created_root_source_with_native_work<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: std::sync::Arc<P>, preparation: PiRootPreparation,
    current_read_owner: &TaskJournalOwner<P>, key: &str,
    expected_allocation: &PiRootAllocation, work: &NativeMaterializationWork,
) -> Result<acyclic_fs::NativeRootIdentity> {
    work.admit(WorkCounters::UNCHARGED)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let context = Arc::clone(&preparation.context);
    let request = &preparation.request;
    if current_read_owner.task_id() != request.binding.task_id
        || context.context().durable_task_id() != Some(request.binding.task_id)
    {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "created root source differs from the original readonly task" });
    }
    for volume in request.binding.project_volumes.values() {
        context.context().verify_original_volume_read_with_native_work(
            current_read_owner, volume, work).await?;
    }
    if !request.binding.project_volumes.contains_key(key) {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "created source is not an originally selected root" });
    }
    expected_allocation.validate(&request.binding)?;
    let observer = root_provider(owner, resources, filesystem, stream,
        preparation, None, work.clone())?;
    let effect = observer.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let observed = observer.observe_allocation_with_native_work(attempt, work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("created root lacks its original successful output"))?;
    if !same_allocation(expected_allocation, &observed.0.1) {
        return Err(PiRootProvider::<P>::native_failure("created root substitutes original allocation evidence"));
    }
    let PiRootIntent::Project { paths, .. } = &observer.preparation.request.root else {
        return Err(PiRootProvider::<P>::native_failure("scratch-only allocation has no created native source"));
    };
    let path = paths.get(key).ok_or_else(|| PiRootProvider::<P>::native_failure(
        "created source has no original native selection"))?;
    let identity = work.root_identity(Path::new(path))
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let expected = acyclic_fs::NativeRootIdentity::from_bytes(observed.0.1.roots[key].native_root_identity);
    if identity != expected {
        return Err(Error::NativeWork { cause: NativeTreeMaterializationError::AllocationChanged {
            expected, actual: identity }, custody: None });
    }
    context.context().verify_original_volume_read_with_native_work(current_read_owner,
        &observer.preparation.request.binding.project_volumes[key], work).await?;
    Ok(identity)
}

pub(crate) fn service_resources_with_native_work(
    resources: &ServiceResources, work: &NativeMaterializationWork,
) -> Result<(ServiceResources, NativeAllocation)> {
    let native = |cause| Error::NativeWork { cause, custody: None };
    let data = work.copy_path(&resources.data).map_err(native)?;
    let mut binary = work.display(&resources.binary_identity).map_err(native)?;
    let mut instance = work.display(&resources.instance_id).map_err(native)?;
    let mut owned = (ServiceResources {
            data: data.0, fs: resources.fs.clone(), store: resources.store.clone(),
            shared_roots: resources.shared_roots.clone(), binary_identity: binary.0,
            instance_id: instance.0, private_state_root: resources.private_state_root.clone(),
        }, data.1);
    owned.1.absorb(&mut binary.1).map_err(native)?;
    owned.1.absorb(&mut instance.1).map_err(native)?;
    Ok((owned.0, owned.1))
}

#[allow(clippy::too_many_arguments, reason = "original protected owner and issued context remain explicit")]
pub(crate) fn root_provider<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: std::sync::Arc<P>, preparation: PiRootPreparation,
    journal_owner: Option<Arc<TaskJournalOwner<P>>>,
    original: NativeMaterializationWork,
) -> Result<Arc<PiRootProvider<P>>> {
    let native = |cause| Error::NativeWork { cause, custody: None };
    original.admit(WorkCounters::UNCHARGED).map_err(native)?;
    if !original.budget().has_finite_allowances() {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
            reason: "root allocation requires finite original work allowances" });
    }
    let request = &preparation.request;
    if request.binding.task_id != owner.policy().root_task || request.binding.agent_id != owner.policy().agent
        || journal_owner.as_ref().is_some_and(|current|
            current.task_id() != request.binding.task_id
                || preparation.context.context().durable_task_id() != Some(request.binding.task_id))
        || !matches!(request.binding.scratch.allocation, super::PiScratchAllocation::Preparing)
        || preparation.plan.provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan.effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan.guarantee != EffectGuarantee::AtMostOnce {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "root allocator differs from its original admission and issued context" });
    }
    let authority = owner.authority_with_native_work(&original)?;
    for grant in ["effect:run", "conversation:bind"] {
        if !authority.0.scope.grants().contains(grant) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "original root allocation scope lacks a required capability" });
        }
    }
    let provider_grant = original.display(&format_args!("effect:provider:{ROOT_ALLOCATION_PROVIDER}")).map_err(native)?;
    if !authority.0.scope.grants().contains(&provider_grant.0) {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root allocation provider grant is absent" });
    }
    for volume in request.binding.project_volumes.values().chain(std::iter::once(&request.binding.scratch.volume)) {
        let grant = {
            let mut io = acyclic_harness::store::history::native_work_io(&original).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
            let value = volume.capability(VolumeOperation::Read, &mut Some(&mut io))?;
            (value, io.into_custody())
        };
        if volume.provider() != owner.policy().journal.provider()
            || !authority.0.scope.grants().contains(&grant.0) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "original root volume provider or read grant differs" });
        }
    }
    let grant = {
        let mut io = acyclic_harness::store::history::native_work_io(&original).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let value = request.binding.scratch.volume.capability(VolumeOperation::Write, &mut Some(&mut io))?;
        (value, io.into_custody())
    };
    if !authority.0.scope.grants().contains(&grant.0) {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root scratch write grant is absent" });
    }
    drop(grant);
    drop(authority);
    let mut custody = original.claim(std::mem::size_of::<PiRootProvider<P>>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native)?;
    let (value, resource_custody) = service_resources_with_native_work(resources, &original)?;
    let mut resources = (value, resource_custody);
    custody.absorb(&mut resources.1).map_err(native)?;
    original.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
    Ok(Arc::new(PiRootProvider {
        owner, resources: resources.0, filesystem, stream, preparation, journal_owner, original,
        state: Mutex::new(ProviderState { dispatched: false, handles: BTreeMap::new() }),
        creation_cancelled: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        fault: std::sync::atomic::AtomicU8::new(0),
        _custody: custody,
    }))
}

/// Reopens historical successful creation after the root released its lease.
#[allow(clippy::too_many_arguments, reason = "original archived owner, producers and finite observation")]
pub(crate) async fn observe_root_allocation<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: std::sync::Arc<P>, preparation: PiRootPreparation,
    work: &NativeMaterializationWork,
) -> Result<Option<((FileRef, PiRootAllocation), NativeAllocation)>> {
    let observer = root_provider(owner, resources, filesystem, stream, preparation,
        None, work.clone())?;
    let attempt = EffectAttemptId::from_bytes(observer.effect_id_with_native_work(work)?.into_bytes());
    observer.observe_allocation_with_native_work(attempt, work).await
}


/// Root output is its genuinely created private scratch, never the protected bootstrap journal.
#[allow(clippy::too_many_arguments, reason = "actual caller, original protected creator and bounded producers")]
pub(crate) async fn bind_root_output_context_with_native_work<P: StreamProvider + 'static>(
    context: Arc<acyclic_harness::runtime::NativeTaskContext>, current_owner: &Arc<TaskJournalOwner<P>>,
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>,
    stream: std::sync::Arc<P>, binding: &super::PiTaskBinding, root: &PiRootIntent, work: &NativeMaterializationWork,
) -> Result<Arc<acyclic_harness::runtime::NativeTaskContext>> {
    work.admit(WorkCounters::UNCHARGED).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let invalid = || Error::NativeBoundary { code: ErrorCode::Unauthorized,
        reason: "root output context differs from its original admitted task scope" };
    if context.context().durable_task_id() != Some(current_owner.task_id())
        || current_owner.task_id() != owner.policy().root_task
        || context.context().scope().grants() != &owner.policy().grants
        || binding.task_id != owner.policy().root_task {
        return Err(invalid());
    }
    current_owner.require_volume_grant(&binding.scratch.volume, VolumeOperation::Write, &mut Some(&mut acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?))?;
    current_owner.verify_with_native_work(false, work).await?;
    let preparation = load_original_preparation_with_native_work(&owner, &filesystem, binding, root,
        Arc::clone(&context), work).await?;
    let observer = root_provider(owner, resources, Arc::clone(&filesystem), stream,
        preparation, None, work.clone())?;
    let effect = observer.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let (scratch, operation, _scratch_custody) = observer.prove_scratch_allocation_with_native_work(attempt, work).await?
        .ok_or_else(invalid)?;
    let binding = &observer.preparation.request.binding;
    let (authority, custody) = observer.owner.conversation_authority_with_native_work(
        binding.task_id, binding.agent_id, &observer.owner.policy().grants, work)?;
    super::pi_conversation::bind_task_conversation(
        &observer.stream, authority, custody, current_owner,
        &binding.scratch.volume, operation, observer.owner.policy().limits, work).await?;
    if current_owner.authenticated_binding_agent_with_native_work(work)? != Some(binding.agent_id) {
        return Err(invalid());
    }
    let generation = observer.creation_with_native_work(effect, attempt, "scratch",
        &binding.scratch.volume, VolumeConfig::portable(Lifecycle::Durable), work).await?.ok_or_else(invalid)?;
    let actual = observer.scratch_creation_with_native_work(&generation, work)?;
    if actual.0.workspace_id != scratch.workspace_id || actual.0.generation != scratch.generation {
        return Err(invalid());
    }
    let authority = observer.owner.conversation_authority_with_native_work(
        binding.task_id, binding.agent_id, &observer.owner.policy().grants, work)?;
    let verifier = {
        let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let value = authority.0.1.verifier(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    let source = filesystem.root_output_source(current_owner,
        &binding.scratch.volume, operation, &generation, &verifier.0, &authority.0.2, work).await?;
    context.bind_root_output_context_with_native_work(source, work).await?;
    Ok(context)
}

/// Selected original-root source evidence under the caller's remaining quota.
#[allow(clippy::too_many_arguments, reason = "independent original source and bounded observer arguments")]
pub(crate) async fn prove_root_project_source_with_native_work<P: StreamProvider + 'static>(
    owner: Arc<PiOwnerConfig>, resources: &ServiceResources, filesystem: Arc<Host>, stream: std::sync::Arc<P>,
    binding: &super::PiTaskBinding, intent: &PiRootIntent, project: &VolumeRef, snapshot: acyclic_fs::LazySnapshotRef,
    context: Arc<acyclic_harness::runtime::NativeTaskContext>, current_owner: &TaskJournalOwner<P>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    current_owner.require_volume_grant(project, VolumeOperation::Read, &mut Some(&mut acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?))?;
    context.context().verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    let preparation = load_original_preparation_with_native_work(&owner, &filesystem, binding, intent,
        Arc::clone(&context), work).await?;
    let provider = root_provider(owner, resources, filesystem, stream,
        preparation, None, work.clone())?;
    let effect = provider.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let original = provider.authenticate_with_native_work(attempt, true, work).await?;
    let reference = provider.original_artifact_with_native_work(effect, attempt, "result", work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("root source has no canonical successful allocation output"))?;
    let recorded = provider.read_with_native_work::<PiRootAllocation>(&reference.0, work).await?;
    recorded.0.validate(binding)?;
    let key = binding.project_volumes.iter().find_map(|(key, volume)| (volume == project).then_some(key))
        .ok_or_else(|| PiRootProvider::<P>::native_failure("project is not an originally selected root source"))?;
    let root = &recorded.0.roots[key];
    if snapshot.workspace_id.into_bytes() != root.repository_workspace_id
        || snapshot.source.identity != root.source_identity || snapshot.source.epoch < root.source_epoch {
        return Err(PiRootProvider::<P>::native_failure("snapshot substitutes original root workspace or source"));
    }
    let inputs_reference = provider.original_artifact_with_native_work(effect, attempt, "attachments", work).await?
        .ok_or_else(|| PiRootProvider::<P>::native_failure("root source has no original attachment input"))?;
    let inputs = provider.read_with_native_work::<BTreeMap<String, AttachmentInput>>(&inputs_reference.0, work).await?;
    let PiRootIntent::Project { paths, .. } = intent else {
        return Err(PiRootProvider::<P>::native_failure("scratch-only root has no native project source"));
    };
    if !inputs.0.keys().eq(paths.keys()) {
        return Err(PiRootProvider::<P>::native_failure("root source selection differs from original attachment"));
    }
    let input = &inputs.0[key];
    provider.validate_attachment_with_native_work(input, effect, attempt, original.0.request_digest,
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
    provider.preparation.context.context().verify_original_volume_read_with_native_work(current_owner, project, work).await?;
    Ok(())
}


fn pi_input_write_grant_with_native_work(
    owner: &PiOwnerConfig, work: &NativeMaterializationWork,
) -> Result<(ContentGrant, NativeAllocation)> {
    let authority = owner.authority_with_native_work(work)?;
    let authority = (authority.0, authority.1);
    let grant = {
        let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let grant = ContentGrant::verify(&authority.0.verifier, &authority.0.signed, &owner.policy().journal, VolumeOperation::Write, u64::MAX, &mut Some(&mut io))?;
        (grant, io.into_custody())
    };
    Ok((grant.0, grant.1))
}

fn validate_pi_input_media(
    owner: &PiOwnerConfig, file: &FileRef, path: &str, display_name: &str, media_type: &str,
) -> Result<()> {
    owner.policy().limits.validate_file(file, &mut None)?;
    if file.volume() != &owner.policy().journal || file.path() != path || file.display_name() != display_name
        || file.descriptor().media_type() != media_type
        || file.descriptor().byte_length() > owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES) {
        return Err(Error::Invalid("Pi original input operation returned another immutable file".into()));
    }
    Ok(())
}

/// Same original upload key and protected bootstrap grant as stage_pi_input, with the caller's original native ledger owning both codec and publication.
pub(crate) async fn stage_pi_input<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    stage_pi_input_namespace_with_native_work(owner, filesystem, operation, label, value, work,
        ROOT_INPUT_DIRECTORY, "input").await
}

pub(crate) async fn stage_pi_operator_input<T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str, value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    stage_pi_input_namespace_with_native_work(owner, filesystem, operation, label, value, work,
        OPERATOR_INPUT_DIRECTORY, "operator-input").await
}

/// Readonly original operation metadata, never a mutable path or new upload.
pub(crate) async fn observe_pi_input(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let coordinates = pi_input_coordinates_with_native_work(
        owner, operation, label, ROOT_INPUT_DIRECTORY, "input", work)?;
    observe_pi_input_at(owner, filesystem, coordinates, "application/json", work).await
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

/// Fixed original root artifact coordinates, including the original schema media type; this is not an operation/label input alias or a new upload key.
pub(crate) async fn stage_pi_root_input_with_native_work<A, O, T: Serialize>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, name: &str, value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let media_type = root_input_media(name)?;
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
    let display_name = (display_name, custody);
    let (path, custody) = work.display(&format_args!("{ROOT_INPUT_DIRECTORY}/{}", display_name.0))
        .map_err(native_error)?;
    let path = (path, custody);
    if path.0.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "original Pi root artifact path exceeds owner limit" });
    }
    let (key, custody) = work.display(&format_args!(
        "acyclic.pi/{name}-input/{}", owner.policy().root_task)).map_err(native_error)?;
    let key = IdempotencyKey::new(key)?;
    let key = (key, custody);
    Ok(NativeInputCoordinates { path, display_name, key })
}

/// Same fixed canonical root upload operation as staging, without creating or trusting the current file at its path.
pub(crate) async fn observe_pi_root_input_with_native_work<A, O>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, name: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let media_type = root_input_media(name)?;
    let coordinates = root_input_coordinates_with_native_work(owner, name, work)?;
    observe_pi_input_at(owner, filesystem, coordinates, media_type, work).await
}

async fn observe_pi_input_at<A, O>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, coordinates: NativeInputCoordinates,
    media_type: &str, work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let grant = pi_input_write_grant_with_native_work(owner, work)?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let result = filesystem.content_for_operation_with_native_work(
        &owner.policy().journal, &grant.0, &coordinates.key.0, &coordinates.path.0, maximum, work).await;
    match result {
        Ok(Some((reference, custody))) => {
            if let Err(error) = validate_pi_input_media(owner, &reference, &coordinates.path.0,
                &coordinates.display_name.0, media_type) {
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
    let encoded = (bytes, bytes_custody);
    let length = encoded.0.len() as u64;
    let result = filesystem.put_content_with_native_work(&owner.policy().journal, &grant.0, &coordinates.path.0,
        &encoded.0, media_type, &coordinates.display_name.0, maximum, &coordinates.key.0, work).await;
    drop(encoded);
    let result = match result {
        Ok((reference, custody)) => {
            let validation = validate_pi_input_media(owner, &reference, &coordinates.path.0,
                &coordinates.display_name.0, media_type)
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

async fn read_root_artifact_with_native_work<T: DeserializeOwned + Serialize>(
    owner: &PiOwnerConfig, context: &TaskContext, reference: &FileRef,
    work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    if reference.descriptor().byte_length() > maximum || reference.path().len() > owner.policy().limits.path_bytes
        || !matches!(reference.descriptor().media_type(), "application/json" | "application/schema+json") {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
            reason: "root artifact exceeds original header bounds or media type" });
    }
    let bytes = context.read_file_with_native_work(reference, work).await?;
    let maximum_owned = std::mem::size_of::<T>() as u64;
    let decoded = guarded_pi_decode(bytes.as_ref(), maximum, maximum_owned,
        acyclic_harness::wire::ErrorCode::Invalid, if reference.descriptor().media_type() == "application/schema+json" { JsonValueSchema::Root } else { JsonValueSchema::None },
        ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES, work)?;
    let encoded = guarded_pi_bytes(&decoded.0, maximum, work)?;
    if encoded.0.as_slice() != bytes.as_ref() {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
            reason: "root artifact is not its immutable first-party encoding" });
    }
    Ok(decoded)
}

pub(crate) async fn load_original_preparation_with_native_work(
    owner: &PiOwnerConfig, filesystem: &Host, expected_binding: &super::PiTaskBinding, expected_root: &PiRootIntent,
    context: Arc<acyclic_harness::runtime::NativeTaskContext>, work: &NativeMaterializationWork,
) -> Result<PiRootPreparation> {
    let missing = || Error::NativeBoundary { code: ErrorCode::Unauthorized,
        reason: "original root preparation upload operation is absent" };
    let mut payload = observe_pi_root_input_with_native_work(owner, filesystem, "preparation", work).await?
        .ok_or_else(missing)?;
    let plan = read_root_artifact_with_native_work::<acyclic_harness::effect_host::TaskEffectPlan>(
        owner, context.context(), &payload.0, work).await?;
    let root = observe_pi_root_input_with_native_work(owner, filesystem, "root", work).await?
        .ok_or_else(missing)?;
    let schema_reference = observe_pi_root_input_with_native_work(owner, filesystem, "allocation-schema", work).await?
        .ok_or_else(missing)?;
    if plan.0.provider != ROOT_ALLOCATION_PROVIDER || plan.0.effect_kind != ROOT_ALLOCATION_EFFECT
        || plan.0.guarantee != EffectGuarantee::AtMostOnce
        || plan.0.request != root.0 || plan.0.result_schema != schema_reference.0 {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root plan differs from canonical input operations" });
    }
    let request = read_root_artifact_with_native_work::<PiRootRequest>(owner, context.context(), &root.0, work).await?;
    if &request.0.binding != expected_binding || &request.0.root != expected_root {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "root request differs from original canonical input" });
    }
    let schema = read_root_artifact_with_native_work::<serde_json::Value>(
        owner, context.context(), &schema_reference.0, work).await?;
    let expected_schema = PiRootAllocation::schema(&request.0.binding, work)?;
    if schema.0 != expected_schema.0 {
        return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
            reason: "original root allocation schema differs" });
    }
    let mut kind = work.display(acyclic_harness::filesystem::EFFECT_TASK_COMMAND_KIND)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    payload.1.absorb(&mut kind.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut plan_custody = plan.1;
    let mut request_custody = request.1;
    let mut preparation = PiRootPreparation { command: acyclic_harness::workflow::WorkflowCommand {
        operation_id: super::pi_root_inputs::preparation_operation(owner.policy().root_task, work)?,
        kind: kind.0, payload: payload.0,
    }, plan: plan.0, request: request.0, context, custody: payload.1 };
    preparation.custody.absorb(&mut plan_custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    preparation.custody.absorb(&mut request_custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(preparation)
}

struct NativeInputCoordinates {
    path: (String, NativeAllocation),
    display_name: (String, NativeAllocation),
    key: (IdempotencyKey, NativeAllocation),
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
    let display_name = (display_name, custody);
    let (path, custody) = work.display(&format_args!("{directory}/{operation}/{}", display_name.0))
        .map_err(native_error)?;
    let path = (path, custody);
    if path.0.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary { code: ErrorCode::Invalid, reason: "Pi input path exceeds its original owner limit" });
    }
    let (key, custody) = work.display(&format_args!(
        "acyclic.pi/{purpose}/{}/{operation}/{label}", owner.policy().root_task)).map_err(native_error)?;
    let key = IdempotencyKey::new(key)?;
    let key = (key, custody);
    Ok(NativeInputCoordinates { path, display_name, key })
}

pub(crate) fn guarded_pi_bytes<T: Serialize>(
    value: &T, maximum_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    encode_pi(Some(maximum_bytes), None, work, |bounds, charge| {
        json_bytes_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| charge(event))
    })
}

fn encode_pi<T>(
    maximum_bytes: Option<u64>, maximum_owned_bytes: Option<u64>, work: &NativeMaterializationWork,
    encode: impl FnOnce(JsonBounds, &mut dyn FnMut(JsonCodecCharge) -> Result<()>) -> Result<T>,
) -> Result<(T, NativeAllocation)> {
    let native = |cause| Error::NativeWork { cause, custody: None };
    let remaining = work.remaining().map_err(native)?;
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native)?;
    let mut diagnostic = work.claim(sdk_json_diagnostic_bytes(), 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native)?;
    let mut parser_quota: Option<NativeAllocation> = None;
    let bounds = JsonBounds {
        maximum_bytes: maximum_bytes.map_or(remaining.bytes_encoded, |maximum| maximum.min(remaining.bytes_encoded)),
        maximum_items: remaining.items_examined, maximum_depth: 64,
    };
    let result = encode(bounds, &mut |charge| {
        if let JsonCodecCharge::Release { bytes } = charge {
            return custody.release(bytes).map_err(native);
        }
        work.cancellation().check().map_err(|_| native(NativeTreeMaterializationError::Cancelled))?;
        match charge {
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                let parser_owned = maximum_owned_bytes.checked_sub(sdk_json_diagnostic_bytes())
                    .ok_or(Error::NativeBoundary { code: ErrorCode::Invalid,
                        reason: "SDK raw parser quota omits its diagnostic inventory" })?;
                let mut quota = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native)?;
                quota.resize_retained(parser_owned).map_err(native)?;
                parser_quota = Some(quota);
                Ok(())
            }
            JsonCodecCharge::Retain { bytes } => {
                let observed = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| native(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                if let Some(maximum) = maximum_owned_bytes {
                    if observed > maximum {
                        return Err(native(NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                            counter: "pi_value_owned_bytes", observed, maximum,
                        })));
                    }
                }
                if let Some(mut quota) = parser_quota.take() {
                    custody.absorb(&mut quota).map_err(native)?;
                    custody.substitute(observed).map_err(native)
                } else { custody.resize_retained(observed).map_err(native) }
            }
            charge => native_buffer_codec_charge(work, &mut custody, charge),
        }
    });
    match result {
        Ok(value) => Ok((value, custody)),
        Err(Error::NativeEncode { code, source, custody: None }) => {
            if let Some(mut quota) = parser_quota { diagnostic.absorb(&mut quota).map_err(native)?; }
            if custody.bytes() != 0 { diagnostic.absorb(&mut custody).map_err(native)?; }
            Err(Error::NativeEncode { code, source, custody: Some(diagnostic) })
        }
        Err(error @ Error::NativeWork { custody: None, .. }) if custody.bytes() == 0 => Err(error),
        Err(Error::NativeWork { cause, custody: None }) => Err(Error::NativeWork { cause, custody: Some(custody) }),
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

/// Reads only the original immutable upload.
#[allow(clippy::too_many_arguments, reason = "original immutable operation plus actual scoped content reader")]
pub(crate) async fn read_pi_input_with_native_work<T: DeserializeOwned>(
    owner: &PiOwnerConfig, filesystem: &Host, operation: OperationId, label: &str,
    maximum_owned_bytes: u64, value_schema: JsonValueSchema, schema_label_bytes: u64,
    context: &TaskContext, work: &NativeMaterializationWork,
) -> Result<Option<(T, NativeAllocation)>> {
    let Some((reference, custody)) = observe_pi_input(
        owner, filesystem, operation, label, work).await? else {
        return Ok(None);
    };
    let reference = (reference, custody);
    let bytes = context.read_file_with_native_work(&reference.0, work).await?;
    let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
    let decoded = guarded_pi_decode(bytes.as_ref(), maximum, maximum_owned_bytes,
        ErrorCode::Storage, value_schema, schema_label_bytes, work)?;
    drop(bytes);
    drop(reference);
    Ok(Some(decoded))
}

/// Original SDK canonical bytes, with each actual producer callback using the caller's existing native ledger and cancellation rather than a fresh meter.
pub(crate) fn canonical_pi_value<T: Serialize + ?Sized>(
    value: &T, maximum_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation)> {
    encode_pi(Some(maximum_bytes), None, work, |bounds, charge| {
        canonical_json_bytes_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| charge(event))
    })
}

/// The exact existing BLAKE3 identity over those canonical bytes.
pub(crate) fn digest_pi_value<T: Serialize + ?Sized>(
    value: &T, maximum: u64, work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let bytes = canonical_pi_value(value, maximum, work)?;
    work.charge(WorkCounters { bytes_hashed: bytes.0.len() as u64, ..WorkCounters::default() })
        .map_err(native_error)?;
    let digest = blake3::hash(&bytes.0);
    drop(bytes);
    work.display(&format_args!("{}", digest.to_hex())).map_err(native_error)
}


pub(crate) fn guarded_pi_decode<T: DeserializeOwned>(
    bytes: &[u8], maximum_bytes: u64, maximum_owned_bytes: u64, code: ErrorCode,
    value_schema: JsonValueSchema, schema_label_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let native_error = |cause| Error::NativeWork { cause, custody: None };
    let remaining = work.remaining().map_err(native_error)?;
    let mut custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native_error)?;
    let mut retained_owned = maximum_owned_bytes;
    let bounds = JsonBounds {
        maximum_bytes,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = json_from_slice_bounded(std::marker::PhantomData::<T>, bytes, bounds, maximum_owned_bytes, Some(code), Some(value_schema), &mut |charge| {
        if let JsonCodecCharge::Release { bytes } = charge {
            return custody.release(bytes).map_err(native_error);
        }
        work.cancellation().check()
            .map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        match charge {

            JsonCodecCharge::Admission { maximum_owned_bytes: admission_bytes, value_shape } => {
                let diagnostic = json_typed_diagnostic_owned_bytes(value_shape, schema_label_bytes, Some(code))?;
                let owned = json_value_decode_owned_bytes(value_shape)?;
                retained_owned = maximum_owned_bytes.checked_add(owned)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                let admitted = admission_bytes.checked_add(owned).and_then(|bytes| bytes.checked_add(diagnostic))
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                custody.resize_retained(admitted).map_err(native_error)
            }
            JsonCodecCharge::Retain { bytes } => {
                let retained = custody.bytes().checked_add(bytes)
                    .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
                custody.resize_retained(retained).map_err(native_error)
            }

        charge => native_buffer_codec_charge(work, &mut custody, charge),}
    });
    match result {
        Ok(value) => {
            custody.substitute(retained_owned).map_err(native_error)?;
            Ok((value, custody))
        }
        Err(Error::NativeDecode { code, source, custody: None }) => {
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

/// Produces the SDK encoder's actual Value on the caller's original native ledger.
pub(crate) fn guarded_pi_value<T: Serialize>(
    value: &T, maximum_owned_bytes: u64, work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    encode_pi(None, Some(maximum_owned_bytes), work, |bounds, charge| {
        json_value_bounded(value, bounds, Some(ErrorCode::Invalid), &mut |event| charge(event))
    })
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
        let mut recorded = self.read_with_native_work::<PiRootAllocation>(&reference.0, work).await?;
        let Some(actual) = self.observe_resources_with_native_work(effect, attempt, original.0.request_digest, work).await?
            else { return Ok(None); };
        if !same_allocation(&recorded.0, &actual.0)
            || !self.observe_conversation_with_native_work(effect, attempt, work).await? {
            return Err(Self::native_failure("successful root output differs from original resources or binding"));
        }
        reference.1.absorb(&mut recorded.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Some(((reference.0, recorded.0), reference.1)))
    }

    pub(crate) async fn prove_scratch_allocation_with_native_work(&self, attempt: EffectAttemptId,
        work: &NativeMaterializationWork) -> Result<Option<(PiScratchCreation, acyclic_fs::OperationId, NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let _state = self.state.lock().await;
        self.authenticate_with_native_work(attempt, true, work).await?;
        let effect = self.effect_id_with_native_work(work)?;
        let Some(output) = self.original_artifact_with_native_work(effect, attempt, "result", work).await?
            else { return Ok(None); };
        let recorded = self.read_with_native_work::<PiRootAllocation>(&output.0, work).await?;
        let binding = &self.preparation.request.binding;
        recorded.0.validate(binding)?;
        if binding.scratch.volume.class() != acyclic_harness::conversation::VolumeClass::AgentPrivate
            || binding.scratch.volume.owner() != &acyclic_harness::conversation::VolumeOwner::Agent(binding.agent_id) {
            return Err(Self::native_failure("root scratch is not its original agent-private output resource"));
        }
        let Some(generation) = self.creation_with_native_work(effect, attempt, "scratch", &binding.scratch.volume,
            VolumeConfig::portable(Lifecycle::Durable), work).await? else { return Ok(None); };
        let scratch = self.scratch_creation_with_native_work(&generation, work)?;
        if recorded.0.scratch != scratch.0
            || !self.observe_conversation_with_native_work(effect, attempt, work).await? {
            return Err(Self::native_failure("root scratch output differs from original creation or binding"));
        }
        let operation = self.creation_operation_with_native_work(effect, attempt, "scratch", work)?;
        Ok(Some((scratch.0, operation, scratch.1)))
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
                let identity = guard_root_preparation_source_with_native_work(self.preparation.context.context(),
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
        -> Result<(BTreeMap<String, AttachmentInput>, NativeAllocation)> {
        let mut inputs = (BTreeMap::new(), work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(|cause| Error::NativeWork { cause, custody: None })?);
        let PiRootIntent::Project { paths, .. } = &self.preparation.request.root else { return Ok(inputs); };
        for key in paths.keys().map(String::as_str).chain(std::iter::once("scratch")) {
            let (volume, config) = if key == "scratch" {
                (&self.preparation.request.binding.scratch.volume, VolumeConfig::portable(Lifecycle::Durable))
            } else {
                (&self.preparation.request.binding.project_volumes[key], VolumeConfig::native(Lifecycle::Durable))
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
            let mut provider = work.display(ROOT_ALLOCATION_PROVIDER).map_err(native)?;
            let mut kind = work.display(ROOT_ALLOCATION_EFFECT).map_err(native)?;
            let mut root = work.display(key).map_err(native)?;
            let mut native_path = work.display(path).map_err(native)?;
            let mut volume = {
                let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
                let value = self.preparation.request.binding.project_volumes[key].copy(&mut Some(&mut io))?;
                (value, io.into_custody())
            };
            volume.1.absorb(&mut provider.1).map_err(native)?;
            volume.1.absorb(&mut kind.1).map_err(native)?;
            volume.1.absorb(&mut root.1).map_err(native)?;
            volume.1.absorb(&mut native_path.1).map_err(native)?;
            let mut record = (AttachmentInput {
                task: self.owner.policy().root_task, command: self.preparation.command.operation_id,
                effect, attempt, request_digest: digest, provider: provider.0,
                effect_kind: kind.0, root: root.0, volume: volume.0, native_path: native_path.0,
                source: physical.source.reference(),
                native_identity: physical.source.inner().root_identity().to_bytes(), budget: allocation_budget(),
            }, volume.1);
            let mut owned_key = work.display(key).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let mut slot = work.claim(std::mem::size_of::<(String, AttachmentInput)>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs.1.absorb(&mut slot).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs.1.absorb(&mut record.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs.1.absorb(&mut owned_key.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            inputs.0.insert(owned_key.0, record.0);
        }
        #[cfg(test)]
        self.test_fault_native("before-attachments-stage")?;
        let published = self.stage_with_native_work(effect, attempt, "attachments", &inputs.0, work).await?;
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

        if let PiRootIntent::Project { paths, .. } = &self.preparation.request.root {
            let inputs = match self.original_artifact_with_native_work(effect, attempt, "attachments", work).await? {
                Some(reference) => self.read_with_native_work::<BTreeMap<String, AttachmentInput>>(&reference.0, work).await?,
                None => self.capture_inputs_with_native_work(effect, attempt, original.request_digest, work).await?,
            };
            if !inputs.0.keys().eq(paths.keys()) {
                return Err(Self::native_failure("root attachment selection differs from original creator"));
            }
            for (key, path) in paths {
                let volume = &self.preparation.request.binding.project_volumes[key];
                let input = &inputs.0[key];
                self.validate_attachment_with_native_work(input, effect, attempt, original.request_digest,
                    key, volume, path)?;
                owner.verify_with_native_work(false, work).await?;
                self.require_native_creation()?;
                let identity = guard_root_preparation_source_with_native_work(self.preparation.context.context(),
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
                        let name = {
                            let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
                            let value = volume.storage_name(&mut Some(&mut io))?;
                            (value, io.into_custody())
                        };
                        acyclic_fs::LazyWorkspace::attach_with_operation_with_native_work(&self.resources.fs, &name.0, Arc::clone(&physical.source), self.resources.store.clone(), VolumeConfig::native(Lifecycle::Durable), operation, work, work.cancellation()).await
                            .map_err(|(source, custody)| Error::NativeSource { source, custody })?.0
                    }
                };
                let mut owned_key = work.display(key).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut slot = work.claim(std::mem::size_of::<(String, PiRootHandle)>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                slot.absorb(&mut owned_key.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let workspace = workspace.into_arc_with_native_work(work)
                    .map_err(|source| Error::NativeSource { source, custody: None })?;
                state.handles.insert(owned_key.0, PiRootHandle { workspace, physical, _custody: slot });
                #[cfg(test)]
                self.test_fault_native("after-root-creation")?;
            }
        }
        owner.verify_with_native_work(false, work).await?;
        let volume = &self.preparation.request.binding.scratch.volume;
        owner.require_volume_grant(volume, VolumeOperation::Write, &mut Some(&mut acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?))?;
        if self.creation_with_native_work(effect, attempt, "scratch", volume,
            VolumeConfig::portable(Lifecycle::Durable), work).await?.is_none() {
            owner.verify_with_native_work(false, work).await?;
            self.require_native_creation()?;
            let operation = self.creation_operation_with_native_work(effect, attempt, "scratch", work)?;
            let name = {
                let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
                let value = volume.storage_name(&mut Some(&mut io))?;
                (value, io.into_custody())
            };
            self.resources.fs.create_workspace_with_operation_with_native_work(
                &name.0, VolumeConfig::portable(Lifecycle::Durable), operation, work, work.cancellation()).await
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
        if request.effect_id != effect || request.provider != original.0.provider
            || request.effect_kind != original.0.effect_kind || request.request != original.0.request
            || request.guarantee != original.0.guarantee || request.request_digest != original.0.request_digest {
            return Err(Self::native_failure("root dispatch differs from original durable attempt"));
        }
        self.execution_owner()?.verify_with_native_work(false, work).await?;
        if let Some(observation) = self.observe_result_with_native_work(effect, request.attempt_id,
            &original.0, false, work).await? {
            state.dispatched = true;
            return Ok(observation);
        }
        if state.dispatched { return Err(Self::native_failure("original root dispatch remains indeterminate")); }
        state.dispatched = true;
        self.allocate_with_native_work(effect, request.attempt_id, &original.0, &mut state, work).await
    }

    pub(crate) async fn reconcile_native(&self, attempt: EffectAttemptId,
        work: &NativeMaterializationWork) -> Result<Option<(EffectObservation, NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let mut state = self.state.lock().await;
        self.execution_owner()?.verify_with_native_work(true, work).await?;
        let original = self.authenticate_with_native_work(attempt, false, work).await?;
        let effect = self.effect_id_with_native_work(work)?;
        if let Some(observation) = self.observe_result_with_native_work(effect, attempt, &original.0, true, work).await? {
            return Ok(Some(observation));
        }
        if self.creation_cancelled.load(std::sync::atomic::Ordering::SeqCst) { return Ok(None); }
        match self.execution_owner()?.verify_with_native_work(false, work).await {
            Ok(()) => {
                state.dispatched = true;
                self.allocate_with_native_work(effect, attempt, &original.0, &mut state, work).await.map(Some)
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
            || input.command != self.preparation.command.operation_id
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
        let name = {
            let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
            let value = volume.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        self.resources.fs.workspace_for_creation_operation_with_native_work(&name.0,
            config, operation, work, work.cancellation()).await
            .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })
    }

    fn scratch_creation_with_native_work(&self, generation: &LocalGeneration,
        work: &NativeMaterializationWork) -> Result<(PiScratchCreation, NativeAllocation)> {
        let digest = generation.id().digest().into_bytes();
        let volume = &self.preparation.request.binding.scratch.volume;
        let (reference, custody) = GenerationRef::new_with_native_work(
            volume.provider(), &digest, None, work)?;
        let scratch = (PiScratchCreation {
            workspace_id: generation.workspace().id().into_bytes(), generation: reference,
        }, custody);
        scratch.0.validate(volume)?;
        Ok(scratch)
    }

    async fn observe_resources_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        digest: [u8; 32], work: &NativeMaterializationWork)
        -> Result<Option<(PiRootAllocation, NativeAllocation)>> {
        let mut roots = (BTreeMap::new(), work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(|cause| Error::NativeWork { cause, custody: None })?);
        if let PiRootIntent::Project { paths, .. } = &self.preparation.request.root {
            let Some(reference) = self.original_artifact_with_native_work(effect, attempt, "attachments", work).await?
                else { return Ok(None); };
            let inputs = self.read_with_native_work::<BTreeMap<String, AttachmentInput>>(&reference.0, work).await?;
            if !inputs.0.keys().eq(paths.keys()) {
                return Err(Self::native_failure("root attachment selection differs from original admission"));
            }
            for (key, path) in paths {
                let volume = &self.preparation.request.binding.project_volumes[key];
                let input = &inputs.0[key];
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
                let owned_key = work.display(key).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut slot = work.claim(std::mem::size_of::<(String, RootCreation)>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                roots.1.absorb(&mut slot).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let (value, mut _custody) = owned_key;
                roots.1.absorb(&mut _custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                roots.0.insert(value, RootCreation {
                    repository_workspace_id: generation.workspace().id().into_bytes(),
                    source_identity: input.source.identity, source_epoch: input.source.epoch,
                    native_root_identity: input.native_identity,
                });
            }
        }
        let Some(generation) = self.creation_with_native_work(effect, attempt, "scratch",
            &self.preparation.request.binding.scratch.volume,
            VolumeConfig::portable(Lifecycle::Durable), work).await? else { return Ok(None); };
        let scratch = self.scratch_creation_with_native_work(&generation, work)?;
        let (scratch, mut _custody) = scratch;
        roots.1.absorb(&mut _custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let allocation = (PiRootAllocation { roots: roots.0, scratch }, roots.1);
        allocation.0.validate(&self.preparation.request.binding)?;
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
                let recorded = self.read_with_native_work::<PiRootAllocation>(&reference.0, work).await?;
                if !same_allocation(&recorded.0, &actual.0) {
                    return Err(Self::native_failure("root output substitutes original filesystem resources"));
                }
                reference
            }
            None if stage_missing => self.stage_with_native_work(effect, attempt, "result", &actual.0, work).await?,
            None => return Ok(None),
        };
        let mut provider = work.display(ROOT_ALLOCATION_PROVIDER)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut result = result;
        result.1.absorb(&mut provider.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Some((EffectObservation { provider: provider.0, effect_id: effect, attempt_id: attempt,
            request_digest: state.request_digest, guarantee: state.guarantee,
            status: EffectStatus::Succeeded { result: result.0 } }, result.1)))
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
        let display_name = work.display(&format_args!("{name}.json")).map_err(native)?;
        let path = work.display(&format_args!(
            "{ROOT_INPUT_DIRECTORY}/allocation/{effect}/{attempt}/{}", display_name.0)).map_err(native)?;
        if path.0.len() > self.owner.policy().limits.path_bytes {
            return Err(Error::NativeBoundary { code: ErrorCode::Invalid,
                reason: "root allocation artifact exceeds original path limit" });
        }
        let (value, custody) = work.display(&format_args!(
            "acyclic.pi/root-allocation/{}/{}/{effect}/{attempt}/{name}",
            self.owner.policy().root_task, self.preparation.command.operation_id)).map_err(native)?;
        let key = (IdempotencyKey::new(value)?, custody);
        Ok(NativeInputCoordinates { path, display_name, key })
    }

    async fn original_artifact_with_native_work(&self, effect: EffectId,
        attempt: EffectAttemptId, name: &str, work: &NativeMaterializationWork)
        -> Result<Option<(FileRef, NativeAllocation)>> {
        let coordinates = self.artifact_with_native_work(effect, attempt, name, work)?;
        observe_pi_input_at(&self.owner, &self.filesystem, coordinates, "application/json", work).await
    }

    async fn stage_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        name: &str, value: &impl Serialize, work: &NativeMaterializationWork)
        -> Result<(FileRef, NativeAllocation)> {
        let coordinates = self.artifact_with_native_work(effect, attempt, name, work)?;
        publish_pi_input_with_native_coordinates(&self.owner,
            &self.filesystem, value, "application/json", coordinates, work).await
    }

    async fn read_with_native_work<T: DeserializeOwned + Serialize>(&self, reference: &FileRef,
        work: &NativeMaterializationWork) -> Result<(T, NativeAllocation)> {
        read_root_artifact_with_native_work(&self.owner, self.preparation.context.context(), reference, work).await
    }

    async fn authenticate_with_native_work(&self, attempt: EffectAttemptId,
        require_success: bool, work: &NativeMaterializationWork) -> Result<(EffectState, NativeAllocation)> {
        self.check_native_ledger(work)?;
        let effect = self.effect_id_with_native_work(work)?;
        let verifier = self.owner.effect_verifier_with_native_work(work)?;
        let reader = {
            let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
            let value = HistoryReader::new(&self.stream, verifier.0.audience(), &verifier.0, &mut Some(&mut io))?;
            (value, io.into_custody())
        };
        let cursor = reader.0.pin_with_native_work(0, work).await?;
        let (value, custody) = reader.0.effect_with_native_work(&cursor.0, effect,
            HistoryReadLimits { maximum_events: self.owner.policy().limits.context_messages,
                maximum_bytes: self.maximum() }, work).await?;
        let state = (value.ok_or(Error::NativeBoundary {
            code: ErrorCode::Unauthorized, reason: "original root effect is absent" })?, custody);
        let plan = &self.preparation.plan;
        if state.0.provider != ROOT_ALLOCATION_PROVIDER
            || state.0.effect_kind != ROOT_ALLOCATION_EFFECT
            || state.0.guarantee != EffectGuarantee::AtMostOnce
            || state.0.request != plan.request || state.0.result_schema != plan.result_schema
            || state.0.attempts.as_slice() != [attempt]
            || !matches!(state.0.status, EffectStatus::Dispatched | EffectStatus::Indeterminate | EffectStatus::Succeeded { .. })
            || (require_success && !matches!(state.0.status, EffectStatus::Succeeded { .. })) {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "root effect differs from its original request, schema, provider or attempt" });
        }
        let retained_plan = self.read_with_native_work::<acyclic_harness::effect_host::TaskEffectPlan>(
            &self.preparation.command.payload, work).await?;
        let request = self.read_with_native_work::<PiRootRequest>(&plan.request, work).await?;
        let schema = self.read_with_native_work::<serde_json::Value>(&plan.result_schema, work).await?;
        let expected_schema = PiRootAllocation::schema(&request.0.binding, work)?;
        if retained_plan.0 != *plan || request.0 != self.preparation.request
            || schema.0 != expected_schema.0 {
            return Err(Error::NativeBoundary { code: ErrorCode::Unauthorized,
                reason: "original immutable root preparation differs" });
        }
        if let EffectStatus::Succeeded { result } = &state.0.status {
            let canonical = self.original_artifact_with_native_work(effect, attempt, "result", work).await?;
            if canonical.as_ref().map(|reference| &reference.0) != Some(result) {
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
        acyclic_harness::effect_host::task_effect_id(
            self.owner.policy().root_task, self.preparation.command.operation_id, &mut Some(&mut acyclic_harness::store::history::native_work_io(work)
                .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?))
    }

    fn maximum(&self) -> u64 { self.owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES) }

    fn creation_operation_with_native_work(&self, effect: EffectId, attempt: EffectAttemptId,
        resource: &str, original: &NativeMaterializationWork) -> Result<acyclic_fs::OperationId> {
        let (bytes, custody) = guarded_pi_bytes(&("acyclic.pi/root-resource/v1",
            self.owner.policy().root_task, self.preparation.command.operation_id,
            effect, attempt, resource), self.maximum(), original)?;
        let bytes = (bytes, custody);
        original.charge(WorkCounters { bytes_hashed: bytes.0.len() as u64, ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut id = [0; 16];
        id.copy_from_slice(&blake3::hash(&bytes.0).as_bytes()[..16]);
        drop(bytes);
        Ok(acyclic_fs::OperationId::from_bytes(id))
    }

    async fn bind_conversation_with_native_work(
        &self, effect: EffectId, attempt: EffectAttemptId, original: &NativeMaterializationWork,
    ) -> Result<()> {
        let authority = self.owner.conversation_authority_with_native_work(
            self.preparation.request.binding.task_id,
            self.preparation.request.binding.agent_id, &self.owner.policy().grants, original)?;
        let original_creation = self.creation_operation_with_native_work(effect, attempt, "scratch", original)?;
        super::pi_conversation::bind_task_conversation(
            &self.stream, authority.0, authority.1, self.execution_owner()?,
            &self.preparation.request.binding.scratch.volume, original_creation,
            self.owner.policy().limits, original).await
    }

    async fn observe_conversation_with_native_work(
        &self, effect: EffectId, attempt: EffectAttemptId, original: &NativeMaterializationWork,
    ) -> Result<bool> {
        let authority = self.owner.conversation_authority_with_native_work(
            self.preparation.request.binding.task_id,
            self.preparation.request.binding.agent_id, &self.owner.policy().grants, original)?;
        let original_creation = self.creation_operation_with_native_work(effect, attempt, "scratch", original)?;
        super::pi_conversation::observe_task_conversation(
            &self.stream, authority.0, authority.1,
            &self.preparation.request.binding.scratch.volume, original_creation, self.maximum(), original).await
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
