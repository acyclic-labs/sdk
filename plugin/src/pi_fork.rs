//! Recursive Pi admission over the original task and provider journals.

use crate::{LocalAuthorityBackend, LocalObjectBackend, LocalLazyWorkspace,
    PiScratchAllocation, PiScratchCreation, PiTaskBinding, ServiceResources,
    pi_lazy_capture::PiLazyCapture, pi_native::PiNativeLaunchIntent,
    pi_owner::PiOwnerConfig, pi_runtime::PiTaskRuntime,
    pi_task::{PiTaskExecution, PiTaskInput, NATIVE_LAUNCH_TASK_COMMAND_KIND}};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_fs::{CancellationToken, GenerationId, Digest, LazySnapshotRef, WorkBudget, NativeMaterializationWork};
use acyclic_harness::{AgentId, Capabilities, Error, OperationId, Outcome, Result, TaskId,
    core::{AggregateKind, Authority, Reducer, SchemaRegistry, Snapshot},
    conversation::{FileRef, VolumeRef},
    distributed::{Worker, WorkPull},
    durable_host::TaskJournalOwner,
    filesystem::{FilesystemHost, ForkPreparationLimits, RecursiveForkConfig,
        RecursiveSourceSnapshot, FORK_PREPARE_TASK_COMMAND_KIND},
    fork::{ForkReport, ForkRequest},
    resource_preparation::ResourcePreparationRequest,
    runtime::{DurableTaskHost, TaskAdmissionRecord, task_id_for_admission},
    scheduler::{LeaseFence, ResourceSnapshot},
    source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider},
    store::{HistoryIo, HistoryReadLimits, StreamAggregate},
    workflow::WorkflowCommand};
use acyclic_stream::{LocalStream};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

type Runtime = PiTaskRuntime<LocalStream>;
type Filesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

const SDK_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/sdk-operation/v1\0";
const FORK_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/fork-operation/v1\0";

pub(crate) fn original_operation(root: TaskId, caller: TaskId, sdk_tool_call_id: &str,
    work: &NativeMaterializationWork) -> Result<OperationId> {
    let bytes = (SDK_OPERATION_DOMAIN.len() as u64).checked_add(16 + 16 + 8)
        .and_then(|bytes| bytes.checked_add(sdk_tool_call_id.len() as u64))
        .ok_or(Error::NativeWork { cause: acyclic_fs::WorkError::Overflow.into(), custody: None })?;
    work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut hash = blake3::Hasher::new();
    hash.update(SDK_OPERATION_DOMAIN);
    hash.update(&root.into_bytes());
    hash.update(&caller.into_bytes());
    hash.update(&(sdk_tool_call_id.len() as u64).to_le_bytes());
    hash.update(sdk_tool_call_id.as_bytes());
    Ok(OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().expect("fixed digest prefix")))
}

pub(crate) fn derived(original: OperationId, role: &str, work: &NativeMaterializationWork) -> Result<OperationId> {
    let bytes = (FORK_OPERATION_DOMAIN.len() as u64).checked_add(16 + 8)
        .and_then(|bytes| bytes.checked_add(role.len() as u64))
        .ok_or(Error::NativeWork { cause: acyclic_fs::WorkError::Overflow.into(), custody: None })?;
    work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut hash = blake3::Hasher::new();
    hash.update(FORK_OPERATION_DOMAIN);
    hash.update(&original.into_bytes());
    hash.update(&(role.len() as u64).to_le_bytes());
    hash.update(role.as_bytes());
    Ok(OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().expect("fixed digest prefix")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkOperations {
    pub(crate) child: OperationId,
    pub(crate) preparation: OperationId,
    pub(crate) fork: OperationId,
    pub(crate) native: OperationId,
    pub(crate) agent: AgentId,
}
impl PiForkOperations {
    pub(crate) fn new(original: OperationId, work: &NativeMaterializationWork) -> Result<Self> {
        Ok(Self { child: derived(original, "child-task", work)?, preparation: derived(original, "preparation-task", work)?,
            fork: derived(original, "fork", work)?, native: derived(original, "native-effect", work)?,
            agent: AgentId::from_bytes(derived(original, "child-agent", work)?.into_bytes()) })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedSpawnCommit {
    pub(crate) sdk_operation: String,
    pub(crate) alias: String,
    pub(crate) prompt: String,
    pub(crate) caller: TaskId,
    pub(crate) source_task: TaskId,
    pub(crate) child: PiTaskBinding,
    pub(crate) launch: PiNativeLaunchIntent,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkPlan {
    pub(crate) original: OperationId,
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) preparation: TaskAdmissionRecord,
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) source: RecursiveSourceSnapshot,
    pub(crate) frozen: LazySnapshotRef,
    pub(crate) project_creation: acyclic_fs::OperationId,
    pub(crate) history_limits: HistoryReadLimits,
    #[serde(skip)]
    _input_allocations: Option<Arc<acyclic_fs::NativeAllocation>>,
}

impl PartialEq for PiForkPlan {
    fn eq(&self, other: &Self) -> bool {
        self.original == other.original && self.commit == other.commit
            && self.preparation == other.preparation && self.request == other.request
            && self.source == other.source
            && self.frozen == other.frozen
            && self.project_creation == other.project_creation && self.history_limits == other.history_limits
    }
}

pub(crate) const FORK_PLAN_DIAGNOSTIC_LABEL_BYTES: u64 = crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiForkPlan", "original", "commit", "preparation", "request", "source_checkpoint", "source",
        "frozen", "project_creation", "history_limits", "RetainedSpawnCommit",
        "sdk_operation", "alias", "prompt", "caller", "source_task", "child", "launch",
        "TaskAdmissionRecord", "operation_id", "task", "machine", "input", "input_schema",
        "output_schema", "parent", "resource_proofs", "grants", "limits", "run_limits", "policy",
        "extensions", "execution", "ComponentIdentity", "MachineIdentity", "name", "version", "digest",
        "TaskRunLimits", "concurrency", "max_steps", "deadline_epoch_ms", "ResourceCreationProofRef",
        "provider", "resource", "evidence", "ResourcePreparationRequest", "resources", "creation_grant",
        "maximum_resources", "maximum_work", "maximum_bytes", "payload", "command", "input_files",
        "read_authorization", "RecursiveSourceSnapshot", "project", "project_generation", "private_volume",
        "private_generation", "HistoryReadLimits", "maximum_events", "LazySnapshotRef", "id", "workspace_id",
        "authored_generation", "overlay", "shadows", "SourceReference", "identity", "epoch",
        "ExtensionAdmission", "source", "selected", "configurations", "ExecutionPlacement", "build",
        "environment", "readiness_revision", "LeaseFence", "reservation_id", "placement", "an array of length 16",
    ]);

pub(crate) struct PreparedPiFork {
    pub(crate) binding: PiTaskBinding,
    pub(crate) report: ForkReport,
    pub(crate) admission: TaskAdmissionRecord,
    pub(crate) source: RecursiveSourceSnapshot,
    pub(crate) frozen_source: LazySnapshotRef,
    pub(crate) native_lazy: LazySnapshotRef,
    pub(crate) workspace: Arc<LocalLazyWorkspace>,
    pub(crate) _metadata_custody: acyclic_fs::NativeAllocation,
}

impl PreparedPiFork {
    pub(crate) fn into_native_shared(mut self) -> Result<Arc<Self>> {
        let mut slot = { let claim_bytes = (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64; self._metadata_custody.work().claim(claim_bytes, u64::from(claim_bytes != 0), acyclic_fs::WorkCounters::UNCHARGED) }
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self._metadata_custody.absorb(&mut slot).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(self))
    }
}

pub(crate) struct PiForkCoordinator {
    runtime: Arc<Runtime>,
    filesystem: Arc<Filesystem>,
    stream: std::sync::Arc<LocalStream>,
    resources: ServiceResources,
    owner: Arc<PiOwnerConfig>,
    work: NativeMaterializationWork,
    _resource_custody: acyclic_fs::NativeAllocation,
}
impl PiForkCoordinator {

    pub(crate) fn new(runtime: Arc<Runtime>, filesystem: Arc<Filesystem>,
        stream: std::sync::Arc<LocalStream>, resources: &ServiceResources, owner: Arc<PiOwnerConfig>,
        work: &NativeMaterializationWork) -> Result<Self> {
        runtime.verify_owner_with_native_work(&owner, work)?;
        let resources = crate::pi_root_provider::service_resources_with_native_work(resources, work)?;
        Ok(Self { runtime, filesystem, stream, resources: resources.0, owner, work: work.clone(),
            _resource_custody: resources.1 })
    }




    fn remaining(&self, fallback: WorkBudget) -> Result<WorkBudget> {
        self.work.remaining().map(|remaining| remaining.intersect_budget(fallback))
            .map_err(|cause| Error::NativeWork { cause, custody: None })
    }


    async fn authenticate(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, fence: &LeaseFence) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        let host = self.runtime.runtime.task_host();
        let admission = { let work = &self.work;
            self.runtime.verify_owner_with_native_work(&self.owner, work)?;
            owner.verify_with_native_work(false, work).await?;
            let (value, allocation) = host.observe_admission_with_native_work(owner.task_id(), work).await?;
            (value, allocation)
         };
        if context.durable_task_id() != Some(owner.task_id()) || context.owned_task_fence()? != *fence
            || admission.0.grants != *context.scope().grants() || admission.0.limits != context.scope().limits()
            || admission.0.run_limits != context.scope().run_limits() {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork context differs from the original caller admission or fence" });
        }
        Ok(admission)
    }


    /// The same original-root policy seals inspect/import/fork reads.
    #[allow(clippy::too_many_arguments, reason = "exact original caller, source selection and bounds")]
    pub(crate) async fn authorize_selected_project_with_native_work(
        &self, context: &acyclic_harness::runtime::TaskContext,
        owner: &Arc<TaskJournalOwner<LocalStream>>, source_task: TaskId, source: Arc<LocalLazyWorkspace>,
        frozen: LazySnapshotRef, operation: OperationId, maximum_work: u64, maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(acyclic_harness::source_authorization::ProjectSourceReadHandle, acyclic_fs::NativeAllocation)> {
        self.runtime.verify_owner_with_native_work(&self.owner, work)?;
        crate::pi_inspection::authorize_selected_source(&self.runtime.runtime, self.owner.policy().root_task,
            context, owner, source_task, self.owner.policy().session_limits.depth, work).await?;
        let admitted = self.runtime.runtime.task_host().observe_admission_with_native_work(source_task, work).await?;
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(&admitted.0.input, maximum, work)?;
        let original = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(&bytes.0, maximum, maximum,
                acyclic_harness::wire::ErrorCode::Invalid, acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work)?;
        let project = original.0.binding.project_volumes.values().find(|volume|
            volume.storage_name().is_ok_and(|name| name == source.workspace().name().as_str()))
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi selected project has no actual original admitted source binding" })?;
        #[derive(Serialize)]
        struct Generation<'a> {
            kind: acyclic_harness::resources::ResourceKind,
            provider: &'a acyclic_harness::resources::ProviderRef,
            key: &'a [u8], version: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct Selection<'a> {
            project: &'a VolumeRef, generation: Generation<'a>, native_snapshot: &'a LazySnapshotRef,
        }
        let bytes = crate::pi_root_provider::canonical_pi_value(&Selection {
            project, generation: Generation { kind: acyclic_harness::resources::ResourceKind::Generation,
                provider: project.provider(), key: frozen.authored_generation.digest().as_bytes(), version: None },
            native_snapshot: &frozen,
        }, maximum, work)?;
        let selected = crate::pi_root_provider::guarded_pi_decode::<ProjectSourceSnapshot>(
            &bytes.0, maximum, maximum, acyclic_harness::wire::ErrorCode::Invalid, acyclic_harness::JsonValueSchema::ProjectSourceAuthorization,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, work)?;
        let native_snapshot = selected.0.native_snapshot.as_ref().ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi selected native cut is absent" })?;
        let policy = PiProjectSourcePolicy { coordinator: self, source, frozen, caller: owner.task_id(),
            caller_context: context, caller_owner: owner, selected: source_task, operation,
            native_snapshot };
        self.owner.authorize_project_source_with_native_work(owner, context, &policy, source_task,
            operation, &selected.0, maximum_work, maximum_bytes, work).await
    }

    async fn selected_graph(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, source: TaskId) -> Result<()> { let work = &self.work;
            return crate::pi_inspection::authorize_selected_source(&self.runtime.runtime,
                self.owner.policy().root_task, context, owner, source,
                self.owner.policy().session_limits.depth, work).await; }
    async fn authenticate_launch(&self, context: &acyclic_harness::runtime::TaskContext,
        plan: &PiForkPlan) -> Result<()> {
        let child_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&plan.request.child.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let PiTaskExecution::Child { launch } = &child_input.0.execution else {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi retained child is not its native launch task" });
        };
        let reference = crate::pi_root_provider::observe_pi_input(&self.owner, &self.filesystem, derived(plan.original, "native-input", &self.work)?, "native-input", &self.work).await?
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi launch input has no original immutable publication" })?;
        if launch.payload != reference.0 {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native launch changed its original immutable input locator" });
        }
        self.require_input_read(context)?;
        let bytes = self.read_original_input(&reference.0).await?;
        let actual: (PiNativeLaunchIntent, acyclic_fs::NativeAllocation) = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::None, crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES)?;
        if actual.0 != plan.commit.launch {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native SDK source cut, session bytes, or invocation changed" });
        }
        Ok(())
    }

    fn native_copy<T: serde::de::DeserializeOwned, S: Serialize>(&self, value: &S,
        schema: acyclic_harness::JsonValueSchema, work: &NativeMaterializationWork) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
        crate::pi_root_provider::guarded_pi_decode(&bytes.0, maximum, maximum, acyclic_harness::wire::ErrorCode::Invalid, schema, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, work)
    }

    async fn native_generation(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, volume: &VolumeRef, work: &NativeMaterializationWork)
        -> Result<(acyclic_harness::resources::GenerationRef, acyclic_fs::NativeAllocation)> {
        context.verify_original_volume_read_with_native_work(owner, volume, work).await?;
        let name = volume.storage_name_with_native_work(work)?;
        let workspace = self.resources.fs.open_workspace_with_native_work(&name.0, work, work.cancellation()).await
            .map_err(native_workspace_error)?;
        (owner).verify_with_native_work(false, &self.work).await?;
        let head = workspace.head(work, work.cancellation()).await.map_err(|error| native_workspace_error((error, None)))?;
        (owner).verify_with_native_work(false, &self.work).await?;
        acyclic_harness::resources::GenerationRef::new_with_native_work(
            volume.provider(), head.id().digest().as_bytes(), None, work)
    }

    #[allow(clippy::too_many_arguments, reason = "exact caller snapshot descriptors remain borrowed")]
    fn native_source_snapshot(&self, task: TaskId, project: &VolumeRef,
        project_generation: &acyclic_harness::resources::GenerationRef, private_volume: &VolumeRef,
        private_generation: &acyclic_harness::resources::GenerationRef, work: &NativeMaterializationWork)
        -> Result<(RecursiveSourceSnapshot, acyclic_fs::NativeAllocation)> {
        #[derive(Serialize)]
        struct Source<'a> {
            task: TaskId, project: &'a VolumeRef, project_generation: &'a acyclic_harness::resources::GenerationRef,
            private_volume: &'a VolumeRef, private_generation: &'a acyclic_harness::resources::GenerationRef,
        }
        self.native_copy(&Source { task, project, project_generation, private_volume, private_generation },
            acyclic_harness::JsonValueSchema::None, work)
    }

    #[allow(clippy::too_many_arguments, reason = "actual original owner, canonical parent, and native cut are independent authorities")]
    pub(crate) async fn begin(&self, shared: &Arc<acyclic_harness::runtime::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, parent: StreamAggregate<LocalStream>,
        source: Arc<LocalLazyWorkspace>, frozen: LazySnapshotRef, mut commit: RetainedSpawnCommit,
        limits: ForkPreparationLimits, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<PiForkPlan> {
        let context = shared.context();
        let work = &self.work;
        let admission = self.authenticate(context, owner, fence).await?;
        let caller_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&admission.0.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let caller = &caller_input.0;
        let original = original_operation(self.owner.policy().root_task, owner.task_id(), &commit.sdk_operation, &self.work)?;
        let operations = PiForkOperations::new(original, &self.work)?;
        if commit.sdk_operation.is_empty() || commit.alias.is_empty() || commit.caller != owner.task_id()
            || commit.alias != commit.launch.agent_alias
            || commit.child.task_id != task_id_for_admission(operations.child) || commit.child.agent_id != operations.agent
            || commit.child.native_session_id != commit.launch.child_session_id
            || commit.prompt != commit.launch.invocation.prompt
            || commit.launch.source.parent_session_id != caller.binding.native_session_id
            || !matches!(commit.child.scratch.allocation, PiScratchAllocation::Preparing) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi spawn changed its original SDK, native, or task identity" });
        }
        commit.launch.validate(work)?;
        commit.launch.invocation.environment.retain(|key, _| !reserved_native_key(key));
        if let Some(plan) = self.load_plan(context, original).await? {
            self.validate_plan(&plan, owner.task_id())?;
            if plan.commit != commit {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi SDK retry changed its immutable spawn commit" });
            }
            self.prepare_native_source(owner, &plan, source, &cancel).await?;
            return Ok(plan);
        }
        let authority = self.owner.conversation_authority_with_native_work(owner.task_id(),
            caller.binding.agent_id, context.scope().grants(), work)?;
        let verifier = authority.0.1.verifier_with_native_work(work)?;
        let signed = &authority.0.2;
        let caller_id = work.display(&owner.task_id()).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if parent.reducer().authority().kind != AggregateKind::Conversation
            || parent.reducer().authority().id != caller_id.0
            || parent.reducer().conversation().and_then(|state| state.agent) != Some(caller.binding.agent_id) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork requires the actual bound caller conversation" });
        }
        verifier.0.verify_audience(parent.reducer().authority())?;
        self.selected_graph(context, owner, commit.source_task).await?;
        let source_admission = self.runtime.runtime.task_host().observe_admission_with_native_work(commit.source_task, &self.work).await?;
        let source_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&source_admission.0.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let source_input = &source_input.0;
        let mut selected_project = None;
        for (key, volume) in &source_input.binding.project_volumes {
            let name = volume.storage_name_with_native_work(work)?;
            if name.0 == source.workspace().name().as_str() { selected_project = Some((key, volume)); break; }
        }
        let (root_key, project) = selected_project.ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source handle is not its originally admitted project" })?;
        let generation = acyclic_harness::resources::GenerationRef::new_with_native_work(
            project.provider(), frozen.authored_generation.digest().as_bytes(), None, work)?;
        let scratch = &caller.binding.scratch.volume;
        let private_generation = self.native_generation(context, owner, scratch, work).await?;
        let mut source_snapshot = self.native_source_snapshot(owner.task_id(), project, &generation.0,
            scratch, &private_generation.0, work)?;
        let original_caller_project = caller.binding.project_volumes.get(root_key)
            .or_else(|| caller.binding.project_volumes.values().next()).ok_or_else(|| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi caller has no actual project binding" })?;
        let caller_generation = if original_caller_project == project {
            acyclic_harness::resources::GenerationRef::new_with_native_work(
                project.provider(), frozen.authored_generation.digest().as_bytes(), None, work)?
        } else { self.native_generation(context, owner, original_caller_project, work).await? };
        let caller_snapshot = self.native_source_snapshot(owner.task_id(), original_caller_project, &caller_generation.0,
            scratch, &private_generation.0, work)?;
        let source_read = if commit.source_task != owner.task_id() {
            Some(self.authorize_selected_project_with_native_work(context, owner, commit.source_task,
                source.clone(), frozen, operations.fork, limits.work, limits.bytes, work).await?)
        } else { None };
        if commit.child.project_volumes.len() != 1 { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi fork requires its one actually selected child project" }); }
        let child_project = commit.child.project_volumes.get(root_key).ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi child changed its selected root key" })?;
        let results = commit.child.results.as_ref().ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi child requires its separate unmounted result allocation" })?;
        if !matches!(results.allocation, PiScratchAllocation::Preparing) { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi immutable result allocation must remain Preparing" }); }
        let child_project = child_project.clone_with_native_work(work)?;
        let child_private = commit.child.scratch.volume.clone_with_native_work(work)?;
        let child_result = results.volume.clone_with_native_work(work)?;
        let child_id = work.display(&commit.child.task_id).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        #[derive(Serialize)]
        struct Arguments<'a> { id: AgentId, msg: &'a str, source: Option<TaskId> }
        let arguments = self.native_copy::<acyclic_harness::filesystem::ForkToolArguments, _>(
            &Arguments { id: operations.agent, msg: &commit.prompt, source: Some(commit.source_task) },
            acyclic_harness::JsonValueSchema::None, work)?;
        let history_limits = HistoryReadLimits { maximum_events: limits.work.min(u64::from(u32::MAX)) as u32,
            maximum_bytes: limits.bytes };
        let config = RecursiveForkConfig { child: Authority { kind: AggregateKind::Conversation, id: child_id.0 },
            child_project: child_project.0, child_private: child_private.0, child_result_volume: Some(child_result.0),
            limits: context.scope().limits(), inherited_messages: context.scope().limits().context_messages as u64,
            inherited_bytes: limits.bytes, inherited_references: limits.resources, history_limits };
        let checkpoint_snapshot = parent.reducer().snapshot_native(usize::MAX, work)?;
        let checkpoint_bytes = crate::pi_root_provider::guarded_pi_bytes(&checkpoint_snapshot,
            self.owner.policy().maximum_payload_bytes, work)?;
        let checkpoint_path = work.display(&format_args!("pi-fork/{original}/source-checkpoint.json"))
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let checkpoint = (context).stage_file_once_with_native_work(derived(original, "source-checkpoint", &self.work)?, &checkpoint_path.0, &checkpoint_bytes.0, "application/json", "Pi original caller cut", &self.work).await?;
        let resolver = shared.content_residency_verifier_with_native_work(owner.clone(), work)?;
        let stream_provider = acyclic_harness::resources::ProviderRef::new_with_native_work("local", "stream", "1", work)?;
        let source_binding = acyclic_harness::filesystem::recursive_fork_source_with_native_work(self.filesystem.clone(),
            &self.stream, parent.into_reducer(), &verifier.0, signed, context, &caller_snapshot.0,
            &stream_provider.0, operations.fork, &arguments.0, &config,
            source_read.as_ref().map(|pair| &pair.0), resolver, &cancel, work).await?;
        let capture = PiLazyCapture::new(source.clone(), frozen, self.remaining(budget)?, work).await?;
        let mut request = source_binding.0.request;
        let mut request_custody = source_binding.1;
        let selection = capture.selection(work)?;
        request.add_selection_with_native_work(selection.0, &mut request_custody, selection.1, work)?;
        let preparer = Arc::try_unwrap(source_binding.0.preparer).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi original native preparer escaped its construction" })?;
        let preparer = preparer.with_capture_provider_with_native_work(capture, work)?;
        let mut source_read_custody = source_read.map(|pair| pair.1);
        let snapshot = source_snapshot.0;
        request_custody.absorb(&mut source_snapshot.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let input_read = self.owner.policy().journal.directory_read_capability_with_native_work(
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY, work)?;
        if !context.scope().grants().contains(&input_read.0) { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi caller lacks immutable input-directory READ" }); }
        let mut launch_reference = crate::pi_root_provider::stage_pi_input(&self.owner, &self.filesystem, derived(original, "native-input", &self.work)?, "native-input", &commit.launch, &self.work).await?;
        #[derive(Serialize)]
        struct Command<'a> { operation_id: OperationId, kind: &'a str, payload: &'a FileRef }
        let input = crate::pi_root_provider::guarded_pi_value(&PiTaskInput::<_, crate::pi_root::PiRootIntent, _> { binding: &commit.child,
            execution: PiTaskExecution::Child { launch: Command { operation_id: operations.native,
                kind: NATIVE_LAUNCH_TASK_COMMAND_KIND, payload: &launch_reference.0 } } },
            self.owner.policy().maximum_payload_bytes, work)?;
        let mut child = self.runtime.runtime.harness().registered_child_admission_with_native_work(operations.child,
            "acyclic.pi", "1", input, owner.task_id(), context.scope(), work)?;
        let mut child_grants = Capabilities::new_with_native_work(child.0.grants.iter().filter(|grant|
            !grant.starts_with("volume:") && !grant.starts_with("directory:") && !grant.starts_with("file:"))
            .chain(std::iter::once(input_read.0.as_str())), work)?;
        child.0.grants = child_grants.0;
        child.0.resource_proofs.clear();
        child.1.absorb(&mut child_grants.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let bytes = crate::pi_root_provider::guarded_pi_bytes(&request, self.owner.policy().maximum_payload_bytes, work)?;
        let fork_path = work.display(&format_args!("pi-fork/{original}/fork-input.json"))
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut fork_input = (context).stage_file_once_with_native_work(derived(original, "fork-input", &self.work)?, &fork_path.0, &bytes.0, "application/json", "Pi original fork input", &self.work).await?;
        let command_kind = work.display(&FORK_PREPARE_TASK_COMMAND_KIND)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let command = WorkflowCommand { operation_id: operations.fork, kind: command_kind.0, payload: fork_input.0 };
        let project_creation = preparer.project_creation_operation(&request)?;
        let mut checkpoint_copy = self.native_copy::<FileRef, _>(&checkpoint.0, acyclic_harness::JsonValueSchema::None, work)?;
        drop(checkpoint);
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(work)?;
        let prepared_request = acyclic_harness::filesystem::fork_preparation_request(&preparer,
            &request, task_id_for_admission(operations.preparation), child.0, command, limits,
            Some(checkpoint_copy.0), Vec::new(), Some(&io))?;
        let retained = io.owned_bytes()?;
        let mut prepared_request = (prepared_request, io.into_native_custody(retained)?);
        let input = crate::pi_root_provider::guarded_pi_value(&prepared_request.0,
            self.owner.policy().maximum_payload_bytes, work)?;
        let mut prepared_admission = self.runtime.runtime.harness().registered_child_admission_with_native_work(
            operations.preparation, "acyclic.fork_preparation", "1", input, owner.task_id(), context.scope(), work)?;
        request_custody.absorb(&mut prepared_request.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        request_custody.absorb(&mut prepared_admission.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        request_custody.absorb(&mut child.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        request_custody.absorb(&mut checkpoint_copy.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let request = prepared_request.0;
        let preparation = prepared_admission.0;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        (owner).verify_with_native_work(false, &self.work).await?;
        let container = { let claim_bytes = (std::mem::size_of::<acyclic_fs::NativeAllocation>()
            + 2 * std::mem::size_of::<usize>()) as u64; work.claim(claim_bytes, u64::from(claim_bytes != 0), acyclic_fs::WorkCounters::UNCHARGED) }.map_err(|cause| Error::NativeWork { cause, custody: None })?;
        drop(launch_reference.0);
        let mut plan = PiForkPlan { original, commit, preparation, request,
            source: snapshot, frozen, history_limits, project_creation,
            _input_allocations: Some(Arc::new(container)) };
        let allocations = Arc::get_mut(plan._input_allocations.as_mut().expect("new plan custody")).expect("unshared new plan");
        for part in [&mut fork_input.1, &mut launch_reference.1, &mut request_custody]
            .into_iter().chain(source_read_custody.as_mut()) {
            allocations.absorb(part).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let mut publication = crate::pi_root_provider::stage_pi_input(&self.owner, &self.filesystem, original, "fork-plan", &plan, &self.work).await?;
        drop(publication.0);
        Arc::get_mut(plan._input_allocations.as_mut().expect("new plan custody")).expect("unshared new plan")
            .absorb(&mut publication.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.prepare_native_source(owner, &plan, source, &cancel).await?;
        Ok(plan)
    }

    pub(crate) async fn load_plan(&self, context: &acyclic_harness::runtime::TaskContext,
        original: OperationId,
    ) -> Result<Option<PiForkPlan>> {
        self.require_input_read(context)?;
        self.load_original_plan(original).await
    }

    /// Reads only the original protected piInputs directory.
    pub(crate) async fn load_original_plan(&self, original: OperationId) -> Result<Option<PiForkPlan>> {
        self.runtime.verify_owner_with_native_work(&self.owner, &self.work)?;
        let Some(mut reference) = crate::pi_root_provider::observe_pi_input(&self.owner, &self.filesystem, original, "fork-plan", &self.work).await? else { return Ok(None); };
        let bytes = self.read_original_input(&reference.0).await?;
        let mut decoded: (PiForkPlan, acyclic_fs::NativeAllocation) = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::RecursiveForkToolPlan, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?;
        self.validate_plan(&decoded.0, decoded.0.commit.caller)?;
        if decoded.0.original != original { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi original plan locator changed" }); }
        { let work = &self.work;
            let container = { let claim_bytes = (std::mem::size_of::<acyclic_fs::NativeAllocation>()
                + 2 * std::mem::size_of::<usize>()) as u64; work.claim(claim_bytes, u64::from(claim_bytes != 0), acyclic_fs::WorkCounters::UNCHARGED) }.map_err(|cause| Error::NativeWork { cause, custody: None })?;
            drop(reference.0);
            decoded.0._input_allocations = Some(Arc::new(container));
            let allocations = Arc::get_mut(decoded.0._input_allocations.as_mut().expect("loaded plan custody")).expect("unshared loaded plan");
            allocations.absorb(&mut reference.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            allocations.absorb(&mut decoded.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
         }
        Ok(Some(decoded.0))
    }

    pub(crate) async fn read_original_input(&self, reference: &FileRef) -> Result<acyclic_stream::OwnedBytes> { let work = &self.work;
            self.runtime.verify_owner_with_native_work(&self.owner, work)?;
            let actual = self.runtime.runtime.readonly_task_context_with_native_work(self.owner.policy().root_task, work).await?;
            return actual.1.context().read_file_with_native_work(reference, work).await; }

    fn require_input_read(&self, context: &acyclic_harness::runtime::TaskContext) -> Result<()> {
        let native = self.owner.policy().journal.directory_read_capability_with_native_work(
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY, &self.work)?;
        let capability = native.0.as_str();
        if !context.scope().grants().contains(capability) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller lacks its original immutable input-directory read grant" });
        }
        Ok(())
    }

    fn decode_original_input<T: serde::de::DeserializeOwned>(&self, bytes: &[u8], value_schema: acyclic_harness::JsonValueSchema, schema_label_bytes: u64) -> Result<(T, acyclic_fs::NativeAllocation)> { let work = &self.work; crate::pi_root_provider::guarded_pi_decode(bytes, self.owner.policy().maximum_payload_bytes,
                self.owner.policy().maximum_payload_bytes, acyclic_harness::wire::ErrorCode::Invalid, value_schema, schema_label_bytes, work) }

    fn decode_input_value<T: serde::de::DeserializeOwned>(&self, value: &serde_json::Value,
        schema: acyclic_harness::JsonValueSchema, labels: u64) -> Result<(T, acyclic_fs::NativeAllocation)> { let work = &self.work;
            let maximum = self.owner.policy().maximum_payload_bytes;
            let bytes = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
            let (value, allocation) = crate::pi_root_provider::guarded_pi_decode(
                &bytes.0, maximum, maximum, acyclic_harness::wire::ErrorCode::Invalid, schema, labels, work)?;
            Ok((value, allocation)) }


    fn same_payload<T: Serialize>(&self, expected: &serde_json::Value, actual: &T) -> Result<bool> { let work = &self.work;
                let actual = crate::pi_root_provider::guarded_pi_value(actual, self.owner.policy().maximum_payload_bytes, work)?;
                Ok(expected == &actual.0) }

    async fn admit_original(&self, request: &TaskAdmissionRecord, fence: &LeaseFence) -> Result<()> { let work = &self.work;
                let request = request.clone_with_native_work(work)?;
                let reservation = work.display(&fence.reservation_id)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut placement = work.display(&fence.placement)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut fence = (LeaseFence { reservation_id: reservation.0, placement: placement.0 }, reservation.1);
                fence.1.absorb(&mut placement.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let admitted = self.runtime.runtime.task_host().admit_owned_with_native_work(request, fence, work)?.await?;
                drop(admitted);
                Ok(()) }




    /// Observes genuine completed preparation, admission and historical bind.
    pub(crate) async fn observe_prepared(&self, plan: &PiForkPlan, source: Arc<LocalLazyWorkspace>,
        cancel: CancellationToken) -> Result<Option<PreparedPiFork>> {
        let original = self.load_original_plan(plan.original).await?
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared observation lost its original immutable plan" })?;
        if original != *plan {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared observation substituted its original plan" });
        }
        { let work = &self.work;
            let preparation = match self.runtime.runtime.task_host().observe_admission_with_native_work(plan.preparation.task_id(), &self.work).await {
                Ok(value) => value, Err(Error::NotFound(_)) => return Ok(None), Err(error) => return Err(error),
            };
            if preparation.0 != plan.preparation { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi original preparation admission changed" }); }
            let outcome = self.runtime.runtime.task_host().outcome_with_native_work(plan.preparation.task_id(), &self.work).await?;
            let report: (ForkReport, acyclic_fs::NativeAllocation) = match &outcome.0 {
                Some(Outcome::Succeeded(value)) => self.decode_input_value(value,
                    acyclic_harness::JsonValueSchema::ForkReport, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?,
                _ => return Ok(None),
            };
            report.0.validate_with_native_work(work)?;
            if !self.same_payload(&plan.request.payload, &report.0.request)? { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi actual creator report changed its original request" }); }
            let admission = match self.runtime.runtime.task_host().observe_admission_with_native_work(plan.request.child.task_id(), &self.work).await {
                Ok(value) => value, Err(Error::NotFound(_)) => return Ok(None), Err(error) => return Err(error),
            };
            if admission.0 != plan.request.child { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi original child admission changed" }); }
            let reference = crate::pi_root_provider::observe_pi_input(&self.owner, &self.filesystem, derived(plan.original, "native-input", &self.work)?, "native-input", &self.work).await?.ok_or(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi original native input disappeared" })?;
            let child: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&admission.0.input,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
            let PiTaskExecution::Child { launch } = &child.0.execution else { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi admitted child substituted Root execution" }); };
            let bytes = self.read_original_input(&reference.0).await?;
            let intent: (PiNativeLaunchIntent, acyclic_fs::NativeAllocation) = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::None, crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES)?;
            if launch.payload != reference.0 || intent.0 != plan.commit.launch { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi admitted child changed its sealed native intent" }); }
            let parent_admission = self.runtime.runtime.task_host().observe_admission_with_native_work(plan.commit.caller, &self.work).await?;
            let parent_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&parent_admission.0.input,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
            let parent_authority = self.owner.conversation_authority_with_native_work(plan.commit.caller,
                parent_input.0.binding.agent_id, &parent_admission.0.grants, work)?;
            let parent_verifier = parent_authority.0.1.verifier_with_native_work(work)?;
            let child_authority = self.owner.conversation_authority_with_native_work(plan.request.child.task_id(),
                plan.commit.child.agent_id, &admission.0.grants, work)?;
            let child_verifier = child_authority.0.1.verifier_with_native_work(work)?;
            let parent = acyclic_harness::store::HistoryReader::new_with_native_work(
                &self.stream, &parent_authority.0.0, &parent_verifier.0, work)?;
            let child = acyclic_harness::store::HistoryReader::new_with_native_work(
                &self.stream, &child_authority.0.0, &child_verifier.0, work)?;
            let seed = report.0.clone_with_native_work(work)?;
            let seed_value = seed.0.into_seed()?;
            child.0.verify_published_child_with_native_work(&parent.0, &seed_value, plan.history_limits, work).await?;
            source.validate_snapshot_with_native_work(plan.frozen, work).await.map_err(crate::pi_lazy_capture::native_lazy_error)?;
            return Ok(Some(self.created_fork(plan, report.0, admission.0, source, &cancel, report.1, admission.1).await?));
         }}

    async fn prepare_native_source(&self, owner: &TaskJournalOwner<LocalStream>, plan: &PiForkPlan, source: Arc<LocalLazyWorkspace>, cancel: &CancellationToken) -> Result<()> {
        (owner).verify_with_native_work(false, &self.work).await?;
        { let work = &self.work;
            let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.decode_input_value(&plan.request.payload,
                acyclic_harness::JsonValueSchema::ForkRequest, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?;
            let name = request.0.preparation.child_project_volume.storage_name_with_native_work(work)?;
            let prepared = source.prepare_fork_snapshot_with_native_work(&name.0, plan.frozen,
                plan.project_creation, work, cancel).await.map_err(crate::pi_lazy_capture::native_lazy_error)?;
            if prepared != plan.frozen { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "native preparation changed its immutable source cut" }); }
            (owner).verify_with_native_work(false, &self.work).await?;
            return Ok(());
         }}

    #[allow(clippy::too_many_arguments, reason = "original native owner, source cut, and invocation work stay distinct")]
    async fn native_preparer(&self, shared: &Arc<acyclic_harness::runtime::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, plan: &PiForkPlan, source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget, cancel: &CancellationToken, work: &NativeMaterializationWork)
        -> Result<Arc<acyclic_harness::filesystem::FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>>> {
        let context = shared.context();
        let admission = self.runtime.runtime.task_host().observe_admission_with_native_work(owner.task_id(), &self.work).await?;
        let caller: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&admission.0.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let caller_project = plan.commit.child.project_volumes.keys().next()
            .and_then(|key| caller.0.binding.project_volumes.get(key))
            .or_else(|| caller.0.binding.project_volumes.values().next())
            .ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller has no actual project binding" })?;
        let authority = self.owner.conversation_authority_with_native_work(owner.task_id(),
            caller.0.binding.agent_id, context.scope().grants(), work)?;
        let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.native_copy(&plan.request.payload,
            acyclic_harness::JsonValueSchema::ForkRequest, work)?;
        let checkpoint = context.read_file_with_native_work(
            plan.request.source_checkpoint.as_ref().ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork plan lacks its original source checkpoint",
            })?, work,
        ).await?;
        (owner).verify_with_native_work(false, &self.work).await?;
        let snapshot = Snapshot::decode_with_native_work(checkpoint.as_ref(), plan.request.maximum_bytes, work)?;
        if snapshot.authority != request.0.parent || snapshot.revision != request.0.parent_revision {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi original checkpoint differs from its canonical parent revision" });
        }
        let verifier = authority.0.1.verifier_with_native_work(work)?;
        let reducer = Reducer::restore_native(snapshot, verifier.0, SchemaRegistry::new(), work)?;
        let source_read = match &request.0.authorized_source {
            Some(authorization) => Some(owner.restore_project_source_authorization_with_native_work(
                context, authorization, work)?.await?),
            None => None,
        };
        let resolver = shared.content_residency_verifier_with_native_work(owner.clone(), work)?;
        let verifier = authority.0.1.verifier_with_native_work(work)?;
        let provider = acyclic_harness::resources::ProviderRef::new_with_native_work("local", "stream", "1", work)?;
        let restored = acyclic_harness::filesystem::restore_recursive_fork_source_with_native_work(
            self.filesystem.clone(), &self.stream, reducer, &verifier.0, &authority.0.2,
            caller_project, &provider.0, request, plan.history_limits,
            source_read.as_ref().map(|pair| &pair.0), resolver, cancel, work).await?;
        let capture = PiLazyCapture::new(source, plan.frozen, self.remaining(budget)?, work).await?;
        let preparer = Arc::try_unwrap(restored.0.preparer).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi restored preparer escaped its original native ownership" })?;
        let preparer = preparer.with_capture_provider_with_native_work(capture, work)?;
        if preparer.project_creation_operation(&restored.0.request)? != plan.project_creation {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native companion changed its original filesystem operation" });
        }
        (owner).verify_with_native_work(false, &self.work).await?;
        preparer.into_native_shared(work)
    }

    async fn require_original_plan(&self, plan: &PiForkPlan) -> Result<()> {
        let original = self.load_original_plan(plan.original).await?
            .ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation lost its protected original fork publication" })?;
        if original != *plan {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation substituted its protected original fork publication" });
        }
        Ok(())
    }

    /// The service persists its existing Preparing route after this ordinary admission and before advance may run the provider's FS creator.
    pub(crate) async fn admit_preparation(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, fence: &LeaseFence, plan: &PiForkPlan,
    ) -> Result<()> {
        self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.require_original_plan(plan).await?;
        (owner).verify_with_native_work(false, &self.work).await?;
        self.admit_original(&plan.preparation, fence).await?;
        Ok(())
    }

    /// Drives only the retained ordinary preparation.
    #[allow(clippy::too_many_arguments, reason = "exact parent lease and immutable source selection")]
    /// Advances only the ordinary registered preparation task.
    pub(crate) async fn prepare(&self, shared: &Arc<acyclic_harness::runtime::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<Option<(ForkReport, acyclic_fs::NativeAllocation)>> {
        let context = shared.context();
        let work = &self.work;
        self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.require_original_plan(plan).await?;
        (owner).verify_with_native_work(false, &self.work).await?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        self.authenticate_launch(context, plan).await?;
        let preparer = self.native_preparer(shared, owner, plan, source, budget, &cancel, work).await?;
        let host = self.runtime.runtime.task_host();
        (owner).verify_with_native_work(false, &self.work).await?;
        self.admit_original(&plan.preparation, fence).await?;
        if self.runtime.runtime.task_host().observe_admission_with_native_work(plan.preparation.task_id(), &self.work).await?.0 != plan.preparation {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation belongs to another original operation" });
        }
        if self.runtime.runtime.task_host().outcome_with_native_work(plan.preparation.task_id(), &self.work).await?.0.is_none() {
            let commands = self.runtime.runtime.commands().with_fork_preparer(preparer);
            let recovered = host.recover_work_with_native_work(plan.preparation.task_id(), work).await?;
            match recovered.0 {
                WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => {
                    let result = self.runtime.runtime.resume_task_with_native_work(lease, recovered.1, &commands, 8, work).await?;
                    drop(result);
                }
                WorkPull::Idle => {
                    let worker_id = work.display(&format_args!("pi-fork:{}", self.resources.instance_id))
                        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    let worker = Worker { id: worker_id.0, available: ResourceSnapshot::default(), labels: BTreeMap::new() };
                    let result = self.runtime.runtime.run_operation_with_native_work(&worker,
                        plan.preparation.task_id(), &commands, 8, work).await?;
                    drop(result);
                }
            }
        }
        let outcome = self.runtime.runtime.task_host().outcome_with_native_work(plan.preparation.task_id(), &self.work).await?;
        let report: (ForkReport, acyclic_fs::NativeAllocation) = match &outcome.0 {
            Some(Outcome::Succeeded(value)) => self.decode_input_value(value,
                acyclic_harness::JsonValueSchema::ForkReport, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?,
            Some(Outcome::Failed { .. }) => return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi original preparation failed" }),
            Some(Outcome::Cancelled) => return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi preparation cancelled without admitting a child" }),
            None | Some(Outcome::Indeterminate { .. }) => return Ok(None),
        };
        report.0.validate_with_native_work(work)?;
        if !self.same_payload(&plan.request.payload, &report.0.request)? { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi report changed the retained original request" }); }
        (owner).verify_with_native_work(false, &self.work).await?;
        Ok(Some(report))
    }

    #[allow(clippy::too_many_arguments, reason = "exact parent lease and immutable source selection")]
    pub(crate) async fn advance(&self, shared: &Arc<acyclic_harness::runtime::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<Option<PreparedPiFork>> {
        let context = shared.context();
        let Some(report) = self.prepare(shared, owner, fence, plan, source.clone(), self.remaining(budget)?, cancel.clone()).await? else {
            return Ok(None);
        };
        let host = self.runtime.runtime.task_host();
        (owner).verify_with_native_work(false, &self.work).await?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        self.admit_original(&plan.request.child, fence).await?;
        let admission = self.runtime.runtime.task_host().observe_admission_with_native_work(plan.request.child.task_id(), &self.work).await?;
        if admission.0 != plan.request.child {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi child admission differs from its actual creator request" });
        }
        (owner).verify_with_native_work(false, &self.work).await?;
        self.publish(context, owner, plan, &report.0, source.clone(), self.remaining(budget)?, &cancel).await?;
        let prepared = self.created_fork(plan, report.0, admission.0, source, &cancel, report.1, admission.1).await?;
        (owner).verify_with_native_work(false, &self.work).await?;
        Ok(Some(prepared))
    }

    /// Reopens only a previously published and bound admitted child.
    pub(crate) async fn restore_admitted(&self, shared: &Arc<acyclic_harness::runtime::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, cancel: CancellationToken) -> Result<PreparedPiFork> {
        let context = shared.context();
        let admission = self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, plan.commit.caller)?;
        if owner.task_id() != plan.request.child.task_id() || admission.0 != plan.request.child {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold restore does not own the original admitted child" });
        }
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        let prepared = self.observe_prepared(plan, source, cancel).await?
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi cold child has no successful published original creator" })?;
        (owner).verify_with_native_work(false, &self.work).await?;
        Ok(prepared)
    }

    #[allow(clippy::too_many_arguments, reason = "actual report and admission ownership precede their original guards")]
    async fn created_fork(&self, plan: &PiForkPlan, report: ForkReport, admission: TaskAdmissionRecord, source: Arc<LocalLazyWorkspace>, cancel: &CancellationToken, report_allocation: acyclic_fs::NativeAllocation, mut admission_allocation: acyclic_fs::NativeAllocation) -> Result<PreparedPiFork> { let work = &self.work;
            let project_name = report.request.preparation.child_project_volume.storage_name_with_native_work(work)?;
            let project = self.resources.fs.open_workspace_with_native_work(&project_name.0, work, cancel)
                .await.map_err(native_workspace_error)?;
            let generation = report.captures.iter().find_map(|capture| match capture {
                acyclic_harness::fork::Capture::Captured(resource) => match &resource.revision {
                    acyclic_harness::fork::ResourceRevision::Project { volume, generation }
                        if volume == &report.request.preparation.child_project_volume => Some(generation),
                    _ => None,
                },
                _ => None,
            }).ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi creator report lost its exact child project generation" })?;
            let generation: [u8; 32] = generation.as_resource().key().try_into().map_err(|_| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi created project generation has a different identity shape" })?;
            let (workspace, native_lazy) = source.bind_prepared_fork_with_native_work(plan.frozen, project,
                GenerationId::new(Digest::from_bytes(generation)), plan.project_creation, work, cancel)
                .await.map_err(crate::pi_lazy_capture::native_lazy_error)?;
            let scratch_name = report.child_private_volume.storage_name_with_native_work(work)?;
            let scratch = self.resources.fs.open_workspace_with_native_work(&scratch_name.0, work, cancel)
                .await.map_err(native_workspace_error)?;
            let mut binding = self.native_copy::<PiTaskBinding, _>(&plan.commit.child, acyclic_harness::JsonValueSchema::None, work)?;
            let mut scratch_generation = self.native_copy::<acyclic_harness::resources::GenerationRef, _>(
                &report.child_private_generation, acyclic_harness::JsonValueSchema::None, work)?;
            binding.0.scratch.allocation = PiScratchAllocation::Created(PiScratchCreation {
                workspace_id: scratch.id().into_bytes(), generation: scratch_generation.0 });
            binding.1.absorb(&mut scratch_generation.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let result_binding = binding.0.results.as_mut().ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi actual child lost its unmounted result intent" })?;
            if report.child_result_volume.as_ref() != Some(&result_binding.volume) { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi creator report changed its exact child result volume" }); }
            let generation = report.child_result_generation.as_ref().ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi creator report lost its actual result generation" })?;
            let result_name = result_binding.volume.storage_name_with_native_work(work)?;
            let results = self.resources.fs.open_workspace_with_native_work(&result_name.0, work, cancel)
                .await.map_err(native_workspace_error)?;
            let mut generation = self.native_copy::<acyclic_harness::resources::GenerationRef, _>(
                generation, acyclic_harness::JsonValueSchema::None, work)?;
            result_binding.allocation = PiScratchAllocation::Created(PiScratchCreation {
                workspace_id: results.id().into_bytes(), generation: generation.0 });
            binding.1.absorb(&mut generation.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            binding.0.validate_with_native_work(None, binding.0.project_volumes.keys().map(String::as_str), false, work)?;
            let mut original_source = self.native_copy::<RecursiveSourceSnapshot, _>(&plan.source,
                acyclic_harness::JsonValueSchema::None, work)?;
            let workspace = workspace.into_arc_with_native_work(work).map_err(crate::pi_lazy_capture::native_lazy_error)?;
            let mut prepared = PreparedPiFork { binding: binding.0, report, admission, source: original_source.0,
                frozen_source: plan.frozen, native_lazy, workspace, _metadata_custody: report_allocation };
            prepared._metadata_custody.absorb(&mut admission_allocation).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            prepared._metadata_custody.absorb(&mut binding.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            prepared._metadata_custody.absorb(&mut original_source.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            return Ok(prepared); }

    async fn publish(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, plan: &PiForkPlan, report: &ForkReport,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken,
    ) -> Result<()> { let work = &self.work;
            let admission = self.runtime.runtime.task_host().observe_admission_with_native_work(owner.task_id(), &self.work).await?;
            let parent_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&admission.0.input,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
            let authority = self.owner.conversation_authority_with_native_work(owner.task_id(),
                parent_input.0.binding.agent_id, context.scope().grants(), work)?;
            let verifier = authority.0.1.verifier_with_native_work(work)?;
            let mut seed_io = acyclic_harness::store::native_work_io(work)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            seed_io.retain_owned(std::mem::size_of::<ForkReport>() as u64)?;
            let (seed, _) = report.copy(&mut Some(&mut seed_io))?;
            let seed_value = seed.into_seed(&mut Some(&mut seed_io))?;
            let _seed_custody = seed_io.into_custody();
            let capture = PiLazyCapture::new(source, plan.frozen, self.remaining(budget)?, work).await?;
            let filesystem: Arc<dyn acyclic_harness::fork::ForkSeedVerifier> =
                acyclic_harness::filesystem::FilesystemForkVerifier::new_with_native_work(self.filesystem.clone(),
                    plan.request.maximum_bytes, &verifier.0, &authority.0.2, work)?.into_native_shared(work)?;
            let stream_provider = acyclic_harness::resources::ProviderRef::new_with_native_work("local", "stream", "1", work)?;
            let history: Arc<dyn acyclic_harness::fork::ForkSeedVerifier> =
                acyclic_harness::fork::StreamHistoryForkVerifier::new_with_native_work(&stream_provider.0, work)?
                    .into_native_shared(work)?;
            let forks = acyclic_harness::fork::CompositeForkVerifier::new_with_native_work(
                &[filesystem, history, capture], work)?.into_native_shared(work)?;
            let parent = StreamAggregate::open_with_native_work(&self.stream, authority.0.0, verifier.0,
                SchemaRegistry::new(), work).await?;
            let mut parent_value = parent.0;
            parent_value.set_limits(context.scope().limits())?;
            parent_value = parent_value.with_fork_verifier(forks);
            {
                let mut io = acyclic_harness::store::native_work_io(work)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                io.retain_owned(std::mem::size_of::<acyclic_harness::core::ApplyResult>() as u64)?;
                let (report, report_bytes) = report.copy(&mut Some(&mut io))?;
                let (scope, scope_custody) = authority.0.2.clone_with_native_work(work)?;
                let scope_bytes = scope_custody.bytes();
                io.absorb_native_custody(scope_custody)?;
                let published = parent_value.publish_fork_report(report, scope, Some(owner), &mut Some(&mut io)).await;
                let released_report = io.release_owned(report_bytes);
                let released_scope = io.release_owned(scope_bytes);
                let published = published?;
                released_report?;
                released_scope?;
                drop(published);
                drop(io.into_custody());
            }
            let child_authority = self.owner.conversation_authority_with_native_work(
                plan.request.child.task_id(), seed_value.child_agent, &plan.request.child.grants, work)?;
            let child_verifier = child_authority.0.1.verifier_with_native_work(work)?;
            let child = StreamAggregate::open_with_native_work(&self.stream, child_authority.0.0, child_verifier.0,
                SchemaRegistry::new(), work).await?;
            let mut child_value = child.0;
            child_value.set_limits(plan.request.child.limits)?;
            {
                let mut io = acyclic_harness::store::native_work_io(work)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let (scope, scope_custody) = child_authority.0.2.clone_with_native_work(work)?;
                let scope_bytes = scope_custody.bytes();
                io.absorb_native_custody(scope_custody)?;
                let bound = child_value.bind_published_child(&parent_value, &seed_value,
                    scope, Some(owner), &mut Some(&mut io)).await;
                let released = io.release_owned(scope_bytes);
                bound?;
                released?;
            }
            return Ok(()); }

    fn validate_plan(&self, plan: &PiForkPlan, caller: TaskId) -> Result<()> {
        let original = original_operation(self.owner.policy().root_task, caller, &plan.commit.sdk_operation, &self.work)?;
        let operations = PiForkOperations::new(original, &self.work)?;
        { let work = &self.work;
                plan.request.validate(&mut Some(&mut acyclic_harness::store::native_work_io(work)?))?;
                plan.preparation.validate_with_native_work(work)?;
             }
        let child_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(&plan.request.child.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let child_input = &child_input.0;
        let decoded_request: (ForkRequest, acyclic_fs::NativeAllocation) = self.decode_input_value(&plan.request.payload,
            acyclic_harness::JsonValueSchema::ForkRequest, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?;
        let request = &decoded_request.0;
        let child_launch = match &child_input.execution {
            PiTaskExecution::Child { launch } => launch,
            PiTaskExecution::Host { .. } => return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi fork admission substituted a root task" }),
        };
        { let work = &self.work; plan.commit.launch.validate(work)? }
        let project_selected = request.selections.iter().any(|selection| matches!(
            &selection.revision, acyclic_harness::fork::ResourceRevision::Project { volume, generation }
                if volume == &plan.source.project && generation == &plan.source.project_generation));
        let scratch_selected = request.selections.iter().any(|selection| matches!(
            &selection.revision, acyclic_harness::fork::ResourceRevision::PrivateVolume { volume, generation, paths }
                if volume == &plan.source.private_volume && generation == &plan.source.private_generation && paths.is_empty()));
        let native_snapshot = match request.authorized_source.as_ref().and_then(|source| source.native_snapshot.as_ref()) {
            Some(snapshot) => self.same_payload(snapshot, &plan.frozen)?,
            None => plan.commit.source_task == caller,
        };
        let source_authorized = match (&request.authorized_source, plan.commit.source_task == caller) {
            (None, true) => true,
            (Some(source), false) => source.caller_task == caller && source.source_task == plan.commit.source_task
                && source.operation_id == operations.fork && source.graph_root == self.owner.policy().root_task
                && source.project == plan.source.project && source.generation == plan.source.project_generation
                && native_snapshot,
            _ => false,
        };
        if plan.original != original || plan.commit.caller != caller
            || plan.commit.alias != plan.commit.launch.agent_alias
            || plan.preparation.operation_id != operations.preparation
            || plan.preparation.parent != Some(caller)
            || !self.same_payload(&plan.preparation.input, &plan.request)?
            || plan.request.child.operation_id != operations.child
            || plan.request.child.parent != Some(caller)
            || !plan.request.child.resource_proofs.is_empty()
            || plan.request.source_checkpoint.is_none()
            || plan.source.task != caller
            || plan.request.command.as_ref().is_none_or(|command| command.operation_id != operations.fork)
            || request.operation_id != operations.fork || request.child_agent != operations.agent
            || !task_id_text_matches(plan.commit.child.task_id, &request.child.id)
            || child_input.binding != plan.commit.child
            || child_launch.operation_id != operations.native || child_launch.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND
            || !project_selected || !scratch_selected
            || !source_authorized
            || request.preparation.child_result_volume.as_ref() != plan.commit.child.results.as_ref().map(|result| &result.volume)
            || plan.commit.child.results.as_ref().is_none_or(|result| !matches!(result.allocation, PiScratchAllocation::Preparing))
            || plan.commit.launch.invocation.environment.keys().any(|key| reserved_native_key(key))
            || plan.source.project_generation.as_resource().key() != plan.frozen.authored_generation.digest().as_bytes()
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork plan changed the original admission, operation, or caller cut" });
        }
        Ok(())
    }
}

/// RootService's explicit same-session selected-PROJECT policy.
struct PiProjectSourcePolicy<'a> {
    coordinator: &'a PiForkCoordinator,
    source: Arc<LocalLazyWorkspace>,
    frozen: LazySnapshotRef,
    native_snapshot: &'a serde_json::Value,
    caller: TaskId,
    caller_context: &'a acyclic_harness::runtime::TaskContext,
    caller_owner: &'a Arc<TaskJournalOwner<LocalStream>>,
    selected: TaskId,
    operation: OperationId,
}

impl RootProjectSourceProvider for PiProjectSourcePolicy<'_> {
    fn verify<'a>(&'a self, _context: RootProjectSourceContext<'a>, _snapshot: &'a ProjectSourceSnapshot)
        -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi source authorization requires its original native work" }) })
    }

    fn supports_native_source(&self) -> bool { true }

    fn verify_with_native_work<'a>(&'a self, context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot, work: &'a NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, ()>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let coordinator = self.coordinator;
            coordinator.runtime.verify_owner_with_native_work(&coordinator.owner, work)?;
            self.caller_owner.verify_with_native_work(false, work).await?;
            if self.caller_context.durable_task_id() != Some(self.caller)
                || self.caller_owner.task_id() != self.caller
                || context.graph_root != coordinator.owner.policy().root_task
                || context.caller.task_id() != self.caller || context.source.task_id() != self.selected
                || context.operation_id != self.operation || context.caller.task.name != "acyclic.pi"
                || context.source.task.name != "acyclic.pi"
                || snapshot.native_snapshot.as_ref() != Some(self.native_snapshot)
                || snapshot.generation.as_resource().key() != self.frozen.authored_generation.digest().as_bytes()
                || context.maximum_work == 0 || context.maximum_bytes == 0 {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected source changed its original operation, graph or native cut" });
            }
            let admitted_authority = coordinator.owner.authority_with_native_work(work)?;
            if context.owner != admitted_authority.0.verifier.audience() {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected source changed its original protected audience" });
            }
            crate::pi_inspection::authorize_selected_source(&coordinator.runtime.runtime,
                coordinator.owner.policy().root_task, self.caller_context, self.caller_owner,
                self.selected, coordinator.owner.policy().session_limits.depth, work).await?;
            let maximum = coordinator.owner.policy().maximum_payload_bytes;
            let bytes = crate::pi_root_provider::canonical_pi_value(&context.source.input, maximum, work)?;
            let original = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(&bytes.0, maximum, maximum,
                acyclic_harness::wire::ErrorCode::Invalid, acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work)?;
            if !original.0.binding.project_volumes.values().any(|volume| volume == &snapshot.project)
                || snapshot.project.storage_name()? != self.source.workspace().name().as_str() {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected source changed its originally admitted project" });
            }
            self.caller_owner.verify_with_native_work(false, work).await?;
            self.source.validate_snapshot_with_native_work(self.frozen, work).await
                .map_err(crate::pi_lazy_capture::native_lazy_error)?;
            self.caller_owner.verify_with_native_work(false, work).await?;
            if !context.created_project_authenticated {
                if self.selected != context.graph_root {
                    return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi selected child lacks its actual project creator proof" });
                }
                let PiTaskExecution::Host { root, .. } = &original.0.execution else {
                    return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi original Root was not its admitted bootstrap task" });
                };
                self.caller_owner.verify_with_native_work(false, work).await?;
                let actual_source = coordinator.runtime.runtime.readonly_task_context_with_native_work(self.selected, work).await?;
                self.caller_owner.verify_with_native_work(false, work).await?;
                crate::pi_root_provider::prove_root_project_source_with_native_work(coordinator.owner.clone(),
                    &coordinator.resources, coordinator.filesystem.clone(), coordinator.stream.clone(),
                    &original.0.binding, root, &snapshot.project, self.frozen,
                    actual_source.1.clone(), &actual_source.0, work).await?;
                self.caller_owner.verify_with_native_work(false, work).await?;
            }
            Ok(())
        })
    }
}

fn reserved_native_key(key: &str) -> bool {
    ["ACYCLIC_PI_CALLER", "ACYCLIC_PI_ROUTE", "ACYCLIC_PI_TASK", "ACYCLIC_PI_TOKEN", "ACYCLIC_PI_SCOPE", "ACYCLIC_PI_LEASE"]
        .iter().any(|reserved| key.eq_ignore_ascii_case(reserved))
        || key.get(.."ACYCLIC_PI_CALLER_".len()).is_some_and(|prefix| prefix.eq_ignore_ascii_case("ACYCLIC_PI_CALLER_"))
}

#[cfg(test)]
#[path = "pi_fork_tests.rs"]
pub(crate) mod tests;

fn native_workspace_error((source, custody): (acyclic_fs::WorkspaceError, Option<acyclic_fs::NativeAllocation>)) -> Error {
    crate::pi_lazy_capture::native_lazy_error(acyclic_fs::LazyWorkspaceError::NativeWorkspaceCause(
        acyclic_fs::lazy_workspace::NativeLazyWorkspaceCause { source, custody }))
}

fn task_id_text_matches(task: TaskId, expected: &str) -> bool {
    struct Text { bytes: [u8; 64], length: usize }
    impl std::fmt::Write for Text {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let end = self.length.checked_add(value.len()).ok_or(std::fmt::Error)?;
            let destination = self.bytes.get_mut(self.length..end).ok_or(std::fmt::Error)?;
            destination.copy_from_slice(value.as_bytes());
            self.length = end;
            Ok(())
        }
    }
    let mut text = Text { bytes: [0; 64], length: 0 };
    std::fmt::write(&mut text, format_args!("{task}")).is_ok() && &text.bytes[..text.length] == expected.as_bytes()
}
