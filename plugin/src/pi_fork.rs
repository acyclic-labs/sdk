//! Recursive Pi admission over the original task and provider journals.
//! Planned native locators are not creator evidence or execution authority.
//! Native begin/advance retain the SDK-issued context and the original work:
//! current parent checkpoint, immutable qualified source, ordinary preparation,
//! sealed child admission, then guarded publication/binding. Cold observation
//! verifies the existing publication without obtaining a replacement lease.

use crate::{LocalAuthorityBackend, LocalObjectBackend, LocalLazyWorkspace,
    PiScratchAllocation, PiScratchCreation, PiTaskBinding, ServiceResources,
    pi_lazy_capture::PiLazyCapture, pi_native::PiNativeLaunchIntent,
    pi_owner::PiOwnerConfig, pi_runtime::PiTaskRuntime,
    pi_task::{PiTaskExecution, PiTaskInput, NATIVE_LAUNCH_TASK_COMMAND_KIND}};
use acyclic_fs::{CancellationToken, GenerationId, Digest, LazySnapshotRef, WorkBudget, NativeMaterializationWork};
use acyclic_harness::{AgentId, Capabilities, Error, OperationId, Outcome, Result, TaskId,
    core::{AggregateKind, Authority, Reducer, SchemaRegistry, Snapshot},
    conversation::{FileRef, VolumeOperation, VolumeRef},
    distributed::{Worker, WorkPull},
    durable_host::TaskJournalOwner,
    filesystem::{FilesystemHost, ForkPreparationLimits, RecursiveForkConfig,
        RecursiveSourceSnapshot, FORK_PREPARE_TASK_COMMAND_KIND},
    fork::{ForkReport, ForkRequest},
    resource_preparation::ResourcePreparationRequest,
    runtime::{DurableTaskHost, TaskAdmissionRecord, task_id_for_admission},
    scheduler::{LeaseFence, ResourceSnapshot},
    source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider},
    store::{HistoryReadLimits, StreamAggregate},
    workflow::WorkflowCommand};
use acyclic_stream::{LocalStream, StreamClient};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

type Runtime = PiTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
type Filesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

const SDK_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/sdk-operation/v1\0";
const FORK_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/fork-operation/v1\0";
const FORK_OPERATION_ROLES: [&str; 5] = ["child-task", "preparation-task", "fork", "native-effect", "child-agent"];

pub(crate) fn original_operation(root: TaskId, caller: TaskId, sdk_tool_call_id: &str) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(SDK_OPERATION_DOMAIN);
    hash.update(&root.into_bytes());
    hash.update(&caller.into_bytes());
    hash.update(&(sdk_tool_call_id.len() as u64).to_le_bytes());
    hash.update(sdk_tool_call_id.as_bytes());
    OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().expect("fixed digest prefix"))
}

pub(crate) fn original_operation_with_native_work(root: TaskId, caller: TaskId, sdk_tool_call_id: &str,
    work: &NativeMaterializationWork) -> Result<OperationId> {
    let bytes = (SDK_OPERATION_DOMAIN.len() as u64).checked_add(16 + 16 + 8)
        .and_then(|bytes| bytes.checked_add(sdk_tool_call_id.len() as u64))
        .ok_or(Error::NativeWork { cause: acyclic_fs::WorkError::Overflow.into(), custody: None })?;
    work.admit(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let operation = original_operation(root, caller, sdk_tool_call_id);
    work.spent(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(operation)
}

pub(crate) fn derived(original: OperationId, role: &str) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(FORK_OPERATION_DOMAIN);
    hash.update(&original.into_bytes());
    hash.update(&(role.len() as u64).to_le_bytes());
    hash.update(role.as_bytes());
    OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().expect("fixed digest prefix"))
}

pub(crate) fn derived_with_native_work(original: OperationId, role: &str, work: &NativeMaterializationWork)
    -> Result<OperationId> {
    let bytes = (FORK_OPERATION_DOMAIN.len() as u64).checked_add(16 + 8)
        .and_then(|bytes| bytes.checked_add(role.len() as u64))
        .ok_or(Error::NativeWork { cause: acyclic_fs::WorkError::Overflow.into(), custody: None })?;
    work.admit(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let operation = derived(original, role);
    work.spent(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(operation)
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
    pub(crate) fn new(original: OperationId) -> Self {
        Self { child: derived(original, FORK_OPERATION_ROLES[0]), preparation: derived(original, FORK_OPERATION_ROLES[1]),
            fork: derived(original, FORK_OPERATION_ROLES[2]), native: derived(original, FORK_OPERATION_ROLES[3]),
            agent: AgentId::from_bytes(derived(original, FORK_OPERATION_ROLES[4]).into_bytes()) }
    }

    pub(crate) fn new_with_native_work(original: OperationId, work: &NativeMaterializationWork) -> Result<Self> {
        let bytes = FORK_OPERATION_ROLES.iter().try_fold(0_u64, |total, role| total
            .checked_add(FORK_OPERATION_DOMAIN.len() as u64 + 16 + 8 + role.len() as u64)
            .ok_or(Error::NativeWork { cause: acyclic_fs::WorkError::Overflow.into(), custody: None }))?;
        work.admit(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let operations = Self::new(original);
        work.spent(acyclic_fs::WorkCounters { bytes_hashed: bytes, ..acyclic_fs::WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(operations)
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
    pub(crate) source_checkpoint: FileRef,
    pub(crate) source: RecursiveSourceSnapshot,
    pub(crate) caller_project: VolumeRef,
    pub(crate) frozen: LazySnapshotRef,
    pub(crate) project_creation: acyclic_fs::OperationId,
    pub(crate) history_limits: HistoryReadLimits,
    #[serde(skip)]
    _input_allocations: Option<Arc<PiForkInputAllocations>>,
}

impl PartialEq for PiForkPlan {
    fn eq(&self, other: &Self) -> bool {
        self.original == other.original && self.commit == other.commit
            && self.preparation == other.preparation && self.request == other.request
            && self.source_checkpoint == other.source_checkpoint && self.source == other.source
            && self.caller_project == other.caller_project && self.frozen == other.frozen
            && self.project_creation == other.project_creation && self.history_limits == other.history_limits
    }
}

pub(crate) const FORK_PLAN_DIAGNOSTIC_LABEL_BYTES: u64 = crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiForkPlan", "original", "commit", "preparation", "request", "source_checkpoint", "source",
        "caller_project", "frozen", "project_creation", "history_limits", "RetainedSpawnCommit",
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

struct PiForkInputAllocations {
    _references: [Option<acyclic_fs::NativeAllocation>; 4],
    _source_read: Option<acyclic_fs::NativeAllocation>,
    _preparation: Option<acyclic_fs::NativeAllocation>,
    _container: acyclic_fs::NativeAllocation,
}

struct PiStagedInput {
    reference: FileRef,
    allocation: Option<acyclic_fs::NativeAllocation>,
}

pub(crate) struct PiDecodedInput<T> {
    pub(crate) value: T,
    pub(crate) allocation: Option<acyclic_fs::NativeAllocation>,
}

pub(crate) struct PreparedPiFork {
    pub(crate) binding: PiTaskBinding,
    pub(crate) report: ForkReport,
    pub(crate) admission: TaskAdmissionRecord,
    pub(crate) source: RecursiveSourceSnapshot,
    pub(crate) frozen_source: LazySnapshotRef,
    pub(crate) native_lazy: LazySnapshotRef,
    pub(crate) workspace: LocalLazyWorkspace,
    pub(crate) _metadata_custody: [Option<acyclic_fs::NativeAllocation>; 4],
}

impl PreparedPiFork {
    pub(crate) fn original_work(&self) -> Result<&NativeMaterializationWork> {
        self._metadata_custody.iter().flatten().next().map(acyclic_fs::NativeAllocation::work)
            .ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason: "Pi native dispatch requires its original prepared metadata custody" })
    }
}

pub(crate) struct PiForkCoordinator {
    runtime: Arc<Runtime>,
    filesystem: Arc<Filesystem>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    owner: Arc<PiOwnerConfig>,
    work: Option<NativeMaterializationWork>,
    _resource_custody: Option<[acyclic_fs::NativeAllocation; 3]>,
}
impl PiForkCoordinator {
    pub(crate) fn new(runtime: Arc<Runtime>, filesystem: Arc<Filesystem>, stream: StreamClient<LocalStream>,
        resources: ServiceResources, owner: Arc<PiOwnerConfig>) -> Result<Self> {
        runtime.verify_owner(&owner)?;
        Ok(Self { runtime, filesystem, stream, resources, owner, work: None, _resource_custody: None })
    }

    pub(crate) fn new_with_native_work(runtime: Arc<Runtime>, filesystem: Arc<Filesystem>,
        stream: StreamClient<LocalStream>, resources: &ServiceResources, owner: Arc<PiOwnerConfig>,
        work: &NativeMaterializationWork) -> Result<Self> {
        runtime.verify_owner_with_native_work(&owner, work)?;
        let native_error = |cause| Error::NativeWork { cause, custody: None };
        let data = work.copy_path(&resources.data).map_err(native_error)?;
        let binary_identity = work.display(&resources.binary_identity).map_err(native_error)?;
        let instance_id = work.display(&resources.instance_id).map_err(native_error)?;
        let resources = ServiceResources { data: data.0, fs: resources.fs.clone(), store: resources.store.clone(),
            shared_roots: resources.shared_roots.clone(), binary_identity: binary_identity.0, instance_id: instance_id.0,
            private_state_root: resources.private_state_root.clone() };
        Ok(Self { runtime, filesystem, stream, resources, owner, work: Some(work.clone()),
            _resource_custody: Some([data.1, binary_identity.1, instance_id.1]) })
    }

    fn original(&self, caller: TaskId, sdk_tool_call_id: &str) -> Result<OperationId> {
        match &self.work {
            Some(work) => original_operation_with_native_work(self.owner.policy().root_task, caller, sdk_tool_call_id, work),
            None => Ok(original_operation(self.owner.policy().root_task, caller, sdk_tool_call_id)),
        }
    }

    fn operations(&self, original: OperationId) -> Result<PiForkOperations> {
        match &self.work { Some(work) => PiForkOperations::new_with_native_work(original, work),
            None => Ok(PiForkOperations::new(original)) }
    }

    fn derive(&self, original: OperationId, role: &str) -> Result<OperationId> {
        match &self.work { Some(work) => derived_with_native_work(original, role, work),
            None => Ok(derived(original, role)) }
    }


    fn remaining(&self, fallback: WorkBudget) -> Result<WorkBudget> {
        self.work.as_ref().map_or(Ok(fallback), |work| work.remaining()
            .map(|remaining| remaining.intersect_budget(fallback)).map_err(|cause| Error::NativeWork { cause, custody: None }))
    }

    fn accept<T, E: std::fmt::Display>(&self, result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, E>)
        -> Result<acyclic_fs::OperationReceipt<T>> {
        let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        if let Some(work) = &self.work {
            work.spent(spent).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        result.map_err(measured)
    }

    async fn capture(&self, source: Arc<LocalLazyWorkspace>, frozen: LazySnapshotRef,
        budget: WorkBudget, cancel: &CancellationToken) -> Result<Arc<PiLazyCapture>> {
        let capture = PiLazyCapture::new(source, frozen, self.remaining(budget)?, cancel.clone(), self.work.clone()).await?;
        match &self.work { Some(work) => capture.into_arc_with_native_work(work), None => Ok(Arc::new(capture)) }
    }

    fn accept_root<T>(&self, result: crate::pi_root_provider::PiMeasuredResult<acyclic_fs::OperationReceipt<T>>)
        -> Result<acyclic_fs::OperationReceipt<T>> {
        let spent = match &result { Ok(receipt) => receipt.work, Err((_, spent)) => *spent };
        if let Some(work) = &self.work {
            work.spent(spent).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        result.map_err(|(cause, _)| cause)
    }

    async fn verify_owner(&self, owner: &TaskJournalOwner<LocalStream>) -> Result<()> {
        match &self.work { Some(work) => owner.verify_with_native_work(false, work).await,
            None => owner.verify_dispatch().await }
    }

    async fn authenticate(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, fence: &LeaseFence) -> Result<PiDecodedInput<TaskAdmissionRecord>> {
        let host = self.runtime.runtime.task_host();
        let admission = if let Some(work) = &self.work {
            self.runtime.verify_owner_with_native_work(&self.owner, work)?;
            owner.verify_with_native_work(false, work).await?;
            let (value, allocation) = host.observe_admission_with_native_work(owner.task_id(), work).await?;
            PiDecodedInput { value, allocation: Some(allocation) }
        } else {
            self.runtime.verify_owner(&self.owner)?;
            crate::pi_inspection::authenticate(context, owner).await?;
            host.verify_dispatch_owner(owner.task_id(), fence.clone()).await?;
            PiDecodedInput { value: host.observe_admission(owner.task_id()).await?, allocation: None }
        };
        if context.durable_task_id() != Some(owner.task_id()) || context.owned_task_fence()? != *fence
            || admission.value.grants != *context.scope().grants() || admission.value.limits != context.scope().limits()
            || admission.value.run_limits != context.scope().run_limits() {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork context differs from the original caller admission or fence" });
        }
        Ok(admission)
    }

    async fn observe_admission(&self, task: TaskId) -> Result<PiDecodedInput<TaskAdmissionRecord>> {
        if let Some(work) = &self.work {
            let (value, allocation) = self.runtime.runtime.task_host().observe_admission_with_native_work(task, work).await?;
            Ok(PiDecodedInput { value, allocation: Some(allocation) })
        } else {
            Ok(PiDecodedInput { value: self.runtime.runtime.task_host().observe_admission(task).await?, allocation: None })
        }
    }

    /// The same original-root policy seals inspect/import/fork reads. It never
    /// broadens the caller's grants or exports the source's private authority.
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
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(&admitted.0.input, maximum, work)?;
        let original = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(&bytes.0, maximum, maximum,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work)?;
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
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(&Selection {
            project, generation: Generation { kind: acyclic_harness::resources::ResourceKind::Generation,
                provider: project.provider(), key: frozen.authored_generation.digest().as_bytes(), version: None },
            native_snapshot: &frozen,
        }, maximum, work)?;
        let selected = crate::pi_root_provider::guarded_pi_decode::<ProjectSourceSnapshot>(
            &bytes.0, maximum, maximum, acyclic_harness::JsonValueSchema::ProjectSourceAuthorization,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, work)?;
        let native_snapshot = selected.0.native_snapshot.as_ref().ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi selected native cut is absent" })?;
        let policy = PiProjectSourcePolicy { coordinator: self, source, frozen, caller: owner.task_id(),
            caller_context: context, caller_owner: owner, selected: source_task, operation,
            native_snapshot, budget: work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?,
            cancel: work.cancellation().clone() };
        self.owner.authorize_project_source_with_native_work(owner, context, &policy, source_task,
            operation, &selected.0, maximum_work, maximum_bytes, work).await
    }

    async fn selected_graph(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, source: TaskId) -> Result<()> {
        if let Some(work) = &self.work {
            return crate::pi_inspection::authorize_selected_source(&self.runtime.runtime,
                self.owner.policy().root_task, context, owner, source,
                self.owner.policy().session_limits.depth, work).await;
        }
        self.verify_owner(owner).await?;
        if context.durable_task_id() != Some(owner.task_id()) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi selected source changed its original caller" });
        }
        for selected in [owner.task_id(), source] {
            let mut task = selected;
            let mut rooted = false;
            for _ in 0..self.owner.policy().session_limits.depth {
                let admission = self.runtime.runtime.task_host().observe_admission(task).await?;
                if task == self.owner.policy().root_task && admission.parent.is_none() {
                    rooted = true;
                    break;
                }
                task = admission.parent.ok_or_else(|| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi source belongs to another original root" })?;
            }
            if !rooted { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi selected source exceeds its original graph bound" }); }
        }
        self.verify_owner(owner).await
    }
    async fn authenticate_launch(&self, context: &acyclic_harness::runtime::TaskContext,
        plan: &PiForkPlan, budget: WorkBudget, cancel: &CancellationToken) -> Result<()> {
        let child_input: PiDecodedInput<PiTaskInput> = self.decode_input_value(&plan.request.child.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let PiTaskExecution::Child { launch } = &child_input.value.execution else {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi retained child is not its native launch task" });
        };
        let reference = self.observe_original_input(self.derive(plan.original, "native-input")?, "native-input", budget, cancel).await?
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi launch input has no original immutable publication" })?;
        if launch.payload != reference.reference {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native launch changed its original immutable input locator" });
        }
        self.require_input_read(context)?;
        let bytes = self.read_original_input(&reference.reference, budget, cancel).await?;
        let actual: PiDecodedInput<PiNativeLaunchIntent> = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::None,
            crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, budget, cancel)?;
        if actual.value != plan.commit.launch {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native SDK source cut, session bytes, or invocation changed" });
        }
        Ok(())
    }


    fn native_copy<T: serde::de::DeserializeOwned, S: Serialize>(&self, value: &S,
        schema: acyclic_harness::JsonValueSchema, work: &NativeMaterializationWork) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(value, maximum, work)?;
        crate::pi_root_provider::guarded_pi_decode(&bytes.0, maximum, maximum, schema, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, work)
    }

    fn child_content_grants(&self, original: &Capabilities, input_read: &str,
        work: &NativeMaterializationWork) -> Result<(Capabilities, acyclic_fs::NativeAllocation)> {
        Capabilities::new_with_native_work(original.iter().filter(|grant|
            !grant.starts_with("volume:") && !grant.starts_with("directory:") && !grant.starts_with("file:"))
            .chain(std::iter::once(input_read)), work)
    }

    async fn native_generation(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, volume: &VolumeRef, work: &NativeMaterializationWork)
        -> Result<(acyclic_harness::resources::GenerationRef, acyclic_fs::NativeAllocation)> {
        context.verify_original_volume_read_with_native_work(owner, volume, work).await?;
        let name = volume.storage_name_with_native_work(work)?;
        let workspace = self.resources.fs.open_workspace_with_native_work(&name.0, work, work.cancellation()).await
            .map_err(native_workspace_error)?;
        self.verify_owner(owner).await?;
        let head = workspace.head_with_native_work(work, work.cancellation()).await.map_err(native_workspace_error)?;
        self.verify_owner(owner).await?;
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
    pub(crate) async fn begin(&self, shared: &Arc<acyclic_harness::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, parent: StreamAggregate<LocalStream>,
        source: Arc<LocalLazyWorkspace>, frozen: LazySnapshotRef, mut commit: RetainedSpawnCommit,
        limits: ForkPreparationLimits, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<PiForkPlan> {
        let context = shared.context();
        let work = self.work.as_ref().ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi fork admission requires its original native context and operation work" })?;
        let admission = self.authenticate(context, owner, fence).await?;
        let caller_input: PiDecodedInput<PiTaskInput> = self.decode_input_value(&admission.value.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let caller = &caller_input.value;
        let original = self.original(owner.task_id(), &commit.sdk_operation)?;
        let operations = self.operations(original)?;
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
        commit.launch.validate_with_native_work(work)?;
        commit.launch.invocation.environment.retain(|key, _| !reserved_native_key(key));
        if let Some(plan) = self.load_plan(context, original, self.remaining(budget)?, &cancel).await? {
            self.validate_plan(&plan, owner.task_id())?;
            if plan.commit != commit {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi SDK retry changed its immutable spawn commit" });
            }
            self.prepare_native_source(owner, &plan, source, self.remaining(budget)?, &cancel).await?;
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
        let source_admission = self.observe_admission(commit.source_task).await?;
        let source_input: PiDecodedInput<PiTaskInput> = self.decode_input_value(&source_admission.value.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let source_input = &source_input.value;
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
        let mut caller_project = original_caller_project.clone_with_native_work(work)?;
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
        let mut checkpoint = self.stage_input(context, self.derive(original, "source-checkpoint")?,
            &checkpoint_path.0, &checkpoint_bytes.0, "Pi original caller cut").await?;
        let resolver = shared.content_residency_verifier_with_native_work(owner.clone(), work)?;
        let stream_provider = acyclic_harness::resources::ProviderRef::new_with_native_work("local", "stream", "1", work)?;
        let source_binding = acyclic_harness::filesystem::recursive_fork_source_with_native_work(self.filesystem.clone(),
            &self.stream, parent.into_reducer(), &verifier.0, signed, context, &caller_snapshot.0,
            &stream_provider.0, operations.fork, &arguments.0, &config,
            source_read.as_ref().map(|pair| &pair.0), resolver, &cancel, work).await?;
        let capture = self.capture(source.clone(), frozen, budget, &cancel).await?;
        let mut request = source_binding.0.request;
        let mut request_custody = source_binding.1;
        let selection = capture.selection_with_native_work(work)?;
        request.add_selection_with_native_work(selection.0, &mut request_custody, selection.1, work)?;
        let preparer = Arc::try_unwrap(source_binding.0.preparer).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi original native preparer escaped its construction" })?;
        let preparer = preparer.with_capture_provider_with_native_work(capture, work)?;
        let source_read_custody = source_read.map(|pair| pair.1);
        let snapshot = source_snapshot.0;
        request_custody.absorb(&mut source_snapshot.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        request_custody.absorb(&mut caller_project.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let caller_project = caller_project.0;
        let input_read = self.owner.policy().journal.directory_read_capability_with_native_work(
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY, work)?;
        if !context.scope().grants().contains(&input_read.0) { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi caller lacks immutable input-directory READ" }); }
        let mut launch_reference = self.stage_original_input(self.derive(original, "native-input")?,
            "native-input", &commit.launch, budget, &cancel).await?;
        #[derive(Serialize)]
        struct Command<'a> { operation_id: OperationId, kind: &'a str, payload: &'a FileRef }
        #[derive(Serialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum Execution<'a> { Child { launch: Command<'a> } }
        #[derive(Serialize)]
        struct Input<'a> { binding: &'a PiTaskBinding, execution: Execution<'a> }
        let input = crate::pi_root_provider::guarded_pi_value(&Input { binding: &commit.child,
            execution: Execution::Child { launch: Command { operation_id: operations.native,
                kind: NATIVE_LAUNCH_TASK_COMMAND_KIND, payload: &launch_reference.reference } } },
            self.owner.policy().maximum_payload_bytes, work)?;
        let mut child = self.runtime.runtime.harness().registered_child_admission_with_native_work(operations.child,
            "acyclic.pi", "1", input, owner.task_id(), context.scope(), work)?;
        let mut child_grants = self.child_content_grants(&child.0.grants, &input_read.0, work)?;
        child.0.grants = child_grants.0;
        // The ordinary preparation will seal only its actual child creations.
        // Parent creation attestations never become child authority or metadata.
        child.0.resource_proofs.clear();
        child.1.absorb(&mut child_grants.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let bytes = crate::pi_root_provider::guarded_pi_bytes(&request, self.owner.policy().maximum_payload_bytes, work)?;
        let fork_path = work.display(&format_args!("pi-fork/{original}/fork-input.json"))
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut fork_input = self.stage_input(context, self.derive(original, "fork-input")?,
            &fork_path.0, &bytes.0, "Pi original fork input").await?;
        let command_kind = work.display(&FORK_PREPARE_TASK_COMMAND_KIND)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let command = WorkflowCommand { operation_id: operations.fork, kind: command_kind.0, payload: fork_input.reference };
        let project_creation = preparer.project_creation_operation(&request)?;
        let mut checkpoint_copy = self.native_copy::<FileRef, _>(&checkpoint.reference, acyclic_harness::JsonValueSchema::None, work)?;
        let mut prepared_request = acyclic_harness::filesystem::fork_preparation_request_with_native_work(&preparer,
            &request, task_id_for_admission(operations.preparation), child.0, command, limits,
            Some(checkpoint_copy.0), Vec::new(), work)?;
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
        self.verify_owner(owner).await?;
        let container = work.claim((std::mem::size_of::<PiForkInputAllocations>()
            + 2 * std::mem::size_of::<usize>()) as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let checkpoint_allocation = checkpoint.allocation.take().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi checkpoint lost original input custody" })?;
        let fork_allocation = fork_input.allocation.take().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi fork input lost original input custody" })?;
        let input_allocations = Some(Arc::new(PiForkInputAllocations {
            _references: [Some(checkpoint_allocation), Some(fork_allocation), launch_reference.allocation.take(), None],
            _source_read: source_read_custody, _preparation: Some(request_custody), _container: container }));
        let mut plan = PiForkPlan { original, commit, preparation, request, source_checkpoint: checkpoint.reference,
            source: snapshot, caller_project, frozen, history_limits, project_creation, _input_allocations: input_allocations };
        // This is an immutable original piInputs argument, not a retry or
        // launch ledger. Persist the exact cut before companion custody work.
        let mut publication = self.stage_original_input(original, "fork-plan", &plan, budget, &cancel).await?;
        if let Some(allocations) = plan._input_allocations.as_mut() {
            Arc::get_mut(allocations).ok_or_else(|| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi first plan custody escaped before publication" })?
                ._references[3] = publication.allocation.take();
        }
        self.prepare_native_source(owner, &plan, source, self.remaining(budget)?, &cancel).await?;
        Ok(plan)
    }

    pub(crate) async fn load_plan(&self, context: &acyclic_harness::runtime::TaskContext,
        original: OperationId, budget: WorkBudget, cancel: &CancellationToken,
    ) -> Result<Option<PiForkPlan>> {
        self.require_input_read(context)?;
        self.load_original_plan(original, budget, cancel).await
    }

    /// Reads only the original protected piInputs directory. No executing
    /// source lease, writable content binding, or replacement admission exists.
    pub(crate) async fn load_original_plan(&self, original: OperationId, budget: WorkBudget,
        cancel: &CancellationToken) -> Result<Option<PiForkPlan>> {
        self.runtime.verify_owner(&self.owner)?;
        let Some(mut reference) = self.observe_original_input(original, "fork-plan", budget, cancel).await? else { return Ok(None); };
        let bytes = self.read_original_input(&reference.reference, self.remaining(budget)?, cancel).await?;
        let mut decoded: PiDecodedInput<PiForkPlan> = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::RecursiveForkToolPlan,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, budget, cancel)?;
        self.validate_plan(&decoded.value, decoded.value.commit.caller)?;
        if decoded.value.original != original { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi original plan locator changed" }); }
        if let Some(work) = &self.work {
            let container = work.claim((std::mem::size_of::<PiForkInputAllocations>()
                + 2 * std::mem::size_of::<usize>()) as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            decoded.value._input_allocations = Some(Arc::new(PiForkInputAllocations {
                _references: [reference.allocation.take(), decoded.allocation.take(), None, None],
                _source_read: None, _preparation: None, _container: container }));
        }
        Ok(Some(decoded.value))
    }

    pub(crate) async fn read_original_input(&self, reference: &FileRef, budget: WorkBudget,
        cancel: &CancellationToken) -> Result<acyclic_stream::OwnedBytes> {
        if let Some(work) = &self.work {
            self.runtime.verify_owner_with_native_work(&self.owner, work)?;
            let actual = self.runtime.runtime.readonly_task_context_with_native_work(self.owner.policy().root_task, work).await?;
            return actual.1.context().read_file_with_native_work(reference, work).await;
        }
        let authority = self.owner.authority()?;
        let grant = acyclic_harness::conversation::ContentGrant::verify_directory_read(
            &authority.verifier, &authority.signed, &self.owner.policy().journal,
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY)?;
        grant.require_file_read(reference)?;
        Ok(self.accept(self.filesystem.read_content_measured(reference, &grant,
            self.owner.policy().maximum_payload_bytes, self.remaining(budget)?, cancel).await)?.value.into())
    }

    fn require_input_read(&self, context: &acyclic_harness::runtime::TaskContext) -> Result<()> {
        let native = self.work.as_ref().map(|work| self.owner.policy().journal
            .directory_read_capability_with_native_work(crate::pi_root_inputs::ROOT_INPUT_DIRECTORY, work)).transpose()?;
        let ordinary = if native.is_none() {
            Some(self.owner.policy().journal.directory_read_capability(crate::pi_root_inputs::ROOT_INPUT_DIRECTORY)?)
        } else { None };
        let capability = native.as_ref().map(|value| value.0.as_str())
            .or_else(|| ordinary.as_deref()).expect("one original input-read capability");
        if !context.scope().grants().contains(capability) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller lacks its original immutable input-directory read grant" });
        }
        Ok(())
    }

    fn decode_original_input<T: serde::de::DeserializeOwned>(&self, bytes: &[u8], value_schema: acyclic_harness::JsonValueSchema,
        schema_label_bytes: u64, budget: WorkBudget, cancel: &CancellationToken) -> Result<PiDecodedInput<T>> {
        match &self.work {
            Some(work) => crate::pi_root_provider::guarded_pi_decode(bytes, self.owner.policy().maximum_payload_bytes,
                self.owner.policy().maximum_payload_bytes, value_schema, schema_label_bytes, work)
                .map(|(value, allocation)| PiDecodedInput { value, allocation: Some(allocation) }),
            None => Ok(PiDecodedInput { value: self.accept_root(crate::pi_root_provider::decode_pi_value(bytes,
                self.owner.policy().maximum_payload_bytes, self.owner.policy().maximum_payload_bytes,
                self.remaining(budget)?, cancel))?.value, allocation: None }),
        }
    }

    fn decode_input_value<T: serde::de::DeserializeOwned>(&self, value: &serde_json::Value,
        schema: acyclic_harness::JsonValueSchema, labels: u64) -> Result<PiDecodedInput<T>> {
        if let Some(work) = &self.work {
            let maximum = self.owner.policy().maximum_payload_bytes;
            let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(value, maximum, work)?;
            let (value, allocation) = crate::pi_root_provider::guarded_pi_decode(
                &bytes.0, maximum, maximum, schema, labels, work)?;
            Ok(PiDecodedInput { value, allocation: Some(allocation) })
        } else {
            Ok(PiDecodedInput { value: serde_json::from_value(value.clone()).map_err(invalid_json)?, allocation: None })
        }
    }

    async fn observe_outcome(&self, task: TaskId) -> Result<PiDecodedInput<Option<Outcome<serde_json::Value>>>> {
        match &self.work {
            Some(work) => {
                let (value, allocation) = self.runtime.runtime.task_host().outcome_with_native_work(task, work).await?;
                Ok(PiDecodedInput { value, allocation: Some(allocation) })
            }
            None => Ok(PiDecodedInput { value: self.runtime.runtime.task_host().outcome(task).await?, allocation: None }),
        }
    }

    fn same_payload<T: Serialize>(&self, expected: &serde_json::Value, actual: &T) -> Result<bool> {
        match &self.work {
            Some(work) => {
                let actual = crate::pi_root_provider::guarded_pi_value(actual, self.owner.policy().maximum_payload_bytes, work)?;
                Ok(expected == &actual.0)
            }
            None => Ok(expected == &serde_json::to_value(actual).map_err(invalid_json)?),
        }
    }

    async fn admit_original(&self, request: &TaskAdmissionRecord, fence: &LeaseFence) -> Result<()> {
        match &self.work {
            Some(work) => {
                let request = request.clone_with_native_work(work)?;
                let reservation = work.display(&fence.reservation_id)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut placement = work.display(&fence.placement)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut fence = (LeaseFence { reservation_id: reservation.0, placement: placement.0 }, reservation.1);
                fence.1.absorb(&mut placement.1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let admitted = self.runtime.runtime.task_host().admit_owned_with_native_work(request, fence, work)?.await?;
                drop(admitted);
                Ok(())
            }
            None => self.runtime.runtime.task_host().admit_owned(request.clone(), fence.clone()).await.map(|_| ()),
        }
    }

    async fn stage_input(&self, context: &acyclic_harness::runtime::TaskContext, operation: OperationId,
        path: &str, bytes: &[u8], display: &str) -> Result<PiStagedInput> {
        match &self.work {
            Some(work) => context.stage_file_once_with_native_work(operation, path, bytes, "application/json", display, work)
                .await.map(|(reference, allocation)| PiStagedInput { reference, allocation: Some(allocation) }),
            None => context.stage_file_once(operation, path, bytes, "application/json", display).await
                .map(|reference| PiStagedInput { reference, allocation: None }),
        }
    }

    async fn stage_original_input<T: Serialize>(&self, operation: OperationId, label: &str, value: &T,
        budget: WorkBudget, cancel: &CancellationToken) -> Result<PiStagedInput> {
        match &self.work {
            Some(work) => crate::pi_root_provider::stage_pi_input_with_native_work(&self.owner,
                &self.filesystem, operation, label, value, work).await
                .map(|(reference, allocation)| PiStagedInput { reference, allocation: Some(allocation) }),
            None => Ok(PiStagedInput { reference: self.accept_root(crate::pi_root_provider::stage_pi_input(
                &self.owner, &self.filesystem, operation, label, value, self.remaining(budget)?, cancel).await)?.value,
                allocation: None }),
        }
    }

    async fn observe_original_input(&self, operation: OperationId, label: &str,
        budget: WorkBudget, cancel: &CancellationToken) -> Result<Option<PiStagedInput>> {
        match &self.work {
            Some(work) => crate::pi_root_provider::observe_pi_input_with_native_work(&self.owner,
                &self.filesystem, operation, label, work).await
                .map(|value| value.map(|(reference, allocation)| PiStagedInput { reference, allocation: Some(allocation) })),
            None => Ok(self.accept_root(crate::pi_root_provider::observe_pi_input(&self.owner, &self.filesystem,
                operation, label, self.remaining(budget)?, cancel).await)?.value
                .map(|reference| PiStagedInput { reference, allocation: None })),
        }
    }

    /// Observes genuine completed preparation, admission and historical bind.
    /// It cannot drive preparation, admit, publish, acquire a lease, or launch.
    pub(crate) async fn observe_prepared(&self, plan: &PiForkPlan, source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget, cancel: CancellationToken) -> Result<Option<PreparedPiFork>> {
        let original = self.load_original_plan(plan.original, self.remaining(budget)?, &cancel).await?
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared observation lost its original immutable plan" })?;
        if original != *plan {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi prepared observation substituted its original plan" });
        }
        if let Some(work) = &self.work {
            let preparation = match self.observe_admission(plan.preparation.task_id()).await {
                Ok(value) => value, Err(Error::NotFound(_)) => return Ok(None), Err(error) => return Err(error),
            };
            if preparation.value != plan.preparation { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi original preparation admission changed" }); }
            let outcome = self.observe_outcome(plan.preparation.task_id()).await?;
            let report: PiDecodedInput<ForkReport> = match &outcome.value {
                Some(Outcome::Succeeded(value)) => self.decode_input_value(value,
                    acyclic_harness::JsonValueSchema::ForkReport, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?,
                _ => return Ok(None),
            };
            report.value.validate_with_native_work(work)?;
            if !self.same_payload(&plan.request.payload, &report.value.request)? { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi actual creator report changed its original request" }); }
            let admission = match self.observe_admission(plan.request.child.task_id()).await {
                Ok(value) => value, Err(Error::NotFound(_)) => return Ok(None), Err(error) => return Err(error),
            };
            if admission.value != plan.request.child { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi original child admission changed" }); }
            let reference = self.observe_original_input(self.derive(plan.original, "native-input")?,
                "native-input", budget, &cancel).await?.ok_or(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi original native input disappeared" })?;
            let child: PiDecodedInput<PiTaskInput> = self.decode_input_value(&admission.value.input,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
            let PiTaskExecution::Child { launch } = &child.value.execution else { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi admitted child substituted Root execution" }); };
            let bytes = self.read_original_input(&reference.reference, self.remaining(budget)?, &cancel).await?;
            let intent: PiDecodedInput<PiNativeLaunchIntent> = self.decode_original_input(bytes.as_ref(),
                acyclic_harness::JsonValueSchema::None, crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, budget, &cancel)?;
            if launch.payload != reference.reference || intent.value != plan.commit.launch { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi admitted child changed its sealed native intent" }); }
            let parent_admission = self.observe_admission(plan.commit.caller).await?;
            let parent_input: PiDecodedInput<PiTaskInput> = self.decode_input_value(&parent_admission.value.input,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
            let parent_authority = self.owner.conversation_authority_with_native_work(plan.commit.caller,
                parent_input.value.binding.agent_id, &parent_admission.value.grants, work)?;
            let parent_verifier = parent_authority.0.1.verifier_with_native_work(work)?;
            let child_authority = self.owner.conversation_authority_with_native_work(plan.request.child.task_id(),
                plan.commit.child.agent_id, &admission.value.grants, work)?;
            let child_verifier = child_authority.0.1.verifier_with_native_work(work)?;
            let parent = acyclic_harness::store::HistoryReader::new_with_native_work(
                &self.stream, &parent_authority.0.0, &parent_verifier.0, work)?;
            let child = acyclic_harness::store::HistoryReader::new_with_native_work(
                &self.stream, &child_authority.0.0, &child_verifier.0, work)?;
            let seed = report.value.clone_with_native_work(work)?;
            let seed_value = seed.0.into_seed()?;
            child.0.verify_published_child_with_native_work(&parent.0, &seed_value, plan.history_limits, work).await?;
            source.validate_snapshot_with_native_work(plan.frozen, work).await.map_err(crate::pi_lazy_capture::native_lazy_error)?;
            return Ok(Some(self.created_fork(plan, report.value, admission.value, source, self.remaining(budget)?,
                &cancel, report.allocation, admission.allocation).await?));
        }
        let host = self.runtime.runtime.task_host();
        let preparation = match host.observe_admission(plan.preparation.task_id()).await {
            Ok(value) => value, Err(Error::NotFound(_)) => return Ok(None), Err(error) => return Err(error),
        };
        if preparation != plan.preparation { return Err(Error::Unauthorized("Pi preparation admission changed".into())); }
        let report = match host.outcome(plan.preparation.task_id()).await? {
            Some(Outcome::Succeeded(value)) => serde_json::from_value::<ForkReport>(value).map_err(invalid_json)?,
            _ => return Ok(None),
        };
        report.validate()?;
        if serde_json::to_value(&report.request).map_err(invalid_json)? != plan.request.payload {
            return Err(Error::Unauthorized("Pi creator report changed the original request".into()));
        }
        let admission = match host.observe_admission(plan.request.child.task_id()).await {
            Ok(value) => value, Err(Error::NotFound(_)) => return Ok(None), Err(error) => return Err(error),
        };
        if admission != plan.request.child { return Err(Error::Unauthorized("Pi prepared child admission changed".into())); }
        let reference = self.observe_original_input(self.derive(plan.original, "native-input")?, "native-input", budget, &cancel).await?
            .ok_or_else(|| Error::Unauthorized("Pi prepared observation lost its native input".into()))?;
        let child_input: PiTaskInput = serde_json::from_value(admission.input.clone()).map_err(invalid_json)?;
        let PiTaskExecution::Child { launch } = child_input.execution else {
            return Err(Error::Unauthorized("Pi prepared observation substituted Root".into()));
        };
        let bytes = self.read_original_input(&reference.reference, self.remaining(budget)?, &cancel).await?;
        let intent: PiDecodedInput<PiNativeLaunchIntent> = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::None,
            crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, budget, &cancel)?;
        if launch.payload != reference.reference || intent.value != plan.commit.launch {
            return Err(Error::Unauthorized("Pi prepared observation changed its sealed native intent".into()));
        }
        let parent_admission = host.observe_admission(plan.commit.caller).await?;
        let parent_input: PiTaskInput = serde_json::from_value(parent_admission.input).map_err(invalid_json)?;
        let (parent_authority, parent_issuer, _) = self.owner.conversation_authority(plan.commit.caller,
            parent_input.binding.agent_id, parent_admission.grants)?;
        let (child_authority, child_issuer, _) = self.owner.conversation_authority(plan.request.child.task_id(),
            plan.commit.child.agent_id, admission.grants.clone())?;
        let parent = acyclic_harness::store::HistoryReader::new(&self.stream, &parent_authority, parent_issuer.verifier())?;
        let child = acyclic_harness::store::HistoryReader::new(&self.stream, &child_authority, child_issuer.verifier())?;
        self.accept(child.verify_published_child_measured(&parent, &report.clone().into_seed()?, self.remaining(budget)?, &cancel).await)?;
        PiLazyCapture::new(source.clone(), plan.frozen, self.remaining(budget)?, cancel.clone(), self.work.clone()).await?;
        Ok(Some(self.created_fork(plan, report, admission, source, self.remaining(budget)?, &cancel, None, None).await?))
    }

    async fn prepare_native_source(&self, owner: &TaskJournalOwner<LocalStream>, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken,
    ) -> Result<()> {
        self.verify_owner(owner).await?;
        if let Some(work) = &self.work {
            let request: PiDecodedInput<ForkRequest> = self.decode_input_value(&plan.request.payload,
                acyclic_harness::JsonValueSchema::ForkRequest, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?;
            let name = request.value.preparation.child_project_volume.storage_name_with_native_work(work)?;
            let prepared = source.prepare_fork_snapshot_with_native_work(&name.0, plan.frozen,
                plan.project_creation, work, cancel).await.map_err(crate::pi_lazy_capture::native_lazy_error)?;
            if prepared != plan.frozen { return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "native preparation changed its immutable source cut" }); }
            self.verify_owner(owner).await?;
            return Ok(());
        }
        let request: ForkRequest = serde_json::from_value(plan.request.payload.clone()).map_err(invalid_json)?;
        self.accept(source.prepare_fork_snapshot_measured(request.preparation.child_project_volume.storage_name()?,
            plan.frozen, plan.project_creation, self.remaining(budget)?, cancel).await)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments, reason = "original native owner, source cut, and invocation work stay distinct")]
    async fn native_preparer(&self, shared: &Arc<acyclic_harness::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, plan: &PiForkPlan, source: Arc<LocalLazyWorkspace>,
        budget: WorkBudget, cancel: &CancellationToken, work: &NativeMaterializationWork)
        -> Result<Arc<acyclic_harness::filesystem::FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>>> {
        let context = shared.context();
        let admission = self.observe_admission(owner.task_id()).await?;
        let caller: PiDecodedInput<PiTaskInput> = self.decode_input_value(&admission.value.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let authority = self.owner.conversation_authority_with_native_work(owner.task_id(),
            caller.value.binding.agent_id, context.scope().grants(), work)?;
        let request: (ForkRequest, acyclic_fs::NativeAllocation) = self.native_copy(&plan.request.payload,
            acyclic_harness::JsonValueSchema::ForkRequest, work)?;
        let checkpoint = context.read_file_with_native_work(&plan.source_checkpoint, work).await?;
        self.verify_owner(owner).await?;
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
            &plan.caller_project, &provider.0, request, plan.history_limits,
            source_read.as_ref().map(|pair| &pair.0), resolver, cancel, work).await?;
        let capture = self.capture(source, plan.frozen, budget, cancel).await?;
        let preparer = Arc::try_unwrap(restored.0.preparer).map_err(|_| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi restored preparer escaped its original native ownership" })?;
        let preparer = preparer.with_capture_provider_with_native_work(capture, work)?;
        if preparer.project_creation_operation(&restored.0.request)? != plan.project_creation {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native companion changed its original filesystem operation" });
        }
        self.verify_owner(owner).await?;
        preparer.into_native_shared(work)
    }

    async fn require_original_plan(&self, plan: &PiForkPlan) -> Result<()> {
        let work = self.work.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi preparation requires its original native invocation work" })?;
        let original = self.load_original_plan(plan.original, work.remaining()
            .map_err(|cause| Error::NativeWork { cause, custody: None })?, work.cancellation()).await?
            .ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation lost its protected original fork publication" })?;
        if original != *plan {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation substituted its protected original fork publication" });
        }
        Ok(())
    }

    /// The service persists its existing Preparing route after this ordinary
    /// admission and before advance may run the provider's FS creator.
    pub(crate) async fn admit_preparation(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, fence: &LeaseFence, plan: &PiForkPlan,
    ) -> Result<()> {
        self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.require_original_plan(plan).await?;
        self.verify_owner(owner).await?;
        self.admit_original(&plan.preparation, fence).await?;
        Ok(())
    }

    /// Drives only the retained ordinary preparation. Unknown acknowledgements
    /// never become replacement tasks, resources, or a new session budget.
    #[allow(clippy::too_many_arguments, reason = "exact parent lease and immutable source selection")]
    /// Advances only the ordinary registered preparation task. Child grants
    /// remain unsealed and no child task is admitted by this method.
    pub(crate) async fn prepare(&self, shared: &Arc<acyclic_harness::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<Option<PiDecodedInput<ForkReport>>> {
        let context = shared.context();
        let work = self.work.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported, reason: "Pi preparation requires its original native invocation work" })?;
        self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.require_original_plan(plan).await?;
        self.verify_owner(owner).await?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        self.authenticate_launch(context, plan, self.remaining(budget)?, &cancel).await?;
        let preparer = self.native_preparer(shared, owner, plan, source, budget, &cancel, work).await?;
        let host = self.runtime.runtime.task_host();
        self.verify_owner(owner).await?;
        self.admit_original(&plan.preparation, fence).await?;
        if self.observe_admission(plan.preparation.task_id()).await?.value != plan.preparation {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi preparation belongs to another original operation" });
        }
        if self.observe_outcome(plan.preparation.task_id()).await?.value.is_none() {
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
        let outcome = self.observe_outcome(plan.preparation.task_id()).await?;
        let report: PiDecodedInput<ForkReport> = match &outcome.value {
            Some(Outcome::Succeeded(value)) => self.decode_input_value(value,
                acyclic_harness::JsonValueSchema::ForkReport, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?,
            Some(Outcome::Failed { .. }) => return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi original preparation failed" }),
            Some(Outcome::Cancelled) => return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict, reason: "Pi preparation cancelled without admitting a child" }),
            None | Some(Outcome::Indeterminate { .. }) => return Ok(None),
        };
        report.value.validate_with_native_work(work)?;
        if !self.same_payload(&plan.request.payload, &report.value.request)? { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi report changed the retained original request" }); }
        self.verify_owner(owner).await?;
        Ok(Some(report))
    }

    #[allow(clippy::too_many_arguments, reason = "exact parent lease and immutable source selection")]
    pub(crate) async fn advance(&self, shared: &Arc<acyclic_harness::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<Option<PreparedPiFork>> {
        let context = shared.context();
        let Some(report) = self.prepare(shared, owner, fence, plan, source.clone(), self.remaining(budget)?, cancel.clone()).await? else {
            return Ok(None);
        };
        let host = self.runtime.runtime.task_host();
        self.verify_owner(owner).await?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        // The existing host seals actual creator receipts under this same
        // original parent lease before any child adapter may dispatch.
        self.admit_original(&plan.request.child, fence).await?;
        let admission = self.observe_admission(plan.request.child.task_id()).await?;
        if admission.value != plan.request.child {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi child admission differs from its actual creator request" });
        }
        self.verify_owner(owner).await?;
        self.publish(context, owner, plan, &report.value, source.clone(), self.remaining(budget)?, &cancel).await?;
        let prepared = self.created_fork(plan, report.value, admission.value, source, self.remaining(budget)?, &cancel,
            report.allocation, admission.allocation).await?;
        self.verify_owner(owner).await?;
        Ok(Some(prepared))
    }

    /// Reopens only a previously published and bound admitted child. A terminal
    /// original caller never receives a replacement lease or fabricated admit.
    pub(crate) async fn restore_admitted(&self, shared: &Arc<acyclic_harness::NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken) -> Result<PreparedPiFork> {
        let context = shared.context();
        self.work.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi cold restore requires its original native context and invocation work" })?;
        let admission = self.authenticate(context, owner, fence).await?;
        self.validate_plan(plan, plan.commit.caller)?;
        if owner.task_id() != plan.request.child.task_id() || admission.value != plan.request.child {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold restore does not own the original admitted child" });
        }
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        let prepared = self.observe_prepared(plan, source, self.remaining(budget)?, cancel).await?
            .ok_or_else(|| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi cold child has no successful published original creator" })?;
        self.verify_owner(owner).await?;
        Ok(prepared)
    }

    #[allow(clippy::too_many_arguments, reason = "actual report and admission ownership precede their original guards")]
    async fn created_fork(&self, plan: &PiForkPlan, report: ForkReport, admission: TaskAdmissionRecord,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken,
        report_allocation: Option<acyclic_fs::NativeAllocation>, admission_allocation: Option<acyclic_fs::NativeAllocation>) -> Result<PreparedPiFork> {
        if let Some(work) = &self.work {
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
            let original_source = self.native_copy::<RecursiveSourceSnapshot, _>(&plan.source,
                acyclic_harness::JsonValueSchema::None, work)?;
            return Ok(PreparedPiFork { binding: binding.0, report, admission, source: original_source.0,
                frozen_source: plan.frozen, native_lazy, workspace,
                _metadata_custody: [report_allocation, admission_allocation, Some(binding.1), Some(original_source.1)] });
        }
        let project = self.accept(self.resources.fs.open_workspace_measured(report.request.preparation.child_project_volume.storage_name()?, self.remaining(budget)?, &cancel).await)?.value;
        let captured_generation = report.captures.iter().find_map(|capture| match capture {
            acyclic_harness::fork::Capture::Captured(resource) => match &resource.revision {
                acyclic_harness::fork::ResourceRevision::Project { generation, .. } => Some(generation),
                _ => None,
            },
            _ => None,
        }).ok_or_else(|| Error::Unauthorized("Pi report lost the created project generation".into()))?;
        let generation: [u8; 32] = captured_generation.as_resource().key().try_into()
            .map_err(|_| Error::Invalid("Pi created generation identity differs".into()))?;
        let (workspace, native_lazy) = self.accept(source.bind_prepared_fork_measured(plan.frozen, project, GenerationId::new(Digest::from_bytes(generation)), plan.project_creation, self.remaining(budget)?, cancel).await)?.value;
        let scratch = self.accept(self.resources.fs.open_workspace_measured(report.child_private_volume.storage_name()?, self.remaining(budget)?, &cancel).await)?.value;
        let mut binding = plan.commit.child.clone();
        binding.scratch.allocation = PiScratchAllocation::Created(PiScratchCreation {
            workspace_id: scratch.id().into_bytes(), generation: report.child_private_generation.clone(),
        });
        let result_binding = binding.results.as_mut()
            .ok_or_else(|| Error::Unauthorized("Pi child lost its unmounted native result allocation".into()))?;
        if report.child_result_volume.as_ref() != Some(&result_binding.volume) {
            return Err(Error::Unauthorized("Pi report changed its actual native result volume".into()));
        }
        let generation = report.child_result_generation.as_ref()
            .ok_or_else(|| Error::Unauthorized("Pi report lacks its actual result creation generation".into()))?;
        let results = self.accept(self.resources.fs.open_workspace_measured(result_binding.volume.storage_name()?, self.remaining(budget)?, &cancel).await)?.value;
        result_binding.allocation = PiScratchAllocation::Created(PiScratchCreation {
            workspace_id: results.id().into_bytes(), generation: generation.clone(),
        });
        binding.validate(None, binding.project_volumes.keys().map(String::as_str), false)?;
        Ok(PreparedPiFork { binding, report, admission, source: plan.source.clone(),
            frozen_source: plan.frozen, native_lazy, workspace,
            _metadata_custody: [report_allocation, admission_allocation, None, None] })
    }

    async fn publish(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, plan: &PiForkPlan, report: &ForkReport,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken,
    ) -> Result<()> {
        if let Some(work) = &self.work {
            let admission = self.observe_admission(owner.task_id()).await?;
            let parent_input: PiDecodedInput<PiTaskInput> = self.decode_input_value(&admission.value.input,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
            let authority = self.owner.conversation_authority_with_native_work(owner.task_id(),
                parent_input.value.binding.agent_id, context.scope().grants(), work)?;
            let verifier = authority.0.1.verifier_with_native_work(work)?;
            let seed = report.clone_with_native_work(work)?;
            let seed_value = seed.0.into_seed()?;
            let capture = self.capture(source, plan.frozen, budget, cancel).await?;
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
            let mut parent_value = parent.0.with_limits(context.scope().limits())?.with_fork_verifier(forks);
            let published = parent_value.publish_fork_report_with_native_work(report, &authority.0.2, owner, work).await?;
            drop(published.0);
            drop(published.1);
            let child_authority = self.owner.conversation_authority_with_native_work(
                plan.request.child.task_id(), seed_value.child_agent, &plan.request.child.grants, work)?;
            let child_verifier = child_authority.0.1.verifier_with_native_work(work)?;
            let child = StreamAggregate::open_with_native_work(&self.stream, child_authority.0.0, child_verifier.0,
                SchemaRegistry::new(), work).await?;
            let mut child_value = child.0.with_limits(plan.request.child.limits)?;
            child_value.bind_published_child_with_native_work(&parent_value, &seed_value,
                &child_authority.0.2, owner, work).await?;
            return Ok(());
        }
        let parent_input: PiTaskInput = serde_json::from_value(self.runtime.runtime.task_host()
            .observe_admission(owner.task_id()).await?.input).map_err(invalid_json)?;
        let (parent_authority, parent_issuer, parent_scope) = self.owner.conversation_authority(
            owner.task_id(), parent_input.binding.agent_id, context.scope().grants().clone(),
        )?;
        let seed = report.clone().into_seed()?;
        let capture = self.capture(source, plan.frozen, budget, cancel).await?;
        let verifier = Arc::new(acyclic_harness::fork::CompositeForkVerifier::new(vec![
            Arc::new(acyclic_harness::filesystem::FilesystemForkVerifier::new(
                self.filesystem.clone(), plan.request.maximum_bytes)?),
            Arc::new(acyclic_harness::fork::StreamHistoryForkVerifier::new(
                acyclic_harness::resources::ProviderRef::new("local", "stream", "1")?)?),
            capture,
        ])?);
        let mut parent = self.accept(StreamAggregate::open_measured(&self.stream, parent_authority, parent_issuer.verifier(), SchemaRegistry::new(), self.remaining(budget)?, cancel).await)?
            .value.with_limits(context.scope().limits())?.with_fork_verifier(verifier);
        self.accept(parent.publish_fork_report_measured(report.clone(), parent_scope, owner, self.remaining(budget)?, cancel).await)?;
        let (child_authority, child_issuer, child_scope) = self.owner.conversation_authority(
            plan.request.child.task_id(), seed.child_agent, plan.request.child.grants.clone(),
        )?;
        let mut child = self.accept(StreamAggregate::open_measured(&self.stream, child_authority, child_issuer.verifier(), SchemaRegistry::new(), self.remaining(budget)?, cancel).await)?
            .value.with_limits(plan.request.child.limits)?;
        self.accept(child.bind_published_child_measured(&parent, &seed, child_scope, owner, self.remaining(budget)?, cancel).await)?;
        Ok(())
    }

    fn validate_plan(&self, plan: &PiForkPlan, caller: TaskId) -> Result<()> {
        let original = self.original(caller, &plan.commit.sdk_operation)?;
        let operations = self.operations(original)?;
        match &self.work {
            Some(work) => {
                plan.request.validate_with_native_work(work)?;
                plan.preparation.validate_with_native_work(work)?;
            }
            None => {
                plan.request.validate()?;
                plan.preparation.validate()?;
            }
        }
        let child_input: PiDecodedInput<PiTaskInput> = self.decode_input_value(&plan.request.child.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        let child_input = &child_input.value;
        let decoded_request: PiDecodedInput<ForkRequest> = self.decode_input_value(&plan.request.payload,
            acyclic_harness::JsonValueSchema::ForkRequest, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES)?;
        let request = &decoded_request.value;
        let child_launch = match &child_input.execution {
            PiTaskExecution::Child { launch } => launch,
            PiTaskExecution::Host { .. } => return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "Pi fork admission substituted a root task" }),
        };
        match &self.work {
            Some(work) => plan.commit.launch.validate_with_native_work(work)?,
            None => plan.commit.launch.validate()?,
        }
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
            || plan.request.source_checkpoint.as_ref() != Some(&plan.source_checkpoint)
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

/// RootService's explicit same-session selected-PROJECT policy. The kernel
/// supplies both actual admission lineages; this provider additionally proves
/// the actual pinned native/FS snapshot and the root's successful bootstrap.
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
    budget: WorkBudget,
    cancel: CancellationToken,
}

impl RootProjectSourceProvider for PiProjectSourcePolicy<'_> {
    fn verify<'a>(&'a self, context: RootProjectSourceContext<'a>, snapshot: &'a ProjectSourceSnapshot)
        -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let coordinator = self.coordinator;
            coordinator.runtime.verify_owner(&coordinator.owner)?;
            let root = coordinator.owner.policy().root_task;
            let authority = coordinator.owner.authority()?;
            if context.owner != authority.verifier.audience() || context.graph_root != root
                || context.caller.task_id() != self.caller || context.source.task_id() != self.selected
                || context.operation_id != self.operation
                || context.caller.task.name != "acyclic.pi" || context.source.task.name != "acyclic.pi"
                || snapshot.native_snapshot.as_ref() != Some(self.native_snapshot)
                || snapshot.generation.as_resource().key() != self.frozen.authored_generation.digest().as_bytes()
                || snapshot.project.storage_name()? != self.source.workspace().name().as_str()
                || context.maximum_work == 0 || context.maximum_bytes == 0
            {
                return Err(Error::Unauthorized("Pi selected source is outside its explicit original session policy".into()));
            }
            let original: PiTaskInput = serde_json::from_value(context.source.input.clone()).map_err(invalid_json)?;
            if !original.binding.project_volumes.values().any(|volume| volume == &snapshot.project) {
                return Err(Error::Unauthorized("Pi selected source changed its original admitted project".into()));
            }
            let budget = self.budget.intersect_budget(project_source_budget(context.maximum_work, context.maximum_bytes));
            let validated = coordinator.accept(self.source.validate_snapshot_measured(self.frozen, coordinator.remaining(budget)?, &self.cancel).await)?;
            let remaining = validated.work.remaining(budget).map_err(|cause| Error::NativeWork { cause: cause.into(), custody: None })?;
            if !context.created_project_authenticated {
                if self.selected != root {
                    return Err(Error::Unauthorized("Pi selected child has no actual project creator proof".into()));
                }
                let PiTaskExecution::Host { root, .. } = original.execution else {
                    return Err(Error::Unauthorized("Pi protected root was not its original bootstrap task".into()));
                };
                let request = crate::pi_root_inputs::PiRootRequest { binding: original.binding, root };
                let work = coordinator.work.as_ref().ok_or_else(|| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unsupported,
                    reason: "Root source proof requires its original native observed context" })?;
                let source_authority = coordinator.runtime.runtime.readonly_task_context_with_native_work(self.selected, work).await?;
                coordinator.verify_owner(self.caller_owner).await?;
                crate::pi_root_provider::prove_root_project_source_with_native_work(coordinator.owner.clone(),
                    &coordinator.resources, coordinator.filesystem.clone(), coordinator.stream.clone(),
                    &request, &snapshot.project, self.frozen, source_authority.1.clone(), &source_authority.0, work).await?;
                coordinator.verify_owner(self.caller_owner).await?;
            }
            Ok(())
        })
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
            let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(&context.source.input, maximum, work)?;
            let original = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(&bytes.0, maximum, maximum,
                acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work)?;
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
                #[derive(Serialize)]
                struct RootRequest<'b> { binding: &'b PiTaskBinding, root: &'b crate::pi_root::PiRootIntent }
                let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(
                    &RootRequest { binding: &original.0.binding, root }, maximum, work)?;
                let request = crate::pi_root_provider::guarded_pi_decode::<crate::pi_root_inputs::PiRootRequest>(
                    &bytes.0, maximum, maximum, acyclic_harness::JsonValueSchema::None,
                    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES, work)?;
                self.caller_owner.verify_with_native_work(false, work).await?;
                let actual_source = coordinator.runtime.runtime.readonly_task_context_with_native_work(self.selected, work).await?;
                self.caller_owner.verify_with_native_work(false, work).await?;
                crate::pi_root_provider::prove_root_project_source_with_native_work(coordinator.owner.clone(),
                    &coordinator.resources, coordinator.filesystem.clone(), coordinator.stream.clone(),
                    &request.0, &snapshot.project, self.frozen, actual_source.1.clone(), &actual_source.0, work).await?;
                self.caller_owner.verify_with_native_work(false, work).await?;
            }
            Ok(())
        })
    }
}

fn project_source_budget(units: u64, bytes: u64) -> WorkBudget {
    WorkBudget {
        authority_records_read: units, authority_records_appended: units,
        authority_bytes_read: bytes, authority_bytes_written: bytes,
        object_probes: units, backend_read_operations: units, backend_write_operations: units,
        durability_operations: units, page_reads: units, page_writes: units,
        object_bytes_read: bytes, object_bytes_written: bytes,
        bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
        source_bytes_read: bytes, source_path_components: units, source_entries_visited: units,
        output_bytes: bytes, items_examined: units, items_returned: units,
        allocation_operations: units, peak_allocation_bytes: bytes, materializations: units,
    }
}

fn reserved_native_key(key: &str) -> bool {
    ["ACYCLIC_PI_CALLER", "ACYCLIC_PI_ROUTE", "ACYCLIC_PI_TASK", "ACYCLIC_PI_TOKEN", "ACYCLIC_PI_SCOPE", "ACYCLIC_PI_LEASE"]
        .iter().any(|reserved| key.eq_ignore_ascii_case(reserved))
        || key.get(.."ACYCLIC_PI_CALLER_".len()).is_some_and(|prefix| prefix.eq_ignore_ascii_case("ACYCLIC_PI_CALLER_"))
}

fn invalid_json(error: serde_json::Error) -> Error { Error::Invalid(error.to_string()) }
fn measured<E: std::fmt::Display>(failure: acyclic_fs::OperationFailure<E>) -> Error {
    Error::Storage(format!("{}; work={:?}", failure.error, failure.work))
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
