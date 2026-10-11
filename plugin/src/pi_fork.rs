//! Recursive Pi admission over the original task and provider journals.
//! Planned native locators are not creator evidence or execution authority.

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
        RecursiveSourceSnapshot, FORK_PREPARE_TASK_COMMAND_KIND, fork_preparation_request,
        recursive_fork_source, restore_recursive_fork_source},
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

pub(crate) fn original_operation(root: TaskId, caller: TaskId, sdk_tool_call_id: &str) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.pi/sdk-operation/v1\0");
    hash.update(&root.into_bytes());
    hash.update(&caller.into_bytes());
    hash.update(&(sdk_tool_call_id.len() as u64).to_le_bytes());
    hash.update(sdk_tool_call_id.as_bytes());
    OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().expect("fixed digest prefix"))
}

pub(crate) fn derived(original: OperationId, role: &str) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.pi/fork-operation/v1\0");
    hash.update(&original.into_bytes());
    hash.update(&(role.len() as u64).to_le_bytes());
    hash.update(role.as_bytes());
    OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().expect("fixed digest prefix"))
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
        Self { child: derived(original, "child-task"), preparation: derived(original, "preparation-task"),
            fork: derived(original, "fork"), native: derived(original, "native-effect"),
            agent: AgentId::from_bytes(derived(original, "child-agent").into_bytes()) }
    }
}

#[derive(Clone, Serialize, Deserialize)]
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

struct PiForkInputAllocations {
    _references: [Option<acyclic_fs::NativeAllocation>; 4],
    _container: acyclic_fs::NativeAllocation,
}

struct PiStagedInput {
    reference: FileRef,
    allocation: Option<acyclic_fs::NativeAllocation>,
}

struct PiDecodedInput<T> {
    value: T,
    allocation: Option<acyclic_fs::NativeAllocation>,
}

pub(crate) struct PreparedPiFork {
    pub(crate) binding: PiTaskBinding,
    pub(crate) report: ForkReport,
    pub(crate) admission: TaskAdmissionRecord,
    pub(crate) source: RecursiveSourceSnapshot,
    pub(crate) frozen_source: LazySnapshotRef,
    pub(crate) native_lazy: LazySnapshotRef,
    pub(crate) workspace: LocalLazyWorkspace,
}

pub(crate) struct PiForkCoordinator {
    runtime: Arc<Runtime>,
    filesystem: Arc<Filesystem>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    owner: PiOwnerConfig,
    work: Option<NativeMaterializationWork>,
}
impl PiForkCoordinator {
    pub(crate) fn new(runtime: Arc<Runtime>, filesystem: Arc<Filesystem>, stream: StreamClient<LocalStream>,
        resources: ServiceResources, owner: PiOwnerConfig) -> Result<Self> {
        runtime.verify_owner(&owner)?;
        Ok(Self { runtime, filesystem, stream, resources, owner, work: None })
    }

    pub(crate) fn with_work(mut self, work: NativeMaterializationWork) -> Self {
        self.work = Some(work);
        self
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
        owner: &TaskJournalOwner<LocalStream>, fence: &LeaseFence) -> Result<TaskAdmissionRecord> {
        self.runtime.verify_owner(&self.owner)?;
        crate::pi_inspection::authenticate(context, owner).await?;
        let host = self.runtime.runtime.task_host();
        host.verify_dispatch_owner(owner.task_id(), fence.clone()).await?;
        let admission = host.observe_admission(owner.task_id()).await?;
        if admission.grants != *context.scope().grants() || admission.limits != context.scope().limits()
            || admission.run_limits != context.scope().run_limits() {
            return Err(Error::Unauthorized("Pi fork context differs from the original caller admission".into()));
        }
        Ok(admission)
    }

    /// The same original-root policy seals inspect/import/fork reads. It never
    /// broadens the caller's grants or exports the source's private authority.
    #[allow(clippy::too_many_arguments, reason = "original caller, exact source cut and operation-scoped bounds")]
    pub(crate) async fn authorize_selected_project(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &Arc<TaskJournalOwner<LocalStream>>, source_task: TaskId, source: Arc<LocalLazyWorkspace>,
        frozen: LazySnapshotRef, operation: OperationId, maximum_work: u64, maximum_bytes: u64,
        budget: WorkBudget, cancel: CancellationToken)
        -> Result<acyclic_harness::source_authorization::ProjectSourceReadHandle> {
        self.runtime.verify_owner(&self.owner)?;
        crate::pi_inspection::authorize_selected_source(&self.runtime.runtime, self.owner.policy().root_task,
            context, owner, source_task, self.owner.policy().session_limits.depth).await?;
        let original: PiTaskInput = serde_json::from_value(self.runtime.runtime.task_host()
            .observe_admission(source_task).await?.input).map_err(invalid_json)?;
        let project = original.binding.project_volumes.values().find(|volume|
            volume.storage_name().is_ok_and(|name| name == source.workspace().name().as_str()))
            .ok_or_else(|| Error::Unauthorized("Pi selected project has no actual admitted source binding".into()))?.clone();
        let generation = acyclic_harness::resources::GenerationRef::new(project.provider().clone(),
            frozen.authored_generation.digest().into_bytes(), None)?;
        let native_snapshot = serde_json::to_value(frozen).map_err(invalid_json)?;
        let policy = PiProjectSourcePolicy { coordinator: self, source, frozen, caller: owner.task_id(),
            selected: source_task, operation, native_snapshot: native_snapshot.clone(), budget, cancel };
        owner.authorize_project_source(context, &self.owner.authority()?.issuer,
            &policy, source_task, operation, ProjectSourceSnapshot { project, generation, native_snapshot: Some(native_snapshot) },
            maximum_work, maximum_bytes).await
    }
    async fn authenticate_launch(&self, context: &acyclic_harness::runtime::TaskContext,
        plan: &PiForkPlan, budget: WorkBudget, cancel: &CancellationToken) -> Result<()> {
        let child_input: PiTaskInput = serde_json::from_value(plan.request.child.input.clone()).map_err(invalid_json)?;
        let PiTaskExecution::Child { launch } = child_input.execution else {
            return Err(Error::Unauthorized("Pi retained child is not its native launch task".into()));
        };
        let reference = self.observe_original_input(derived(plan.original, "native-input"), "native-input", budget, cancel).await?
            .ok_or_else(|| Error::Unauthorized("Pi launch input has no original immutable publication".into()))?;
        if launch.payload != reference.reference {
            return Err(Error::Unauthorized("Pi native launch changed its original immutable input locator".into()));
        }
        self.require_input_read(context)?;
        let bytes = self.read_original_input(&reference.reference, budget, cancel).await?;
        let actual: PiDecodedInput<PiNativeLaunchIntent> = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::None, budget, cancel)?;
        if actual.value != plan.commit.launch {
            return Err(Error::Unauthorized("Pi native SDK source cut, session bytes, or invocation changed".into()));
        }
        Ok(())
    }


    #[allow(clippy::too_many_arguments, reason = "actual original owner, canonical parent, and native cut are independent authorities")]
    pub(crate) async fn begin(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: LeaseFence, parent: StreamAggregate<LocalStream>,
        source: Arc<LocalLazyWorkspace>, frozen: LazySnapshotRef, mut commit: RetainedSpawnCommit,
        limits: ForkPreparationLimits, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<PiForkPlan> {
        let admission = self.authenticate(context, owner, &fence).await?;
        let caller: PiTaskInput = serde_json::from_value(admission.input).map_err(invalid_json)?;
        let original = original_operation(self.owner.policy().root_task, owner.task_id(), &commit.sdk_operation);
        let operations = PiForkOperations::new(original);
        if commit.sdk_operation.is_empty() || commit.alias.is_empty() || commit.caller != owner.task_id()
            || commit.alias != commit.launch.agent_alias
            || commit.child.task_id != task_id_for_admission(operations.child) || commit.child.agent_id != operations.agent
            || commit.child.native_session_id != commit.launch.child_session_id
            || commit.prompt != commit.launch.invocation.prompt
            || commit.launch.source.parent_session_id != caller.binding.native_session_id
            || !matches!(commit.child.scratch.allocation, PiScratchAllocation::Preparing) {
            return Err(Error::Unauthorized("Pi spawn commit changed its original SDK/native/task identities".into()));
        }
        commit.launch.validate()?;
        commit.launch.invocation.environment.retain(|key, _| !reserved_native_key(key));
        if let Some(plan) = self.load_plan(context, original, self.remaining(budget)?, &cancel).await? {
            self.validate_plan(&plan, owner.task_id())?;
            if serde_json::to_value(&plan.commit).map_err(invalid_json)? != serde_json::to_value(&commit).map_err(invalid_json)? {
                return Err(Error::Unauthorized("Pi SDK retry changed its retained original spawn commit".into()));
            }
            self.prepare_native_source(owner, &plan, source, self.remaining(budget)?, &cancel).await?;
            return Ok(plan);
        }
        let (_, issuer, signed) = self.owner.conversation_authority(owner.task_id(), caller.binding.agent_id,
            context.scope().grants().clone())?;
        if parent.reducer().authority().kind != AggregateKind::Conversation
            || parent.reducer().authority().id != owner.task_id().to_string()
            || parent.reducer().conversation().and_then(|state| state.agent) != Some(caller.binding.agent_id) {
            return Err(Error::Unauthorized("Pi fork requires the actual bound caller conversation".into()));
        }
        issuer.verifier().verify_audience(parent.reducer().authority())?;
        crate::pi_inspection::authorize_selected_source(&self.runtime.runtime, self.owner.policy().root_task,
            context, owner, commit.source_task, self.owner.policy().session_limits.depth).await?;
        let source_input: PiTaskInput = serde_json::from_value(self.runtime.runtime.task_host()
            .observe_admission(commit.source_task).await?.input).map_err(invalid_json)?;
        let (root_key, project) = source_input.binding.project_volumes.iter().find(|(_, volume)| {
            volume.storage_name().is_ok_and(|name| name == source.workspace().name().as_str())
        }).ok_or_else(|| Error::Unauthorized("Pi source handle is not its original admitted project".into()))?;
        if commit.source_task != self.owner.policy().root_task {
            let selected = self.runtime.runtime.inspect_source(owner.task_id(), commit.source_task).await?;
            if !selected.volumes().iter().any(|snapshot| &snapshot.volume == project) {
                return Err(Error::Unauthorized("Pi selected child project has no actual creator proof".into()));
            }
        }
        let generation = acyclic_harness::resources::GenerationRef::new(project.provider().clone(),
            frozen.authored_generation.digest().into_bytes(), None)?;
        let scratch = &caller.binding.scratch.volume;
        let scratch_workspace = self.accept(self.resources.fs.open_workspace_measured(scratch.storage_name()?, self.remaining(budget)?, &cancel).await)?.value;
        let scratch_generation = self.accept(scratch_workspace.head_measured(self.remaining(budget)?, &cancel).await)?.value;
        let snapshot = RecursiveSourceSnapshot { task: owner.task_id(), project: project.clone(),
            project_generation: generation, private_volume: scratch.clone(),
            private_generation: acyclic_harness::resources::GenerationRef::new(scratch.provider().clone(),
                scratch_generation.id().digest().into_bytes(), None)? };
        let caller_project = caller.binding.project_volumes.get(root_key)
            .or_else(|| caller.binding.project_volumes.values().next())
            .ok_or_else(|| Error::Unauthorized("Pi caller has no actual parent project binding".into()))?.clone();
        let mut caller_snapshot = snapshot.clone();
        if caller_project != snapshot.project {
            let workspace = self.accept(self.resources.fs.open_workspace_measured(caller_project.storage_name()?, self.remaining(budget)?, &cancel).await)?.value;
            let generation = self.accept(workspace.head_measured(self.remaining(budget)?, &cancel).await)?.value;
            caller_snapshot.project_generation = acyclic_harness::resources::GenerationRef::new(
                caller_project.provider().clone(), generation.id().digest().into_bytes(), None)?;
            caller_snapshot.project = caller_project.clone();
        }
        let source_read = if commit.source_task != owner.task_id() {
            let native_snapshot = serde_json::to_value(frozen).map_err(invalid_json)?;
            let policy = PiProjectSourcePolicy { coordinator: self, source: source.clone(), frozen,
                caller: owner.task_id(), selected: commit.source_task, operation: operations.fork,
                native_snapshot: native_snapshot.clone(), budget, cancel: cancel.clone() };
            Some(owner.authorize_project_source(context,
                &self.owner.authority()?.issuer, &policy,
                commit.source_task, operations.fork, ProjectSourceSnapshot {
                    project: snapshot.project.clone(), generation: snapshot.project_generation.clone(),
                    native_snapshot: Some(native_snapshot),
                }, limits.work, limits.bytes).await?)
        } else { None };
        if commit.child.project_volumes.len() != 1 {
            return Err(Error::Invalid("Pi fork must retain exactly its one canonical selected project".into()));
        }
        let child_project = commit.child.project_volumes.get(root_key)
            .ok_or_else(|| Error::Unauthorized("Pi child project changed the original selected root key".into()))?.clone();
        let history_limits = HistoryReadLimits { maximum_events: limits.work.min(u64::from(u32::MAX)) as u32,
            maximum_bytes: limits.bytes };
        let arguments = acyclic_harness::filesystem::ForkToolArguments { id: operations.agent,
            msg: commit.prompt.clone(), source: None };
        let config = RecursiveForkConfig { child: Authority { kind: AggregateKind::Conversation, id: commit.child.task_id.to_string() },
            child_project, child_private: commit.child.scratch.volume.clone(), limits: context.scope().limits(),
            inherited_messages: context.scope().limits().context_messages as u64,
            inherited_bytes: limits.bytes, inherited_references: limits.resources, history_limits };
        let source_binding = recursive_fork_source(&self.runtime.runtime, self.filesystem.clone(), &self.stream,
            &parent, issuer.verifier(), signed, context, caller_snapshot,
            acyclic_harness::resources::ProviderRef::new("local", "stream", "1")?, operations.fork, &arguments, config).await?;
        let capture = Arc::new(PiLazyCapture::new(source.clone(), frozen, self.remaining(budget)?, cancel.clone(), self.work.clone()).await?);
        let mut request = source_binding.request;
        acyclic_harness::filesystem::ForkToolArguments { id: operations.agent, msg: commit.prompt.clone(),
            source: Some(commit.source_task) }.select_project(owner.task_id(), commit.source_task,
            snapshot.project.clone(), snapshot.project_generation.clone(), context.scope().limits(), &mut request)?;
        let results = commit.child.results.as_ref()
            .ok_or_else(|| Error::Invalid("Pi native child requires its distinct unmounted result allocation intent".into()))?;
        if !matches!(results.allocation, PiScratchAllocation::Preparing) {
            return Err(Error::Invalid("Pi result allocation must remain preparing in immutable child input".into()));
        }
        request.preparation.child_result_volume = Some(results.volume.clone());
        request.selections.push(capture.selection());
        let mut preparer = Arc::try_unwrap(source_binding.preparer)
            .map_err(|_| Error::Conflict("Pi source preparer escaped its original construction".into()))?
            .with_capture_provider(capture)?;
        if let Some(source_read) = source_read {
            request.authorized_source = Some(source_read.authorization().clone());
            request = request.with_history_policy(parent.reducer(), acyclic_harness::fork::ForkHistoryPolicy::Fresh)?;
            preparer = preparer.with_project_source_read_handle(source_read)?;
        }
        let checkpoint_bytes = acyclic_harness::contract::canonical_json_bytes(&parent.reducer().snapshot()?)?;
        let mut checkpoint = self.stage_input(context, derived(original, "source-checkpoint"),
            &format!("pi-fork/{original}/source-checkpoint.json"), &checkpoint_bytes, "Pi original caller cut").await?;
        let input_read = self.owner.policy().journal.directory_read_capability(
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY)?;
        if !context.scope().grants().contains(&input_read) {
            return Err(Error::Unauthorized("Pi caller lacks its original immutable input-directory read grant".into()));
        }
        let mut launch_reference = self.stage_original_input(derived(original, "native-input"), "native-input", &commit.launch, budget, &cancel).await?;
        let input = PiTaskInput { binding: commit.child.clone(), execution: PiTaskExecution::Child {
            launch: WorkflowCommand { operation_id: operations.native, kind: NATIVE_LAUNCH_TASK_COMMAND_KIND.into(),
                payload: launch_reference.reference } } };
        let mut child = self.runtime.runtime.harness().registered_child_admission(operations.child,
            "acyclic.pi", "1", serde_json::to_value(input).map_err(invalid_json)?, owner.task_id(), context.scope())?;
        let mut grants = child.grants.iter().filter(|grant| !grant.starts_with("volume:") && !grant.starts_with("directory:")
            && !grant.starts_with("file:")).map(str::to_owned).collect::<Vec<_>>();
        grants.push(input_read);
        child.grants = Capabilities::new(grants);
        let bytes = acyclic_harness::contract::canonical_json_bytes(&request)?;
        let mut fork_input = self.stage_input(context, derived(original, "fork-input"),
            &format!("pi-fork/{original}/fork-input.json"), &bytes, "Pi original fork input").await?;
        let command = WorkflowCommand { operation_id: operations.fork, kind: FORK_PREPARE_TASK_COMMAND_KIND.into(),
            payload: fork_input.reference };
        let project_creation = preparer.project_creation_operation(&request)?;
        let request = fork_preparation_request(&preparer, request, task_id_for_admission(operations.preparation),
            child, command, limits, Some(checkpoint.reference.clone()))?;
        let preparation = self.runtime.runtime.harness().registered_child_admission(operations.preparation,
            "acyclic.fork_preparation", "1", serde_json::to_value(&request).map_err(invalid_json)?, owner.task_id(), context.scope())?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        self.verify_owner(owner).await?;
        let input_allocations = if let Some(work) = &self.work {
            let container = work.claim((std::mem::size_of::<PiForkInputAllocations>()
                + 2 * std::mem::size_of::<usize>()) as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let checkpoint_allocation = checkpoint.allocation.take()
                .ok_or_else(|| Error::Conflict("Pi checkpoint lost its original input custody".into()))?;
            let fork_allocation = fork_input.allocation.take()
                .ok_or_else(|| Error::Conflict("Pi fork input lost its original custody".into()))?;
            Some(Arc::new(PiForkInputAllocations { _references: [Some(checkpoint_allocation), Some(fork_allocation),
                launch_reference.allocation.take(), None], _container: container }))
        } else { None };
        let mut plan = PiForkPlan { original, commit, preparation, request, source_checkpoint: checkpoint.reference,
            source: snapshot, caller_project, frozen, history_limits, project_creation, _input_allocations: input_allocations };
        // This is an immutable original piInputs argument, not a retry or
        // launch ledger. Persist the exact cut before companion custody work.
        let mut publication = self.stage_original_input(original, "fork-plan", &plan, budget, &cancel).await?;
        if let Some(allocations) = plan._input_allocations.as_mut() {
            Arc::get_mut(allocations).ok_or_else(|| Error::Conflict("Pi first plan custody escaped before publication".into()))?
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
        let mut decoded: PiDecodedInput<PiForkPlan> = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::RecursiveForkToolPlan, budget, cancel)?;
        self.validate_plan(&decoded.value, decoded.value.commit.caller)?;
        if decoded.value.original != original { return Err(Error::Unauthorized("Pi original plan locator changed".into())); }
        if let Some(work) = &self.work {
            let container = work.claim((std::mem::size_of::<PiForkInputAllocations>()
                + 2 * std::mem::size_of::<usize>()) as u64).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            decoded.value._input_allocations = Some(Arc::new(PiForkInputAllocations {
                _references: [reference.allocation.take(), decoded.allocation.take(), None, None], _container: container }));
        }
        Ok(Some(decoded.value))
    }

    pub(crate) async fn read_original_input(&self, reference: &FileRef, budget: WorkBudget,
        cancel: &CancellationToken) -> Result<impl AsRef<[u8]>> {
        let authority = self.owner.authority()?;
        let grant = acyclic_harness::conversation::ContentGrant::verify_directory_read(
            &authority.verifier, &authority.signed, &self.owner.policy().journal,
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY)?;
        grant.require_file_read(reference)?;
        Ok(self.accept(self.filesystem.read_content_measured(reference, &grant, self.owner.policy().maximum_payload_bytes, self.remaining(budget)?, cancel).await)?.value)
    }

    fn require_input_read(&self, context: &acyclic_harness::runtime::TaskContext) -> Result<()> {
        if !context.scope().grants().contains(&self.owner.policy().journal.directory_read_capability(
            crate::pi_root_inputs::ROOT_INPUT_DIRECTORY)?) {
            return Err(Error::Unauthorized("Pi caller lacks its original immutable input-directory read grant".into()));
        }
        Ok(())
    }

    fn decode_original_input<T: serde::de::DeserializeOwned>(&self, bytes: &[u8], value_schema: acyclic_harness::JsonValueSchema,
        budget: WorkBudget, cancel: &CancellationToken) -> Result<PiDecodedInput<T>> {
        match &self.work {
            Some(work) => crate::pi_root_provider::guarded_pi_decode(bytes, self.owner.policy().maximum_payload_bytes,
                self.owner.policy().maximum_payload_bytes, value_schema, work)
                .map(|(value, allocation)| PiDecodedInput { value, allocation: Some(allocation) }),
            None => Ok(PiDecodedInput { value: self.accept_root(crate::pi_root_provider::decode_pi_value(bytes,
                self.owner.policy().maximum_payload_bytes, self.owner.policy().maximum_payload_bytes,
                self.remaining(budget)?, cancel))?.value, allocation: None }),
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
            .ok_or_else(|| Error::Unauthorized("Pi prepared observation lost its original immutable plan".into()))?;
        if serde_json::to_value(&original).map_err(invalid_json)? != serde_json::to_value(plan).map_err(invalid_json)? {
            return Err(Error::Unauthorized("Pi prepared observation substituted its original plan".into()));
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
        let reference = self.observe_original_input(derived(plan.original, "native-input"), "native-input", budget, &cancel).await?
            .ok_or_else(|| Error::Unauthorized("Pi prepared observation lost its native input".into()))?;
        let child_input: PiTaskInput = serde_json::from_value(admission.input.clone()).map_err(invalid_json)?;
        let PiTaskExecution::Child { launch } = child_input.execution else {
            return Err(Error::Unauthorized("Pi prepared observation substituted Root".into()));
        };
        let bytes = self.read_original_input(&reference.reference, self.remaining(budget)?, &cancel).await?;
        let intent: PiDecodedInput<PiNativeLaunchIntent> = self.decode_original_input(bytes.as_ref(), acyclic_harness::JsonValueSchema::None, budget, &cancel)?;
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
        Ok(Some(self.created_fork(plan, report, admission, source, self.remaining(budget)?, &cancel).await?))
    }

    async fn prepare_native_source(&self, owner: &TaskJournalOwner<LocalStream>, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken,
    ) -> Result<()> {
        self.verify_owner(owner).await?;
        let request: ForkRequest = serde_json::from_value(plan.request.payload.clone()).map_err(invalid_json)?;
        self.accept(source.prepare_fork_snapshot_measured(request.preparation.child_project_volume.storage_name()?, plan.frozen, plan.project_creation, self.remaining(budget)?, cancel).await)?;
        Ok(())
    }

    /// The service persists its existing Preparing route after this ordinary
    /// admission and before advance may run the provider's FS creator.
    pub(crate) async fn admit_preparation(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, fence: LeaseFence, plan: &PiForkPlan,
    ) -> Result<()> {
        self.authenticate(context, owner, &fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        self.runtime.runtime.task_host().admit_owned(plan.preparation.clone(), fence).await?;
        Ok(())
    }

    /// Drives only the retained ordinary preparation. Unknown acknowledgements
    /// never become replacement tasks, resources, or a new session budget.
    #[allow(clippy::too_many_arguments, reason = "exact parent lease and immutable source selection")]
    /// Advances only the ordinary registered preparation task. Child grants
    /// remain unsealed and no child task is admitted by this method.
    pub(crate) async fn prepare(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<Option<ForkReport>> {
        self.authenticate(context, owner, &fence).await?;
        self.validate_plan(plan, owner.task_id())?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        self.authenticate_launch(context, plan, self.remaining(budget)?, &cancel).await?;
        let caller: PiTaskInput = serde_json::from_value(self.runtime.runtime.task_host()
            .observe_admission(owner.task_id()).await?.input).map_err(invalid_json)?;
        let (_, issuer, signed) = self.owner.conversation_authority(owner.task_id(),
            caller.binding.agent_id, context.scope().grants().clone())?;
        let checkpoint_grant = acyclic_harness::conversation::ContentGrant::verify_read(
            &issuer.verifier(), &signed, &plan.source_checkpoint)?;
        let checkpoint_bytes = self.accept(self.filesystem.read_content_measured(&plan.source_checkpoint,
            &checkpoint_grant, plan.request.maximum_bytes, self.remaining(budget)?, &cancel).await)?.value;
        let snapshot: Snapshot = self.accept_root(crate::pi_root_provider::decode_pi_value(checkpoint_bytes.as_ref(),
            plan.request.maximum_bytes, plan.request.maximum_bytes, self.remaining(budget)?, &cancel))?.value;
        let reducer = Reducer::restore(snapshot, issuer.verifier(), SchemaRegistry::new())?;
        let request: ForkRequest = serde_json::from_value(plan.request.payload.clone()).map_err(invalid_json)?;
        let restored = restore_recursive_fork_source(self.filesystem.clone(), &self.stream, &reducer,
            issuer.verifier(), signed, plan.caller_project.clone(),
            acyclic_harness::resources::ProviderRef::new("local", "stream", "1")?,
            request.clone(), plan.history_limits).await?;
        let capture = Arc::new(PiLazyCapture::new(source.clone(), plan.frozen, self.remaining(budget)?, cancel.clone(), self.work.clone()).await?);
        let mut preparer = Arc::try_unwrap(restored.preparer)
            .map_err(|_| Error::Conflict("restored Pi preparer escaped its original owner".into()))?
            .with_capture_provider(capture)?;
        if let Some(source_read) = owner.restore_project_source(context, &plan.request).await? {
            preparer = preparer.with_project_source_read_handle(source_read)?;
        }
        let preparer = Arc::new(preparer);
        if preparer.project_creation_operation(&request)? != plan.project_creation {
            return Err(Error::Unauthorized("Pi native companion changed its original canonical FS operation".into()));
        }
        let restored_request = fork_preparation_request(preparer.as_ref(), request,
            plan.preparation.task_id(), plan.request.child.clone(),
            plan.request.command.clone().ok_or_else(|| Error::Invalid("Pi preparation command missing".into()))?,
            ForkPreparationLimits { resources: plan.request.maximum_resources,
                work: plan.request.maximum_work, bytes: plan.request.maximum_bytes },
            Some(plan.source_checkpoint.clone()))?;
        if restored_request != plan.request {
            return Err(Error::Unauthorized("Pi restored preparation changed its original source or child admission".into()));
        }
        let host = self.runtime.runtime.task_host();
        self.verify_owner(owner).await?;
        host.admit_owned(plan.preparation.clone(), fence.clone()).await?;
        if host.observe_admission(plan.preparation.task_id()).await? != plan.preparation {
            return Err(Error::Unauthorized("Pi preparation belongs to another original operation".into()));
        }
        if host.outcome(plan.preparation.task_id()).await?.is_none() {
            let commands = self.runtime.runtime.commands().with_fork_preparer(preparer.clone());
            match host.recover_work(plan.preparation.task_id()).await? {
                WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => {
                    self.runtime.runtime.resume_task(lease, &commands, 8).await;
                }
                WorkPull::Idle => {
                    self.runtime.runtime.run_operation(&Worker {
                        id: format!("pi-fork:{}", self.resources.instance_id),
                        available: ResourceSnapshot::default(), labels: BTreeMap::new(),
                    }, plan.preparation.task_id(), &commands, 8).await?;
                }
            }
        }
        let report = match host.outcome(plan.preparation.task_id()).await? {
            Some(Outcome::Succeeded(value)) => serde_json::from_value::<ForkReport>(value).map_err(invalid_json)?,
            Some(Outcome::Failed { message }) => return Err(Error::Conflict(message)),
            Some(Outcome::Cancelled) => return Err(Error::Conflict("Pi preparation cancelled; no child was admitted".into())),
            None | Some(Outcome::Indeterminate { .. }) => return Ok(None),
        };
        report.validate()?;
        if serde_json::to_value(&report.request).map_err(invalid_json)? != plan.request.payload {
            return Err(Error::Unauthorized("Pi preparation report changed the retained original request".into()));
        }
        self.verify_owner(owner).await?;
        Ok(Some(report))
    }

    #[allow(clippy::too_many_arguments, reason = "exact parent lease and immutable source selection")]
    pub(crate) async fn advance(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken,
    ) -> Result<Option<PreparedPiFork>> {
        let Some(report) = self.prepare(context, owner, fence.clone(), plan, source.clone(), self.remaining(budget)?, cancel.clone()).await? else {
            return Ok(None);
        };
        let host = self.runtime.runtime.task_host();
        self.verify_owner(owner).await?;
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        // The existing host seals actual creator receipts under this same
        // original parent lease before any child adapter may dispatch.
        host.admit_owned(plan.request.child.clone(), fence).await?;
        let admission = host.observe_admission(plan.request.child.task_id()).await?;
        if admission != plan.request.child {
            return Err(Error::Unauthorized("Pi child admission differs from its actual creator request".into()));
        }
        self.verify_owner(owner).await?;
        self.publish(context, owner, plan, &report, source.clone(), self.remaining(budget)?, &cancel).await?;
        let prepared = self.created_fork(plan, report, admission, source, self.remaining(budget)?, &cancel).await?;
        self.verify_owner(owner).await?;
        Ok(Some(prepared))
    }

    /// Reopens only a previously published and bound admitted child. A terminal
    /// original caller never receives a replacement lease or fabricated admit.
    pub(crate) async fn restore_admitted(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, fence: LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken) -> Result<PreparedPiFork> {
        let admission = self.authenticate(context, owner, &fence).await?;
        self.validate_plan(plan, plan.commit.caller)?;
        if owner.task_id() != plan.request.child.task_id() || admission != plan.request.child {
            return Err(Error::Unauthorized("Pi cold restore does not own the original admitted child".into()));
        }
        cancel.check().map_err(|_| Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })?;
        let prepared = self.observe_prepared(plan, source, self.remaining(budget)?, cancel).await?
            .ok_or_else(|| Error::Conflict("Pi cold child has no successful published original creator".into()))?;
        self.verify_owner(owner).await?;
        Ok(prepared)
    }

    async fn created_fork(&self, plan: &PiForkPlan, report: ForkReport, admission: TaskAdmissionRecord,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken) -> Result<PreparedPiFork> {
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
            frozen_source: plan.frozen, native_lazy, workspace })
    }

    async fn publish(&self, context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>, plan: &PiForkPlan, report: &ForkReport,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: &CancellationToken,
    ) -> Result<()> {
        let parent_input: PiTaskInput = serde_json::from_value(self.runtime.runtime.task_host()
            .observe_admission(owner.task_id()).await?.input).map_err(invalid_json)?;
        let (parent_authority, parent_issuer, parent_scope) = self.owner.conversation_authority(
            owner.task_id(), parent_input.binding.agent_id, context.scope().grants().clone(),
        )?;
        let seed = report.clone().into_seed()?;
        let capture = Arc::new(PiLazyCapture::new(source, plan.frozen, self.remaining(budget)?, cancel.clone(), self.work.clone()).await?);
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
        let original = original_operation(self.owner.policy().root_task, caller, &plan.commit.sdk_operation);
        let operations = PiForkOperations::new(original);
        plan.request.validate()?;
        plan.preparation.validate()?;
        let child_input: PiTaskInput = serde_json::from_value(plan.request.child.input.clone()).map_err(invalid_json)?;
        let request: ForkRequest = serde_json::from_value(plan.request.payload.clone()).map_err(invalid_json)?;
        let child_launch = match child_input.execution {
            PiTaskExecution::Child { launch } => launch,
            PiTaskExecution::Host { .. } => return Err(Error::Unauthorized("Pi fork admission substituted a root task".into())),
        };
        plan.commit.launch.validate()?;
        let project_selected = request.selections.iter().any(|selection| matches!(
            &selection.revision, acyclic_harness::fork::ResourceRevision::Project { volume, generation }
                if volume == &plan.source.project && generation == &plan.source.project_generation));
        let scratch_selected = request.selections.iter().any(|selection| matches!(
            &selection.revision, acyclic_harness::fork::ResourceRevision::PrivateVolume { volume, generation, paths }
                if volume == &plan.source.private_volume && generation == &plan.source.private_generation && paths.is_empty()));
        let native_snapshot = serde_json::to_value(plan.frozen).map_err(invalid_json)?;
        let source_authorized = match (&request.authorized_source, plan.commit.source_task == caller) {
            (None, true) => true,
            (Some(source), false) => source.caller_task == caller && source.source_task == plan.commit.source_task
                && source.operation_id == operations.fork && source.graph_root == self.owner.policy().root_task
                && source.project == plan.source.project && source.generation == plan.source.project_generation
                && source.native_snapshot.as_ref() == Some(&native_snapshot),
            _ => false,
        };
        if plan.original != original || plan.commit.caller != caller
            || plan.commit.alias != plan.commit.launch.agent_alias
            || plan.preparation.operation_id != operations.preparation
            || plan.preparation.parent != Some(caller)
            || plan.preparation.input != serde_json::to_value(&plan.request).map_err(invalid_json)?
            || plan.request.child.operation_id != operations.child
            || plan.request.child.parent != Some(caller)
            || plan.request.source_checkpoint.as_ref() != Some(&plan.source_checkpoint)
            || plan.source.task != caller
            || plan.request.command.as_ref().is_none_or(|command| command.operation_id != operations.fork)
            || request.operation_id != operations.fork || request.child_agent != operations.agent
            || request.child.id != plan.commit.child.task_id.to_string()
            || child_input.binding != plan.commit.child
            || child_launch.operation_id != operations.native || child_launch.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND
            || !project_selected || !scratch_selected
            || !source_authorized
            || request.preparation.child_result_volume.as_ref() != plan.commit.child.results.as_ref().map(|result| &result.volume)
            || plan.commit.child.results.as_ref().is_none_or(|result| !matches!(result.allocation, PiScratchAllocation::Preparing))
            || plan.commit.launch.invocation.environment.keys().any(|key| reserved_native_key(key))
            || plan.source.project_generation.as_resource().key() != plan.frozen.authored_generation.digest().as_bytes()
        {
            return Err(Error::Unauthorized("Pi fork plan changed the original admission, operation, or caller cut".into()));
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
    native_snapshot: serde_json::Value,
    caller: TaskId,
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
                || snapshot.native_snapshot.as_ref() != Some(&self.native_snapshot)
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
                coordinator.accept_root(crate::pi_root_provider::prove_root_project_source(coordinator.owner.clone(),
                    coordinator.resources.clone(), coordinator.filesystem.clone(), coordinator.stream.clone(),
                    &request, &snapshot.project, self.frozen, coordinator.remaining(remaining)?, &self.cancel).await)?;
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
