//! Ordinary Pi fork controls over the one retained catalog and task journal.
//! Preparation admission precedes the catalog power-loss barrier; creation never does.
use crate::{AdapterState, PendingSpawn, PendingSpawnLifecycle, PluginTaskBinding, ServiceResources,
    pi_fork::{PiForkCoordinator, PiForkOperations, PiForkPlan, PreparedPiFork, RetainedSpawnCommit, original_operation, derived},
    pi_native::{planned_native_paths}, pi_native_host::{PiNativeCommandHost, PiNativeRuntime, PiNativeFilesystem},
    pi_owner::PiOwnerConfig, pi_root_inputs::PiRootRequest, pi_root_provider::PiRootHandle,
    pi_task::{PiTaskInput, PiTaskExecution}};
use acyclic_fs::{CancellationToken, LazySnapshotRef, WorkBudget, NativeMaterializationWork};
use acyclic_harness::{Error, Result, TaskId, OperationId,
    durable_host::TaskJournalOwner, filesystem::{TaskCommandHost, TaskCommandProgress, ForkPreparationLimits},
    core::SchemaRegistry, runtime::TaskContext, scheduler::LeaseFence,
    store::StreamAggregate, workflow::WorkflowCommand};
use acyclic_stream::{LocalStream, StreamClient};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::{Arc, Mutex}};

pub(crate) const PI_FORK_COMMAND_KIND: &str = "acyclic.pi.fork";
pub(crate) const PI_OBSERVE_SOURCE_COMMAND_KIND: &str = "acyclic.pi.observe-source";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkCommand {
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) root_source: Option<LazySnapshotRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiObserveSourceCommand {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) root_key: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservedSource {
    caller: TaskId,
    sdk_operation: String,
    root_key: String,
    snapshot: LazySnapshotRef,
}

const OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES: u64 = crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
    "ObservedSource", "caller", "sdk_operation", "root_key", "snapshot", "TaskId",
    "LazySnapshotRef", "id", "workspace_id", "authored_generation", "source", "overlay", "shadows",
    "SourceReference", "identity", "epoch", "an array of length 16",
]);

#[derive(Serialize)]
struct PiPreparedReply<'a> {
    task: TaskId,
    binding: &'a crate::PiTaskBinding,
    state: &'static str,
}

#[derive(Serialize)]
struct PiObservedSourceReply<'a> {
    snapshot: LazySnapshotRef,
    root_key: &'a str,
}

pub(crate) struct PiForkHost<'a> {
    runtime: Arc<PiNativeRuntime>,
    filesystem: Arc<PiNativeFilesystem>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    protected: PiOwnerConfig,
    retained: &'a AdapterState,
    roots: &'a BTreeMap<String, PiRootHandle>,
    native: Arc<PiNativeCommandHost>,
    pending: Mutex<Vec<PendingSpawn>>,
    prepared: Mutex<BTreeMap<TaskId, PreparedPiFork>>,
    work: Option<NativeMaterializationWork>,
}

impl<'a> PiForkHost<'a> {
    #[allow(clippy::too_many_arguments, reason = "original catalog, provider handles and protected runtime are independent")]
    pub(crate) fn new(runtime: Arc<PiNativeRuntime>, filesystem: Arc<PiNativeFilesystem>,
        stream: StreamClient<LocalStream>, resources: ServiceResources, protected: PiOwnerConfig,
        retained: &'a AdapterState, roots: &'a BTreeMap<String, PiRootHandle>, native: Arc<PiNativeCommandHost>) -> Self {
        Self { runtime, filesystem, stream, resources, protected, retained, roots, native,
            pending: Mutex::new(Vec::new()), prepared: Mutex::new(BTreeMap::new()), work: None }
    }

    pub(crate) fn with_work(mut self, work: NativeMaterializationWork) -> Self {
        self.work = Some(work);
        self
    }

    fn command_work(&self, cancel: &CancellationToken) -> Result<NativeMaterializationWork> {
        match &self.work { Some(work) => Ok(work.clone()), None => NativeMaterializationWork::new(
            crate::pi_root_provider::allocation_budget(), cancel).map_err(|(cause, work)| Error::NativeConstruction { cause, work }) }
    }

    /// The actor durably saves these existing Preparing records before another pass.
    pub(crate) fn pending_to_save(&self) -> Result<Vec<PendingSpawn>> {
        Ok(std::mem::take(&mut *self.pending.lock().map_err(|_| invalid("Pi pending control lock poisoned"))?))
    }

    /// The actor saves the Created route before handing this to the native host.
    pub(crate) fn take_prepared(&self, task: TaskId) -> Result<Option<PreparedPiFork>> {
        Ok(self.prepared.lock().map_err(|_| invalid("Pi prepared control lock poisoned"))?.remove(&task))
    }

    fn coordinator(&self, work: &NativeMaterializationWork) -> Result<PiForkCoordinator> {
        Ok(PiForkCoordinator::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            self.resources.clone(), self.protected.clone())?.with_work(work.clone()))
    }

    async fn context(&self, context: TaskContext, fence: &LeaseFence,
        cancel: &CancellationToken, work: &NativeMaterializationWork) -> Result<(TaskContext, Arc<TaskJournalOwner<LocalStream>>, PiTaskInput)> {
        self.runtime.verify_owner(&self.protected)?;
        let task = context.durable_task_id().ok_or_else(|| invalid("Pi fork control has no original task"))?;
        let owner = Arc::new(self.runtime.runtime.task_host().journal_owner(task, fence.clone()).await?);
        crate::pi_inspection::authenticate(&context, &owner).await?;
        let admission = self.runtime.runtime.task_host().observe_admission(task).await?;
        let input: PiTaskInput = serde_json::from_value(admission.input).map_err(json_error)?;
        let context = if task == self.protected.policy().root_task {
            let PiTaskExecution::Host { root, .. } = &input.execution else {
                return Err(unauthorized("Pi original root input substituted a child"));
            };
            let request = PiRootRequest { binding: input.binding.clone(), root: root.clone() };
            accept_root(&work, crate::pi_root_provider::bind_root_output_context(context, &owner, self.protected.clone(), self.resources.clone(), self.filesystem.clone(), self.stream.clone(), &request, remaining(&work)?, cancel).await)?.value
        } else {
            context.with_output_volume(&input.binding.scratch.volume)?
        };
        Ok((context, owner, input))
    }

    async fn source(&self, context: &TaskContext, owner: &TaskJournalOwner<LocalStream>,
        command: &PiForkCommand, plan: Option<&PiForkPlan>, cancel: &CancellationToken,
        work: &NativeMaterializationWork)
        -> Result<(Arc<crate::LocalLazyWorkspace>, LazySnapshotRef, Option<acyclic_fs::NativeAllocation>)> {
        let root_key = command.commit.child.project_volumes.keys().next()
            .ok_or_else(|| invalid("Pi fork has no selected project"))?;
        if command.commit.child.project_volumes.len() != 1 {
            return Err(invalid("Pi fork requires exactly its actually selected project"));
        }
        let source = if command.commit.source_task == self.protected.policy().root_task {
            let actual = self.roots.get(root_key).ok_or_else(|| invalid("Pi original selected root has no actual lazy handle"))?;
            Arc::new(actual.workspace.clone())
        } else {
            self.native.live_workspace(command.commit.source_task)
                .ok_or_else(|| invalid("Pi selected child has no actual reopened creator workspace"))?
        };
        if let Some(plan) = plan {
            accept(&work, source.validate_snapshot_measured(plan.frozen, remaining(&work)?, cancel).await)?;
            return Ok((source, plan.frozen, None));
        }
        let mut source_custody = None;
        let frozen = if command.commit.source_task == self.protected.policy().root_task {
            if let Some(frozen) = command.root_source {
                accept(&work, source.validate_snapshot_measured(frozen, remaining(&work)?, cancel).await)?;
                frozen
            } else if owner.task_id() == self.protected.policy().root_task {
                let actual = self.roots.get(root_key).ok_or_else(|| invalid("Pi selected root handle missing"))?;
                accept_root(&work, crate::pi_lazy_capture::freeze_root_source(owner.task_id(), context, owner, &actual.physical, &source, remaining(&work)?, cancel).await)?.value
            } else {
                return Err(invalid("Pi selected Root requires its real original Root observe control receipt"));
            }
        } else {
            let outcome = self.runtime.runtime.task_host().outcome(command.commit.source_task).await?;
            if outcome.as_ref().is_some_and(|outcome| outcome.is_terminal()) {
                let (snapshot, custody) = self.native.selection_snapshot(command.commit.source_task, work).await?;
                source_custody = Some(custody);
                snapshot
            } else if let Some(live) = self.native.live_source(command.commit.source_task, work).await? {
                if live.binding.task_id != command.commit.source_task || live.workspace.workspace().id() != source.workspace().id() {
                    return Err(unauthorized("Pi live source substituted its actual creator workspace"));
                }
                let (snapshot, custody) = crate::pi_lazy_capture::freeze_child_source(
                    live.context.context(), &live.owner, &live.view, &source, work).await?;
                source_custody = Some(custody);
                snapshot
            } else {
                let frozen = self.native.source_frozen(command.commit.source_task)
                    .ok_or_else(|| invalid("Pi undispatched source lost its actual first native companion"))?;
                accept(&work, source.validate_snapshot_measured(frozen, remaining(&work)?, cancel).await)?;
                frozen
            }
        };
        Ok((source, frozen, source_custody))
    }

    fn parent_alias(&self, task: TaskId) -> Result<String> {
        if task == self.protected.policy().root_task { return Ok(self.retained.root_session_id.clone()); }
        self.retained.routes.values().find_map(|route| match &route.task_binding {
            PluginTaskBinding::Pi(binding) if binding.task_id == task => Some(route.agent_id.clone()),
            _ => None,
        }).ok_or_else(|| unauthorized("Pi original caller has no actual persisted route alias"))
    }

    pub(crate) fn reservation(&self, plan: &PiForkPlan) -> Result<PendingSpawn> {
        let root_key = plan.commit.child.project_volumes.keys().next().ok_or_else(|| invalid("Pi project missing"))?;
        let root: [u8; 16] = crate::decode_fixed(root_key).map_err(Error::Invalid)?;
        let paths = planned_native_paths(&self.resources, &self.retained.root_session_id, plan.commit.child.task_id, Some(root_key))?;
        if paths.cwd != plan.commit.launch.invocation.cwd || paths.session_file != plan.commit.launch.invocation.session_file {
            return Err(unauthorized("Pi SDK retained native paths differ from its original planned namespace"));
        }
        Ok(PendingSpawn { task_binding: PluginTaskBinding::Pi(Box::new(plan.commit.child.clone())),
            agent_id: Some(plan.commit.alias.clone()),
            parent_agent_id: self.parent_alias(plan.commit.caller)?, tool_use_id: plan.commit.sdk_operation.clone(),
            active_root_id: Some(root), expires_at_millis: crate::now_millis().saturating_add(2 * 60 * 1_000),
            workspace_name: plan.commit.child.project_volumes.values().next().ok_or_else(|| invalid("Pi project missing"))?.storage_name()?,
            fork_key: PiForkOperations::new(plan.original).fork.into_bytes(), roots: BTreeMap::new(),
            mount_path: paths.view_root, lifecycle: PendingSpawnLifecycle::Preparing })
    }

    async fn fork(&self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value, reconcile: bool) -> Result<TaskCommandProgress> {
        let mut input: PiForkCommand = serde_json::from_value(payload).map_err(json_error)?;
        input.commit.launch.invocation.environment.retain(|key, _| !reserved_key(key));
        let cancel = CancellationToken::new();
        let work = self.command_work(&cancel)?;
        let cancel = work.cancellation().clone();
        let budget = remaining(&work)?;
        let (context, owner, caller) = self.context(context, &fence, &cancel, &work).await?;
        let original = original_operation(self.protected.policy().root_task, owner.task_id(), &input.commit.sdk_operation);
        if command.operation_id != original || input.commit.caller != owner.task_id()
            || input.commit.launch.source.parent_session_id != caller.binding.native_session_id {
            return Err(unauthorized("Pi fork control changed its original caller/SDK operation"));
        }
        if self.retained.pending.iter().any(|pending| pending.agent_id.as_deref() == Some(input.commit.alias.as_str())
            && pending.fork_key != PiForkOperations::new(original).fork.into_bytes()) {
            return Err(Error::Conflict("Pi requested alias is already reserved by another original fork".into()));
        }
        if self.retained.routes.values().any(|route| route.agent_id == input.commit.alias
            && !matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if same_identity(binding, &input.commit.child))) {
            return Err(Error::Conflict("Pi requested alias already belongs to another admitted child".into()));
        }
        let coordinator = self.coordinator(&work)?;
        let existing = coordinator.load_plan(&context, original, remaining(&work)?, &cancel).await?;
        if reconcile && existing.is_none() { return Ok(TaskCommandProgress::Pending); }
        let (source, frozen, _source_custody) = self.source(&context, &owner, &input, existing.as_ref(), &cancel, &work).await?;
        let (authority, issuer, _) = self.protected.conversation_authority(owner.task_id(), caller.binding.agent_id,
            context.scope().grants().clone())?;
        let parent = accept(&work, StreamAggregate::open_measured(&self.stream, authority, issuer.verifier(), SchemaRegistry::new(), remaining(&work)?, &cancel).await)?.value.with_limits(context.scope().limits())?;
        let plan = coordinator.begin(&context, &owner, fence.clone(), parent, source.clone(), frozen, input.commit, ForkPreparationLimits { resources: 128, work: 1_000_000,
            bytes: self.protected.policy().maximum_payload_bytes }, remaining(&work)?, cancel.clone()).await?;
        let saved = self.retained.pending.iter().find(|pending| pending.fork_key == PiForkOperations::new(original).fork.into_bytes());
        if let Some(saved) = saved {
            let PluginTaskBinding::Pi(binding) = &saved.task_binding else { return Err(unauthorized("Pi pending substituted host-managed admission")); };
            let root_key = plan.commit.child.project_volumes.keys().next().ok_or_else(|| invalid("Pi child project missing"))?;
            let root: [u8; 16] = crate::decode_fixed(root_key).map_err(Error::Invalid)?;
            let paths = planned_native_paths(&self.resources, &self.retained.root_session_id,
                plan.commit.child.task_id, Some(root_key))?;
            if !same_identity(binding, &plan.commit.child) || saved.tool_use_id != plan.commit.sdk_operation
                || saved.agent_id.as_deref() != Some(plan.commit.alias.as_str())
                || saved.parent_agent_id != self.parent_alias(plan.commit.caller)?
                || saved.workspace_name != plan.commit.child.project_volumes.values().next().ok_or_else(|| invalid("Pi child project missing"))?.storage_name()?
                || saved.active_root_id != root || saved.mount_path != paths.view_root
                || !saved.roots.is_empty() || !matches!(&saved.lifecycle, PendingSpawnLifecycle::Preparing)
                || saved.expires_at_millis <= crate::now_millis() {
                return Err(unauthorized("Pi pending changed or expired its original preparation identity"));
            }
        } else if let Some(route) = self.retained.routes.values().find(|route| route.agent_id == plan.commit.alias) {
            let PluginTaskBinding::Pi(binding) = &route.task_binding else { return Err(unauthorized("Pi existing route is not its original admitted child")); };
            if !same_identity(binding, &plan.commit.child) || route.turn_id != plan.commit.sdk_operation
                || route.parent_agent_id != self.parent_alias(plan.commit.caller)? {
                return Err(unauthorized("Pi existing route changed its original preparation identity"));
            }
            // A previously saved Created route already crossed the same catalog
            // barrier. Observe its original creator rather than reserve anew.
        } else {
            // This ordinary admit is durable. The caller MUST save the catalog
            // record before executing this command again; no creator runs here.
            let reservation = self.reservation(&plan)?;
            coordinator.admit_preparation(&context, &owner, fence, &plan).await?;
            self.pending.lock().map_err(|_| invalid("Pi pending control lock poisoned"))?.push(reservation);
            return Ok(TaskCommandProgress::Pending);
        }
        let Some(prepared) = coordinator.advance(&context, &owner, fence, &plan, source, remaining(&work)?, cancel).await? else {
            return Ok(TaskCommandProgress::Pending);
        };
        let task = prepared.binding.task_id;
        let result = ready(&PiPreparedReply { task, binding: &prepared.binding, state: "prepared" }, &work)?;
        self.prepared.lock().map_err(|_| invalid("Pi prepared control lock poisoned"))?.insert(task, prepared);
        Ok(result)
    }

    async fn observe_source(&self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value) -> Result<TaskCommandProgress> {
        let input: PiObserveSourceCommand = serde_json::from_value(payload).map_err(json_error)?;
        let cancel = CancellationToken::new();
        let work = self.command_work(&cancel)?;
        let cancel = work.cancellation().clone();
        let (context, owner, _) = self.context(context, &fence, &cancel, &work).await?;
        let original = original_operation(self.protected.policy().root_task, input.caller, &input.sdk_operation);
        if owner.task_id() != self.protected.policy().root_task || input.sdk_operation.is_empty()
            || command.operation_id != derived(original, "observe-root-source") {
            return Err(unauthorized("Pi Root observer changed its actual owner or original SDK operation"));
        }
        crate::pi_inspection::authorize_selected_source(&self.runtime.runtime, self.protected.policy().root_task,
            &context, &owner, input.caller, self.protected.policy().session_limits.depth).await?;
        let actual = self.roots.get(&input.root_key).ok_or_else(|| invalid("Pi observed root has no actual lazy producer"))?;
        if let Some(reference) = crate::pi_root_provider::observe_pi_input_with_native_work(&self.protected, &self.filesystem, command.operation_id, "root-source-observation", &work).await? {
            let bytes = self.coordinator(&work)?.read_original_input(&reference.0, remaining(&work)?, &cancel).await?;
            let decoded: (ObservedSource, _) = crate::pi_root_provider::guarded_pi_decode(bytes.as_ref(),
                self.protected.policy().maximum_payload_bytes, self.protected.policy().maximum_payload_bytes,
                acyclic_harness::JsonValueSchema::None, OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES, &work)?;
            let observed = &decoded.0;
            if observed.caller != input.caller || observed.sdk_operation != input.sdk_operation || observed.root_key != input.root_key {
                return Err(unauthorized("Pi original Root observation retry changed its source request"));
            }
            accept(&work, actual.workspace.validate_snapshot_measured(observed.snapshot, remaining(&work)?, &cancel).await)?;
            return ready(&PiObservedSourceReply { snapshot: observed.snapshot, root_key: &observed.root_key }, &work);
        }
        let snapshot = accept_root(&work, crate::pi_lazy_capture::freeze_root_source(owner.task_id(), &context, &owner, &actual.physical, &actual.workspace, remaining(&work)?, &cancel).await)?.value;
        owner.verify_with_native_work(false, &work).await?;
        let observed = ObservedSource { caller: input.caller, sdk_operation: input.sdk_operation,
            root_key: input.root_key, snapshot };
        let _published = crate::pi_root_provider::stage_pi_input_with_native_work(&self.protected, &self.filesystem, command.operation_id, "root-source-observation", &observed, &work).await?;
        owner.verify_with_native_work(false, &work).await?;
        ready(&PiObservedSourceReply { snapshot: observed.snapshot, root_key: &observed.root_key }, &work)
    }
}

impl TaskCommandHost for PiForkHost<'_> {
    fn execute<'a>(&'a self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value) -> acyclic_stream::BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move { match command.kind.as_str() {
            PI_FORK_COMMAND_KIND => self.fork(context, fence, command, payload, false).await,
            PI_OBSERVE_SOURCE_COMMAND_KIND => self.observe_source(context, fence, command, payload).await,
            _ => Err(Error::Unsupported("command is not an original Pi fork/source control".into())),
        } })
    }
    fn reconcile<'a>(&'a self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value) -> acyclic_stream::BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move { match command.kind.as_str() {
            PI_FORK_COMMAND_KIND => self.fork(context, fence, command, payload, true).await,
            PI_OBSERVE_SOURCE_COMMAND_KIND => self.observe_source(context, fence, command, payload).await,
            _ => Err(Error::Unsupported("command is not an original Pi fork/source control".into())),
        } })
    }
}
fn same_identity(left: &crate::PiTaskBinding, right: &crate::PiTaskBinding) -> bool {
    left.task_id == right.task_id && left.agent_id == right.agent_id && left.native_session_id == right.native_session_id
        && left.project_volumes == right.project_volumes && left.scratch.volume == right.scratch.volume
        && left.results.as_ref().map(|value| &value.volume) == right.results.as_ref().map(|value| &value.volume)
}
fn reserved_key(key: &str) -> bool {
    ["ACYCLIC_PI_CALLER", "ACYCLIC_PI_ROUTE", "ACYCLIC_PI_TASK", "ACYCLIC_PI_TOKEN", "ACYCLIC_PI_SCOPE", "ACYCLIC_PI_LEASE"]
        .iter().any(|reserved| key.eq_ignore_ascii_case(reserved))
        || key.get(.."ACYCLIC_PI_CALLER_".len()).is_some_and(|prefix| prefix.eq_ignore_ascii_case("ACYCLIC_PI_CALLER_"))
}
fn json_error(error: serde_json::Error) -> Error { Error::Invalid(error.to_string()) }
fn invalid(message: &str) -> Error { Error::Invalid(message.into()) }
fn unauthorized(message: &str) -> Error { Error::Unauthorized(message.into()) }

fn ready<T: Serialize>(response: &T, work: &NativeMaterializationWork) -> Result<TaskCommandProgress> {
    let (value, custody) = crate::pi_root_provider::guarded_pi_value(response,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
    Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
}
fn measured<E: std::fmt::Display>(failure: acyclic_fs::OperationFailure<E>) -> Error {
    Error::Storage(format!("{}; work={:?}", failure.error, failure.work))
}

fn remaining(work: &NativeMaterializationWork) -> Result<WorkBudget> {
    work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })
}

fn accept<T, E: std::fmt::Display>(work: &NativeMaterializationWork,
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, E>) -> Result<acyclic_fs::OperationReceipt<T>> {
    let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
    work.spent(spent).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    remaining(work)?;
    result.map_err(measured)
}

fn accept_root<T>(work: &NativeMaterializationWork,
    result: crate::pi_root_provider::PiMeasuredResult<acyclic_fs::OperationReceipt<T>>) -> Result<acyclic_fs::OperationReceipt<T>> {
    let spent = match &result { Ok(receipt) => receipt.work, Err((_, spent)) => *spent };
    work.spent(spent).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
    result.map_err(|(cause, _)| cause)
}
