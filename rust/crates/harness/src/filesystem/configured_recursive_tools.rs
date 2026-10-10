//! Explicit authenticated provider bindings for the shared portable default tools.
use crate::{
    Capabilities, Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, ConversationMessage, Limits, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Authority, AuthorityIssuer, AuthorityVerifier, Reducer, SchemaRegistry, Scope, Snapshot},
    durable_tool::ResumableTool,
    filesystem::{FilesystemHost, FilesystemTaskRuntime, FilesystemContentVerifier, FilesystemForkVerifier, FilesystemProjectMergeVerifier,
        ForkPreparationLimits, ForkToolArguments, MergeToolArguments, RecursiveDefaultToolSource,
        RecursiveDefaultToolset, RecursiveForkConfig, RecursiveForkSourceIntent, RecursiveForkToolBinding, RecursiveForkToolPlan,
        RecursiveMergeToolBinding, RecursiveMergeToolPlan, RecursiveSourceSnapshot, RecursiveToolOwner,
        RetainedSourceBinding, StreamHistoryForkVerifier, recursive_default_tools, recursive_fork_source,
        restore_recursive_fork_source, workspace_ref},
    fork::{CompositeForkVerifier, ForkRequest},
    resource_preparation::TaskResourcePreparation,
    resources::ProviderRef,
    runtime::{DurableTaskHost, TaskAdmissionRecord, TaskContext, ToolContext},
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
    source_authorization::{ProjectSourceReadHandle, ProjectSourceSnapshot, RootProjectSourceProvider},
    tool::ToolInvocation,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget, WorkCounters};
use acyclic_stream::{BoxProviderFuture, StreamClient, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, OnceLock, Weak};

type Owner<P, A, O> = RecursiveToolOwner<P, A, O>;
type ForkBinding<P, A, O> = RecursiveForkToolBinding<P, A, O>;
type MergeBinding<P, A, O> = RecursiveMergeToolBinding<P, A, O>;

/// Host-owned conversation signing credentials and its exact signed actor.
/// These are configuration, never model arguments or a foreign scope import.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveAuthorityBinding {
    pub authority: Authority,
    pub issuer: String,
    pub key: [u8; 32],
    pub scope: Scope,
}

/// Explicit root binding or descendant binding checked against real admission.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveCaller {
    pub task: TaskId,
    pub owner: ConfiguredRecursiveAuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    /// Actual unmounted output binding; never substituted for source scratch.
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    /// Genuine producer descriptor, verified by the installed trusted policy.
    #[serde(default)]
    pub native_snapshot: Option<Value>,
}

/// One independently allocated child, its ordinary launch input and prep bounds.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveFork {
    pub caller: TaskId,
    pub child: ConfiguredRecursiveAuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    #[serde(deserialize_with = "canonical_admission")]
    pub admission: TaskAdmissionRecord,
    pub child_message_pointer: String,
    #[serde(default)]
    pub child_message_file_pointer: Option<String>,
    #[serde(default)]
    pub input_files: Vec<crate::conversation::FileRef>,
    pub limits: ForkPreparationLimits,
}

fn canonical_admission<'de, D: serde::Deserializer<'de>>(deserializer: D)
    -> std::result::Result<TaskAdmissionRecord, D::Error> {
    TaskAdmissionRecord::from_canonical_value(Value::deserialize(deserializer)?)
        .map_err(serde::de::Error::custom)
}

/// An explicit read-only Project import and the caller-owned canonical notice.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveMerge { pub caller: TaskId, pub source: TaskId, pub notice: ConversationMessage }

/// Existing trusted host-retained original cut, never a model-provided ledger.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveSourceCheckpoint {
    pub caller: TaskId,
    pub request: ForkRequest,
    pub snapshot: Snapshot,
}

/// Finite trusted configuration shared by native and browser default consumers.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveToolsConfig {
    pub callers: Vec<ConfiguredRecursiveCaller>,
    pub forks: Vec<ConfiguredRecursiveFork>,
    /// Browser host's explicit actual graph root for the creator-only policy.
    /// Native consumers may install their separately verified policy directly.
    #[serde(default)]
    pub project_policy_graph_root: Option<TaskId>,
    pub merges: Vec<ConfiguredRecursiveMerge>,
    #[serde(default)]
    pub original_sources: Vec<ConfiguredRecursiveSourceCheckpoint>,
    pub stream_provider: ProviderRef,
    pub history_limits: HistoryReadLimits,
    pub inherited_messages: u64,
    pub inherited_references: u32,
    /// Finite original-checkpoint observer, reader and publisher allowance.
    pub content_budget: WorkBudget,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceCheckpoint { snapshot: Snapshot, request: ForkRequest, caller: TaskId, arguments: ForkToolArguments }

/// Portable source factory using the runtime's actual Filesystem and Stream.
pub struct ConfiguredRecursiveToolSource<P, A, O> {
    config: ConfiguredRecursiveToolsConfig,
    stream: StreamClient<P>,
    filesystem: Arc<FilesystemHost<A, O>>,
    limits: Limits,
    runtime: OnceLock<Weak<FilesystemTaskRuntime<P, A, O>>>,
    project_policy: OnceLock<(AuthorityIssuer, Arc<dyn RootProjectSourceProvider>)>,
}

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    /// Validates trusted bindings without publishing, allocating or opening tasks.
    pub fn new(config: ConfiguredRecursiveToolsConfig, stream: StreamClient<P>,
        filesystem: Arc<FilesystemHost<A, O>>, provider: &ProviderRef, limits: Limits,
    ) -> Result<Arc<Self>> {
        if config.callers.len() > 64 || config.forks.len() > 64 || config.merges.len() > 64 || config.original_sources.len() > 64
            || config.history_limits.maximum_events == 0 || config.history_limits.maximum_bytes == 0
            || config.history_limits.maximum_bytes == u64::MAX || config.inherited_messages == u64::MAX
            || !config.content_budget.has_finite_allowances() {
            return Err(Error::Invalid("configured recursive configuration must have finite producer bounds".into()));
        }
        for (index, caller) in config.callers.iter().enumerate() {
            verifier(&caller.owner)?;
            caller.project.validate()?;
            caller.private_volume.validate()?;
            let agent = caller.owner.scope.agent().ok_or_else(|| Error::Unauthorized("configured caller has no signed agent".into()))?;
            if let Some(native) = &caller.native_snapshot {
                crate::contract::validate_json_byte_bound(native, limits.file_bytes)?;
            }
            if let Some(result) = &caller.result_volume {
                validate_result_volume(result, &caller.private_volume, agent, provider)?;
            }
            if caller.private_volume.owner() != &VolumeOwner::Agent(agent)
                || caller.project.provider() != provider || caller.private_volume.provider() != provider
                || config.callers[..index].iter().any(|previous| previous.task == caller.task) {
                return Err(Error::Unauthorized("configured caller is not a unique signed provider binding".into()));
            }
        }
        for (index, fork) in config.forks.iter().enumerate() {
            verifier(&fork.child)?;
            fork.project.validate()?;
            fork.private_volume.validate()?;
            fork.admission.validate()?;
            let agent = fork.child.scope.agent().ok_or_else(|| Error::Unauthorized("configured child has no signed agent".into()))?;
            if let Some(result) = &fork.result_volume {
                validate_result_volume(result, &fork.private_volume, agent, provider)?;
            }
            if fork.admission.parent != Some(fork.caller) || fork.private_volume.owner() != &VolumeOwner::Agent(agent)
                || fork.project.provider() != provider || fork.private_volume.provider() != provider
                || !config.callers.iter().any(|caller| caller.task == fork.caller)
                || config.forks[..index].iter().any(|previous| previous.caller == fork.caller && previous.child.scope.agent() == Some(agent))
                || fork.limits.resources == 0 || fork.limits.work == 0 || fork.limits.bytes == 0
                || fork.limits.work == u64::MAX || fork.limits.bytes == u64::MAX {
                return Err(Error::Unauthorized("configured child allocation is not a unique bounded caller request".into()));
            }
        }
        for (index, merge) in config.merges.iter().enumerate() {
            merge.notice.validate()?;
            if !config.callers.iter().any(|caller| caller.task == merge.caller)
                || !config.callers.iter().any(|caller| caller.task == merge.source)
                || config.merges[..index].iter().any(|previous| previous.caller == merge.caller && previous.source == merge.source) {
                return Err(Error::Unauthorized("configured merge lacks unique configured caller/source bindings".into()));
            }
        }
        for (index, original) in config.original_sources.iter().enumerate() {
            original.request.validate()?;
            let caller = config.callers.iter().find(|caller| caller.task == original.caller)
                .ok_or_else(|| Error::Unauthorized("original source lacks its actual configured caller".into()))?;
            if original.request.parent != caller.owner.authority
                || original.snapshot.authority != original.request.parent
                || original.snapshot.revision != original.request.parent_revision
                || config.original_sources[..index].iter().any(|prior| prior.request.operation_id == original.request.operation_id) {
                return Err(Error::Unauthorized("original source record changed its unique caller or exact canonical cut".into()));
            }
            crate::contract::validate_json_byte_bound(&original.snapshot, limits.file_bytes)?;
        }
        Ok(Arc::new(Self { config, stream, filesystem, limits, runtime: OnceLock::new(), project_policy: OnceLock::new() }))
    }

    /// Binds the same configured runtime once, without a strong reference cycle.
    pub fn bind_runtime(&self, runtime: &Arc<FilesystemTaskRuntime<P, A, O>>) -> Result<()> {
        self.runtime.set(Arc::downgrade(runtime)).map_err(|_| Error::Conflict("configured source runtime was already bound".into()))
    }

    /// Installs actual coordinator credentials and an explicitly trusted read
    /// policy. Missing approval denies foreign imports without widening callers.
    pub fn with_project_source_policy(&self, issuer: AuthorityIssuer,
        provider: Arc<dyn RootProjectSourceProvider>) -> Result<()> {
        self.project_policy.set((issuer, provider))
            .map_err(|_| Error::Conflict("configured source project policy was already bound".into()))
    }

    /// Builds the real portable file and recursive durable tools over this source.
    pub fn tools(self: &Arc<Self>, maximum_bytes: u64) -> Result<RecursiveDefaultToolset> {
        recursive_default_tools(self.clone(), maximum_bytes)
    }

    fn runtime(&self) -> Result<Arc<FilesystemTaskRuntime<P, A, O>>> {
        self.runtime.get().and_then(Weak::upgrade).ok_or_else(|| Error::Unauthorized("configured source has no original runtime".into()))
    }

    fn caller(&self, task: TaskId) -> Result<&ConfiguredRecursiveCaller> {
        self.config.callers.iter().find(|caller| caller.task == task)
            .ok_or_else(|| Error::Unauthorized("task has no explicit signed configured source binding".into()))
    }

    async fn authenticate_caller(&self, context: &ToolContext, require_project_read: bool) -> Result<ConfiguredRecursiveCaller> {
        let task = context.durable_task_id().ok_or_else(|| Error::Unauthorized("configured tool has no admitted caller".into()))?;
        let caller = self.caller(task)?;
        let runtime = self.runtime()?;
        let admission = runtime.task_host().observe_admission(task).await?;
        if admission.parent.is_some() {
            let inspection = runtime.inspect_source(task, task).await?;
            if inspection.agent()? != caller.owner.scope.agent().ok_or_else(|| Error::Unauthorized("configured caller has no signed agent".into()))?
                || inspection.project()?.volume != caller.project || inspection.private_volume()?.volume != caller.private_volume {
                return Err(Error::Unauthorized("configured descendant is not its actual admitted source allocation".into()));
            }
        }
        let verifier = verifier(&caller.owner)?;
        for volume in [&caller.project, &caller.private_volume] {
            if volume == &caller.project && !require_project_read { continue; }
            let read = volume.capability(VolumeOperation::Read)?;
            if !context.scope().grants().contains(&read) {
                return Err(Error::Unauthorized("configured caller lacks its exact selected source read".into()));
            }
            ContentGrant::verify(&verifier, &caller.owner.scope, volume, VolumeOperation::Read)?;
        }
        let mut caller = caller.clone();
        // The real task owner authenticated these effective grants at admission,
        // including exact inherited ReadFile proofs. Retain the configured
        // conversation authority and agent, never another task's scope.
        caller.owner = effective_owner(&caller.owner, context.scope().grants())?;
        Ok(caller)
    }

    fn bind_aggregate(&self, binding: &ConfiguredRecursiveAuthorityBinding,
        aggregate: StreamAggregate<P>) -> Result<StreamAggregate<P>> {
        let verifier = verifier(binding)?;
        let content = Arc::new(FilesystemContentVerifier::new(self.filesystem.clone(), verifier, binding.scope.clone(), self.limits.file_bytes)?);
        let forks = Arc::new(CompositeForkVerifier::new(vec![
            Arc::new(FilesystemForkVerifier::new(self.filesystem.clone(), self.limits.file_bytes)?),
            Arc::new(StreamHistoryForkVerifier::new(self.config.stream_provider.clone())?),
        ])?);
        Ok(aggregate.with_content_verifier(content).with_fork_verifier(forks)
            .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(self.filesystem.clone())))
            .with_limits(self.limits))
    }

    async fn aggregate(&self, binding: &ConfiguredRecursiveAuthorityBinding,
        cancellation: &CancellationToken) -> Result<StreamAggregate<P>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let aggregate = StreamAggregate::open_with_read_limits(&self.stream, binding.authority.clone(),
            verifier(binding)?, SchemaRegistry::default(), self.config.history_limits).await?;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        self.bind_aggregate(binding, aggregate)
    }

    fn allocation(&self, caller: TaskId, arguments: &ForkToolArguments) -> Result<&ConfiguredRecursiveFork> {
        self.config.forks.iter().find(|fork| fork.caller == caller && fork.child.scope.agent() == Some(arguments.id))
            .ok_or_else(|| Error::Unauthorized("new child has no explicit signed configured allocation".into()))
    }

    async fn child_claims(&self, context: &ToolContext, caller: &ConfiguredRecursiveCaller,
        allocation: &ConfiguredRecursiveFork, arguments: &ForkToolArguments, source: &RetainedSourceBinding<A, O>,
    ) -> Result<(TaskAdmissionRecord, Scope)> {
        let mut child = allocation.admission.clone();
        if child.parent != Some(caller.task) {
            return Err(Error::Unauthorized("configured child is not allocated by the actual caller".into()));
        }
        let mut grants = child.grants.iter().map(str::to_owned).collect::<Vec<_>>();
        let mut scoped = allocation.child.scope.capabilities().iter().map(str::to_owned).collect::<Vec<_>>();
        for volume in [&allocation.project, &allocation.private_volume].into_iter().chain(allocation.result_volume.as_ref()) {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                let grant = volume.capability(operation)?;
                grants.push(grant.clone());
                scoped.push(grant);
            }
        }
        for file in &allocation.input_files {
            if !crate::runtime::read_granted(context.scope().grants(), file)? {
                return Err(Error::Unauthorized("current caller cannot delegate the configured exact launch file".into()));
            }
            ContentGrant::verify_read(&verifier(&caller.owner)?, &caller.owner.scope, file)?;
            let grant = file.read_capability()?;
            grants.push(grant.clone());
            scoped.push(grant);
        }
        for reference in source.preparer.required_reference_grants(&source.request).await? {
            if reference.reader != arguments.id {
                return Err(Error::Unauthorized("configured inherited reference targets an unlisted child reader".into()));
            }
            if !crate::runtime::read_granted(context.scope().grants(), &reference.file)? {
                return Err(Error::Unauthorized("current caller cannot delegate this exact inherited file".into()));
            }
            ContentGrant::verify_read(&verifier(&caller.owner)?, &caller.owner.scope, &reference.file)?;
            if let Some(manifest) = &reference.attachment_manifest {
                if !crate::runtime::read_granted(context.scope().grants(), manifest)? {
                    return Err(Error::Unauthorized("current caller cannot delegate the authenticated attachment manifest".into()));
                }
                ContentGrant::verify_read(&verifier(&caller.owner)?, &caller.owner.scope, manifest)?;
            }
            let grant = reference.capability()?;
            grants.push(grant.clone());
            scoped.push(grant);
        }
        child.grants = Capabilities::new(grants);
        let scope = AuthorityIssuer::new(&allocation.child.issuer, allocation.child.key, allocation.child.authority.clone())
            .root_for_agent(arguments.id, allocation.child.scope.id(), Capabilities::new(scoped));
        Ok((child, scope))
    }

    async fn select_source(&self, context: &ToolContext, arguments: &ForkToolArguments,
        cancellation: &CancellationToken) -> Result<RecursiveForkSourceIntent> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context,
            arguments.source.is_none_or(|source| Some(source) == context.durable_task_id())).await?;
        let allocation = self.allocation(caller.task, arguments)?;
        let runtime = self.runtime()?;
        let parent = self.aggregate(&caller.owner, cancellation).await?;
        let project = self.filesystem.resolve(&workspace_ref(caller.project.provider().clone(), &caller.project.storage_name()?)?).await?.generation;
        let private = self.filesystem.resolve(&workspace_ref(caller.private_volume.provider().clone(), &caller.private_volume.storage_name()?)?).await?.generation;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let selected = arguments.source.unwrap_or(caller.task);
        let source_read = if selected != caller.task {
            let (issuer, provider) = self.project_policy.get().ok_or_else(|| Error::Unauthorized(
                "foreign project selection lacks an explicitly configured RootService approval policy".into()))?;
            let chosen = self.caller(selected)?;
            if let Some(native) = &chosen.native_snapshot {
                crate::contract::validate_json_byte_bound(native, context.scope().limits().file_bytes)?;
            }
            let generation = self.filesystem.resolve(&workspace_ref(chosen.project.provider().clone(), &chosen.project.storage_name()?)?).await?.generation;
            cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
            let original_owner = Arc::new(runtime.task_host().journal_owner(caller.task, context.task().owned_task_fence()?).await?
                .with_invocation_cancellation(cancellation));
            Some(original_owner.authorize_project_source(context.task(), issuer, provider.as_ref(), selected,
                context.operation_id(), ProjectSourceSnapshot { project: chosen.project.clone(), generation,
                    native_snapshot: chosen.native_snapshot.clone() }, allocation.limits.work, allocation.limits.bytes).await?)
        } else { None };
        let source = recursive_fork_source(runtime.as_ref(), self.filesystem.clone(), &self.stream, &parent,
            verifier(&caller.owner)?, caller.owner.scope.clone(), context.task(), RecursiveSourceSnapshot {
                task: caller.task, project: caller.project.clone(), project_generation: project,
                private_volume: caller.private_volume.clone(), private_generation: private,
            }, self.config.stream_provider.clone(), context.operation_id(), arguments, RecursiveForkConfig {
                child: allocation.child.authority.clone(), child_project: allocation.project.clone(), child_private: allocation.private_volume.clone(),
                child_result_volume: allocation.result_volume.clone(),
                limits: context.scope().limits(), inherited_messages: self.config.inherited_messages,
                inherited_bytes: context.scope().limits().file_bytes, inherited_references: self.config.inherited_references,
                history_limits: self.config.history_limits,
            }, source_read, cancellation).await?;
        let (child, scope) = self.child_claims(context, &caller, allocation, arguments, &source).await?;
        Ok(RecursiveForkSourceIntent {
            arguments: arguments.clone(), caller: caller.task, source_task: arguments.source.unwrap_or(caller.task),
            child_operation_id: child.operation_id, allocation_digest: allocation_digest(allocation, &child, &scope)?,
            request: source.request,
        })
    }

    async fn fork_binding(&self, context: &ToolContext, arguments: &ForkToolArguments,
        intent: &RecursiveForkSourceIntent, original: Option<&RecursiveForkToolPlan>,
        cancellation: &CancellationToken) -> Result<ForkBinding<P, A, O>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context,
            arguments.source.is_none_or(|source| Some(source) == context.durable_task_id())).await?;
        let allocation = self.allocation(caller.task, arguments)?;
        if intent.arguments != *arguments || intent.caller != caller.task
            || intent.source_task != arguments.source.unwrap_or(caller.task)
            || intent.child_operation_id != allocation.admission.operation_id
            || intent.request.operation_id != context.operation_id() || intent.request.parent != caller.owner.authority
            || intent.request.child != allocation.child.authority || intent.request.child_agent != arguments.id
            || intent.request.preparation.child_project_volume != allocation.project
            || intent.request.preparation.child_private_volume != allocation.private_volume
            || intent.request.preparation.child_result_volume != allocation.result_volume {
            return Err(Error::Unauthorized("configured fork changed its committed original source/allocation".into()));
        }
        if let Some(original) = original {
            let request: ForkRequest = serde_json::from_value(original.request.payload.clone()).map_err(invalid_json)?;
            if original.caller != caller.task || original.arguments != *arguments || request != intent.request
                || original.request.child.operation_id != intent.child_operation_id
                || original.request.input_files != allocation.input_files
                || original.child_message_pointer != allocation.child_message_pointer
                || original.child_message_file_pointer != allocation.child_message_file_pointer {
                return Err(Error::Unauthorized("configured fork retry changed its retained original plan".into()));
            }
        }
        // The durable workflow has already committed this intent. Reconstruct
        // only its authoritative cut, never the current conversation tail.
        let pinned = StreamAggregate::open_at_revision_measured(&self.stream, caller.owner.authority.clone(),
            verifier(&caller.owner)?, SchemaRegistry::default(), intent.request.parent_revision,
            self.config.history_limits, self.config.content_budget, cancellation).await.map_err(|failure| failure.error)?;
        let mut content_work = pinned.work;
        let pinned = self.bind_aggregate(&caller.owner, pinned.value)?;
        let source_read = self.restore_intent_source(context.task(), &intent.request, cancellation).await?;
        let pinned_source = restore_recursive_fork_source(self.filesystem.clone(), &self.stream, pinned.reducer(),
            verifier(&caller.owner)?, caller.owner.scope.clone(), caller.project.clone(), self.config.stream_provider.clone(),
            intent.request.clone(), self.config.history_limits, source_read.clone(), cancellation).await?;
        let (child, child_scope) = self.child_claims(context, &caller, allocation, arguments, &pinned_source).await?;
        if allocation_digest(allocation, &child, &child_scope)? != intent.allocation_digest {
            return Err(Error::Unauthorized("configured producer claims changed after source selection".into()));
        }
        let checkpoint = if let Some(original) = original {
            Some(original.source_checkpoint.clone())
        } else {
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let observed = context.observe_staged_file_once_measured(checkpoint_operation(context.operation_id()),
                remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(observed.work).map_err(work_error)?;
            observed.value
        };
        let source_checkpoint = if let Some(checkpoint) = checkpoint { checkpoint } else {
            // Absence of a receipt is not permission to select another cut.
            // Repeated publication always carries the already committed intent.
            let capture = SourceCheckpoint { snapshot: pinned.reducer().snapshot_with_event_limit(1)?,
                request: intent.request.clone(), caller: caller.task, arguments: arguments.clone() };
            let bytes = serde_json::to_vec(&capture).map_err(invalid_json)?;
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let published = context.stage_file_once_measured(checkpoint_operation(context.operation_id()),
                &format!("recursive-tools/{}/source-checkpoint.json", context.operation_id()), &bytes,
                "application/json", "source-checkpoint.json", remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(published.work).map_err(work_error)?;
            published.value
        };
        let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
        let retained = context.read_file_measured(&source_checkpoint, remaining, cancellation).await.map_err(|failure| failure.error)?;
        content_work = content_work.checked_add(retained.work).map_err(work_error)?;
        let capture: SourceCheckpoint = serde_json::from_slice(&retained.value).map_err(invalid_json)?;
        if capture.caller != caller.task || capture.arguments != *arguments || capture.request != intent.request {
            return Err(Error::Unauthorized("published checkpoint differs from the committed original source intent".into()));
        }
        let reducer = self.restore_snapshot(&caller.owner, &capture.request, capture.snapshot, cancellation).await?;
        let source = restore_recursive_fork_source(self.filesystem.clone(), &self.stream, &reducer,
            verifier(&caller.owner)?, caller.owner.scope.clone(), caller.project.clone(), self.config.stream_provider.clone(),
            capture.request, self.config.history_limits, source_read, cancellation).await?;
        for file in &allocation.input_files {
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let read = context.read_file_measured(file, remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(read.work).map_err(work_error)?;
        }
        Ok(ForkBinding { source, source_checkpoint, child, child_message_pointer: allocation.child_message_pointer.clone(),
            child_message_file_pointer: allocation.child_message_file_pointer.clone(), input_files: allocation.input_files.clone(),
            parent: self.aggregate(&caller.owner, cancellation).await?, child_conversation: self.aggregate(&allocation.child, cancellation).await?,
            parent_scope: caller.owner.scope.clone(), child_scope, limits: allocation.limits })
    }

    async fn merge_binding(&self, context: &ToolContext, arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>, cancellation: &CancellationToken) -> Result<MergeBinding<P, A, O>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context, true).await?;
        let source_owner = self.caller(arguments.source)?;
        let allocation = self.config.merges.iter().find(|merge| merge.caller == caller.task && merge.source == arguments.source)
            .ok_or_else(|| Error::Unauthorized("configured merge has no explicit notice binding".into()))?;
        let runtime = self.runtime()?;
        let inspected = runtime.inspect_source(caller.task, arguments.source).await?;
        if inspected.agent()? != source_owner.owner.scope.agent().ok_or_else(|| Error::Unauthorized("configured source has no signed agent".into()))?
            || inspected.project()?.volume != source_owner.project {
            return Err(Error::Unauthorized("configured merge source is not its admitted project".into()));
        }
        let (project, generation, notice) = if let Some(original) = original {
            if original.caller != caller.task || original.arguments != *arguments || original.destination != caller.owner.authority
                || original.source != source_owner.owner.authority || original.target_project != caller.project || original.source_project != source_owner.project {
                return Err(Error::Unauthorized("configured merge retry changed original owners".into()));
            }
            (original.source_project.clone(), original.source_generation.clone(), original.notice.clone())
        } else { (inspected.project()?.volume.clone(), inspected.project()?.generation.clone(), allocation.notice.clone()) };
        context.read_file_measured(&notice.content, self.config.content_budget, cancellation)
            .await.map_err(|failure| failure.error)?;
        Ok(MergeBinding { filesystem: self.filesystem.clone(), destination: self.aggregate(&caller.owner, cancellation).await?, verifier: verifier(&caller.owner)?,
            scope: caller.owner.scope.clone(), project: caller.project.clone(), source: source_owner.owner.authority.clone(), source_project: project,
            source_generation: generation, notice })
    }

    async fn restore_intent_source(&self, context: &TaskContext, request: &ForkRequest,
        cancellation: &CancellationToken) -> Result<Option<ProjectSourceReadHandle>> {
        let Some(authorization) = &request.authorized_source else { return Ok(None); };
        let caller = context.durable_task_id().ok_or_else(|| Error::Unauthorized("source intent has no original caller".into()))?;
        let runtime = self.runtime()?;
        let owner = Arc::new(runtime.task_host().journal_owner(caller, context.owned_task_fence()?).await?
            .with_invocation_cancellation(cancellation));
        Ok(Some(owner.restore_project_source_authorization(context, authorization).await?))
    }

    pub async fn restore_preparation_source(&self, reader: &TaskContext, token: &TaskResourcePreparation,
        cancellation: &CancellationToken) -> Result<RetainedSourceBinding<A, O>> {
        let preparation = token.request();
        preparation.validate()?;
        let caller = preparation.child.parent.ok_or_else(|| Error::Unauthorized("configured preparation has no original caller".into()))?;
        let mut binding = self.caller(caller)?.clone();
        if reader.durable_task_id() != Some(token.origin().preparation_task)
            || token.origin().parent_task != caller {
            return Err(Error::Unauthorized("source restore lacks the actual sealed helper and original caller".into()));
        }
        let runtime = self.runtime()?;
        let admitted = runtime.task_host().observe_admission(caller).await?;
        if admitted.parent.is_some() {
            let actual = runtime.inspect_source(caller, caller).await?;
            if actual.agent()? != binding.owner.scope.agent().ok_or_else(|| Error::Unauthorized("original caller lacks signed actor".into()))?
                || actual.project()?.volume != binding.project || actual.private_volume()?.volume != binding.private_volume {
                return Err(Error::Unauthorized("restored descendant cannot reuse another caller's source volumes".into()));
            }
        }
        binding.owner = effective_owner(&binding.owner, reader.scope().grants())?;
        let request: ForkRequest = serde_json::from_value(preparation.payload.clone()).map_err(invalid_json)?;
        let restored;
        let aggregate;
        let reducer = if let Some(checkpoint) = &preparation.source_checkpoint {
            ContentGrant::verify_read(&verifier(&binding.owner)?, &binding.owner.scope, checkpoint)?;
            let receipt = reader.read_file_measured(checkpoint, self.config.content_budget, cancellation)
                .await.map_err(|failure| failure.error)?;
            let capture: SourceCheckpoint = serde_json::from_slice(&receipt.value).map_err(invalid_json)?;
            if capture.request != request || capture.caller != caller {
                return Err(Error::Unauthorized("configured preparation checkpoint changed original caller/request".into()));
            }
            restored = self.restore_snapshot(&binding.owner, &request, capture.snapshot, cancellation).await?;
            &restored
        } else if let Some(original) = self.config.original_sources.iter().find(|original| original.request.operation_id == request.operation_id) {
            if original.caller != caller || original.request != request {
                return Err(Error::Unauthorized("trusted original record differs from the sealed preparation request".into()));
            }
            restored = self.restore_snapshot(&binding.owner, &request, original.snapshot.clone(), cancellation).await?;
            &restored
        } else {
            aggregate = StreamAggregate::open_at_revision_measured(&self.stream, binding.owner.authority.clone(),
                verifier(&binding.owner)?, SchemaRegistry::default(), request.parent_revision,
                self.config.history_limits, self.config.content_budget, cancellation).await.map_err(|failure| failure.error)?.value;
            aggregate.reducer()
        };
        restore_recursive_fork_source(self.filesystem.clone(), &self.stream, reducer, verifier(&binding.owner)?,
            binding.owner.scope.clone(), binding.project.clone(), self.config.stream_provider.clone(), request, self.config.history_limits,
            token.project_source().cloned(), cancellation).await
    }

    async fn restore_snapshot(&self, binding: &ConfiguredRecursiveAuthorityBinding, request: &ForkRequest, snapshot: Snapshot,
        cancellation: &CancellationToken) -> Result<Reducer> {
        if snapshot.authority != request.parent || snapshot.revision != request.parent_revision || snapshot.authority != binding.authority || snapshot.revision == 0 {
            return Err(Error::Unauthorized("configured checkpoint is not original authenticated caller cut".into()));
        }
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let page = HistoryReader::new(&self.stream, &binding.authority, verifier(binding)?)?.read_page(&HistoryCursor {
            authority: binding.authority.clone(), after_revision: snapshot.revision - 1, through_revision: snapshot.revision,
        }, HistoryReadLimits { maximum_events: 1, maximum_bytes: self.config.history_limits.maximum_bytes }).await?;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        if snapshot.events.last().is_none() || snapshot.events.last() != page.events.last() {
            return Err(Error::Unauthorized("configured checkpoint head differs from original canonical Stream event".into()));
        }
        Reducer::restore(snapshot, verifier(binding)?, SchemaRegistry::default())
    }
}

impl<P, A, O> RecursiveDefaultToolSource<P, A, O> for ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn owner<'a>(&'a self, context: &'a ToolContext, tool: &'a ResumableTool, invocation: &'a ToolInvocation,
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<Owner<P, A, O>>> {
        Box::pin(async move { self.authenticate_caller(context, false).await?; self.runtime()?.recursive_tool_owner(context, tool, invocation, cancellation).await })
    }
    fn select<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveForkSourceIntent>> {
        Box::pin(self.select_source(context, arguments, cancellation))
    }
    fn fork<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments,
        intent: &'a RecursiveForkSourceIntent, original: Option<&'a RecursiveForkToolPlan>, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<ForkBinding<P, A, O>>> {
        Box::pin(self.fork_binding(context, arguments, intent, original, cancellation))
    }
    fn preparation<'a>(&'a self, context: &'a TaskContext, token: &'a TaskResourcePreparation,
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RetainedSourceBinding<A, O>>> {
        Box::pin(self.restore_preparation_source(context, token, cancellation))
    }
    fn merge<'a>(&'a self, context: &'a ToolContext, arguments: &'a MergeToolArguments, original: Option<&'a RecursiveMergeToolPlan>,
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<MergeBinding<P, A, O>>> {
        Box::pin(self.merge_binding(context, arguments, original, cancellation))
    }
}

fn validate_result_volume(result: &VolumeRef, private: &VolumeRef, agent: crate::AgentId, provider: &ProviderRef) -> Result<()> {
    result.validate()?;
    if result == private || result.provider() != provider
        || result.owner() != &VolumeOwner::Agent(agent)
        || result.class() != crate::conversation::VolumeClass::AgentPrivate {
        return Err(Error::Unauthorized("result is not a distinct actual child-owned output namespace".into()));
    }
    Ok(())
}

fn verifier(binding: &ConfiguredRecursiveAuthorityBinding) -> Result<AuthorityVerifier> {
    let verifier = AuthorityIssuer::new(&binding.issuer, binding.key, binding.authority.clone()).verifier();
    verifier.verify(&binding.scope)?;
    Ok(verifier)
}

fn effective_owner(binding: &ConfiguredRecursiveAuthorityBinding, admitted: &Capabilities) -> Result<ConfiguredRecursiveAuthorityBinding> {
    verifier(binding)?;
    let agent = binding.scope.agent().ok_or_else(|| Error::Unauthorized("configured owner has no authenticated agent".into()))?;
    let grants = binding.scope.capabilities().iter().chain(admitted.iter()).map(str::to_owned);
    let mut owner = binding.clone();
    owner.scope = AuthorityIssuer::new(&binding.issuer, binding.key, binding.authority.clone())
        .root_for_agent(agent, binding.scope.id(), Capabilities::new(grants));
    Ok(owner)
}
fn allocation_digest(allocation: &ConfiguredRecursiveFork, child: &TaskAdmissionRecord, scope: &Scope) -> Result<[u8; 32]> {
    // Creation proofs are bound later to the engine's actual preparation
    // identity. All independently selected admission/reader claims are fixed
    // here, without hashing signed token bytes or random mint metadata.
    let mut admission = child.clone();
    admission.resource_proofs.clear();
    crate::contract::canonical_json_digest(&serde_json::json!({
        "contract": "harness.configured-recursive-allocation.v2",
        "admission": admission.canonical_value(),
        "authority": allocation.child.authority,
        "issuer": allocation.child.issuer,
        "agent": scope.agent(),
        "grants": scope.capabilities(),
        "message_pointer": allocation.child_message_pointer,
        "message_file_pointer": allocation.child_message_file_pointer,
        "input_files": allocation.input_files,
        "limits": allocation.limits,
    }))
}
fn checkpoint_operation(effect: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/configured-recursive-checkpoint\0"); hash.update(&effect.into_bytes());
    let mut bytes = [0; 16]; bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]); OperationId::from_bytes(bytes)
}
fn invalid_json(error: serde_json::Error) -> Error { Error::Invalid(error.to_string()) }
fn work_error(error: acyclic_fs::WorkError) -> Error { Error::Storage(error.to_string()) }
