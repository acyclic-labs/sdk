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
        RetainedSourceBinding, recursive_default_tools, recursive_fork_source,
        restore_recursive_fork_source, workspace_ref},
    fork::{CompositeForkVerifier, ForkRequest, StreamHistoryForkVerifier},
    resource_preparation::TaskResourcePreparation,
    resources::ProviderRef,
    runtime::{DurableTaskHost, TaskAdmissionRecord, TaskContext, ToolContext},
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
    source_authorization::{ProjectSourceReadHandle, ProjectSourceSnapshot, RootProjectSourceProvider},
    tool::ToolInvocation,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, OnceLock, Weak};
#[cfg(not(target_arch = "wasm32"))]
use crate::store::history::HistoryIo;

type Owner<P, A, O> = RecursiveToolOwner<P, A, O>;
type ForkBinding<P, A, O> = RecursiveForkToolBinding<P, A, O>;
type MergeBinding<P, A, O> = RecursiveMergeToolBinding<P, A, O>;

/// Host-owned conversation signing credentials and its exact signed actor.
#[derive(Clone)]
pub struct ConfiguredRecursiveAuthorityBinding {
    issuer: AuthorityIssuer,
    pub scope: Scope,
}

impl ConfiguredRecursiveAuthorityBinding {
    /// Retains the original installed signer; setup authenticates its scope through the same configured source validation used for wire bindings.
    #[must_use]
    pub fn new(issuer: AuthorityIssuer, scope: Scope) -> Result<Self> { Ok(Self { issuer, scope }) }

    #[must_use]
    pub fn authority(&self) -> &Authority { self.issuer.audience() }

    #[must_use]
    pub const fn issuer(&self) -> &AuthorityIssuer { &self.issuer }
}

impl<'de> Deserialize<'de> for ConfiguredRecursiveAuthorityBinding {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D)
        -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireBinding {
            authority: Authority,
            issuer: String,
            key: [u8; 32],
            scope: Scope,
        }
        let wire = WireBinding::deserialize(deserializer)?;
        Ok(Self { issuer: AuthorityIssuer::new(wire.issuer, wire.key, wire.authority), scope: wire.scope })
    }
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveSourceCheckpoint {
    pub caller: TaskId,
    pub request: ForkRequest,
    pub snapshot: Snapshot,
}

/// Finite trusted configuration shared by native and browser default consumers.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveToolsConfig {
    pub callers: Vec<ConfiguredRecursiveCaller>,
    pub forks: Vec<ConfiguredRecursiveFork>,
    /// Browser host's explicit actual graph root for the creator-only policy.
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

#[cfg(not(target_arch = "wasm32"))]
impl crate::filesystem::PreparationRecord for SourceCheckpoint {
    fn maximum_slot_bytes() -> u64 {
        [
            crate::filesystem::preparation_slot_bytes::<Self>(),
            std::mem::size_of::<crate::fork::ForkRequest>() as u64,
            std::mem::size_of::<crate::fork::ForkSelection>() as u64,
            std::mem::size_of::<crate::fork::ResourceRevision>() as u64,
            std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>() as u64,
            std::mem::size_of::<crate::core::Scope>() as u64,
            std::mem::size_of::<crate::conversation::FileRef>() as u64,
        ].into_iter().max().unwrap_or(0)
    }

    fn diagnostic_label_bytes() -> u64 {
        const LABELS: &str = concat!(
            "SourceCheckpoint snapshot request caller arguments ForkToolArguments id msg source ",
            "Snapshot Projection ConversationStateWire ArchivedConversationPrefix ",
            "format_version authority revision events projection state_digest attestation ",
            "lifecycle active_extensions active_extension_revision extension_states ",
            "configured_extensions active_configurations effects indexed_terminal_effects ",
            "effects_archived forks published_merges conversation latest_context_checkpoint ",
            "interactions bindings capabilities issuer intent_digest scope causal_parent ",
            "messages archived_prefix through_sequence history_digest ",
            "archived conversation prefix is unbound conversation order or identity is invalid ",
            "message identity is nil message sequence must start at one ",
            "extension key must be namespaced tool linkage does not match message kind ",
            "file or volume label is invalid checkpoint snapshot is invalid ",
        );
        crate::contract::preparation_serde_diagnostic_label_bytes() + LABELS.len() as u64 + 4
    }

    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ConfiguredRecursiveSourceCheckpoint
    }

    fn decode_owned_allowance(
        shape: crate::contract::JsonValueShape, codec_scratch: u64, input_bytes: u64,
        native_code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let overflow = || match native_code {
            Some(code) => Error::NativeBoundary { code,
                reason: "configured source checkpoint ownership overflows" },
            None => Error::Invalid("configured source checkpoint ownership overflows".into()),
        };
        let snapshot = crate::contract::json_decode_owned_bytes(
            shape, true, native_code,
        )?;
        let outer = shape.lexical_items().checked_mul(10)
            .and_then(|items| items.checked_mul(Self::maximum_slot_bytes()))
            .and_then(|bytes| input_bytes.checked_mul(4).and_then(|strings| bytes.checked_add(strings)))
            .and_then(|bytes| bytes.checked_add(codec_scratch.max(input_bytes)))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or_else(overflow)?;
        snapshot.0.checked_sub(snapshot.1)
            .and_then(|bytes| bytes.checked_add(crate::contract::sdk_json_diagnostic_bytes()))
            .and_then(|bytes| bytes.checked_add(outer)).ok_or_else(overflow)
    }
}

struct BoundProjectSourcePolicy {
    issuer: AuthorityIssuer,
    provider: Arc<dyn RootProjectSourceProvider>,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

/// Portable source factory using the runtime's actual Filesystem and Stream.
pub struct ConfiguredRecursiveToolSource<P, A, O> {
    config: ConfiguredRecursiveToolsConfig,
    stream: Arc<P>,
    filesystem: Arc<FilesystemHost<A, O>>,
    limits: Limits,
    runtime: OnceLock<Weak<FilesystemTaskRuntime<P, A, O>>>,
    project_policy: OnceLock<BoundProjectSourcePolicy>,
}

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    /// Validates trusted bindings without publishing, allocating or opening tasks.
    pub fn new(config: ConfiguredRecursiveToolsConfig, stream: Arc<P>,
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
            caller.project.validate(&mut None)?;
            caller.private_volume.validate(&mut None)?;
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
            fork.project.validate(&mut None)?;
            fork.private_volume.validate(&mut None)?;
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
            merge.notice.validate(&mut None)?;
            if !config.callers.iter().any(|caller| caller.task == merge.caller)
                || !config.callers.iter().any(|caller| caller.task == merge.source)
                || config.merges[..index].iter().any(|previous| previous.caller == merge.caller && previous.source == merge.source) {
                return Err(Error::Unauthorized("configured merge lacks unique configured caller/source bindings".into()));
            }
        }
        for (index, original) in config.original_sources.iter().enumerate() {
            original.request.validate(&mut None)?;
            let caller = config.callers.iter().find(|caller| caller.task == original.caller)
                .ok_or_else(|| Error::Unauthorized("original source lacks its actual configured caller".into()))?;
            if &original.request.parent != caller.owner.authority()
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

    /// Installs actual coordinator credentials and an explicitly trusted read policy.
    pub fn with_project_source_policy(&self, issuer: AuthorityIssuer,
        provider: Arc<dyn RootProjectSourceProvider>) -> Result<()> {
        self.project_policy.set(BoundProjectSourcePolicy { issuer, provider,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        })
            .map_err(|_| Error::Conflict("configured source project policy was already bound".into()))
    }

    /// Builds the real portable file and recursive durable tools over this source.
    pub fn tools(self: &Arc<Self>, maximum_bytes: u64) -> Result<RecursiveDefaultToolset> {
        recursive_default_tools(Some(self.clone()), maximum_bytes)
    }

    fn runtime(&self, io: Option<&super::PreparationIo>) -> Result<Arc<FilesystemTaskRuntime<P, A, O>>> {
        self.runtime.get().and_then(Weak::upgrade).ok_or_else(|| super::interaction_host::interaction_boundary(
            io, crate::wire::ErrorCode::Unauthorized, "configured source has no original runtime"))
    }

    fn caller(&self, task: TaskId, io: Option<&super::PreparationIo>) -> Result<&ConfiguredRecursiveCaller> {
        for caller in &self.config.callers {
            if let Some(io) = io { io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 16, items: 1 }, true)?; }
            if caller.task == task { return Ok(caller); }
        }
        Err(super::interaction_host::interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
            "task has no explicit signed configured source binding"))
    }

    async fn authenticate_caller<'a>(&'a self, context: &ToolContext, require_project_read: bool) -> Result<&'a ConfiguredRecursiveCaller> {
        let task = context.durable_task_id().ok_or_else(|| Error::Unauthorized("configured tool has no admitted caller".into()))?;
        let caller = self.caller(task, None)?;
        let runtime = self.runtime(None)?;
        let admission = runtime.task_host().observe_admission(task).await?;
        if admission.parent.is_some() {
            let inspection = runtime.inspect_source(task, task).await?;
            if inspection.project()?.volume != caller.project
                || context.scope().current_created_authority()? != Some(caller.owner.authority())
                || context.scope().current_created_private_volume()? != Some(&caller.private_volume)
                || inspection.volumes().iter().filter(|snapshot| snapshot.volume == caller.private_volume).count() != 1
                || caller.result_volume.as_ref().is_some_and(|result|
                    inspection.volumes().iter().filter(|snapshot| &snapshot.volume == result).count() != 1) {
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
        Ok(caller)
    }

    fn bind_aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, scope: &Scope,
        aggregate: StreamAggregate<P>, content: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
    ) -> Result<StreamAggregate<P>> {
        let content = match content {
            Some(content) => content,
            None => Arc::new(FilesystemContentVerifier::new(self.filesystem.clone(), &(verifier(binding)?), &scope, self.limits.file_bytes, None)?),
        };
        let forks = Arc::new(CompositeForkVerifier::new(&[
            Arc::new(FilesystemForkVerifier::new(self.filesystem.clone(), self.limits.file_bytes)?),
            Arc::new(StreamHistoryForkVerifier::new(&self.config.stream_provider, &mut None)?),
        ], &mut None)?);
        aggregate.with_content_verifier(content).with_fork_verifier(forks)
            .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(self.filesystem.clone())))
            .with_limits(self.limits)
    }

    async fn aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, scope: &Scope,
        content: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
        cancellation: &CancellationToken,
    ) -> Result<StreamAggregate<P>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let aggregate = StreamAggregate::open_with_read_limits(&self.stream, binding.authority().clone(),
            verifier(binding)?, SchemaRegistry::default(), self.config.history_limits).await?;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        self.bind_aggregate(binding, scope, aggregate, content)
    }

    fn allocation(&self, caller: TaskId, arguments: &ForkToolArguments, io: Option<&super::PreparationIo>) -> Result<&ConfiguredRecursiveFork> {
        for allocation in &self.config.forks {
            if let Some(io) = io { io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 32, items: 1 }, true)?; }
            if allocation.caller == caller && allocation.child.scope.agent() == Some(arguments.id) { return Ok(allocation); }
        }
        Err(super::interaction_host::interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
            "new child has no explicit signed configured allocation"))
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
            drop(context.task().read_file(file).await?);
            let grant = file.read_capability()?;
            grants.push(grant.clone());
            scoped.push(grant);
        }
        for reference in source.preparer.required_reference_grants(&source.request, None).await? {
            if reference.reader != arguments.id {
                return Err(Error::Unauthorized("configured inherited reference targets an unlisted child reader".into()));
            }
            if !crate::runtime::read_granted(context.scope().grants(), &reference.file)? {
                return Err(Error::Unauthorized("current caller cannot delegate this exact inherited file".into()));
            }
            drop(context.task().read_file(&reference.file).await?);
            if let Some(manifest) = &reference.attachment_manifest {
                if !crate::runtime::read_granted(context.scope().grants(), manifest)? {
                    return Err(Error::Unauthorized("current caller cannot delegate the authenticated attachment manifest".into()));
                }
                drop(context.task().read_file(manifest).await?);
            }
            let grant = reference.capability()?;
            grants.push(grant.clone());
            scoped.push(grant);
        }
        child.grants = Capabilities::new(grants);
        let scope = allocation.child.issuer()
            .root_for_agent(arguments.id, allocation.child.scope.id(), Capabilities::new(scoped));
        Ok((child, scope))
    }

    async fn select_source(&self, context: &ToolContext, arguments: &ForkToolArguments,
        cancellation: &CancellationToken) -> Result<RecursiveForkSourceIntent> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context,
            arguments.source.is_none_or(|source| Some(source) == context.durable_task_id())).await?;
        let allocation = self.allocation(caller.task, arguments, None)?;
        let runtime = self.runtime(None)?;
        let parent = self.aggregate(&caller.owner, &caller.owner.scope, None, cancellation).await?;
        let project = self.filesystem.resolve(&workspace_ref(caller.project.provider().clone(), &caller.project.storage_name()?)?).await?.generation;
        let private = self.filesystem.resolve(&workspace_ref(caller.private_volume.provider().clone(), &caller.private_volume.storage_name()?)?).await?.generation;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let selected = arguments.source.unwrap_or(caller.task);
        let source_read = if selected != caller.task {
            let BoundProjectSourcePolicy { issuer, provider, .. } = self.project_policy.get().ok_or_else(|| Error::Unauthorized(
                "foreign project selection lacks an explicitly configured RootService approval policy".into()))?;
            let chosen = self.caller(selected, None)?;
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
                child: allocation.child.authority().clone(), child_project: allocation.project.clone(), child_private: allocation.private_volume.clone(),
                child_result_volume: allocation.result_volume.clone(),
                limits: context.scope().limits(), inherited_messages: self.config.inherited_messages,
                inherited_bytes: context.scope().limits().file_bytes, inherited_references: self.config.inherited_references,
                history_limits: self.config.history_limits,
            }, source_read, self.ordinary_resolver(context.task(), cancellation).await?, cancellation).await?;
        let (child, scope) = self.child_claims(context, caller, allocation, arguments, &source).await?;
        Ok(RecursiveForkSourceIntent {
            arguments: arguments.clone(), caller: caller.task, source_task: arguments.source.unwrap_or(caller.task),
            child_operation_id: child.operation_id, allocation_digest: allocation_digest(allocation, child.clone(), &scope, None)?,
            request: source.request,
        })
    }

    async fn fork_binding(&self, context: &ToolContext, arguments: &ForkToolArguments,
        intent: &RecursiveForkSourceIntent, original: Option<&RecursiveForkToolPlan>,
        cancellation: &CancellationToken) -> Result<ForkBinding<P, A, O>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context,
            arguments.source.is_none_or(|source| Some(source) == context.durable_task_id())).await?;
        let allocation = self.allocation(caller.task, arguments, None)?;
        if intent.arguments != *arguments || intent.caller != caller.task
            || intent.source_task != arguments.source.unwrap_or(caller.task)
            || intent.child_operation_id != allocation.admission.operation_id
            || intent.request.operation_id != context.operation_id() || &intent.request.parent != caller.owner.authority()
            || &intent.request.child != allocation.child.authority() || intent.request.child_agent != arguments.id
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
        let pinned = StreamAggregate::open_at_revision_measured(&self.stream, caller.owner.authority().clone(),
            verifier(&caller.owner)?, SchemaRegistry::default(), intent.request.parent_revision,
            self.config.history_limits, self.config.content_budget, cancellation).await.map_err(|failure| failure.error)?;
        let mut content_work = pinned.work;
        let pinned = self.bind_aggregate(&caller.owner, &caller.owner.scope, pinned.value, None)?;
        let source_read = self.restore_intent_source(context.task(), &intent.request, cancellation).await?;
        let pinned_source = restore_recursive_fork_source(self.filesystem.clone(), &self.stream, pinned.reducer(),
            verifier(&caller.owner)?, caller.owner.scope.clone(), caller.project.clone(), self.config.stream_provider.clone(),
            intent.request.clone(), self.config.history_limits, source_read.clone(),
            self.ordinary_resolver(context.task(), cancellation).await?, cancellation).await?;
        let (child, child_scope) = self.child_claims(context, caller, allocation, arguments, &pinned_source).await?;
        if allocation_digest(allocation, child.clone(), &child_scope, None)? != intent.allocation_digest {
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
            let capture = SourceCheckpoint { snapshot: pinned.reducer().snapshot(1, &mut None).map(|(snapshot, _)| snapshot)?,
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
            capture.request, self.config.history_limits, source_read,
            self.ordinary_resolver(context.task(), cancellation).await?, cancellation).await?;
        for file in &allocation.input_files {
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let read = context.read_file_measured(file, remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(read.work).map_err(work_error)?;
        }
        Ok(ForkBinding { source, source_checkpoint, child, child_message_pointer: allocation.child_message_pointer.clone(),
            child_message_file_pointer: allocation.child_message_file_pointer.clone(), input_files: allocation.input_files.clone(),
            parent: self.aggregate(&caller.owner, &caller.owner.scope,
                Some(self.ordinary_resolver(context.task(), cancellation).await?), cancellation).await?,
            child_conversation: self.aggregate(&allocation.child, &child_scope, None, cancellation).await?,
            parent_scope: caller.owner.scope.clone(), child_scope, limits: allocation.limits })
    }

    async fn merge_binding(&self, context: &ToolContext, arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>, cancellation: &CancellationToken) -> Result<MergeBinding<P, A, O>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context, true).await?;
        let source_owner = self.caller(arguments.source, None)?;
        let allocation = self.config.merges.iter().find(|merge| merge.caller == caller.task && merge.source == arguments.source)
            .ok_or_else(|| Error::Unauthorized("configured merge has no explicit notice binding".into()))?;
        let runtime = self.runtime(None)?;
        let inspected = runtime.inspect_source(caller.task, arguments.source).await?;
        if inspected.project()?.volume != source_owner.project
            || inspected.volumes().iter().filter(|snapshot| snapshot.volume == source_owner.private_volume).count() != 1
            || source_owner.result_volume.as_ref().is_some_and(|result|
                inspected.volumes().iter().filter(|snapshot| &snapshot.volume == result).count() != 1) {
            return Err(Error::Unauthorized("configured merge source is not its admitted project".into()));
        }
        let (project, generation, notice) = if let Some(original) = original {
            if original.caller != caller.task || original.arguments != *arguments || &original.destination != caller.owner.authority()
                || &original.source != source_owner.owner.authority() || original.target_project != caller.project || original.source_project != source_owner.project {
                return Err(Error::Unauthorized("configured merge retry changed original owners".into()));
            }
            (original.source_project.clone(), original.source_generation.clone(), original.notice.clone())
        } else { (inspected.project()?.volume.clone(), inspected.project()?.generation.clone(), allocation.notice.clone()) };
        let source_read = if let Some(authorization) = original.and_then(|plan| plan.authorized_source.as_ref()) {
            let owner = Arc::new(runtime.task_host().journal_owner(caller.task, context.task().owned_task_fence()?).await?
                .with_invocation_cancellation(cancellation));
            let handle = owner.restore_project_source_authorization(context.task(), authorization).await?;
            handle.require_selection(context.operation_id(), arguments.source, &project, &generation,
                source_owner.native_snapshot.as_ref(), None)?;
            Some(handle)
        } else if original.is_none() && arguments.source != caller.task
            && !context.scope().grants().contains(&project.capability(VolumeOperation::Read)?) {
            let BoundProjectSourcePolicy { issuer, provider, .. } = self.project_policy.get().ok_or_else(||
                Error::Unauthorized("foreign merge lacks an explicit RootService Project policy".into()))?;
            let owner = Arc::new(runtime.task_host().journal_owner(caller.task, context.task().owned_task_fence()?).await?
                .with_invocation_cancellation(cancellation));
            Some(owner.authorize_project_source(context.task(), issuer, provider.as_ref(), arguments.source,
                context.operation_id(), ProjectSourceSnapshot { project: project.clone(), generation: generation.clone(),
                    native_snapshot: source_owner.native_snapshot.clone() },
                self.config.content_budget.items_examined, self.config.history_limits.maximum_bytes).await?)
        } else { None };
        context.read_file_measured(&notice.content, self.config.content_budget, cancellation)
            .await.map_err(|failure| failure.error)?;
        Ok(MergeBinding { filesystem: self.filesystem.clone(), destination: self.aggregate(
            &caller.owner, &caller.owner.scope, Some(self.ordinary_resolver(context.task(), cancellation).await?),
            cancellation).await?, verifier: verifier(&caller.owner)?,
            scope: caller.owner.scope.clone(), project: caller.project.clone(), source: source_owner.owner.authority().clone(), source_project: project,
            source_generation: generation, source_read, notice })
    }

    async fn restore_intent_source(&self, context: &TaskContext, request: &ForkRequest,
        cancellation: &CancellationToken) -> Result<Option<ProjectSourceReadHandle>> {
        let Some(authorization) = &request.authorized_source else { return Ok(None); };
        let caller = context.durable_task_id().ok_or_else(|| Error::Unauthorized("source intent has no original caller".into()))?;
        let runtime = self.runtime(None)?;
        let owner = Arc::new(runtime.task_host().journal_owner(caller, context.owned_task_fence()?).await?
            .with_invocation_cancellation(cancellation));
        Ok(Some(owner.restore_project_source_authorization(context, authorization).await?))
    }

    pub async fn restore_preparation_source(&self, reader: &TaskContext, token: &TaskResourcePreparation,
        cancellation: &CancellationToken) -> Result<RetainedSourceBinding<A, O>> {
        let preparation = token.request();
        preparation.validate(&mut None)?;
        let caller = preparation.child.parent.ok_or_else(|| Error::Unauthorized("configured preparation has no original caller".into()))?;
        let binding = self.caller(caller, None)?;
        if reader.durable_task_id() != Some(token.origin().preparation_task)
            || token.origin().parent_task != caller {
            return Err(Error::Unauthorized("source restore lacks the actual sealed helper and original caller".into()));
        }
        let runtime = self.runtime(None)?;
        let admitted = runtime.task_host().observe_admission(caller).await?;
        if admitted.parent.is_some() {
            let actual = runtime.inspect_source(caller, caller).await?;
            if actual.project()?.volume != binding.project
                || actual.volumes().iter().filter(|snapshot| snapshot.volume == binding.private_volume).count() != 1
                || binding.result_volume.as_ref().is_some_and(|result|
                    actual.volumes().iter().filter(|snapshot| &snapshot.volume == result).count() != 1) {
                return Err(Error::Unauthorized("restored descendant cannot reuse another caller's source volumes".into()));
            }
        }
        let request: ForkRequest = serde_json::from_value(preparation.payload.clone()).map_err(invalid_json)?;
        let restored;
        let aggregate;
        let reducer = if let Some(checkpoint) = &preparation.source_checkpoint {
            drop(reader.read_file(checkpoint).await?);
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
            restored = self.restore_snapshot(&binding.owner, &request, original.snapshot.copy(&mut None)?, cancellation).await?;
            &restored
        } else {
            aggregate = StreamAggregate::open_at_revision_measured(&self.stream, binding.owner.authority().clone(),
                verifier(&binding.owner)?, SchemaRegistry::default(), request.parent_revision,
                self.config.history_limits, self.config.content_budget, cancellation).await.map_err(|failure| failure.error)?.value;
            aggregate.reducer()
        };
        restore_recursive_fork_source(self.filesystem.clone(), &self.stream, reducer, verifier(&binding.owner)?,
            binding.owner.scope.clone(), binding.project.clone(), self.config.stream_provider.clone(), request, self.config.history_limits,
            token.project_source().cloned(), self.ordinary_resolver(reader, cancellation).await?, cancellation).await
    }

    async fn ordinary_resolver(
        &self, context: &TaskContext, cancellation: &CancellationToken,
    ) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        let task = context.durable_task_id().ok_or_else(||
            Error::Unauthorized("recursive source has no original admitted reader".into()))?;
        let owner = Arc::new(self.runtime(None)?.task_host()
            .journal_owner(task, context.owned_task_fence()?).await?
            .with_invocation_cancellation(cancellation));
        context.content_residency_verifier(owner)
    }

    async fn restore_snapshot(&self, binding: &ConfiguredRecursiveAuthorityBinding, request: &ForkRequest, snapshot: Snapshot,
        cancellation: &CancellationToken) -> Result<Reducer> {
        if snapshot.authority != request.parent || snapshot.revision != request.parent_revision || &snapshot.authority != binding.authority() || snapshot.revision == 0 {
            return Err(Error::Unauthorized("configured checkpoint is not original authenticated caller cut".into()));
        }
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let page = HistoryReader::new(&self.stream, binding.authority(), verifier(binding)?)?.read_page(&HistoryCursor {
            authority: binding.authority().clone(), after_revision: snapshot.revision - 1, through_revision: snapshot.revision,
        }, HistoryReadLimits { maximum_events: 1, maximum_bytes: self.config.history_limits.maximum_bytes }).await?;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        if snapshot.events.last().is_none() || snapshot.events.last() != page.events.last() {
            return Err(Error::Unauthorized("configured checkpoint head differs from original canonical Stream event".into()));
        }
        Reducer::restore(snapshot, verifier(binding)?, SchemaRegistry::default())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn require_native_policy(
        &self, custody: Option<&acyclic_fs::NativeAllocation>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        let custody = custody.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native Project policy requires originally funded installed metadata",
        })?;
        if !custody.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native Project policy belongs to another original ledger" });
        }
        custody.work().remaining()?;
        Ok(())
    }

    async fn native_source_owner(&self, context: &TaskContext,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<Arc<crate::durable_host::TaskJournalOwner<P>>> {
        let (task, fence) = context.owned_task_binding_with_native_work(work)?;
        let owner = self.runtime(Some(&super::PreparationIo::from_native_work(work)?))?.task_host()
            .journal_owner_with_native_work(task, fence, work).await?
            .with_invocation_cancellation(cancellation);
        owner.require_mail_scope_with_native_work(context.scope(), work)?;
        owner.into_native_shared(work)
    }

    async fn authenticate_native_caller<'a>(&'a self, context: &TaskContext,
        owner: &crate::durable_host::TaskJournalOwner<P>, require_project_read: bool,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<&'a ConfiguredRecursiveCaller> {
        let (task, fence) = context.owned_task_binding_with_native_work(work)?;
        owner.require_mail_scope_with_native_work(context.scope(), work)?;
        if owner.task_binding() != (task, fence) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source owner differs from original context" });
        }
        let caller = self.caller(task, Some(&super::PreparationIo::from_native_work(work)?))?;
        if owner.authenticated_binding_agent(&mut Some(&mut crate::store::history::native_work_io(work)?))? != caller.owner.scope.agent() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source actor differs from admitted current creator" });
        }
        let runtime = self.runtime(Some(&super::PreparationIo::from_native_work(work)?))?;
        let admission = runtime.task_host().observe_admission_with_native_work(task, work).await?;
        if admission.0.parent.is_some() {
            let snapshots = runtime.task_host().observe_volume_snapshots_with_native_work(task, work).await?;
            let mut project = 0_usize;
            let mut scratch = 0_usize;
            let mut result = 0_usize;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            for snapshot in &snapshots.0 {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
                project += usize::from(snapshot.volume == caller.project);
                scratch += usize::from(snapshot.volume == caller.private_volume);
                result += usize::from(caller.result_volume.as_ref() == Some(&snapshot.volume));
            }
            if project != 1 || scratch != 1 || (caller.result_volume.is_some() && result != 1)
                || context.scope().current_created_authority()? != Some(caller.owner.authority())
                || context.scope().current_created_private_volume()? != Some(&caller.private_volume) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native source binding differs from authenticated current allocation" });
            }
        }
        owner.require_volume_grant_with_native_work(&caller.private_volume, VolumeOperation::Read, work)?;
        if require_project_read {
            owner.require_volume_grant_with_native_work(&caller.project, VolumeOperation::Read, work)?;
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let verifier = caller.owner.issuer().verifier(&mut observer)?;
        verifier.verify_scope_for_with_work(caller.owner.authority(), &caller.owner.scope, &mut observer)?;
        Ok(caller)
    }

    async fn native_open_aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, cut: Option<u64>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(StreamAggregate<P>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let aggregate = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let verifier = binding.issuer().verifier(&mut observer)?;
            verifier.verify_scope_for_with_work(binding.authority(), &binding.scope, &mut observer)?;
            let authority = observer.as_deref_mut().expect("native aggregate observer").clone_authority(binding.authority())?;
            match cut {
                Some(revision) => StreamAggregate::open_at_revision_with_work(
                    &self.stream, authority, verifier, SchemaRegistry::new(), revision,
                    self.config.history_limits, &mut observer,
                ).await?,
                None => StreamAggregate::open_with_read_limits_with_work(
                    &self.stream, authority, verifier, SchemaRegistry::new(),
                    self.config.history_limits, &mut observer,
                ).await?,
            }
        };
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let retained = io.owned_bytes()?;
        Ok((aggregate, io.into_native_custody(retained)?))
    }

    fn require_native_effective_volume(
        &self, context: &ToolContext, volume: &VolumeRef, operation: VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let capability = volume.capability_with_native_work(operation, work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
        if !context.effective_scope().grants().contains(&capability.0) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native recursive tool effective scope denies its selected volume operation" });
        }
        Ok(())
    }

    fn native_fork_request(
        &self, context: &ToolContext, caller: &ConfiguredRecursiveCaller,
        allocation: &ConfiguredRecursiveFork, arguments: &ForkToolArguments,
        parent: &Reducer, project_generation: &crate::resources::GenerationRef,
        private_generation: &crate::resources::GenerationRef,
        source_read: Option<&ProjectSourceReadHandle>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ForkRequest, acyclic_fs::NativeAllocation)> {
        if parent.authority() != caller.owner.authority()
            || parent.conversation().and_then(|conversation| conversation.agent) != caller.owner.scope.agent() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native fork history is not the admitted current caller" });
        }
        let limits = context.effective_scope().limits();
        let operation = context.operation_id();
        use std::fmt::Write as _;
        let io = super::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let selected = arguments.source_task(caller.task, limits, &mut observer)?;
        let caller_agent = parent.conversation().and_then(|conversation| conversation.agent);
        if caller_agent.is_none() || caller_agent != caller.owner.scope.agent()
            || caller_agent != match caller.private_volume.owner() {
                crate::conversation::VolumeOwner::Agent(agent) => Some(*agent), _ => None,
            }
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native fork source is not the admitted current caller agent" });
        }
        let parent_authority = observer.as_deref_mut().expect("native fork observer").clone_authority(parent.authority())?;
        let child = observer.as_deref_mut().expect("native fork observer").clone_authority(allocation.child.authority())?;
        let path = parent.authority().stream_path(&mut observer)?.into_bytes();
        let mut version = String::new();
        observer.as_deref_mut().expect("native fork observer").reserve_text(&mut version, 20)?;
        observer.as_deref_mut().expect("native fork observer").admit_copy(20)?;
        write!(&mut version, "{}", parent.revision()).map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "native revision formatting failed",
        })?;
        observer.as_deref_mut().expect("native fork observer").copy_bytes(version.len() as u64)?;
        drop(observer);
        drop(history);
        let provider = crate::resources::ProviderRef::new(
            crate::filesystem::fork_preparer_io::clone_text(self.config.stream_provider.namespace(), Some(&io))?, crate::filesystem::fork_preparer_io::clone_text(self.config.stream_provider.family(), Some(&io))?,
            crate::filesystem::fork_preparer_io::clone_text(self.config.stream_provider.version(), Some(&io))?,
        )?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let reference = crate::resources::ResourceRef::new_with_work(
            crate::resources::ResourceKind::Stream, provider, path, Some(version), &mut observer,
        )?;
        let reference = crate::resources::StreamRef::from_resource_with_work(reference, &mut observer)?;
        let child_project = allocation.project.copy(&mut observer)?;
        let child_private = allocation.private_volume.copy(&mut observer)?;
        let child_result = allocation.result_volume.as_ref().map(|volume| volume.copy(&mut observer)).transpose()?;
        let source_project = source_read.map_or(&caller.project, |handle| &handle.authorization().project)
            .copy(&mut observer)?;
        let scratch = caller.private_volume.copy(&mut observer)?;
        let mut selections = Vec::new();
        let slots = 3 * std::mem::size_of::<crate::fork::ForkSelection>() as u64;
        observer.as_deref_mut().expect("native fork observer").admit_owned(slots)?;
        observer.as_deref_mut().expect("native fork observer").admit_allocation(slots)?;
        selections.try_reserve_exact(3).map_err(|cause|
            observer.as_deref().expect("native fork observer").allocation_failure(cause))?;
        observer.as_deref_mut().expect("native fork observer")
            .retain_owned(selections.capacity() as u64 * std::mem::size_of::<crate::fork::ForkSelection>() as u64)?;
        drop(observer);
        drop(history);
        let generation = source_read.map_or(project_generation, |handle| &handle.authorization().generation);
        let generation = crate::resources::GenerationRef::new_with_native_work(
            generation.as_resource().provider(), generation.as_resource().key(), generation.as_resource().version(), work)?;
        io.absorb_native_custody(generation.1)?;
        let private_generation = crate::resources::GenerationRef::new_with_native_work(
            private_generation.as_resource().provider(), private_generation.as_resource().key(),
            private_generation.as_resource().version(), work)?;
        io.absorb_native_custody(private_generation.1)?;
        selections.push(crate::fork::ForkSelection { required: true, revision: crate::fork::ResourceRevision::History(reference) });
        selections.push(crate::fork::ForkSelection { required: true,
            revision: crate::fork::ResourceRevision::Project { volume: source_project, generation: generation.0 } });
        selections.push(crate::fork::ForkSelection { required: true,
            revision: crate::fork::ResourceRevision::PrivateVolume { volume: scratch, generation: private_generation.0, paths: Vec::new() } });
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let authorized_source = match source_read {
            Some(handle) => {
                let authorization = handle.authorization();
                if authorization.caller_task != caller.task {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "native source approval belongs to another caller" });
                }
                handle.require_selection(operation, selected, &authorization.project,
                    &authorization.generation, authorization.native_snapshot.as_ref(), Some(work))?;
                Some(authorization.clone_with_work_and_custody(&mut observer)?.0)
            }
            None if selected != caller.task => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "foreign fork has no operation-sealed Project reader",
            }),
            None => None,
        };
        if selected == caller.task && source_read.is_some() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "current fork cannot substitute a foreign reader" });
        }
        let request = ForkRequest {
            operation_id: operation, parent: parent_authority, parent_revision: parent.revision(),
            child, child_agent: arguments.id, attached_agents: Vec::new(),
            preparation: crate::fork::ForkPreparation {
                child_project_volume: child_project, child_private_volume: child_private,
                child_result_volume: child_result, summary: None, inherited_through_sequence: 0,
                maximum_inherited_messages: self.config.inherited_messages,
                maximum_inherited_bytes: limits.file_bytes,
                maximum_inherited_references: self.config.inherited_references,
            }, selections, boundary: None, authorized_source,
        }.with_history_policy(parent, if selected == caller.task {
            crate::fork::ForkHistoryPolicy::default()
        } else { crate::fork::ForkHistoryPolicy::Fresh }, &mut observer)?;
        drop(observer);
        drop(history);
        let retained = io.owned_bytes()?;
        Ok((request, io.into_native_custody(retained)?))
    }

    fn native_resolver(
        &self, context: &ToolContext, owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        context.native_task().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native recursive source requires its original native task context",
        })?.content_residency_verifier_with_native_work(owner, work)
    }

    async fn native_restore_source(
        &self, context: &ToolContext, caller: &ConfiguredRecursiveCaller,
        owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        request: (ForkRequest, acyclic_fs::NativeAllocation),
        restored: Option<Reducer>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)> {
        let selected = match &request.0.authorized_source {
            Some(authorization) => Some(owner.restore_project_source_authorization_with_native_work(
                context.task(), authorization, work,
            )?.await?),
            None => None,
        };
        let mut _pinned_custody = None;
        let reducer = match restored {
            Some(reducer) => reducer,
            None => {
                let pinned = self.native_open_aggregate(
                    &caller.owner, Some(request.0.parent_revision), cancellation, work,
                ).await?;
                _pinned_custody = Some(pinned.1);
                pinned.0.into_reducer()
            }
        };
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let verifier = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            caller.owner.issuer().verifier(&mut observer)?
        };
        let resolver = self.native_resolver(context, owner, work)?;
        super::recursive::restore_recursive_fork_source_with_native_work(
            Arc::clone(&self.filesystem), &self.stream, reducer, &verifier, &caller.owner.scope,
            &caller.project, &self.config.stream_provider, request, self.config.history_limits,
            selected.as_ref().map(|selected| &selected.0), resolver, cancellation, work,
        ).await
    }

    async fn native_restore_snapshot(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, request: &ForkRequest,
        snapshot: Snapshot, snapshot_custody: Option<acyclic_fs::NativeAllocation>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Reducer> {
        let snapshot = match snapshot_custody {
            Some(custody) => snapshot.adopt_predecoded_native_custody(custody)?,
            None => snapshot,
        };
        if snapshot.authority != request.parent || snapshot.revision != request.parent_revision
            || &snapshot.authority != binding.authority() || snapshot.revision == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "configured native checkpoint differs from original caller cut" });
        }
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let verifier = binding.issuer().verifier(&mut observer)?;
        let reader = HistoryReader::new_with_native_work(&self.stream, binding.authority(), &verifier, work)?;
        let expected = snapshot.events.last().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "native checkpoint has no canonical head" })?;
        let (head, retained) = reader.0.operation_event_bounded_with_work(
            expected.operation_id, self.config.history_limits.maximum_bytes, &mut observer,
        ).await?;
        let head_hold = io.guard_owned(retained)?;
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        if head.as_ref() != Some(expected) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native checkpoint head differs from immutable canonical Stream event" });
        }
        drop(head);
        drop(head_hold);
        Reducer::restore_with_work(snapshot, verifier, SchemaRegistry::new(), &mut observer)
    }

    async fn native_child_claims(
        &self, context: &ToolContext, caller: &ConfiguredRecursiveCaller,
        allocation: &ConfiguredRecursiveFork, arguments: &ForkToolArguments,
        source: &RetainedSourceBinding<A, O>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<((TaskAdmissionRecord, Scope), acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let copied = allocation.admission.clone_with_native_work(work)?;
        io.absorb_native_custody(copied.1)?;
        let mut child = copied.0;
        if child.parent != Some(caller.task) || allocation.child.scope.agent() != Some(arguments.id) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native child allocation differs from original caller or agent" });
        }
        let reference_io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let references = crate::NativeFutureBundle::new(work, ||
            source.preparer.required_reference_grants(&source.request, Some(&reference_io)))?.await?;
        let mut grants = Vec::new();
        let mut scoped = Vec::new();
        let append = |values: &mut Vec<String>, value: &str| -> Result<()> {
            let mut history = io.history_io();
            if values.len() == values.capacity() {
                let old = values.capacity();
                let target = old.checked_mul(2).unwrap_or(usize::MAX).max(4);
                let bytes = target.checked_sub(old).and_then(|slots|
                    slots.checked_mul(std::mem::size_of::<String>())).ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid, reason: "native child claim slots overflow",
                    })? as u64;
                history.admit_owned(bytes)?;
                history.admit_allocation(bytes)?;
                values.try_reserve_exact(target - values.len()).map_err(|cause| history.allocation_failure(cause))?;
                history.retain_owned((values.capacity() - old) as u64 * std::mem::size_of::<String>() as u64)?;
            }
            values.push(history.clone_text(value)?);
            Ok(())
        };
        for grant in child.grants.iter() { append(&mut grants, grant)?; }
        for grant in allocation.child.scope.capabilities().iter() { append(&mut scoped, grant)?; }
        for volume in [&allocation.project, &allocation.private_volume].into_iter().chain(allocation.result_volume.as_ref()) {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                let grant = volume.capability_with_native_work(operation, work)?;
                append(&mut grants, &grant.0)?;
                append(&mut scoped, &grant.0)?;
            }
        }
        for file in &allocation.input_files {
            if !crate::runtime::read_granted_with_work(context.scope().grants(), file,
                &mut Some(&mut io.history_io()))? {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "current tool scope cannot delegate this file" });
            }
            drop(context.task().read_file_with_native_work(file, work).await?);
            let grant = super::fork_preparer::bounded_io::reference_key(file, Some(&io))?;
            append(&mut grants, &grant)?;
            append(&mut scoped, &grant)?;
            let bytes = grant.capacity() as u64;
            drop(grant);
            io.release(bytes)?;
        }
        for reference in &references {
            if reference.reader != arguments.id {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native inherited reference targets another child reader" });
            }
            if !crate::runtime::read_granted_with_work(context.scope().grants(), &reference.file,
                &mut Some(&mut io.history_io()))? {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "current tool scope cannot delegate this file" });
            }
            drop(context.task().read_file_with_native_work(&reference.file, work).await?);
            if let Some(manifest) = &reference.attachment_manifest {
                if !crate::runtime::read_granted_with_work(context.scope().grants(), manifest,
                &mut Some(&mut io.history_io()))? {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "current tool scope cannot delegate this file" });
            }
            drop(context.task().read_file_with_native_work(manifest, work).await?);
            }
            let grant = super::fork_preparer::bounded_io::reference_key(&reference.file, Some(&io))?;
            append(&mut grants, &grant)?;
            append(&mut scoped, &grant)?;
            let bytes = grant.capacity() as u64;
            drop(grant);
            io.release(bytes)?;
        }
        drop(append);
        let grant_slots = grants.capacity() as u64 * std::mem::size_of::<String>() as u64;
        let scoped_slots = scoped.capacity() as u64 * std::mem::size_of::<String>() as u64;
        let mut history = io.history_io();
        history.admit_owned(grants.len() as u64 * std::mem::size_of::<String>() as u64)?;
        history.retain_owned(grants.len() as u64 * std::mem::size_of::<String>() as u64)?;
        history.admit_owned(scoped.len() as u64 * std::mem::size_of::<String>() as u64)?;
        history.retain_owned(scoped.len() as u64 * std::mem::size_of::<String>() as u64)?;
        let old_grants_bytes = child.grants.logical_owned_bytes()?;
        if !grants.is_empty() {
            history.admit_allocation(grants.len() as u64 * std::mem::size_of::<String>() as u64)?;
        }
        let old_grants = std::mem::replace(&mut child.grants, Capabilities::new(grants));
        drop(old_grants);
        history.release_owned(old_grants_bytes)?;
        history.release_owned(grant_slots)?;
        if !scoped.is_empty() {
            history.admit_allocation(scoped.len() as u64 * std::mem::size_of::<String>() as u64)?;
        }
        let capabilities = Capabilities::new(scoped);
        history.release_owned(scoped_slots)?;
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let scope = allocation.child.issuer().root_for_agent_with_work(
            arguments.id, allocation.child.scope.id(), &capabilities, &mut observer,
        )?;
        let temporary = capabilities.logical_owned_bytes()?;
        drop(capabilities);
        observer.as_deref_mut().expect("native child issuer observer").release_owned(temporary)?;
        child.validate_with_work(&mut observer)?;
        drop(observer);
        drop(history);
        let retained = io.owned_bytes()?;
        Ok(((child, scope), io.into_native_custody(retained)?))
    }

    async fn native_select_source(
        &self, context: &ToolContext, arguments: &ForkToolArguments,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RecursiveForkSourceIntent, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let owner = self.native_source_owner(context.task(), cancellation, work).await?;
        let caller = self.authenticate_native_caller(context.task(), owner.as_ref(),
            arguments.source.is_none_or(|task| Some(task) == context.durable_task_id()), work).await?;
        self.require_native_effective_volume(context, &caller.private_volume, VolumeOperation::Read, work)?;
        if arguments.source.is_none_or(|source| source == caller.task) {
            self.require_native_effective_volume(context, &caller.project, VolumeOperation::Read, work)?;
        }
        let allocation = self.allocation(caller.task, arguments, Some(&super::PreparationIo::from_native_work(work)?))?;
        let parent = self.native_open_aggregate(&caller.owner, None, cancellation, work).await?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let current_project = super::fork_preparer::bounded_io::content_workspace(
            caller.project.provider(), &caller.project, Some(&io),
        )?;
        let current_private = super::fork_preparer::bounded_io::content_workspace(
            caller.private_volume.provider(), &caller.private_volume, Some(&io),
        )?;
        let project = super::fork_preparer::bounded_io::resolve(&self.filesystem, &current_project, Some(&io)).await?;
        let private = super::fork_preparer::bounded_io::resolve(&self.filesystem, &current_private, Some(&io)).await?;
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let selected_task = arguments.source.unwrap_or(caller.task);
        let selected = if selected_task != caller.task {
            let BoundProjectSourcePolicy { issuer, provider: policy, custody } = self.project_policy.get().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "foreign native Project selection has no configured original Root policy",
            })?;
            self.require_native_policy(custody.as_ref(), work)?;
            let chosen = self.caller(selected_task, Some(&super::PreparationIo::from_native_work(work)?))?;
            let chosen_workspace = super::fork_preparer::bounded_io::content_workspace(
                chosen.project.provider(), &chosen.project, Some(&io),
            )?;
            let generation = super::fork_preparer::bounded_io::resolve(
                &self.filesystem, &chosen_workspace, Some(&io),
            ).await?;
            let snapshot = ProjectSourceSnapshot {
                project: {
                    let mut history = io.history_io();
                    chosen.project.copy(&mut Some(&mut history))?
                },
                generation: generation.generation.clone_with_io(&io)?,
                native_snapshot: match &chosen.native_snapshot {
                    Some(value) => Some(crate::model::clone_value(value, Some(&io))?),
                    None => None,
                },
            };
            Some(owner.authorize_project_source_with_native_work(context.task(), issuer, policy.as_ref(),
                selected_task, context.operation_id(), &snapshot, allocation.limits.work, allocation.limits.bytes, work,
            )?.await?)
        } else { None };
        let request = self.native_fork_request(context, caller, allocation, arguments, parent.0.reducer(),
            &project.generation, &private.generation, selected.as_ref().map(|selected| &selected.0), work)?;
        let resolver = self.native_resolver(context, Arc::clone(&owner), work)?;
        let verifier = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            caller.owner.issuer().verifier(&mut observer)?
        };
        let source = super::recursive::restore_recursive_fork_source_with_native_work(
            Arc::clone(&self.filesystem), &self.stream, parent.0.into_reducer(), &verifier, &caller.owner.scope,
            &caller.project, &self.config.stream_provider, request, self.config.history_limits,
            selected.as_ref().map(|selected| &selected.0), resolver, cancellation, work,
        ).await?;
        let _parent_custody = parent.1;
        let claims = self.native_child_claims(context, caller, allocation, arguments, &source.0, work).await?;
        let allocation_digest = {
            let (admission, _admission_hold) = claims.0.0.clone_with_native_work(work)?;
            allocation_digest(allocation, admission, &claims.0.1, Some(&io))?
        };
        let retained_arguments = ForkToolArguments { id: arguments.id, msg: super::fork_preparer_io::clone_text(&arguments.msg, Some(&io))?, source: arguments.source };
        let history = io.history_io();
        history.absorb_native_custody(source.1)?;
        drop(history);
        let retained = io.owned_bytes()?;
        Ok((RecursiveForkSourceIntent {
            arguments: retained_arguments, caller: caller.task, source_task: selected_task,
            child_operation_id: claims.0.0.operation_id, allocation_digest, request: source.0.request,
        }, io.into_native_custody(retained)?))
    }

    fn native_bind_aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, scope: &Scope,
        aggregate: StreamAggregate<P>, content: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<StreamAggregate<P>> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let verifier = {
            let mut history = io.history_io();
            binding.issuer().verifier(&mut Some(&mut history))?
        };
        let content = match content {
            Some(content) => content,
            None => FilesystemContentVerifier::new(Arc::clone(&self.filesystem), &verifier, scope, self.limits.file_bytes, Some(&io))?.into_native_shared(work)?,
        };
        let filesystem: Arc<dyn crate::fork::ForkSeedVerifier> = FilesystemForkVerifier::new_with_native_work(
            Arc::clone(&self.filesystem), self.limits.file_bytes, &verifier, scope, work,
        )?.into_native_shared(work)?;
        let history: Arc<dyn crate::fork::ForkSeedVerifier> = StreamHistoryForkVerifier::new(&self.config.stream_provider, &mut Some(&mut io.history_io()))?.into_native_shared(work)?;
        let forks = CompositeForkVerifier::new(&[filesystem, history], &mut Some(&mut io.history_io()))?.into_native_shared(work)?;
        let merge = FilesystemProjectMergeVerifier::new_with_native_work(
            Arc::clone(&self.filesystem), &verifier, scope, work,
        )?.into_native_shared(work)?;
        aggregate.with_content_verifier(content).with_fork_verifier(forks)
            .with_merge_verifier(merge).with_limits(self.limits)
    }

    async fn native_merge_binding(
        &self, context: &ToolContext, arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>, cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MergeBinding<P, A, O>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let owner = self.native_source_owner(context.task(), cancellation, work).await?;
        let caller = self.authenticate_native_caller(context.task(), owner.as_ref(), true, work).await?;
        self.require_native_effective_volume(context, &caller.project, VolumeOperation::Read, work)?;
        self.require_native_effective_volume(context, &caller.project, VolumeOperation::Write, work)?;
        let source_owner = self.caller(arguments.source, Some(&super::PreparationIo::from_native_work(work)?))?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let allocation = {
            let mut found = None;
            for merge in &self.config.merges {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 32, items: 1 }, true)?;
                if merge.caller == caller.task && merge.source == arguments.source {
                    found = Some(merge);
                    break;
                }
            }
            found.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native merge has no configured original notice binding" })?
        };
        let runtime = self.runtime(Some(&super::PreparationIo::from_native_work(work)?))?;
        let snapshots = runtime.task_host().observe_volume_snapshots_with_native_work(arguments.source, work).await?;
        let selected = {
            let mut found = None;
            for snapshot in &snapshots.0 {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
                if snapshot.volume == source_owner.project {
                    if found.is_some() {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                            reason: "native merge repeats its authenticated Project selection" });
                    }
                    found = Some(snapshot);
                }
            }
            found.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native merge selected source has no authenticated created Project" })?
        };
        if let Some(original) = original {
            if original.caller != caller.task || original.arguments != *arguments
                || &original.destination != caller.owner.authority()
                || &original.source != source_owner.owner.authority()
                || original.target_project != caller.project || original.source_project != selected.volume {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native merge retry changed original source or destination" });
            }
        }
        let generation = original.map_or(&selected.generation, |original| &original.source_generation).clone_with_io(&io)?;
        let notice = original.map_or(&allocation.notice, |original| &original.notice).copy(&mut Some(&mut io.history_io()))?;
        let source_read = if let Some(authorization) = original.and_then(|plan| plan.authorized_source.as_ref()) {
            Some(owner.restore_project_source_authorization_with_native_work(
                context.task(), authorization, work,
            )?.await?)
        } else {
            let capability = selected.volume.capability_with_native_work(VolumeOperation::Read, work)?;
            if context.effective_scope().grants().contains(&capability.0) { None }
            else if original.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "retained ambient merge no longer has original Project read authority" });
            } else {
                let BoundProjectSourcePolicy { issuer, provider: policy, custody } = self.project_policy.get().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "foreign native merge has no original RootService Project policy",
                })?;
                self.require_native_policy(custody.as_ref(), work)?;
                let snapshot = ProjectSourceSnapshot {
                    project: {
                        let mut history = io.history_io();
                        selected.volume.copy(&mut Some(&mut history))?
                    },
                    generation: generation.clone_with_io(&io)?,
                    native_snapshot: source_owner.native_snapshot.as_ref()
                        .map(|value| crate::model::clone_value(value, Some(&io))).transpose()?,
                };
                Some(owner.authorize_project_source_with_native_work(context.task(), issuer, policy.as_ref(),
                    arguments.source, context.operation_id(), &snapshot,
                    self.config.content_budget.items_examined, self.config.history_limits.maximum_bytes, work,
                )?.await?)
            }
        };
        if let Some(handle) = &source_read {
            let authorization = handle.0.authorization();
            handle.0.require_selection(context.operation_id(), arguments.source,
                &selected.volume, &generation, authorization.native_snapshot.as_ref(), Some(work))?;
        }
        context.read_file_with_native_work(&notice.content, work).await?;
        let destination = self.native_open_aggregate(&caller.owner, None, cancellation, work).await?;
        let (verifier, scope, project, source, source_project) = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            (caller.owner.issuer().verifier(&mut observer)?,
                caller.owner.scope.copy(&mut observer)?, caller.project.copy(&mut observer)?,
                observer.as_deref_mut().expect("native merge observer").clone_authority(source_owner.owner.authority())?,
                selected.volume.copy(&mut observer)?)
        };
        let aggregate = self.native_bind_aggregate(&caller.owner, &scope, destination.0,
            Some(self.native_resolver(context, Arc::clone(&owner), work)?), work)?;
        io.absorb_native_custody(destination.1)?;
        let source_read = match source_read {
            Some((handle, guard)) => {
                io.absorb_native_custody(guard)?;
                Some(handle)
            }
            None => None,
        };
        let retained = io.owned_bytes()?;
        Ok((MergeBinding { filesystem: Arc::clone(&self.filesystem), destination: aggregate,
            verifier, scope, project, source, source_project, source_generation: generation, source_read, notice,
        }, io.into_native_custody(retained)?))
    }

    async fn native_fork_binding(
        &self, context: &ToolContext, arguments: &ForkToolArguments,
        intent: &RecursiveForkSourceIntent, original: Option<&RecursiveForkToolPlan>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ForkBinding<P, A, O>, acyclic_fs::NativeAllocation)> {
        use std::fmt::Write;
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let owner = self.native_source_owner(context.task(), cancellation, work).await?;
        let caller = self.authenticate_native_caller(context.task(), owner.as_ref(),
            arguments.source.is_none_or(|task| Some(task) == context.durable_task_id()), work).await?;
        self.require_native_effective_volume(context, &caller.private_volume, VolumeOperation::Read, work)?;
        if arguments.source.is_none_or(|source| source == caller.task) {
            self.require_native_effective_volume(context, &caller.project, VolumeOperation::Read, work)?;
        }
        let allocation = self.allocation(caller.task, arguments, Some(&super::PreparationIo::from_native_work(work)?))?;
        if intent.arguments != *arguments || intent.caller != caller.task
            || intent.source_task != arguments.source.unwrap_or(caller.task)
            || intent.child_operation_id != allocation.admission.operation_id
            || intent.request.operation_id != context.operation_id() || &intent.request.parent != caller.owner.authority()
            || &intent.request.child != allocation.child.authority() || intent.request.child_agent != arguments.id
            || intent.request.preparation.child_project_volume != allocation.project
            || intent.request.preparation.child_private_volume != allocation.private_volume
            || intent.request.preparation.child_result_volume != allocation.result_volume {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native fork changed committed source or original child allocation" });
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if let Some(original) = original {
            let retained = io.value_decode_preparation::<ForkRequest>(&original.request.payload, crate::wire::ErrorCode::Storage)?;
            if original.caller != caller.task || original.arguments != *arguments || retained.0 != intent.request
                || original.request.child.operation_id != intent.child_operation_id
                || original.request.input_files != allocation.input_files
                || original.child_message_pointer != allocation.child_message_pointer
                || original.child_message_file_pointer != allocation.child_message_file_pointer {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native fork retry changed retained original plan" });
            }
        }
        let retained_request = {
            let mut history = io.history_io();
            intent.request.copy(&mut Some(&mut history))?
        };
        let retained_guard = io.take_native_custody(retained_request.1)?;
        let pinned_source = self.native_restore_source(context, caller, Arc::clone(&owner),
            (retained_request.0, retained_guard), None, cancellation, work).await?;
        let claims = self.native_child_claims(context, caller, allocation, arguments, &pinned_source.0, work).await?;
        let digest = {
            let (admission, _admission_hold) = claims.0.0.clone_with_native_work(work)?;
            allocation_digest(allocation, admission, &claims.0.1, Some(&io))?
        };
        if digest != intent.allocation_digest {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native child claims changed after immutable source choice" });
        }
        let checkpoint = match original {
            Some(original) => Some(super::fork_preparer_io::clone_file(&original.source_checkpoint, Some(&io))?),
            None => {
                let observed = context.observe_staged_file_once_with_native_work(
                    checkpoint_operation(context.operation_id()), work,
                ).await?;
                io.absorb_native_custody(observed.1)?;
                observed.0
            }
        };
        let source_checkpoint = match checkpoint {
            Some(checkpoint) => checkpoint,
            None => {
                let pinned = self.native_open_aggregate(&caller.owner, Some(intent.request.parent_revision), cancellation, work).await?;
                let request = {
                    let mut history = io.history_io();
                    intent.request.copy(&mut Some(&mut history))?.0
                };
                let capture = SourceCheckpoint {
                    snapshot: pinned.0.reducer().snapshot_native(1, work)?,
                    request, caller: caller.task, arguments: ForkToolArguments { id: arguments.id, msg: super::fork_preparer_io::clone_text(&arguments.msg, Some(&io))?, source: arguments.source },
                };
                let bytes_guard;
                let bytes = io.encode(&capture, context.effective_scope().limits().file_bytes, false)?;
                bytes_guard = io.guard_bytes(&bytes)?;
                let mut path = String::new();
                {
                    let mut history = io.history_io();
                    let length = "recursive-tools/".len() + 36 + "/source-checkpoint.json".len();
                    history.reserve_text(&mut path, length)?;
                    history.admit_copy(length as u64)?;
                    write!(&mut path, "recursive-tools/{}/source-checkpoint.json", context.operation_id())
                        .map_err(|_| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                            reason: "native checkpoint path formatting failed" })?;
                    history.copy_bytes(path.len() as u64)?;
                }
                let staged = context.stage_file_once_with_native_work(checkpoint_operation(context.operation_id()),
                    &path, &bytes, "application/json", "source-checkpoint.json", work).await?;
                io.absorb_native_custody(staged.1)?;
                drop(bytes);
                drop(bytes_guard);
                staged.0
            }
        };
        let retained = context.read_file_with_native_work(&source_checkpoint, work).await?;
        let capture = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<SourceCheckpoint>, retained.as_ref(), crate::wire::ErrorCode::Storage)?);
        if capture.caller != caller.task || capture.arguments != *arguments || capture.request != intent.request {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native published checkpoint differs from immutable source intent" });
        }
        let snapshot_bytes = capture.snapshot.predecoded_native_owned_bytes()?;
        let snapshot_guard = io.take_native_custody(snapshot_bytes)?;
        let restored = self.native_restore_snapshot(&caller.owner, &capture.request,
            capture.snapshot, Some(snapshot_guard), cancellation, work).await?;
        let copied_request = {
            let mut history = io.history_io();
            capture.request.copy(&mut Some(&mut history))?
        };
        let request_guard = io.take_native_custody(copied_request.1)?;
        let source = self.native_restore_source(context, caller, Arc::clone(&owner),
            (copied_request.0, request_guard), Some(restored), cancellation, work).await?;
        for file in &allocation.input_files { context.read_file_with_native_work(file, work).await?; }
        let parent = self.native_open_aggregate(&caller.owner, None, cancellation, work).await?;
        let child = self.native_open_aggregate(&allocation.child, None, cancellation, work).await?;
        let scope = {
            let mut history = io.history_io();
            caller.owner.scope.copy(&mut Some(&mut history))?
        };
        let parent_aggregate = self.native_bind_aggregate(&caller.owner, &scope, parent.0,
            Some(self.native_resolver(context, owner, work)?), work)?;
        let child_aggregate = self.native_bind_aggregate(&allocation.child, &claims.0.1, child.0, None, work)?;
        io.absorb_native_custody(parent.1)?;
        io.absorb_native_custody(child.1)?;
        io.absorb_native_custody(claims.1)?;
        io.absorb_native_custody(source.1)?;
        let child_message_pointer = super::fork_preparer_io::clone_text(&allocation.child_message_pointer, Some(&io))?;
        let child_message_file_pointer = allocation.child_message_file_pointer.as_deref().map(|value| super::fork_preparer_io::clone_text(value, Some(&io))).transpose()?;
        let mut input_files = Vec::new();
        for file in &allocation.input_files {
            let file = super::fork_preparer_io::clone_file(file, Some(&io))?;
            crate::executor::stock_vec_push(&mut input_files, file, Some(&io))?;
        }
        let retained = io.owned_bytes()?;
        Ok((ForkBinding {
            source: source.0, source_checkpoint, child: claims.0.0, child_message_pointer,
            child_message_file_pointer, input_files, parent: parent_aggregate, child_conversation: child_aggregate,
            parent_scope: scope, child_scope: claims.0.1, limits: allocation.limits,
        }, io.into_native_custody(retained)?))
    }

    async fn native_preparation_source(
        &self, context: &Arc<crate::runtime::NativeTaskContext>, token: &TaskResourcePreparation,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
        reconciliation_only: bool,
    ) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let task_context = context.context();
        task_context.owned_task_binding_with_native_work(work)?;
        if reconciliation_only {
            token.verify_reconciliation_with_native_work(work)?.await?;
        } else {
            task_context.require_original_native_dispatch(work)?;
            token.verify_creation_with_native_work(work)?.await?;
        }
        let owner = self.native_source_owner(task_context, cancellation, work).await?;
        let resolver = context.content_residency_verifier_with_native_work(owner, work)?;
        let preparation = token.request();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        {
            let mut history = io.history_io();
            preparation.validate(&mut Some(&mut history))?;
        }
        let caller_task = preparation.child.parent.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native recursive preparation has no original caller",
        })?;
        let caller = self.caller(caller_task, Some(&super::PreparationIo::from_native_work(work)?))?;
        let request = crate::filesystem::retain_decoded(io.value_decode_preparation::<ForkRequest>(&preparation.payload, crate::wire::ErrorCode::Storage)?);
        let runtime = self.runtime(Some(&super::PreparationIo::from_native_work(work)?))?;
        let admission = runtime.task_host().observe_admission_with_native_work(caller_task, work).await?;
        if admission.0.parent.is_some() {
            let snapshots = runtime.task_host().observe_volume_snapshots_with_native_work(caller_task, work).await?;
            let mut project = 0;
            let mut scratch = 0;
            for snapshot in &snapshots.0 {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
                project += usize::from(snapshot.volume == caller.project);
                scratch += usize::from(snapshot.volume == caller.private_volume);
            }
            if project != 1 || scratch != 1 {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native restored preparation differs from original caller allocation" });
            }
        }
        if request.authorized_source.as_ref() != token.project_source().map(ProjectSourceReadHandle::authorization) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native preparation changed its retained sealed Project source" });
        }
        let restored = if let Some(checkpoint) = &preparation.source_checkpoint {
            let retained = context.context().read_file_with_native_work(checkpoint, work).await?;
            let capture = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<SourceCheckpoint>, &retained, crate::wire::ErrorCode::Storage)?);
            if capture.request != request || capture.caller != caller_task {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native preparation checkpoint changed original caller or request" });
            }
            let snapshot_bytes = capture.snapshot.predecoded_native_owned_bytes()?;
            let snapshot_guard = io.take_native_custody(snapshot_bytes)?;
            self.native_restore_snapshot(&caller.owner, &request, capture.snapshot, Some(snapshot_guard), cancellation, work).await?
        } else if let Some(original) = self.config.original_sources.iter()
            .find(|original| original.request.operation_id == request.operation_id) {
            if original.caller != caller_task || original.request != request {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native trusted original source record differs from sealed preparation" });
            }
            let snapshot = original.snapshot.clone_native(work)?;
            self.native_restore_snapshot(&caller.owner, &request, snapshot, None, cancellation, work).await?
        } else {
            let original = self.native_open_aggregate(&caller.owner, Some(request.parent_revision), cancellation, work).await?;
            io.absorb_native_custody(original.1)?;
            original.0.into_reducer()
        };
        let verifier = {
            let mut history = io.history_io();
            caller.owner.issuer().verifier(&mut Some(&mut history))?
        };
        let copied_request = {
            let mut history = io.history_io();
            request.copy(&mut Some(&mut history))?
        };
        let request_guard = io.take_native_custody(copied_request.1)?;
        let source = super::recursive::restore_recursive_fork_source_with_native_work(
            Arc::clone(&self.filesystem), &self.stream, restored, &verifier, &caller.owner.scope,
            &caller.project, &self.config.stream_provider, (copied_request.0, request_guard),
            self.config.history_limits, token.project_source(), resolver, cancellation, work,
        ).await?;
        if reconciliation_only {
            token.verify_reconciliation_with_native_work(work)?.await?;
        } else {
            token.verify_creation_with_native_work(work)?.await?;
        }
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        Ok(source)
    }
}

impl<P, A, O> RecursiveDefaultToolSource<P, A, O> for ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_source(&self) -> bool {
        self.project_policy.get().is_none_or(|binding|
            binding.custody.is_some() && binding.provider.supports_native_source())
    }

    fn owner<'a>(&'a self, context: &'a ToolContext, tool: &'a ResumableTool, invocation: &'a ToolInvocation,
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<Owner<P, A, O>>> {
        Box::pin(async move { self.authenticate_caller(context, false).await?; self.runtime(None)?.recursive_tool_owner(context, tool, invocation, cancellation).await })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn owner_with_native_work<'a>(&'a self, context: &'a ToolContext,
        tool: &'a ResumableTool, invocation: &'a ToolInvocation,
        cancellation: &'a CancellationToken, work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (Owner<P, A, O>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let runtime = self.runtime(Some(&super::PreparationIo::from_native_work(work)?))?;
            let owner = runtime.recursive_tool_owner_with_native_work(
                context, tool, invocation, cancellation, work,
            ).await?;
            self.authenticate_native_caller(
                context.task(), owner.0.journal_owner.as_ref(), false, work,
            ).await?;
            Ok(owner)
        })
    }
    fn select<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveForkSourceIntent>> {
        Box::pin(self.select_source(context, arguments, cancellation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn select_with_native_work<'a>(&'a self, context: &'a ToolContext,
        arguments: &'a ForkToolArguments, cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (RecursiveForkSourceIntent, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || self.native_select_source(context, arguments, cancellation, work))
    }
    fn fork<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments,
        intent: &'a RecursiveForkSourceIntent, original: Option<&'a RecursiveForkToolPlan>, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<ForkBinding<P, A, O>>> {
        Box::pin(self.fork_binding(context, arguments, intent, original, cancellation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fork_with_native_work<'a>(&'a self, context: &'a ToolContext,
        arguments: &'a ForkToolArguments, intent: &'a RecursiveForkSourceIntent,
        original: Option<&'a RecursiveForkToolPlan>, cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (ForkBinding<P, A, O>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || self.native_fork_binding(context, arguments, intent, original, cancellation, work))
    }
    fn preparation<'a>(&'a self, context: &'a TaskContext, token: &'a TaskResourcePreparation,
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RetainedSourceBinding<A, O>>> {
        Box::pin(self.restore_preparation_source(context, token, cancellation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_with_native_work<'a>(&'a self, context: &'a Arc<crate::runtime::NativeTaskContext>,
        token: &'a TaskResourcePreparation, cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || self.native_preparation_source(context, token, cancellation, work, false))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_for_reconciliation_with_native_work<'a>(
        &'a self, context: &'a Arc<crate::runtime::NativeTaskContext>,
        token: &'a TaskResourcePreparation, cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || self.native_preparation_source(context, token, cancellation, work, true))
    }
    fn merge<'a>(&'a self, context: &'a ToolContext, arguments: &'a MergeToolArguments, original: Option<&'a RecursiveMergeToolPlan>,
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<MergeBinding<P, A, O>>> {
        Box::pin(self.merge_binding(context, arguments, original, cancellation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn merge_with_native_work<'a>(&'a self, context: &'a ToolContext,
        arguments: &'a MergeToolArguments, original: Option<&'a RecursiveMergeToolPlan>,
        cancellation: &'a CancellationToken, work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (MergeBinding<P, A, O>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || self.native_merge_binding(context, arguments, original, cancellation, work))
    }
}

fn validate_result_volume(result: &VolumeRef, private: &VolumeRef, agent: crate::AgentId, provider: &ProviderRef) -> Result<()> {
    result.validate(&mut None)?;
    if result == private || result.provider() != provider
        || result.owner() != &VolumeOwner::Agent(agent)
        || result.class() != crate::conversation::VolumeClass::AgentPrivate {
        return Err(Error::Unauthorized("result is not a distinct actual child-owned output namespace".into()));
    }
    Ok(())
}

fn verifier(binding: &ConfiguredRecursiveAuthorityBinding) -> Result<AuthorityVerifier> {
    let verifier = binding.issuer().verifier(&mut None).expect("verifier copy");
    verifier.verify(&binding.scope, &mut None)?;
    Ok(verifier)
}

fn allocation_digest(
    allocation: &ConfiguredRecursiveFork, mut admission: TaskAdmissionRecord, scope: &Scope,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<[u8; 32]> {
    admission.resource_proofs.clear();
    let descriptor = allocation_descriptor(allocation, &admission, scope);
    match io {
        Some(io) => crate::contract::canonical_json_digest_with_work(
            &descriptor, acyclic_stream::MAX_RECORD_BYTES as u64, &mut Some(&mut io.history_io())),
        None => crate::contract::canonical_json_digest(&descriptor),
    }
}

fn allocation_descriptor<'a>(
    allocation: &'a ConfiguredRecursiveFork, child: &'a TaskAdmissionRecord, scope: &'a Scope,
) -> impl Serialize + 'a {
    #[derive(Serialize)]
    struct Descriptor<'a, T: Serialize> {
        contract: &'static str, admission: T, authority: &'a Authority, issuer: &'a str,
        agent: Option<crate::AgentId>, grants: &'a Capabilities, message_pointer: &'a str,
        message_file_pointer: Option<&'a str>, input_files: &'a [crate::conversation::FileRef],
        limits: ForkPreparationLimits,
    }
    Descriptor {
        contract: "harness.configured-recursive-allocation.v2", admission: child.canonical_payload(),
        authority: allocation.child.authority(), issuer: allocation.child.issuer().id(),
        agent: scope.agent(), grants: scope.capabilities(), message_pointer: &allocation.child_message_pointer,
        message_file_pointer: allocation.child_message_file_pointer.as_deref(),
        input_files: &allocation.input_files, limits: allocation.limits,
    }
}

fn checkpoint_operation(effect: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/configured-recursive-checkpoint\0"); hash.update(&effect.into_bytes());
    let mut bytes = [0; 16]; bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]); OperationId::from_bytes(bytes)
}
fn invalid_json(error: serde_json::Error) -> Error { Error::Invalid(error.to_string()) }
fn work_error(error: acyclic_fs::WorkError) -> Error { Error::Storage(error.to_string()) }
