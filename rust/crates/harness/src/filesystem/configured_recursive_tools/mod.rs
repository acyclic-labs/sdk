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
    runtime::{TaskAdmissionRecord, TaskContext, ToolContext},
    store::{HistoryReadLimits, StreamAggregate},
    source_authorization::{ProjectSourceReadHandle, ProjectSourceSnapshot, RootProjectSourceProvider},
    tool::ToolInvocation,
};
use crate::runtime::TaskStateProvider;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, OnceLock, Weak};
use crate::store::history::HistoryIo;


mod config;
mod binding;
mod selection;
mod fork;
mod preparation;
mod merge;
pub use config::{ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller,
    ConfiguredRecursiveFork, ConfiguredRecursiveMerge, ConfiguredRecursiveToolsConfig,
    ConfiguredRecursiveSourceCheckpoint};
use config::SourceCheckpoint;
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
    /// Validates the finite trusted source configuration.
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
            fork.admission.validate(&mut None)?;
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
    /// Binds the same runtime without a reference cycle.
    pub fn bind_runtime(&self, runtime: &Arc<FilesystemTaskRuntime<P, A, O>>) -> Result<()> {
        self.runtime.set(Arc::downgrade(runtime)).map_err(|_| Error::Conflict("configured source runtime was already bound".into()))
    }
    /// Installs the trusted same-graph source policy.
    pub fn with_project_source_policy(&self, issuer: AuthorityIssuer,
        provider: Arc<dyn RootProjectSourceProvider>) -> Result<()> {
        self.project_policy.set(BoundProjectSourcePolicy { issuer, provider,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        })
            .map_err(|_| Error::Conflict("configured source project policy was already bound".into()))
    }
    /// Builds the default tools from this source.
    pub fn tools(self: &Arc<Self>, maximum_bytes: u64) -> Result<RecursiveDefaultToolset> {
        recursive_default_tools(Some(self.clone()), maximum_bytes)
    }
    pub(super) fn runtime(&self, io: Option<&super::PreparationIo>) -> Result<Arc<FilesystemTaskRuntime<P, A, O>>> {
        self.runtime.get().and_then(Weak::upgrade).ok_or_else(|| super::interaction_host::interaction_boundary(
            io, crate::wire::ErrorCode::Unauthorized, "configured source has no original runtime"))
    }
    pub(super) fn caller(&self, task: TaskId, io: Option<&super::PreparationIo>) -> Result<&ConfiguredRecursiveCaller> {
        for caller in &self.config.callers {
            if let Some(io) = io { io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 16, items: 1 }, true)?; }
            if caller.task == task { return Ok(caller); }
        }
        Err(super::interaction_host::interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
            "task has no explicit signed configured source binding"))
    }
    pub(super) fn allocation(&self, caller: TaskId, arguments: &ForkToolArguments, io: Option<&super::PreparationIo>) -> Result<&ConfiguredRecursiveFork> {
        for allocation in &self.config.forks {
            if let Some(io) = io { io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 32, items: 1 }, true)?; }
            if allocation.caller == caller && allocation.child.scope.agent() == Some(arguments.id) { return Ok(allocation); }
        }
        Err(super::interaction_host::interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
            "new child has no explicit signed configured allocation"))
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
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RecursiveToolOwner<P, A, O>>> {
        Box::pin(async move { self.authenticate_caller(context, false).await?; self.runtime(None)?.recursive_tool_owner(context, tool, invocation, cancellation, None).await })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn owner_with_native_work<'a>(&'a self, context: &'a ToolContext,
        tool: &'a ResumableTool, invocation: &'a ToolInvocation,
        cancellation: &'a CancellationToken, work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (RecursiveToolOwner<P, A, O>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            let runtime = self.runtime(Some(&io))?;
            let owner = runtime.recursive_tool_owner(
                context, tool, invocation, cancellation, Some(&io),
            ).await?;
            self.authenticate_native_caller(
                context.task(), owner.journal_owner.as_ref(), false, work,
            ).await?;
            Ok((owner, io.into_native_custody(0)?))
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
        -> BoxProviderFuture<'a, Result<RecursiveForkToolBinding<P, A, O>>> {
        Box::pin(self.fork_binding(context, arguments, intent, original, cancellation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fork_with_native_work<'a>(&'a self, context: &'a ToolContext,
        arguments: &'a ForkToolArguments, intent: &'a RecursiveForkSourceIntent,
        original: Option<&'a RecursiveForkToolPlan>, cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (RecursiveForkToolBinding<P, A, O>, acyclic_fs::NativeAllocation)>> {
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
        cancellation: &'a CancellationToken) -> BoxProviderFuture<'a, Result<RecursiveMergeToolBinding<P, A, O>>> {
        Box::pin(self.merge_binding(context, arguments, original, cancellation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn merge_with_native_work<'a>(&'a self, context: &'a ToolContext,
        arguments: &'a MergeToolArguments, original: Option<&'a RecursiveMergeToolPlan>,
        cancellation: &'a CancellationToken, work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (RecursiveMergeToolBinding<P, A, O>, acyclic_fs::NativeAllocation)>> {
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
    #[derive(Serialize)]
    struct Descriptor<'a, T: Serialize> {
        contract: &'static str, admission: T, authority: &'a Authority, issuer: &'a str,
        agent: Option<crate::AgentId>, grants: &'a Capabilities, message_pointer: &'a str,
        message_file_pointer: Option<&'a str>, input_files: &'a [crate::conversation::FileRef],
        limits: ForkPreparationLimits,
    }
    let descriptor = Descriptor {
        contract: "harness.configured-recursive-allocation.v2", admission: admission.canonical_payload(),
        authority: allocation.child.authority(), issuer: allocation.child.issuer().id(),
        agent: scope.agent(), grants: scope.capabilities(), message_pointer: &allocation.child_message_pointer,
        message_file_pointer: allocation.child_message_file_pointer.as_deref(),
        input_files: &allocation.input_files, limits: allocation.limits,
    };
    super::task_runtime::observed::digest(&descriptor, io, acyclic_stream::MAX_RECORD_BYTES as u64)
}

fn checkpoint_operation(effect: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/configured-recursive-checkpoint\0"); hash.update(&effect.into_bytes());
    let mut bytes = [0; 16]; bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]); OperationId::from_bytes(bytes)
}
fn invalid_json(error: serde_json::Error) -> Error { Error::Invalid(error.to_string()) }
fn work_error(error: acyclic_fs::WorkError) -> Error { Error::Storage(error.to_string()) }
