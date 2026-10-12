//! Explicit authenticated provider bindings for the shared portable default tools.
use crate::runtime::TaskStateProvider;
use crate::store::history::HistoryIo;
use crate::{
    Capabilities, Error, OperationId, Result, TaskId,
    conversation::{
        BoundedContentFuture, ContentGrant, ConversationMessage, Limits, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::{Authority, AuthorityIssuer, AuthorityVerifier, Reducer, SchemaRegistry, Scope},
    durable_tool::ResumableTool,
    filesystem::{
        FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
        FilesystemProjectMergeVerifier, FilesystemTaskRuntime, ForkPreparationLimits,
        ForkToolArguments, MergeToolArguments, RecursiveDefaultToolSource, RecursiveDefaultToolset,
        RecursiveForkConfig, RecursiveForkSourceIntent, RecursiveForkToolBinding,
        RecursiveForkToolPlan, RecursiveMergeToolBinding, RecursiveMergeToolPlan,
        RecursiveToolOwner, RetainedSourceBinding, recursive_default_tools,
        restore_recursive_fork_source,
    },
    fork::{CompositeForkVerifier, ForkRequest, StreamHistoryForkVerifier},
    resource_preparation::TaskResourcePreparation,
    resources::ProviderRef,
    runtime::{TaskAdmissionRecord, TaskContext, ToolContext},
    source_authorization::{
        ProjectSourceReadHandle, ProjectSourceSnapshot, RootProjectSourceProvider,
    },
    store::{HistoryReadLimits, StreamAggregate},
    tool::ToolInvocation,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, OnceLock, Weak};

mod binding;
mod config;
mod fork;
mod merge;
mod ops;
mod preparation;
mod selection;
pub use config::{
    ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller, ConfiguredRecursiveFork,
    ConfiguredRecursiveMerge, ConfiguredRecursiveToolsConfig,
};

/// Portable source factory using the runtime's actual Filesystem and Stream.
pub struct ConfiguredRecursiveToolSource<P, A, O> {
    config: ConfiguredRecursiveToolsConfig,
    stream: Arc<P>,
    filesystem: Arc<FilesystemHost<A, O>>,
    limits: Limits,
    runtime: OnceLock<Weak<FilesystemTaskRuntime<P, A, O>>>,
    project_policy: OnceLock<(AuthorityIssuer, Arc<dyn RootProjectSourceProvider>)>,
}

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Validates the finite trusted source configuration.
    pub fn new(
        config: ConfiguredRecursiveToolsConfig,
        stream: Arc<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        provider: &ProviderRef,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        if config.callers.len() > 64
            || config.forks.len() > 64
            || config.merges.len() > 64
            || config.history_limits.maximum_events == 0
            || config.history_limits.maximum_bytes == 0
            || config.history_limits.maximum_bytes == u64::MAX
            || config.inherited_messages == u64::MAX
            || !config.content_budget.has_finite_allowances()
        {
            return Err(Error::Invalid(
                "configured recursive configuration must have finite producer bounds".into(),
            ));
        }
        for (index, caller) in config.callers.iter().enumerate() {
            verifier(&caller.owner)?;
            caller.project.validate(&mut None)?;
            caller.private_volume.validate(&mut None)?;
            let agent = caller.owner.scope.agent().ok_or_else(|| {
                Error::Unauthorized("configured caller has no signed agent".into())
            })?;
            if let Some(native) = &caller.native_snapshot {
                crate::contract::json_byte_length(native, limits.file_bytes, &mut None)
                    .map(|_| ())?;
            }
            if let Some(result) = &caller.result_volume {
                validate_result_volume(result, &caller.private_volume, agent, provider)?;
            }
            if caller.private_volume.owner() != &VolumeOwner::Agent(agent)
                || caller.project.provider() != provider
                || caller.private_volume.provider() != provider
                || config.callers[..index]
                    .iter()
                    .any(|previous| previous.task == caller.task)
            {
                return Err(Error::Unauthorized(
                    "configured caller is not a unique signed provider binding".into(),
                ));
            }
        }
        for (index, fork) in config.forks.iter().enumerate() {
            verifier(&fork.child)?;
            fork.project.validate(&mut None)?;
            fork.private_volume.validate(&mut None)?;
            fork.admission.validate(&mut None)?;
            let agent = fork.child.scope.agent().ok_or_else(|| {
                Error::Unauthorized("configured child has no signed agent".into())
            })?;
            if let Some(result) = &fork.result_volume {
                validate_result_volume(result, &fork.private_volume, agent, provider)?;
            }
            if fork.admission.parent != Some(fork.caller)
                || fork.private_volume.owner() != &VolumeOwner::Agent(agent)
                || fork.project.provider() != provider
                || fork.private_volume.provider() != provider
                || !config
                    .callers
                    .iter()
                    .any(|caller| caller.task == fork.caller)
                || config.forks[..index].iter().any(|previous| {
                    previous.caller == fork.caller && previous.child.scope.agent() == Some(agent)
                })
                || fork.limits.resources == 0
                || fork.limits.work == 0
                || fork.limits.bytes == 0
                || fork.limits.work == u64::MAX
                || fork.limits.bytes == u64::MAX
            {
                return Err(Error::Unauthorized(
                    "configured child allocation is not a unique bounded caller request".into(),
                ));
            }
        }
        for (index, merge) in config.merges.iter().enumerate() {
            merge.notice.validate(&mut None)?;
            if !config
                .callers
                .iter()
                .any(|caller| caller.task == merge.caller)
                || !config
                    .callers
                    .iter()
                    .any(|caller| caller.task == merge.source)
                || config.merges[..index].iter().any(|previous| {
                    previous.caller == merge.caller && previous.source == merge.source
                })
            {
                return Err(Error::Unauthorized(
                    "configured merge lacks unique configured caller/source bindings".into(),
                ));
            }
        }
        Ok(Arc::new(Self {
            config,
            stream,
            filesystem,
            limits,
            runtime: OnceLock::new(),
            project_policy: OnceLock::new(),
        }))
    }
    /// Binds the same runtime without a reference cycle.
    pub fn bind_runtime(&self, runtime: &Arc<FilesystemTaskRuntime<P, A, O>>) -> Result<()> {
        self.runtime
            .set(Arc::downgrade(runtime))
            .map_err(|_| Error::Conflict("configured source runtime was already bound".into()))
    }
    /// Installs the trusted same-graph source policy.
    pub fn with_project_source_policy(
        &self,
        issuer: AuthorityIssuer,
        provider: Arc<dyn RootProjectSourceProvider>,
    ) -> Result<()> {
        self.project_policy.set((issuer, provider)).map_err(|_| {
            Error::Conflict("configured source project policy was already bound".into())
        })
    }
    /// Builds the default tools from this source.
    pub fn tools(self: &Arc<Self>, maximum_bytes: u64) -> Result<RecursiveDefaultToolset> {
        recursive_default_tools(Some(self.clone()), maximum_bytes)
    }
    pub(super) fn runtime(
        &self,
        io: Option<&super::PreparationIo>,
    ) -> Result<Arc<FilesystemTaskRuntime<P, A, O>>> {
        self.runtime.get().and_then(Weak::upgrade).ok_or_else(|| {
            super::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "configured source has no original runtime",
            )
        })
    }
    pub(super) fn caller(
        &self,
        task: TaskId,
        io: Option<&super::PreparationIo>,
    ) -> Result<&ConfiguredRecursiveCaller> {
        for caller in &self.config.callers {
            if let Some(io) = io {
                io.codec_charge(
                    crate::contract::JsonCodecCharge::Work {
                        bytes: 16,
                        items: 1,
                    },
                    true,
                )?;
            }
            if caller.task == task {
                return Ok(caller);
            }
        }
        Err(super::interaction_host::interaction_boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "task has no explicit signed configured source binding",
        ))
    }
    pub(super) fn allocation(
        &self,
        caller: TaskId,
        arguments: &ForkToolArguments,
        io: Option<&super::PreparationIo>,
    ) -> Result<&ConfiguredRecursiveFork> {
        for allocation in &self.config.forks {
            if let Some(io) = io {
                io.codec_charge(
                    crate::contract::JsonCodecCharge::Work {
                        bytes: 32,
                        items: 1,
                    },
                    true,
                )?;
            }
            if allocation.caller == caller && allocation.child.scope.agent() == Some(arguments.id) {
                return Ok(allocation);
            }
        }
        Err(super::interaction_host::interaction_boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "new child has no explicit signed configured allocation",
        ))
    }
}
impl<P, A, O> RecursiveDefaultToolSource<P, A, O> for ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_source(&self) -> bool {
        self.project_policy.get().is_none()
    }

    fn owner<'a>(
        &'a self,
        context: &'a ToolContext,
        tool: &'a ResumableTool,
        invocation: &'a ToolInvocation,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, RecursiveToolOwner<P, A, O>>> {
        BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
                let owner = self
                    .runtime(Some(io))?
                    .recursive_tool_owner(context, tool, invocation, cancellation, Some(io))
                    .await?;
                self.authenticate_caller(
                    context.task(),
                    Some(owner.journal_owner.as_ref()),
                    false,
                    Some(io),
                )
                .await?;
                return Ok(owner);
            }
            if io.is_some() {
                return Err(super::fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "finite recursive source requires original native work",
                ));
            }
            self.authenticate_caller(context.task(), None, false, None)
                .await?;
            self.runtime(None)?
                .recursive_tool_owner(context, tool, invocation, cancellation, None)
                .await
        })
    }
    fn select<'a>(
        &'a self,
        context: &'a ToolContext,
        arguments: &'a ForkToolArguments,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, RecursiveForkSourceIntent>> {
        BoundedContentFuture::new(io, || async move {
            if io.is_some_and(|io| io.original_native_work().is_none()) {
                return Err(super::fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "finite recursive source requires original native work",
                ));
            }
            self.select_source(context, arguments, cancellation, io)
                .await
        })
    }
    fn fork<'a>(
        &'a self,
        context: &'a ToolContext,
        arguments: &'a ForkToolArguments,
        intent: &'a RecursiveForkSourceIntent,
        original: Option<&'a RecursiveForkToolPlan>,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, RecursiveForkToolBinding<P, A, O>>> {
        BoundedContentFuture::new(io, || async move {
            if io.is_some_and(|io| io.original_native_work().is_none()) {
                return Err(super::fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "finite recursive source requires original native work",
                ));
            }
            self.fork_binding(context, arguments, intent, original, cancellation, io)
                .await
        })
    }
    fn preparation<'a>(
        &'a self,
        context: &'a TaskContext,
        token: &'a TaskResourcePreparation,
        cancellation: &'a CancellationToken,
    ) -> BoxProviderFuture<'a, Result<RetainedSourceBinding<A, O>>> {
        Box::pin(async move {
            self.preparation_source(
                &crate::filesystem::task_runtime::WorkerContext::Ordinary(context.clone()),
                token,
                cancellation,
                false,
                None,
            )
            .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_with_native_work<'a>(
        &'a self,
        context: &'a Arc<crate::runtime::TaskContext>,
        token: &'a TaskResourcePreparation,
        cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            let source = self
                .preparation_source(
                    &crate::filesystem::task_runtime::WorkerContext::Native(Arc::clone(context)),
                    token,
                    cancellation,
                    false,
                    Some(&io),
                )
                .await?;
            crate::filesystem::native_decoded((source, io.guard_owned(io.owned_bytes()?)?))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_for_reconciliation_with_native_work<'a>(
        &'a self,
        context: &'a Arc<crate::runtime::TaskContext>,
        token: &'a TaskResourcePreparation,
        cancellation: &'a CancellationToken,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            let source = self
                .preparation_source(
                    &crate::filesystem::task_runtime::WorkerContext::Native(Arc::clone(context)),
                    token,
                    cancellation,
                    true,
                    Some(&io),
                )
                .await?;
            crate::filesystem::native_decoded((source, io.guard_owned(io.owned_bytes()?)?))
        })
    }
    fn merge<'a>(
        &'a self,
        context: &'a ToolContext,
        arguments: &'a MergeToolArguments,
        original: Option<&'a RecursiveMergeToolPlan>,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, RecursiveMergeToolBinding<P, A, O>>> {
        BoundedContentFuture::new(io, || async move {
            if io.is_some_and(|io| io.original_native_work().is_none()) {
                return Err(super::fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "finite recursive source requires original native work",
                ));
            }
            self.merge_binding(context, arguments, original, cancellation, io)
                .await
        })
    }
}

fn validate_result_volume(
    result: &VolumeRef,
    private: &VolumeRef,
    agent: crate::AgentId,
    provider: &ProviderRef,
) -> Result<()> {
    result.validate(&mut None)?;
    if result == private
        || result.provider() != provider
        || result.owner() != &VolumeOwner::Agent(agent)
        || result.class() != crate::conversation::VolumeClass::AgentPrivate
    {
        return Err(Error::Unauthorized(
            "result is not a distinct actual child-owned output namespace".into(),
        ));
    }
    Ok(())
}

fn verifier(binding: &ConfiguredRecursiveAuthorityBinding) -> Result<AuthorityVerifier> {
    let verifier = binding.issuer().verifier(&mut None).expect("verifier copy");
    verifier.verify(&binding.scope, &mut None)?;
    Ok(verifier)
}

fn allocation_digest(
    allocation: &ConfiguredRecursiveFork,
    admission: &mut TaskAdmissionRecord,
    scope: &Scope,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<[u8; 32]> {
    let proofs = std::mem::take(&mut admission.resource_proofs);
    #[derive(Serialize)]
    struct Descriptor<'a, T: Serialize> {
        contract: &'static str,
        admission: T,
        authority: &'a Authority,
        issuer: &'a str,
        agent: Option<crate::AgentId>,
        grants: &'a Capabilities,
        message_pointer: &'a str,
        message_file_pointer: Option<&'a str>,
        input_files: &'a [crate::conversation::FileRef],
        limits: ForkPreparationLimits,
    }
    let digest = {
        let descriptor = Descriptor {
            contract: "harness.configured-recursive-allocation.v2",
            admission: admission.canonical_payload(),
            authority: allocation.child.authority(),
            issuer: allocation.child.issuer().id(),
            agent: scope.agent(),
            grants: scope.capabilities(),
            message_pointer: &allocation.child_message_pointer,
            message_file_pointer: allocation.child_message_file_pointer.as_deref(),
            input_files: &allocation.input_files,
            limits: allocation.limits,
        };
        crate::executor::with_history_io(io, |work| {
            crate::contract::canonical_json_digest(
                &descriptor,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })
    };
    admission.resource_proofs = proofs;
    digest
}

fn checkpoint_operation(effect: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/configured-recursive-checkpoint\0");
    hash.update(&effect.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
fn invalid_json(error: serde_json::Error) -> Error {
    Error::Invalid(error.to_string())
}

fn check_cancellation(
    cancellation: &CancellationToken,
    io: Option<&super::PreparationIo>,
) -> Result<()> {
    cancellation.check().map_err(|error| {
        #[cfg(not(target_arch = "wasm32"))]
        if io
            .and_then(super::PreparationIo::original_native_work)
            .is_some()
        {
            return Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled);
        }
        Error::Storage(error.to_string())
    })
}
