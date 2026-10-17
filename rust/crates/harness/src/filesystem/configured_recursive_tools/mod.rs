//! Explicit authenticated provider bindings for the shared portable default tools.
use crate::store::history::HistoryIo;
use crate::{
    Capabilities, Error, Result, TaskId,
    conversation::{
        ContentGrant, ConversationMessage, Limits, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Authority, AuthorityIssuer, AuthorityVerifier, Reducer, SchemaRegistry, Scope},
    filesystem::{
        FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
        FilesystemProjectMergeVerifier, FilesystemTaskRuntime, ForkPreparationLimits,
        ForkToolArguments, MergeToolArguments, RecursiveForkConfig, RecursiveForkSourceIntent,
        RecursiveForkToolBinding, RecursiveForkToolPlan, RecursiveMergeToolBinding,
        RecursiveMergeToolPlan, RetainedSourceBinding, restore_recursive_fork_source,
    },
    fork::{CompositeForkVerifier, ForkRequest, StreamHistoryForkVerifier},
    resource_preparation::TaskResourcePreparation,
    resources::ProviderRef,
    runtime::{TaskAdmissionRecord, TaskContext, ToolContext},
    source_authorization::{
        ProjectSourceReadHandle, ProjectSourceSnapshot, RootProjectSourceProvider,
    },
    store::{HistoryReadLimits, StreamAggregate},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget};
use acyclic_stream::StreamProvider;
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

use crate::executor::stock_boundary as failure;
use crate::filesystem::PreparationIo;
use crate::wire::ErrorCode;

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
        let denied = |reason: &str| Error::Unauthorized(reason.into());
        for caller in &config.callers {
            caller.owner.validate_volumes(
                provider,
                &caller.project,
                &caller.private_volume,
                caller.result_volume.as_ref(),
            )?;
        }
        for fork in &config.forks {
            fork.child.validate_volumes(
                provider,
                &fork.project,
                &fork.private_volume,
                fork.result_volume.as_ref(),
            )?;
        }
        let mut callers = std::collections::BTreeSet::new();
        for caller in &config.callers {
            if let Some(native) = &caller.native_snapshot {
                crate::executor::json::json_byte_length(native, limits.file_bytes, &mut None)?;
            }
            if !callers.insert(caller.task) {
                return Err(denied("configured caller is not unique"));
            }
        }
        let mut children = std::collections::BTreeSet::new();
        for fork in &config.forks {
            fork.admission.validate(&mut None)?;
            if fork.admission.parent != Some(fork.caller)
                || !callers.contains(&fork.caller)
                || !children.insert((fork.caller, fork.child.scope.agent()))
                || fork.limits.resources == 0
                || [fork.limits.work, fork.limits.bytes]
                    .into_iter()
                    .any(|limit| limit == 0 || limit == u64::MAX)
            {
                return Err(denied(
                    "configured child is not a unique bounded caller request",
                ));
            }
        }
        let mut merges = std::collections::BTreeSet::new();
        for merge in &config.merges {
            merge.notice.validate(&mut None)?;
            if !callers.contains(&merge.caller)
                || !callers.contains(&merge.source)
                || !merges.insert((merge.caller, merge.source))
            {
                return Err(denied(
                    "configured merge lacks unique caller/source bindings",
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
    pub(super) fn runtime(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<FilesystemTaskRuntime<P, A, O>>> {
        self.runtime.get().and_then(Weak::upgrade).ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "configured source has no original runtime",
            )
        })
    }
    pub(super) fn caller(
        &self,
        task: TaskId,
        io: Option<&PreparationIo>,
    ) -> Result<&ConfiguredRecursiveCaller> {
        for caller in &self.config.callers {
            if let Some(io) = io {
                io.codec_charge(
                    crate::executor::json::JsonCodecCharge::Work {
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
        Err(failure(
            io,
            ErrorCode::Unauthorized,
            "task has no explicit signed configured source binding",
        ))
    }
    pub(super) fn allocation(
        &self,
        caller: TaskId,
        arguments: &ForkToolArguments,
        io: Option<&PreparationIo>,
    ) -> Result<&ConfiguredRecursiveFork> {
        for allocation in &self.config.forks {
            if let Some(io) = io {
                io.codec_charge(
                    crate::executor::json::JsonCodecCharge::Work {
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
        Err(failure(
            io,
            ErrorCode::Unauthorized,
            "new child has no explicit signed configured allocation",
        ))
    }
}

fn native_io(io: Option<&PreparationIo>) -> Result<()> {
    if io.is_some_and(|io| io.original_native_work().is_none()) {
        return Err(failure(
            io,
            ErrorCode::Unsupported,
            "finite recursive source requires original native work",
        ));
    }
    Ok(())
}

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) fn supports_native_source(&self) -> bool {
        self.project_policy.get().is_none()
    }

    pub(in crate::filesystem) async fn owner(
        &self,
        context: &ToolContext,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<(
        Arc<FilesystemTaskRuntime<P, A, O>>,
        Arc<crate::durable_host::TaskJournalOwner<P>>,
    )> {
        native_io(io)?;
        let owner = self.source_owner(context.task(), cancellation, io).await?;
        self.authenticate(context, Some(&owner), false, cancellation, io)
            .await?;
        Ok((self.runtime(io)?, owner))
    }
}

fn verifier(binding: &ConfiguredRecursiveAuthorityBinding) -> Result<AuthorityVerifier> {
    let verifier = binding.issuer.verifier(&mut None)?;
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
            authority: allocation.child.issuer.audience(),
            issuer: allocation.child.issuer.id(),
            agent: scope.agent(),
            grants: scope.capabilities(),
            message_pointer: &allocation.child_message_pointer,
            message_file_pointer: allocation.child_message_file_pointer.as_deref(),
            input_files: &allocation.input_files,
            limits: allocation.limits,
        };
        crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                &descriptor,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })
    };
    admission.resource_proofs = proofs;
    digest
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
        Error::from(error)
    })
}
