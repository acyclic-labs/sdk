//! The stock filesystem command tool over provider-backed generations.
use super::*;
use crate::{
    agent::AgentState,
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_fs::distributed::AuthorityProvider;
use acyclic_fs::{StreamAuthorityStore, StreamGitCompatStore, WorkspaceGraph, WorkspaceResolver};
use acyclic_stream::BoxProviderFuture;

// This boundary is only for reads that own no admitted publication. The
// command future itself must retain an accepted or unresolved commit.
pub(super) async fn read_before_effect<T>(
    cancellation: &mut BoxProviderFuture<'_, Result<()>>,
    read: impl Future<Output = Result<T>>,
) -> Result<T> {
    futures::pin_mut!(read);
    match futures::future::select(cancellation, read).await {
        futures::future::Either::Left((stopped, _)) => {
            stopped?;
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            ))
        }
        futures::future::Either::Right((result, _)) => result,
    }
}

/// Filesystem commands over the remote placement itself, without a machine.
pub struct FilesystemTools<P: ?Sized + 'static, O: 'static> {
    host: Arc<FilesystemHost<StreamAuthorityStore<P>, O>>,
    store: StreamGitCompatStore<P>,
    volumes: BTreeMap<String, VolumeRef>,
    binding_identity: [u8; 32],
    verifier: AuthorityVerifier,
    scope: Scope,
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    /// Binds only the agent's committed volumes and the turn's authenticated scope.
    pub fn new(
        host: Arc<FilesystemHost<StreamAuthorityStore<P>, O>>,
        agent: &AgentState,
        authority: &TurnAuthority,
    ) -> Result<Self> {
        let verifier = authority.issuer.verifier(&mut None)?;
        verifier.verify(&authority.scope, &mut None)?;
        let store =
            StreamGitCompatStore::new(StreamAuthorityStore::new(host.filesystem.stream_provider()));
        let mut volumes = BTreeMap::new();
        for binding in &agent.volumes {
            if volumes
                .insert(binding.name.clone(), binding.volume.clone())
                .is_some()
            {
                return Err(Error::Invalid("duplicate filesystem volume binding".into()));
            }
        }
        // Hash the canonical, sorted binding once, rather than reconstructing
        // or serializing all volume references for every command and replay.
        let mut identity = blake3::Hasher::new();
        identity.update(b"acyclic.filesystem.tool-binding.v1");
        for part in [
            host.provider.namespace(),
            host.provider.family(),
            host.provider.version(),
        ] {
            identity.update(&(part.len() as u64).to_be_bytes());
            identity.update(part.as_bytes());
        }
        for (name, volume) in &volumes {
            identity.update(&(name.len() as u64).to_be_bytes());
            identity.update(name.as_bytes());
            identity.update(&volume.identity_digest(u64::MAX, &mut None)?);
        }
        Ok(Self {
            host,
            store,
            volumes,
            binding_identity: identity.finalize().into(),
            verifier,
            scope: authority.scope.copy(&mut None)?,
        })
    }

    fn target(
        &self,
        parsed: &crate::tools::FsCommand,
        context: &ToolContext,
    ) -> Result<&VolumeRef> {
        let name = parsed.volume.as_deref().unwrap_or("project");
        let volume = self
            .volumes
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("filesystem volume {name}")))?;
        if volume.provider() != &self.host.provider {
            return Err(Error::Unauthorized(
                "filesystem volume belongs to another provider".into(),
            ));
        }
        self.verifier.verify(&self.scope, &mut None)?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::verify(
                &self.verifier,
                &self.scope,
                volume,
                operation,
                u64::MAX,
                &mut None,
            )?;
            if !context
                .effective_scope()
                .grants()
                .contains(&volume.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "filesystem volume is outside the tool's grant".into(),
                ));
            }
        }
        Ok(volume)
    }

    async fn run(
        &self,
        invocation: ToolInvocation,
        context: ToolContext,
        reconcile: bool,
    ) -> Result<Option<ToolResult>> {
        self.authorize(None, Some(&context), &invocation, None)?;
        let mut cancellation = context.model_cancellation(None)?;
        let command = invocation
            .arguments
            .get("command")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("fs requires a command string".into()))?;
        let parsed = crate::tools::parse_fs_command(command)?;
        let volume = self.target(&parsed, &context)?;
        let reference =
            super::workspace_ref(self.host.provider.clone(), &volume.storage_name(&mut None)?)?;
        let root = read_before_effect(&mut cancellation, self.host.open(&reference, None)).await?;
        let access = WorkspaceAccess::new(
            &self.verifier,
            &self.scope,
            context.effective_scope().grants(),
            Some(volume),
        )?;
        read_before_effect(&mut cancellation, access.require(&root)).await?;
        let request = acyclic_fs::GitRequest {
            operation: acyclic_fs::OperationId::from_bytes(context.operation_id().into_bytes()),
            unanswered: Arc::new(|_| true),
        };
        let repository = acyclic_fs::GitCompatRepository::new(root.id(), self.store.clone())
            .for_request(request.clone());
        if let Some(output) = read_before_effect(&mut cancellation, async {
            (crate::tools::git::RepositoryAccess {
                access: &access,
                filesystem: &self.host.filesystem,
                store: &self.store,
                current: &root,
                repository: root.id(),
            })
            .request_result(request.operation)
            .await
        })
        .await?
        {
            return Ok(Some(ToolResult {
                value: serde_json::json!({"result": output}),
            }));
        }
        if reconcile {
            return Ok(None);
        }
        let graph = WorkspaceGraph::new(self.store.clone());
        graph
            .register_root(&root)
            .await
            .map_err(<StreamGitCompatStore<P> as crate::tools::git::CommandStore>::lineage_error)?;
        let (selected, pending) = read_before_effect(&mut cancellation, async {
            repository
                .checkout_selection()
                .await
                .map_err(crate::tools::git::compat_error::<StreamGitCompatStore<P>>)
        })
        .await?;
        if pending.is_some() {
            return Err(Error::Conflict(
                "filesystem checkout requires its original publication reconciliation".into(),
            ));
        }
        let workspace = if selected == root.id() {
            root.clone()
        } else {
            read_before_effect(&mut cancellation, async {
                graph
                    .authorize_descendant(selected, root.id())
                    .await
                    .map_err(
                        <StreamGitCompatStore<P> as crate::tools::git::CommandStore>::lineage_error,
                    )?;
                WorkspaceResolver::resolve(&(self.host.filesystem.clone(), graph), selected)
                    .await
                    .map_err(
                        <StreamGitCompatStore<P> as crate::tools::git::CommandStore>::lineage_error,
                    )
            })
            .await?
        };
        let root_cut = if workspace.id() != root.id() {
            Some(
                read_before_effect(&mut cancellation, async {
                    root.capture_policy_cut(
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| Error::from(failure.error))
                })
                .await?,
            )
        } else {
            None
        };
        drop(cancellation);
        let output = crate::tools::git::run_volume_with_guard(
            crate::tools::git::Workspace {
                access: &access,
                filesystem: &self.host.filesystem,
                store: &self.store,
                view: crate::tools::git::View::Volume(&workspace),
                root: std::path::Path::new("/"),
                repository: root.id(),
                lease: None,
                #[cfg(not(target_arch = "wasm32"))]
                checkout: None,
                reserved: &[],
                author: "agent",
                authored_at_seconds: i64::try_from(
                    acyclic_stream::UnixMillisClock::now_unix_millis(
                        &acyclic_stream::SystemUnixMillisClock,
                    ) / 1000,
                )
                .map_err(|_| Error::Invalid("filesystem author time overflow".into()))?,
                request: Some(request),
            },
            root_cut,
            &parsed.arguments,
            || async {
                read_before_effect(
                    &mut context.model_cancellation(None)?,
                    access.require(&root),
                )
                .await
            },
        )
        .await?;
        let command = output.command.ok_or_else(|| {
            Error::Conflict("filesystem recovered a pending command; retry this command".into())
        })?;
        Ok(Some(ToolResult {
            value: serde_json::json!({"result": command}),
        }))
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>
    crate::runtime::FilesystemExecutor for FilesystemTools<P, O>
{
    fn binding_identity(&self) -> [u8; 32] {
        self.binding_identity
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> ToolExecutor
    for FilesystemTools<P, O>
{
    fn authorize_replay<'a>(
        &'a self,
        context: Option<&'a ToolContext>,
        invocation: &'a ToolInvocation,
        _work: crate::NativeWork<'a>,
    ) -> Option<BoxProviderFuture<'a, Result<()>>> {
        Some(Box::pin(async move {
            let context = context.ok_or_else(|| {
                Error::Unauthorized("filesystem replay has no task context".into())
            })?;
            let command = invocation
                .arguments
                .get("command")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| Error::Invalid("fs requires a command string".into()))?;
            let parsed = match crate::tools::parse_fs_command(command) {
                Ok(parsed) => parsed,
                // Rejected syntax never selected a source or read its data.
                Err(Error::Invalid(_)) => return Ok(()),
                Err(error) => return Err(error),
            };
            let volume = self.target(&parsed, context)?;
            let reference =
                super::workspace_ref(self.host.provider.clone(), &volume.storage_name(&mut None)?)?;
            let mut cancellation = context.model_cancellation(None)?;
            let root =
                read_before_effect(&mut cancellation, self.host.open(&reference, None)).await?;
            let access = WorkspaceAccess::new(
                &self.verifier,
                &self.scope,
                context.effective_scope().grants(),
                Some(volume),
            )?;
            read_before_effect(&mut cancellation, access.require(&root)).await?;
            read_before_effect(&mut cancellation, async {
                (crate::tools::git::RepositoryAccess {
                    access: &access,
                    filesystem: &self.host.filesystem,
                    store: &self.store,
                    current: &root,
                    repository: root.id(),
                })
                .require_saved_result_access()
                .await
            })
            .await
        }))
    }

    fn authorize(
        &self,
        _: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        if work.is_some() {
            return Err(Error::Unsupported(
                "filesystem command requires its ordinary provider context".into(),
            ));
        }
        let context = context
            .ok_or_else(|| Error::Unauthorized("filesystem command has no task context".into()))?;
        if invocation.name != "fs"
            || invocation.operation_id != context.operation_id()
            || invocation.call_id != context.call_id()
            || !context
                .effective_scope()
                .grants()
                .contains(&crate::contract::capability::tool_call("fs"))
        {
            return Err(Error::Unauthorized(
                "filesystem command identity or grant changed".into(),
            ));
        }
        self.verifier.verify(&self.scope, &mut None)
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let context = context.ok_or_else(|| {
                Error::Unauthorized("filesystem command has no task context".into())
            })?;
            match self.run(invocation, context, false).await {
                Ok(Some(result)) => Ok(result),
                Ok(None) => Err(Error::Conflict("filesystem command has no outcome".into())),
                Err(error @ Error::Invalid(_)) => Ok(crate::tools::rejected_input(error)),
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let context = context.ok_or_else(|| {
                Error::Unauthorized("filesystem command has no task context".into())
            })?;
            self.run(invocation, context, true).await
        })
    }
}
