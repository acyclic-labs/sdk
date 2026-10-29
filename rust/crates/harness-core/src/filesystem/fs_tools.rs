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

/// Filesystem commands over the remote placement itself, without a machine.
pub struct FilesystemTools<P: ?Sized + 'static, O: 'static> {
    host: Arc<FilesystemHost<StreamAuthorityStore<P>, O>>,
    store: StreamGitCompatStore<P>,
    volumes: BTreeMap<String, VolumeRef>,
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
        Ok(Self {
            host,
            store,
            volumes,
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
        let command = invocation
            .arguments
            .get("command")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("fs requires a command string".into()))?;
        let parsed = crate::tools::parse_fs_command(command)?;
        let volume = self.target(&parsed, &context)?;
        let reference =
            super::workspace_ref(self.host.provider.clone(), &volume.storage_name(&mut None)?)?;
        let root = self.host.open(&reference, None).await?;
        let access = WorkspaceAccess::new(
            &self.verifier,
            &self.scope,
            context.effective_scope().grants(),
            Some(volume),
        )?;
        access.require(&root).await?;
        let request = acyclic_fs::GitRequest {
            operation: acyclic_fs::OperationId::from_bytes(context.operation_id().into_bytes()),
            unanswered: Arc::new(|_| true),
        };
        let repository = acyclic_fs::GitCompatRepository::new(root.id(), self.store.clone())
            .for_request(request.clone());
        if let Some(output) = (crate::tools::git::RepositoryAccess {
            access: &access,
            filesystem: &self.host.filesystem,
            store: &self.store,
            current: &root,
            repository: root.id(),
        })
        .request_result(request.operation)
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
        let (selected, pending) = repository
            .checkout_selection()
            .await
            .map_err(crate::tools::git::compat_error::<StreamGitCompatStore<P>>)?;
        if pending.is_some() {
            return Err(Error::Conflict(
                "filesystem checkout requires its original publication reconciliation".into(),
            ));
        }
        let workspace = if selected == root.id() {
            root.clone()
        } else {
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
                )?
        };
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
            &parsed.arguments,
            || access.require(&root),
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

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> ToolExecutor
    for FilesystemTools<P, O>
{
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
