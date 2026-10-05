//! Harness-owned model binding for the typed Filesystem Git facade.
//!
//! The compatibility state machine remains in `acyclic-fs` and authority
//! remains in [`super::FilesystemGitFacade`].  This module only adapts that
//! boundary to the Harness tool contract.  A host supplies the current
//! project generation and the typed filesystem action executor; no directory
//! copying, process execution, or merge implementation lives here.

use super::{
    FilesystemGitFacade, FilesystemHost, GIT_FACADE_TOOL_NAME, GIT_FACADE_TOOL_REVISION,
    workspace_ref,
};
use crate::conversation::{VolumeClass, VolumeOperation, VolumeRef};
use crate::core::{AuthorityVerifier, Scope};
use crate::resources::GenerationRef;
use crate::{Error, Result};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Digest, GenerationId, GitCommandOutput, GitCompatStore,
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitTreeRef, IdempotencyKey,
    OperationId as FilesystemOperationId, PublicationPermit, WorkspaceId,
};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

/// Authenticated local project binding used by the default swarm composition.
///
/// It is intentionally backed by the existing Filesystem host. Git-shaped
/// operations therefore address SDK workspace generations and never a host
/// directory or a separate merge implementation.
pub struct LocalProjectWorkspaceTree<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    project: VolumeRef,
    workspace_id: WorkspaceId,
}

impl<A, O> LocalProjectWorkspaceTree<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Binds one project volume to an already-authenticated task scope.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        project: VolumeRef,
        verifier: &AuthorityVerifier,
        scope: Scope,
    ) -> Result<Self> {
        project.validate()?;
        if project.class() != VolumeClass::Project || project.provider() != host.provider() {
            return Err(Error::Invalid(
                "local Git project belongs to another provider or volume class".into(),
            ));
        }
        verifier.verify(&scope)?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !scope
                .capabilities()
                .contains(&project.capability(operation)?)
            {
                let label = match operation {
                    VolumeOperation::Read => "read",
                    VolumeOperation::Write => "write",
                };
                return Err(Error::Unauthorized(format!(
                    "local Git project requires {label} capability"
                )));
            }
        }
        let workspace_id = host
            .workspace_id(project.storage_name()?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Self {
            host,
            project,
            workspace_id,
        })
    }

    /// Returns the bound project identity.
    #[must_use]
    pub const fn project(&self) -> &VolumeRef {
        &self.project
    }

    async fn workspace(&self) -> Result<acyclic_fs::Workspace<A, O>> {
        self.host
            .open_workspace(&workspace_ref(
                self.host.provider().clone(),
                &self.project.storage_name()?,
            )?)
            .await
    }

    async fn generation(&self, tree: GitTreeRef) -> Result<acyclic_fs::Generation<A, O>> {
        if tree.workspace_id() != self.workspace_id {
            return Err(Error::Unauthorized(
                "Git action references a foreign project workspace".into(),
            ));
        }
        let generation = GenerationRef::new(
            self.host.provider().clone(),
            tree.authored_generation().digest().into_bytes(),
            None,
        )?;
        let workspace = self.workspace().await?;
        self.host.open_generation(&workspace, &generation).await
    }

    async fn resulting_tree(
        &self,
        result: acyclic_fs::TransactionCommit<A, O>,
    ) -> Result<GitFilesystemResult> {
        match result {
            acyclic_fs::TransactionCommit::Committed(generation)
            | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => {
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.workspace_id, generation.id())),
                    tracked_paths: None,
                })
            }
            acyclic_fs::TransactionCommit::Conflict { .. } => {
                Err(Error::Conflict("Git workspace generation changed".into()))
            }
            acyclic_fs::TransactionCommit::Fenced => {
                Err(Error::Conflict("Git workspace writer was fenced".into()))
            }
            acyclic_fs::TransactionCommit::IdempotencyConflict => Err(Error::Conflict(
                "Git operation identity was reused with different input".into(),
            )),
        }
    }
}

impl<A, O> ProjectWorkspaceTree for LocalProjectWorkspaceTree<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    fn current_tree<'a>(&'a self) -> BoxFuture<'a, Result<GitTreeRef>> {
        Box::pin(async move {
            let observation = self
                .host
                .resolve(&workspace_ref(
                    self.host.provider().clone(),
                    &self.project.storage_name()?,
                )?)
                .await?;
            let bytes: [u8; 32] = observation
                .generation
                .as_resource()
                .key()
                .try_into()
                .map_err(|_| Error::Invalid("Filesystem generation identity is invalid".into()))?;
            Ok(GitTreeRef::exact(
                self.workspace_id,
                GenerationId::new(Digest::from_bytes(bytes)),
            ))
        })
    }

    fn validate_workspace_tree<'a>(
        &'a self,
        workspace_tree: GitTreeRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.generation(workspace_tree).await.map(|_| ()) })
    }

    fn execute<'a>(
        &'a self,
        operation_id: FilesystemOperationId,
        action: &'a GitFilesystemAction,
    ) -> BoxFuture<'a, Result<GitFilesystemResult>> {
        Box::pin(async move {
            match action {
                GitFilesystemAction::Diff {
                    from,
                    to,
                    tracked_paths,
                } => {
                    let target = self.generation(*to).await?;
                    let before = match from {
                        Some(tree) => Some(self.generation(*tree).await?),
                        None => None,
                    };
                    let counts = match before {
                        Some(before) => acyclic_fs::git_compatible_diff_counts(
                            &before,
                            &target,
                            tracked_paths,
                            100_000,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .await
                        .map_err(|error| Error::Storage(error.to_string()))?,
                        None => acyclic_fs::GitDiffCounts {
                            file_changes: 0,
                            binding_changes: 0,
                        },
                    };
                    Ok(GitFilesystemResult::Data {
                        kind: "diff".into(),
                        value: serde_json::to_value(counts)
                            .map_err(|error| Error::Storage(error.to_string()))?,
                    })
                }
                GitFilesystemAction::Grep {
                    pattern,
                    path,
                    tree,
                } => {
                    let generation = self.generation(*tree).await?;
                    let result = acyclic_fs::grep_git_generation(
                        &generation,
                        pattern,
                        path.as_deref(),
                        10_000,
                        4 * 1024 * 1024,
                        10_000,
                    )
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                    Ok(GitFilesystemResult::Data {
                        kind: "grep".into(),
                        value: serde_json::to_value(result)
                            .map_err(|error| Error::Storage(error.to_string()))?,
                    })
                }
                GitFilesystemAction::Archive { tree } => {
                    let generation = self.generation(*tree).await?;
                    let entries = acyclic_fs::walk_git_tree(&generation, None, 100_000)
                        .await
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    Ok(GitFilesystemResult::Data {
                        kind: "archive".into(),
                        value: serde_json::to_value(entries)
                            .map_err(|error| Error::Storage(error.to_string()))?,
                    })
                }
                GitFilesystemAction::ApplyPatch {
                    patch,
                    expected_workspace_tree,
                } => {
                    let workspace = self.workspace().await?;
                    let current = workspace
                        .head()
                        .await
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    if expected_workspace_tree.is_some_and(|tree| {
                        tree != GitTreeRef::exact(self.workspace_id, current.id())
                    }) {
                        return Err(Error::Conflict(
                            "Git patch workspace generation changed".into(),
                        ));
                    }
                    let committed = acyclic_fs::apply_git_patch_with_permit_if_current(
                        &workspace,
                        &current,
                        patch,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        PublicationPermit::Unrestricted,
                    )
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                    self.resulting_tree(committed).await
                }
                GitFilesystemAction::CaptureCommit {
                    workspace_tree,
                    tracked_paths,
                    ..
                } => {
                    let workspace = self.workspace().await?;
                    let capture = acyclic_fs::capture_git_compatible_generation_at(
                        &workspace,
                        workspace_tree.authored_generation(),
                        &acyclic_fs::GitIgnorePolicy::default(),
                        tracked_paths,
                        operation_id,
                    )
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                    if capture.generation.workspace_id() != self.workspace_id {
                        return Err(Error::Unsupported(
                            "local Git capture with ignored paths requires a lineage store binding"
                                .into(),
                        ));
                    }
                    Ok(GitFilesystemResult::Captured {
                        tree: GitTreeRef::exact(self.workspace_id, capture.generation.id()),
                        tracked_paths: capture.tracked_paths,
                        proof: None,
                    })
                }
                _ => Err(Error::Unsupported(
                    "this local Git action is not wired to the Filesystem host yet".into(),
                )),
            }
        })
    }
}

/// Provider-owned project workspace boundary used by the model Git tool.
///
/// Implementations must read and mutate only the authenticated project bound
/// by the surrounding Harness scope.  Generation checks and operation
/// idempotency are deliberately kept in the provider adapter, while the Git
/// compatibility state remains owned by `GitCompatRepository`.
pub trait ProjectWorkspaceTree: Send + Sync {
    /// Stable workspace identity accepted by the bound Git facade.
    fn workspace_id(&self) -> WorkspaceId;

    /// Reads the current exact workspace generation without resolving model
    /// content or consulting a mutable path.
    fn current_tree<'a>(&'a self) -> BoxFuture<'a, Result<GitTreeRef>>;

    /// Revalidates that a tree is still controlled by this project binding.
    fn validate_workspace_tree<'a>(
        &'a self,
        workspace_tree: GitTreeRef,
    ) -> BoxFuture<'a, Result<()>>;

    /// Applies one typed provider action under its stable Filesystem identity.
    fn execute<'a>(
        &'a self,
        operation_id: FilesystemOperationId,
        action: &'a GitFilesystemAction,
    ) -> BoxFuture<'a, Result<GitFilesystemResult>>;
}

struct ProjectWorkspaceExecutor {
    workspace: Arc<dyn ProjectWorkspaceTree>,
}

impl GitFilesystemExecutor for ProjectWorkspaceExecutor {
    type Error = Error;

    async fn validate_workspace_tree(&self, workspace_tree: GitTreeRef) -> Result<()> {
        self.workspace.validate_workspace_tree(workspace_tree).await
    }

    async fn execute(
        &self,
        operation_id: FilesystemOperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult> {
        self.workspace.execute(operation_id, action).await
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GitToolArguments {
    argv: Vec<String>,
}

/// Harness tool adapter for one authenticated project Git facade.
pub struct FilesystemGitTool<S>
where
    S: GitCompatStore + Send + Sync + 'static,
{
    facade: Arc<FilesystemGitFacade<S>>,
    workspace: Arc<dyn ProjectWorkspaceTree>,
    author: String,
    now_seconds: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl<S> FilesystemGitTool<S>
where
    S: GitCompatStore + Send + Sync + 'static,
{
    /// Binds one already-authorized facade to the provider workspace adapter.
    pub fn new(
        facade: Arc<FilesystemGitFacade<S>>,
        workspace: Arc<dyn ProjectWorkspaceTree>,
        author: impl Into<String>,
        now_seconds: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Result<Self> {
        if workspace.workspace_id() != facade.workspace_id() {
            return Err(Error::Conflict(
                "Git tool workspace does not match its facade".into(),
            ));
        }
        let author = author.into();
        if author.trim().is_empty() {
            return Err(Error::Invalid("Git tool author is empty".into()));
        }
        Ok(Self {
            facade,
            workspace,
            author,
            now_seconds: Arc::new(now_seconds),
        })
    }

    /// Returns the canonical model-facing tool definition and bindings.
    pub fn into_tool(self: Arc<Self>) -> crate::tool::Tool {
        crate::tool::Tool {
            definition: git_tool_definition(),
            executor: self.clone(),
            projection: self,
        }
    }

    /// Returns the pinned definition without constructing a provider binding.
    #[must_use]
    pub fn definition() -> crate::tool::ToolDefinition {
        git_tool_definition()
    }
}

impl<S> crate::tool::ToolExecutor for FilesystemGitTool<S>
where
    S: GitCompatStore + Send + Sync + 'static,
{
    fn execute<'a>(
        &'a self,
        invocation: crate::tool::ToolInvocation,
    ) -> BoxFuture<'a, Result<crate::tool::ToolResult>> {
        Box::pin(async move {
            let arguments: GitToolArguments = serde_json::from_value(invocation.arguments)
                .map_err(|error| Error::Invalid(format!("invalid Git tool arguments: {error}")))?;
            validate_argv(&arguments.argv)?;
            let tree = self.workspace.current_tree().await?;
            let executor = ProjectWorkspaceExecutor {
                workspace: self.workspace.clone(),
            };
            let output = self
                .facade
                .run_argv(
                    &arguments.argv,
                    tree,
                    &self.author,
                    (self.now_seconds)(),
                    &executor,
                )
                .await?;
            Ok(crate::tool::ToolResult {
                value: git_tool_output(output)?,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: crate::tool::ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<crate::tool::ToolResult>>> {
        // A pending Git transition is intentionally not guessed from a model
        // call. Hosts must explicitly inspect and resume it through the bound
        // facade, preserving uncertainty instead of replaying an unknown
        // filesystem mutation.
        Box::pin(async { Ok(None) })
    }
}

impl<S> crate::tool::ToolProjection for FilesystemGitTool<S>
where
    S: GitCompatStore + Send + Sync + 'static,
{
    fn project(
        &self,
        _invocation: &crate::tool::ToolInvocation,
        result: &crate::tool::ToolResult,
    ) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn validate_argv(argv: &[String]) -> Result<()> {
    if argv.is_empty() || argv.len() > 32 {
        return Err(Error::Invalid(
            "Git tool argv must contain between 1 and 32 arguments".into(),
        ));
    }
    if argv.iter().any(|argument| argument.len() > 255) {
        return Err(Error::Invalid("Git tool arguments exceed 255 bytes".into()));
    }
    Ok(())
}

fn git_tool_output(output: GitCommandOutput) -> Result<Value> {
    Ok(json!({
        "output": serde_json::to_value(output)
            .map_err(|error| Error::Storage(format!("Git output encoding failed: {error}")))?
    }))
}

fn git_tool_definition() -> crate::tool::ToolDefinition {
    crate::tool::ToolDefinition {
        name: GIT_FACADE_TOOL_NAME.into(),
        revision: GIT_FACADE_TOOL_REVISION.into(),
        description:
            "Run one typed acyclic Git command in the caller's authorized project workspace.".into(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["argv"],
            "properties": {
                "argv": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 32,
                    "items": {"type": "string", "maxLength": 255}
                }
            }
        }),
        output_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["output"],
            "properties": {"output": {}}
        }),
        model_output_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["output"],
            "properties": {"output": {}}
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_tool_definition_is_closed_and_versioned() -> Result<()> {
        let definition = FilesystemGitTool::<acyclic_fs::MemoryGitCompatStore>::definition();
        definition.validate()?;
        assert_eq!(definition.name, GIT_FACADE_TOOL_NAME);
        assert_eq!(definition.revision, GIT_FACADE_TOOL_REVISION);
        assert_eq!(
            definition.input_schema["additionalProperties"],
            Value::Bool(false)
        );
        assert_eq!(
            definition.output_schema["additionalProperties"],
            Value::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn git_tool_rejects_unbounded_argv() {
        assert!(validate_argv(&[]).is_err());
        assert!(validate_argv(&vec!["status".into(); 33]).is_err());
        assert!(validate_argv(&["x".repeat(256)]).is_err());
        assert!(validate_argv(&["status".into()]).is_ok());
    }

    #[test]
    fn git_tool_output_is_model_visible_object() -> Result<()> {
        let output = git_tool_output(GitCommandOutput::NoOp)?;
        assert!(output.get("output").is_some());
        Ok(())
    }
}
