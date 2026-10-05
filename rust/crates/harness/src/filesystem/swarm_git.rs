//! Harness-owned model binding for the typed Filesystem Git facade.
//!
//! The compatibility state machine remains in `acyclic-fs` and authority
//! remains in [`super::FilesystemGitFacade`].  This module only adapts that
//! boundary to the Harness tool contract.  A host supplies the current
//! project generation and the typed filesystem action executor; no directory
//! copying, process execution, or merge implementation lives here.

use super::{FilesystemGitFacade, GIT_FACADE_TOOL_NAME, GIT_FACADE_TOOL_REVISION};
use crate::{Error, Result};
use acyclic_fs::{
    GitCommandOutput, GitCompatStore, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitTreeRef, OperationId as FilesystemOperationId, WorkspaceId,
};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

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
