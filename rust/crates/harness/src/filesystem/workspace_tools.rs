//! Harness-owned project workspace tools for local coding swarms.
//!
//! These adapters deliberately sit below the local swarm facade.  They use the
//! authenticated runtime task context for authority and the typed
//! [`FilesystemHost`] for every provider operation; no host filesystem API is
//! exposed to model code.

use super::{is_host_owned_internal_path, workspace_ref, FilesystemHost, WorkspaceMutation};
use crate::conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::runtime::{RuntimeScope, ToolContext};
use crate::tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult};
use crate::{Error, IdempotencyKey, Result, TaskId};
use acyclic_fs::kernel::FileKind;
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

const WORKSPACE_EDIT: &str = "acyclic.edit";
const WORKSPACE_READ: &str = "acyclic.read";
const WORKSPACE_SEARCH: &str = "acyclic.search";
const WORKSPACE_REVISION: &str = "1";

/// Owner-selected project binding shared by every local task harness.
#[derive(Clone)]
pub(crate) struct WorkspaceToolsBinding {
    pub(crate) host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    pub(crate) root_project: VolumeRef,
    pub(crate) root_task: TaskId,
    pub(crate) limits: Limits,
}

impl WorkspaceToolsBinding {
    pub(crate) fn tools_for(&self, task: TaskId) -> Result<Vec<Tool>> {
        let project = self.project_for(task)?;
        let host = self.host.clone();
        Ok(vec![
            edit_tool(host.clone(), project.clone(), task, self.limits),
            read_tool(host.clone(), project.clone(), task, self.limits),
            search_tool(host, project, task, self.limits),
        ])
    }

    fn project_for(&self, task: TaskId) -> Result<VolumeRef> {
        if task == self.root_task {
            return Ok(self.root_project.clone());
        }
        child_project_volume(&self.root_project, task)
    }
}

/// Canonical local child-project identity shared by fork allocation and tool routing.
pub(crate) fn child_project_volume(root_project: &VolumeRef, task: TaskId) -> Result<VolumeRef> {
    VolumeRef::new(
        root_project.provider().clone(),
        format!("local-project-{task}"),
        VolumeClass::Project,
        match root_project.owner() {
            VolumeOwner::Project(owner) => VolumeOwner::Project(owner.clone()),
            _ => {
                return Err(Error::Invalid(
                    "root project has a non-project owner".into(),
                ));
            }
        },
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditInput {
    path: String,
    content: String,
    #[serde(default)]
    expected_generation: Option<crate::resources::GenerationRef>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    path: String,
    #[serde(default)]
    expected_generation: Option<crate::resources::GenerationRef>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchInput {
    query: String,
    #[serde(default = "default_search_path")]
    path: String,
    #[serde(default)]
    case_sensitive: bool,
    #[serde(default = "default_max_matches")]
    max_matches: u32,
}

fn default_search_path() -> String {
    "/".into()
}

fn default_max_matches() -> u32 {
    32
}

fn parse<T: for<'a> Deserialize<'a>>(invocation: &ToolInvocation) -> Result<T> {
    serde_json::from_value(invocation.arguments.clone())
        .map_err(|error| Error::Invalid(format!("invalid workspace tool arguments: {error}")))
}

fn validate_path(path: &str, allow_root: bool) -> Result<()> {
    if !path.starts_with('/') || path.contains('\0') || path.contains("//") {
        return Err(Error::Invalid(
            "workspace path must be canonical and absolute".into(),
        ));
    }
    if !allow_root && path == "/" {
        return Err(Error::Invalid(
            "workspace file path cannot be the root".into(),
        ));
    }
    if path.split('/').any(|part| part == "." || part == "..") {
        return Err(Error::Invalid(
            "workspace path cannot contain dot segments".into(),
        ));
    }
    if is_host_owned_internal_path(path.trim_start_matches('/')) {
        return Err(Error::Unauthorized(
            "workspace path is Harness-owned".into(),
        ));
    }
    Ok(())
}

fn operation_key(invocation: &ToolInvocation) -> Result<IdempotencyKey> {
    IdempotencyKey::new(format!(
        "harness-workspace-{}",
        hex::encode(invocation.operation_id.into_bytes())
    ))
}

fn project_workspace(project: &VolumeRef) -> Result<crate::resources::WorkspaceRef> {
    workspace_ref(project.provider().clone(), &project.storage_name()?)
}

fn require_project(
    scope: &RuntimeScope,
    project: &VolumeRef,
    operation: VolumeOperation,
) -> Result<()> {
    let capability = project.capability(operation)?;
    if !scope.grants().contains(&capability) {
        return Err(Error::Unauthorized(
            "task is not granted this project operation".into(),
        ));
    }
    Ok(())
}

fn require_runtime_task(context: &ToolContext, expected: TaskId, operation: &str) -> Result<()> {
    match context.task().durable_task_id() {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(Error::Unauthorized(format!(
            "project {operation} task context does not match the bound task"
        ))),
        None => Err(Error::Unauthorized(format!(
            "project {operation} requires an authenticated task context"
        ))),
    }
}

fn require_model_task(
    context: &crate::tool::ModelToolContext,
    invocation: &ToolInvocation,
    expected: TaskId,
    operation: &str,
) -> Result<()> {
    context.validate_invocation(invocation)?;
    match context.task_id {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(Error::Unauthorized(format!(
            "project {operation} model task does not match the bound task"
        ))),
        None => Err(Error::Unauthorized(format!(
            "project {operation} requires authenticated task context"
        ))),
    }
}

struct EditExecutor {
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    project: VolumeRef,
    task: TaskId,
    maximum_bytes: u64,
}

impl EditExecutor {
    async fn execute_workspace(&self, invocation: ToolInvocation) -> Result<ToolResult> {
        let input: EditInput = parse(&invocation)?;
        validate_path(&input.path, false)?;
        let bytes = input.content.into_bytes();
        if bytes.len() as u64 > self.maximum_bytes {
            return Err(Error::Invalid(
                "workspace edit exceeds the file byte limit".into(),
            ));
        }
        let workspace = project_workspace(&self.project)?;
        let expected = input.expected_generation.ok_or_else(|| {
            Error::Invalid("workspace edit requires an expected generation".into())
        })?;
        let generation = self
            .host
            .apply(
                &workspace,
                Some(&expected),
                &[WorkspaceMutation::PutFile {
                    path: input.path.clone(),
                    bytes,
                }],
                &operation_key(&invocation)?,
            )
            .await?;
        Ok(ToolResult {
            value: json!({
                "path": input.path,
                "generation": serde_json::to_value(generation).map_err(|error| Error::Storage(error.to_string()))?,
                "operation_id": invocation.operation_id.to_string(),
            }),
        })
    }
}

impl ToolExecutor for EditExecutor {
    fn authorize(&self, scope: Option<&RuntimeScope>, _invocation: &ToolInvocation) -> Result<()> {
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("project edit requires an authenticated runtime scope".into())
        })?;
        require_project(scope, &self.project, VolumeOperation::Write)?;
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "project edit requires an authenticated task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            require_runtime_task(&context, self.task, "edit")?;
            require_project(context.scope(), &self.project, VolumeOperation::Write)?;
            self.execute_workspace(invocation).await
        })
    }

    fn execute_in_model_batch<'a>(
        &'a self,
        context: crate::tool::ModelToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            require_model_task(&context, &invocation, self.task, "edit")?;
            self.execute_workspace(invocation).await
        })
    }

    fn reconcile_in_model_batch<'a>(
        &'a self,
        context: crate::tool::ModelToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            require_model_task(&context, &invocation, self.task, "edit")?;
            self.reconcile_workspace(invocation).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            require_runtime_task(&context, self.task, "edit")?;
            require_project(context.scope(), &self.project, VolumeOperation::Write)?;
            self.reconcile_workspace(invocation).await
        })
    }
}

impl EditExecutor {
    async fn reconcile_workspace(&self, invocation: ToolInvocation) -> Result<Option<ToolResult>> {
        let input: EditInput = parse(&invocation)?;
        validate_path(&input.path, false)?;
        let expected = input.expected_generation.ok_or_else(|| {
            Error::Invalid("workspace edit requires an expected generation".into())
        })?;
        let workspace = project_workspace(&self.project)?;
        let Some(generation) = self
            .host
            .operation_generation_with_parent(&workspace, &operation_key(&invocation)?, &expected)
            .await?
        else {
            return Ok(None);
        };
        let bytes = self
            .host
            .read(
                &workspace,
                Some(&generation),
                &input.path,
                self.maximum_bytes,
            )
            .await?;
        if bytes.as_ref() != input.content.as_bytes() {
            return Err(Error::Conflict(
                "filesystem receipt content differs from the admitted edit".into(),
            ));
        }
        Ok(Some(ToolResult {
            value: json!({
                "path": input.path,
                "generation": serde_json::to_value(generation)
                    .map_err(|error| Error::Storage(error.to_string()))?,
                "operation_id": invocation.operation_id.to_string(),
            }),
        }))
    }
}

struct ReadExecutor {
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    project: VolumeRef,
    task: TaskId,
    maximum_bytes: u64,
}

impl ReadExecutor {
    async fn execute_workspace(&self, invocation: ToolInvocation) -> Result<ToolResult> {
        let input: ReadInput = parse(&invocation)?;
        validate_path(&input.path, false)?;
        let workspace = project_workspace(&self.project)?;
        let generation = match input.expected_generation {
            Some(generation) => generation,
            None => self.host.resolve(&workspace).await?.generation,
        };
        let bytes = self
            .host
            .read(
                &workspace,
                Some(&generation),
                &input.path,
                self.maximum_bytes,
            )
            .await?;
        let text = String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::Invalid("workspace read is not UTF-8".into()))?;
        Ok(ToolResult {
            value: json!({
                "path": input.path,
                "content": text,
                "generation": serde_json::to_value(generation)
                    .map_err(|error| Error::Storage(error.to_string()))?,
            }),
        })
    }
}

impl ToolExecutor for ReadExecutor {
    fn authorize(&self, scope: Option<&RuntimeScope>, _invocation: &ToolInvocation) -> Result<()> {
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("project read requires an authenticated runtime scope".into())
        })?;
        require_project(scope, &self.project, VolumeOperation::Read)?;
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "project read requires an authenticated task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            require_runtime_task(&context, self.task, "read")?;
            require_project(context.scope(), &self.project, VolumeOperation::Read)?;
            self.execute_workspace(invocation).await
        })
    }

    fn execute_in_model_batch<'a>(
        &'a self,
        context: crate::tool::ModelToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            require_model_task(&context, &invocation, self.task, "read")?;
            self.execute_workspace(invocation).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

struct SearchExecutor {
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    project: VolumeRef,
    task: TaskId,
    maximum_bytes: u64,
    maximum_files: usize,
    maximum_search_bytes: u64,
}

impl SearchExecutor {
    async fn execute_workspace(&self, invocation: ToolInvocation) -> Result<ToolResult> {
        let input: SearchInput = parse(&invocation)?;
        validate_path(&input.path, true)?;
        if input.query.is_empty() || input.max_matches == 0 || input.max_matches > 256 {
            return Err(Error::Invalid(
                "search query and match bound are invalid".into(),
            ));
        }
        let workspace = project_workspace(&self.project)?;
        let generation = self.host.resolve(&workspace).await?.generation;
        let mut directories = vec![input.path.clone()];
        let mut matches = Vec::new();
        let mut files_seen = 0usize;
        let mut bytes_seen = 0u64;
        let needle = if input.case_sensitive {
            input.query.clone()
        } else {
            input.query.to_lowercase()
        };
        while let Some(directory) = directories.pop() {
            let page = self
                .host
                .list(&workspace, Some(&generation), &directory, 64)
                .await?;
            for entry in page.entries {
                let name = String::from_utf8(entry.name.as_bytes().to_vec())
                    .map_err(|_| Error::Invalid("workspace entry is not UTF-8".into()))?;
                let child = if directory == "/" {
                    format!("/{name}")
                } else {
                    format!("{directory}/{name}")
                };
                match entry.kind {
                    FileKind::Directory => directories.push(child),
                    FileKind::Regular => {
                        files_seen = files_seen.saturating_add(1);
                        if files_seen > self.maximum_files {
                            return Err(Error::Invalid(
                                "workspace search file budget exceeded".into(),
                            ));
                        }
                        let bytes = self
                            .host
                            .read(&workspace, Some(&generation), &child, self.maximum_bytes)
                            .await?;
                        bytes_seen = bytes_seen.saturating_add(bytes.len() as u64);
                        if bytes_seen > self.maximum_search_bytes {
                            return Err(Error::Invalid(
                                "workspace search byte budget exceeded".into(),
                            ));
                        }
                        let Ok(text) = String::from_utf8(bytes.to_vec()) else {
                            continue;
                        };
                        let haystack = if input.case_sensitive {
                            text.clone()
                        } else {
                            text.to_lowercase()
                        };
                        if haystack.contains(&needle) {
                            matches.push(json!({"path": child}));
                            if matches.len() >= input.max_matches as usize {
                                return Ok(ToolResult {
                                    value: json!({"generation": serde_json::to_value(generation).map_err(|error| Error::Storage(error.to_string()))?, "matches": matches, "bounded": true}),
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
            if page.has_more {
                return Err(Error::Invalid(
                    "search directory exceeds one bounded page".into(),
                ));
            }
        }
        Ok(ToolResult {
            value: json!({"generation": serde_json::to_value(generation).map_err(|error| Error::Storage(error.to_string()))?, "matches": matches, "bounded": false}),
        })
    }
}

impl ToolExecutor for SearchExecutor {
    fn authorize(&self, scope: Option<&RuntimeScope>, _invocation: &ToolInvocation) -> Result<()> {
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("project search requires an authenticated runtime scope".into())
        })?;
        require_project(scope, &self.project, VolumeOperation::Read)?;
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unauthorized(
                "project search requires an authenticated task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            require_runtime_task(&context, self.task, "search")?;
            require_project(context.scope(), &self.project, VolumeOperation::Read)?;
            self.execute_workspace(invocation).await
        })
    }

    fn execute_in_model_batch<'a>(
        &'a self,
        context: crate::tool::ModelToolContext,
        invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            require_model_task(&context, &invocation, self.task, "search")?;
            self.execute_workspace(invocation).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: ToolInvocation,
    ) -> futures::future::BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

struct IdentityProjection;
impl ToolProjection for IdentityProjection {
    fn project(&self, _invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn edit_tool(
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    project: VolumeRef,
    task: TaskId,
    limits: Limits,
) -> Tool {
    let implementation = Arc::new(EditExecutor {
        host,
        project,
        task,
        maximum_bytes: limits.file_bytes,
    });
    Tool {
        definition: definition(
            WORKSPACE_EDIT,
            "Write one complete UTF-8 file in the authenticated project workspace",
            json!({"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"},"expected_generation":{"type":"object"}},"required":["path","content","expected_generation"],"additionalProperties":false}),
            json!({"type":"object","properties":{"path":{"type":"string"},"generation":{"type":"object"},"operation_id":{"type":"string"}},"required":["path","generation","operation_id"],"additionalProperties":false}),
        ),
        executor: implementation,
        projection: Arc::new(IdentityProjection),
    }
}

fn read_tool(
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    project: VolumeRef,
    task: TaskId,
    limits: Limits,
) -> Tool {
    let implementation = Arc::new(ReadExecutor {
        host,
        project,
        task,
        maximum_bytes: limits.file_bytes,
    });
    Tool {
        definition: definition(
            WORKSPACE_READ,
            "Read one bounded UTF-8 file from the authenticated project workspace",
            json!({"type":"object","properties":{"path":{"type":"string"},"expected_generation":{"type":["object","null"]}},"required":["path"],"additionalProperties":false}),
            json!({"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"},"generation":{"type":"object"}},"required":["path","content","generation"],"additionalProperties":false}),
        ),
        executor: implementation,
        projection: Arc::new(IdentityProjection),
    }
}

fn search_tool(
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    project: VolumeRef,
    task: TaskId,
    limits: Limits,
) -> Tool {
    let implementation = Arc::new(SearchExecutor {
        host,
        project,
        task,
        maximum_bytes: limits.file_bytes,
        maximum_files: 256,
        maximum_search_bytes: limits.file_bytes.saturating_mul(256),
    });
    Tool {
        definition: definition(
            WORKSPACE_SEARCH,
            "Search bounded UTF-8 files in the authenticated project workspace",
            json!({"type":"object","properties":{"query":{"type":"string"},"path":{"type":"string"},"case_sensitive":{"type":"boolean"},"max_matches":{"type":"integer","minimum":1,"maximum":256}},"required":["query"],"additionalProperties":false}),
            json!({"type":"object","properties":{"generation":{"type":"object"},"matches":{"type":"array","items":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}},"bounded":{"type":"boolean"}},"required":["generation","matches","bounded"],"additionalProperties":false}),
        ),
        executor: implementation,
        projection: Arc::new(IdentityProjection),
    }
}

fn definition(
    name: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        revision: WORKSPACE_REVISION.into(),
        description: description.into(),
        input_schema,
        output_schema: output_schema.clone(),
        model_output_schema: output_schema,
    }
}
