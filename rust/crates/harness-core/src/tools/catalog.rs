//! One descriptor catalog for model contracts, help, grants and approval hooks.
use crate::{
    Result,
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolProjection,
        files::{
            EditFileInput, FileProjection, FileResult, FileTool, ReadFileInput, ReadFileResult,
            WriteFileInput,
        },
        schema::{self, ProjectionMode},
    },
};
use serde_json::{Value, json};
use std::sync::Arc;

/// The task binding required before registration and a call grant are offered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolBinding {
    /// An authenticated content volume.
    Volume,
    /// An executor bound to the task's composed workspace.
    Workspace,
    /// Commands on stream-backed generations; no physical checkout is required.
    Filesystem,
}

/// Immutable builtin data. Bindings allocate implementations only when selected.
/// Default bindings require an admitted approval policy; hooks also apply.
pub struct ToolSpec {
    /// Stable identifier used by model calls, grants and policy routing.
    pub name: &'static str,
    /// Pinned definition, implementation and projection revision.
    pub revision: &'static str,
    /// Shared model and help description.
    pub description: &'static str,
    /// Additional names used to route lifecycle and approval hooks.
    pub hook_names: &'static [&'static str],
    /// Required task resource.
    pub binding: ToolBinding,
    input: fn() -> Result<Value>,
    output: fn() -> Result<Value>,
    projection_schema: fn(ProjectionMode) -> Result<Value>,
    executor: fn(&'static str) -> Arc<dyn ToolExecutor>,
    projection: fn(ProjectionMode) -> Arc<dyn ToolProjection>,
}

impl ToolSpec {
    /// The exact call grant used by registration and authorization.
    pub fn capability(&self) -> String {
        crate::contract::capability::tool_call(self.name)
    }

    /// Generates the model-visible schemas without constructing an executor.
    pub fn definition(&self) -> Result<ToolDefinition> {
        self.definition_with_projection(ProjectionMode::Full)
    }

    fn definition_with_projection(&self, mode: ProjectionMode) -> Result<ToolDefinition> {
        let definition = ToolDefinition {
            name: self.name.into(),
            revision: self.revision.into(),
            description: self.description.into(),
            input_schema: (self.input)()?,
            output_schema: (self.output)()?,
            projection_schema: (self.projection_schema)(mode)?,
        };
        definition.validate(&mut None)?;
        Ok(definition)
    }

    /// Binds the implementation once, using the catalog's full projection.
    pub fn bind(&self) -> Result<Tool> {
        self.bind_with_projection(ProjectionMode::Full)
    }

    pub(crate) fn bind_with_projection(&self, mode: ProjectionMode) -> Result<Tool> {
        Ok(Tool {
            definition: self.definition_with_projection(mode)?,
            hook_names: self.hook_names,
            executor: (self.executor)(self.name),
            projection: (self.projection)(mode),
        })
    }
}

/// The stock coding catalog, in model registration order. No mutable global state.
pub fn catalog() -> impl ExactSizeIterator<Item = &'static ToolSpec> {
    #[cfg(not(target_arch = "wasm32"))]
    let specs = [&SHELL, &FS, &READ, &WRITE, &EDIT, &CODE];
    #[cfg(target_arch = "wasm32")]
    let specs = [&SHELL, &FS, &READ, &WRITE, &EDIT];
    specs.into_iter()
}

#[cfg(not(target_arch = "wasm32"))]
static CODE: ToolSpec = ToolSpec {
    name: "code",
    revision: "1",
    description: "Run JavaScript for multi-step reads, filters, loops and batches of tools. Use await tools[name](arguments) or Promise.all for independent calls, with each tool's existing JSON input schema; functions expose input_schema. tools.changed notices carry ordered JSON tool-table fragments: concatenate body.json by body.offset until body.complete. The table is refreshed for each program. Only the final expression is returned; call logs and console.log(...values) stream to tool-output files. Await console.log for publication before continuing; unawaited logs retire before the program returns. Values use JSON encoding. Intermediate results stay in JavaScript. No host, filesystem, network or environment access exists except granted tools. Inner calls retain normal approvals and receipts; failures throw Error('tool execution failed: <reason>').",
    hook_names: &[],
    binding: ToolBinding::Volume,
    input: || {
        Ok(
            json!({"type":"object", "properties":{"code":{"type":"string"}}, "required":["code"], "additionalProperties":false}),
        )
    },
    output: object,
    projection_schema: |_| Ok(crate::tool::json_projection_schema(json!({}))),
    executor: |_| Arc::new(super::code_mode::CodeMode),
    projection: |_| Arc::new(super::code_mode::CodeMode),
};

static SHELL: ToolSpec = ToolSpec {
    name: super::shell::PLATFORM.name,
    revision: "3",
    description: super::shell::DESCRIPTION,
    hook_names: &[],
    binding: ToolBinding::Workspace,
    input: schema::input::<super::ShellInput>,
    output: object,
    projection_schema: |_| schema::text_projection_schema(),
    executor: super::workspace_executor::executor,
    projection: |_| Arc::new(super::shell::ShellProjection),
};

static FS: ToolSpec = ToolSpec {
    name: "fs",
    revision: "2",
    description: "Inspect or merge a child's fork: diff agents/<name> or merge agents/<name>. --volume <name> selects a mounted volume. Use shell for other git commands",
    hook_names: &[],
    binding: ToolBinding::Filesystem,
    input: || {
        Ok(json!({
            "type":"object", "required":["command"],
            "properties":{"command":{"type":"string","minLength":1}},
            "additionalProperties":false
        }))
    },
    output: object,
    projection_schema: command_projection,
    executor: super::workspace_executor::executor,
    projection: |_| Arc::new(JsonToolProjection),
};

pub(crate) static READ: ToolSpec = ToolSpec {
    name: "read",
    revision: "portable-10",
    description: "Read text, images, PDFs or notebook cells from a project path. Reuse the opaque version to pin later reads/edits; select byte ranges or zero-based line/cell offset and limit. A page returns its source interval and continuation",
    hook_names: &["Read"],
    binding: ToolBinding::Volume,
    input: schema::input::<ReadFileInput>,
    output: schema::output::<ReadFileResult>,
    projection_schema: FileProjection::read_schema,
    executor: |_| Arc::new(FileTool::Read),
    projection: |mode| Arc::new(FileProjection::Read(mode)),
};

pub(crate) static WRITE: ToolSpec = ToolSpec {
    name: "write",
    revision: "portable-6",
    description: "Write UTF-8 content to a workspace path, creating or replacing the file",
    hook_names: &["Write"],
    binding: ToolBinding::Volume,
    input: schema::input::<WriteFileInput>,
    output: schema::output::<FileResult>,
    projection_schema: FileProjection::model_schema,
    executor: |_| Arc::new(FileTool::Write),
    projection: |mode| Arc::new(FileProjection::Publication(mode)),
};

pub(crate) static EDIT: ToolSpec = ToolSpec {
    name: "edit",
    revision: "portable-9",
    description: "Replace one exact UTF-8 match, all non-overlapping matches when replace_all is true, or edit a notebook cell. Reuse a read's opaque version to reject edits against a changed source",
    hook_names: &["Edit"],
    binding: ToolBinding::Volume,
    input: schema::input::<EditFileInput>,
    output: schema::output::<FileResult>,
    projection_schema: FileProjection::model_schema,
    executor: |_| Arc::new(FileTool::Edit),
    projection: |mode| Arc::new(FileProjection::Publication(mode)),
};

fn object() -> Result<Value> {
    Ok(json!({"type":"object"}))
}

fn command_projection(_: ProjectionMode) -> Result<Value> {
    Ok(crate::tool::json_projection_schema(object()?))
}
