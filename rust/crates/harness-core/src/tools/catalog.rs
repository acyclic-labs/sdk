//! One descriptor catalog for model contracts, help, grants and approval hooks.
use crate::{
    Result,
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolProjection,
        files::{
            EditFileInput, FileProjection, FileResult, FileTool, ReadFileInput, WriteFileInput,
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
}

/// Immutable builtin data. Bindings allocate implementations only when selected.
/// Defaults require no approval; configured policies and hooks still apply.
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
    [
        &SHELL, &FS, &READ, &WRITE, &EDIT, &SPAWN, &SEND, &WAIT, &ARCHIVE,
    ]
    .into_iter()
}

static SHELL: ToolSpec = ToolSpec {
    name: super::shell::PLATFORM.name,
    revision: "1",
    description: super::shell::PLATFORM.description,
    hook_names: super::shell::PLATFORM.hook_names,
    binding: ToolBinding::Workspace,
    input: schema::input::<super::ShellInput>,
    output: object,
    projection_schema: command_projection,
    executor: super::workspace_executor::executor,
    projection: |_| Arc::new(JsonToolProjection),
};

static FS: ToolSpec = ToolSpec {
    name: "fs",
    revision: "1",
    description: "Run one git facade command, including diff and merge of agents/<ref>; --volume <name> selects a granted mount for diff, log, show, status or merge",
    hook_names: &[],
    binding: ToolBinding::Workspace,
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
    revision: "portable-6",
    description: "Read exact UTF-8 from an authorized immutable file, optionally selecting [start, end) byte offsets",
    hook_names: &["Read"],
    binding: ToolBinding::Volume,
    input: schema::input::<ReadFileInput>,
    output: schema::output::<String>,
    projection_schema: ProjectionMode::schema::<String>,
    executor: |_| Arc::new(FileTool::Read),
    projection: |mode| Arc::new(FileProjection::Read(mode)),
};

pub(crate) static WRITE: ToolSpec = ToolSpec {
    name: "write",
    revision: "portable-5",
    description: "Write bounded UTF-8 through the original owner-bound content publisher",
    hook_names: &["Write"],
    binding: ToolBinding::Volume,
    input: schema::input::<WriteFileInput>,
    output: schema::output::<FileResult>,
    projection_schema: ProjectionMode::schema::<FileResult>,
    executor: |_| Arc::new(FileTool::Write),
    projection: |mode| Arc::new(FileProjection::Publication(mode)),
};

pub(crate) static EDIT: ToolSpec = ToolSpec {
    name: "edit",
    revision: "portable-5",
    description: "Replace one exact UTF-8 match at an authorized pinned source generation",
    hook_names: &["Edit"],
    binding: ToolBinding::Volume,
    input: schema::input::<EditFileInput>,
    output: schema::output::<FileResult>,
    projection_schema: ProjectionMode::schema::<FileResult>,
    executor: |_| Arc::new(FileTool::Edit),
    projection: |mode| Arc::new(FileProjection::Publication(mode)),
};

fn object() -> Result<Value> {
    Ok(json!({"type":"object"}))
}

fn command_projection(_: ProjectionMode) -> Result<Value> {
    Ok(crate::tool::json_projection_schema(object()?))
}

static SPAWN: ToolSpec = ToolSpec {
    name: "spawn",
    revision: "1",
    description: "Create a child in a lazy fork and return its reference",
    hook_names: &[],
    binding: ToolBinding::Workspace,
    input: || {
        Ok(
            json!({"type":"object","required":["task"],"additionalProperties":false,
        "properties":{"task":{"type":"string"},"name":{"type":["string","null"]},
        "grants":{"type":["array","null"],"items":{"type":"string"},
        "description":"Capability subset; keep model:generate and mail:read"}}}),
        )
    },
    output: object,
    projection_schema: command_projection,
    executor: |name| Arc::new(super::children::Executor(name)),
    projection: |_| Arc::new(JsonToolProjection),
};

static SEND: ToolSpec = ToolSpec {
    name: "send",
    revision: "1",
    description: "Queue a message to a child",
    hook_names: &[],
    binding: ToolBinding::Workspace,
    input: || {
        Ok(
            json!({"type":"object","required":["agent","message"],"additionalProperties":false,
        "properties":{"agent":{"type":"string"},"message":{"type":"string"}}}),
        )
    },
    output: object,
    projection_schema: command_projection,
    executor: |name| Arc::new(super::children::Executor(name)),
    projection: |_| Arc::new(JsonToolProjection),
};

static WAIT: ToolSpec = ToolSpec {
    name: "wait",
    revision: "1",
    description: "Wait for all children (mode:any for the first) and return bounded answer or failure references",
    hook_names: &[],
    binding: ToolBinding::Workspace,
    input: || {
        Ok(
            json!({"type":"object","required":["agents"],"additionalProperties":false,
        "properties":{"agents":{"type":"array","items":{"type":"string"}},
        "mode":{"enum":["any","all",null]}}}),
        )
    },
    output: object,
    projection_schema: command_projection,
    executor: |name| Arc::new(super::children::Executor(name)),
    projection: |_| Arc::new(JsonToolProjection),
};

static ARCHIVE: ToolSpec = ToolSpec {
    name: "archive",
    revision: "1",
    description: "Cancel and drain a child and its descendants, then archive its readable history",
    hook_names: &[],
    binding: ToolBinding::Workspace,
    input: || {
        Ok(
            json!({"type":"object","required":["agent"],"additionalProperties":false,
        "properties":{"agent":{"type":"string"}}}),
        )
    },
    output: object,
    projection_schema: command_projection,
    executor: |name| Arc::new(super::children::Executor(name)),
    projection: |_| Arc::new(JsonToolProjection),
};
