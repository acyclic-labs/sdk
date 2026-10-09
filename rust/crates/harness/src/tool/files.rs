//! Individually assembled portable file tools using owner-bound task providers.

use super::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult};
use crate::{
    Error, Result,
    conversation::FileRef,
    runtime::{RuntimeScope, ToolContext},
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// Reads exact UTF-8 from a pinned ref using the task's authenticated reader.
pub fn read_file() -> Result<Tool> {
    assemble(FileTool::Read)
}

/// Publishes UTF-8 through the task's original owner-bound writer.
pub fn write_file() -> Result<Tool> {
    assemble(FileTool::Write)
}

/// Replaces one exact, unambiguous match at the source's pinned generation.
pub fn edit_file() -> Result<Tool> {
    assemble(FileTool::Edit)
}

/// Explicitly selects exact V4A update-file editing with pinned finite work limits.
///
/// The original task's file-byte ceiling bounds input and output. The limits
/// below are part of the definition schema digest, so different configurations
/// cannot silently reuse an admitted definition.
pub fn patch_file(maximum_work: u64, maximum_hunks: u32) -> Result<Tool> {
    super::patch::PatchLimits {
        maximum_bytes: 1,
        maximum_work,
        maximum_hunks,
    }
    .validate()?;
    assemble(FileTool::Patch {
        maximum_work,
        maximum_hunks,
    })
}

#[derive(Clone, Copy)]
enum FileTool {
    Read,
    Write,
    Edit,
    Patch {
        maximum_work: u64,
        maximum_hunks: u32,
    },
}

fn assemble(adapter: FileTool) -> Result<Tool> {
    let (name, description, mut input_schema, output_schema, projection_schema) = match adapter {
        FileTool::Read => (
            "acyclic.read_file",
            "Read exact bounded UTF-8 from an authorized immutable file",
            super::schema::input::<ReadFileInput>()?,
            super::schema::output::<String>()?,
            super::schema::json_projection::<String>()?,
        ),
        FileTool::Write => (
            "acyclic.write_file",
            "Write bounded UTF-8 through the original owner-bound content publisher",
            super::schema::input::<WriteFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::json_projection::<FileResult>()?,
        ),
        FileTool::Edit => (
            "acyclic.edit_file",
            "Replace one exact UTF-8 match at an authorized pinned source generation",
            super::schema::input::<EditFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::json_projection::<FileResult>()?,
        ),
        FileTool::Patch { .. } => (
            "acyclic.patch_file",
            "Apply exact V4A update-file diff hunks to one authorized immutable file; @@ headers, exact context and optional EOF; ambiguous context fails",
            super::schema::input::<PatchFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::json_projection::<FileResult>()?,
        ),
    };
    if let FileTool::Patch {
        maximum_work,
        maximum_hunks,
    } = adapter
    {
        input_schema
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("generated patch schema is not an object".into()))?
            .insert(
                "x-harness-patch-limits".into(),
                json!({"maximum_work":maximum_work,"maximum_hunks":maximum_hunks}),
            );
    }
    let definition = ToolDefinition {
        name: name.into(),
        revision: match adapter {
            FileTool::Patch { .. } => "portable-patch-3",
            _ => "portable-4",
        }
        .into(),
        description: description.into(),
        input_schema,
        projection_schema,
        output_schema,
    };
    definition.validate()?;
    let implementation = Arc::new(adapter);
    Ok(Tool {
        definition,
        executor: implementation.clone(),
        projection: implementation,
    })
}

/// Exact immutable UTF-8 file read arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}

/// Single-operation UTF-8 publication arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteFileInput {
    /// Valid public destination path.
    pub path: String,
    /// Exact content bytes encoded as UTF-8.
    pub text: String,
    /// Stored MIME type.
    pub media_type: String,
    /// Human-readable file name.
    pub display_name: String,
}

/// Exact replacement against one pinned file generation.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EditFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// One exact nonempty unambiguous needle.
    #[schemars(length(min = 1))]
    pub old_text: String,
    /// Exact replacement, including an empty deletion.
    pub new_text: String,
}

/// Explicit single-file V4A update arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchFileInput {
    /// Owner-authorized immutable source generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Nonempty exact V4A update diff fragment.
    #[schemars(length(min = 1))]
    pub diff: String,
}

/// Canonical successful write/edit result, independent of model projection.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileResult {
    /// Exact retained publication result with workspace and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}

fn decode<T: serde::de::DeserializeOwned>(arguments: Value) -> Result<T> {
    serde_json::from_value(arguments).map_err(|error| Error::Invalid(error.to_string()))
}

fn public_path(path: &str) -> Result<()> {
    crate::conversation::validate_content_path(path)?;
    if crate::conversation::is_internal_path(path) {
        return Err(Error::Unauthorized(
            "file tools cannot access internal storage".into(),
        ));
    }
    Ok(())
}

impl FileTool {
    async fn run(&self, context: ToolContext, invocation: ToolInvocation) -> Result<ToolResult> {
        invocation.validate()?;
        // The admitted registry owns the selected name/definition. Reusing an
        // executor under a consumer's tool name does not grant file authority.
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::Unauthorized(
                "file tool requires its exact admitted call context".into(),
            ));
        }
        let task = context.task();
        self.authorize(Some(task.scope()), &invocation)?;
        let value = match self {
            Self::Read => {
                let input: ReadFileInput = decode(invocation.arguments)?;
                let bytes = task.read_file(&input.file).await?;
                let text = String::from_utf8(bytes)
                    .map_err(|_| Error::Invalid("file read requires UTF-8 content".into()))?;
                Value::String(text)
            }
            Self::Write => {
                let input: WriteFileInput = decode(invocation.arguments)?;
                let file = task
                    .stage_file_once(
                        invocation.operation_id,
                        &input.path,
                        input.text.as_bytes(),
                        &input.media_type,
                        &input.display_name,
                    )
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
            Self::Patch {
                maximum_work,
                maximum_hunks,
            } => {
                let input: PatchFileInput = decode(invocation.arguments)?;
                let limits = super::patch::PatchLimits {
                    maximum_bytes: task.scope().limits().file_bytes,
                    maximum_work: *maximum_work,
                    maximum_hunks: *maximum_hunks,
                };
                limits.validate()?;
                if input.file.descriptor().byte_length() > limits.maximum_bytes
                    || input.diff.len() as u64 > limits.maximum_bytes
                {
                    return Err(Error::Invalid(
                        "patch input exceeds original task file bound".into(),
                    ));
                }
                let bytes = task.read_file(&input.file).await?;
                let source = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("patch editing requires UTF-8 content".into()))?;
                let edited = super::patch::apply_update(source, &input.diff, limits)?;
                let file = task
                    .stage_file_at(invocation.operation_id, &input.file, edited.as_bytes())
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
            Self::Edit => {
                let input: EditFileInput = decode(invocation.arguments)?;
                let maximum = task.scope().limits().file_bytes;
                super::edit::validate_edit_bound(maximum)?;
                let bytes = task.read_file(&input.file).await?;
                let text = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("exact text edit requires UTF-8 content".into()))?;
                let edited =
                    super::edit::exact_replace(text, &input.old_text, &input.new_text, maximum)?;
                let file = task
                    .stage_file_at(invocation.operation_id, &input.file, edited.as_bytes())
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        Ok(ToolResult { value })
    }
}

impl ToolExecutor for FileTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, invocation: &ToolInvocation) -> Result<()> {
        invocation.validate()?;
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("file tool requires original runtime scope".into())
        })?;
        crate::contract::validate_json_byte_bound(
            &invocation.arguments,
            scope.limits().file_bytes,
        )?;
        if let Self::Write = self {
            // The original TaskContext publisher owns the destination volume and
            // checks its exact write grant; the template does not capture a writer.
            let path = invocation
                .arguments
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("file write requires a destination path".into()))?;
            public_path(path)?;
            if path.len() > scope.limits().path_bytes {
                return Err(Error::Invalid(
                    "file path exceeds original task bound".into(),
                ));
            }
        } else {
            let value = invocation.arguments.get("file").ok_or_else(|| {
                Error::Invalid("file tool requires an original source reference".into())
            })?;
            let file: FileRef = decode(value.clone())?;
            public_path(file.path())?;
            scope.limits().validate_file(&file)?;
            if !crate::runtime::read_granted(scope.grants(), &file)? {
                return Err(Error::Unauthorized(
                    "file tool requires original source read grant".into(),
                ));
            }
            if let Self::Read = self {
                let maximum = scope.limits().render_bytes.min(scope.limits().file_bytes);
                super::edit::validate_edit_bound(maximum)?;
                if file.descriptor().byte_length() > maximum {
                    return Err(Error::Invalid("file exceeds text rendering bound".into()));
                }
            }
        }
        Ok(())
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "portable file tool requires owner-bound task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.run(context, invocation))
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "portable file reconciliation requires owner-bound task context".into(),
            ))
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        // Reads always use the original immutable ref; publications resolve the
        // existing operation receipt before attempting a generation-checked CAS.
        Box::pin(async move { self.run(context, invocation).await.map(Some) })
    }
}

/// Replaceable projector for the canonical published file result.
#[derive(Clone, Copy)]
pub struct FileResultProjection(pub super::schema::ProjectionMode);

impl FileResultProjection {
    /// Complete generated model envelope contract for this representation.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::json_projection::<FileResult>(),
            super::schema::ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for FileResultProjection {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let value: FileResult = decode(result.value.clone())?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(value),
            super::schema::ProjectionMode::Reference => super::schema::project_reference(value.file,
                "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.".into()),
        }
    }
}

/// Replaceable projector for the exact reader's unchanged canonical string.
#[derive(Clone, Copy)]
pub struct ReadFileProjection(pub super::schema::ProjectionMode);

impl ReadFileProjection {
    /// Complete generated model envelope contract for this representation.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::json_projection::<String>(),
            super::schema::ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for ReadFileProjection {
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let text: String = decode(result.value.clone())?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(text),
            super::schema::ProjectionMode::Reference => {
                let input: ReadFileInput = decode(invocation.arguments.clone())?;
                input.file.descriptor().verify(text.as_bytes())?;
                super::schema::project_reference(
                    input.file,
                    format!(
                        "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                        text.len()
                    ),
                )
            }
        }
    }
}

impl ToolProjection for FileTool {
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        match self {
            Self::Read => {
                ReadFileProjection(super::schema::ProjectionMode::Full).project(invocation, result)
            }
            Self::Write | Self::Edit | Self::Patch { .. } => {
                FileResultProjection(super::schema::ProjectionMode::Full)
                    .project(invocation, result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, OperationId,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };

    fn file() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("file-preflight", "filesystem", "1")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([61; 16])),
            )?,
            "source.txt",
            "generation-1",
            FileDescriptor::from_bytes(b"exact", "text/plain")?,
            "source.txt",
        )
    }

    #[test]
    fn source_authority_is_checked_by_the_pre_replay_executor_gate() -> Result<()> {
        let file = file()?;
        let missing = RuntimeScope::new(Capabilities::default(), Limits::default())?;
        let granted = RuntimeScope::new(
            Capabilities::new([file.read_capability()?]),
            Limits::default(),
        )?;
        for (tool, arguments) in [
            (
                read_file()?,
                serde_json::to_value(ReadFileInput { file: file.clone() })
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
            (
                edit_file()?,
                serde_json::to_value(EditFileInput {
                    file: file.clone(),
                    old_text: "exact".into(),
                    new_text: "changed".into(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
            (
                patch_file(4096, 16)?,
                serde_json::to_value(PatchFileInput {
                    file: file.clone(),
                    diff: "@@\n-exact\n+changed\n".into(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
        ] {
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "preflight".into(),
                name: tool.definition.name.clone(),
                arguments,
            };
            assert!(matches!(
                tool.executor.authorize(None, &invocation),
                Err(Error::Unauthorized(_))
            ));
            assert!(matches!(
                tool.executor.authorize(Some(&missing), &invocation),
                Err(Error::Unauthorized(_))
            ));
            tool.executor.authorize(Some(&granted), &invocation)?;
        }
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "read".into(),
            name: "acyclic.read_file".into(),
            arguments: json!({"file":file}),
        };
        let narrow = RuntimeScope::new(
            Capabilities::new([file.read_capability()?]),
            Limits {
                render_bytes: 4,
                ..Limits::default()
            },
        )?;
        assert!(matches!(
            read_file()?.executor.authorize(Some(&narrow), &invocation),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn publication_template_rejects_missing_scope_and_invalid_path_before_dispatch() -> Result<()> {
        let tool = write_file()?;
        let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "write".into(),
            name: tool.definition.name.clone(),
            arguments: json!({"path":"../escape.txt","text":"body","media_type":"text/plain","display_name":"body"}),
        };
        assert!(matches!(
            tool.executor.authorize(None, &invocation),
            Err(Error::Unauthorized(_))
        ));
        assert!(tool.executor.authorize(Some(&scope), &invocation).is_err());
        assert_eq!(tool.definition.revision, "portable-4");
        assert_eq!(
            patch_file(4096, 16)?.definition.revision,
            "portable-patch-3"
        );
        Ok(())
    }
}
