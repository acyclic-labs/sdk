//! Individually assembled portable file tools using owner-bound task providers.

use super::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult};
use crate::{Error, Result, conversation::FileRef, runtime::ToolContext};
use acyclic_stream::BoxProviderFuture;
use serde::Deserialize;
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

#[derive(Clone, Copy)]
enum FileTool {
    Read,
    Write,
    Edit,
}

fn assemble(adapter: FileTool) -> Result<Tool> {
    let (name, description, input_schema, output_schema) = match adapter {
        FileTool::Read => (
            "acyclic.read_file",
            "Read exact bounded UTF-8 from an authorized immutable file",
            json!({"type":"object","properties":{"file":{"type":"object"}},
                "required":["file"],"additionalProperties":false}),
            json!({"type":"string"}),
        ),
        FileTool::Write => (
            "acyclic.write_file",
            "Write bounded UTF-8 through the original owner-bound content publisher",
            json!({"type":"object","properties":{"path":{"type":"string"},
                "text":{"type":"string"},"media_type":{"type":"string"},
                "display_name":{"type":"string"}},
                "required":["path","text","media_type","display_name"],
                "additionalProperties":false}),
            json!({"type":"object","properties":{"file":{"type":"object"}},
                "required":["file"],"additionalProperties":false}),
        ),
        FileTool::Edit => (
            "acyclic.edit_file",
            "Replace one exact UTF-8 match at an authorized pinned source generation",
            json!({"type":"object","properties":{"file":{"type":"object"},
                "old_text":{"type":"string","minLength":1},"new_text":{"type":"string"}},
                "required":["file","old_text","new_text"],"additionalProperties":false}),
            json!({"type":"object","properties":{"file":{"type":"object"}},
                "required":["file"],"additionalProperties":false}),
        ),
    };
    let definition = ToolDefinition {
        name: name.into(),
        revision: "portable-2".into(),
        description: description.into(),
        input_schema,
        projection_schema: crate::tool::json_projection_schema(output_schema.clone()),
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    file: FileRef,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteInput {
    path: String,
    text: String,
    media_type: String,
    display_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditInput {
    file: FileRef,
    old_text: String,
    new_text: String,
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
        let value = match self {
            Self::Read => {
                let input: ReadInput = decode(invocation.arguments)?;
                public_path(input.file.path())?;
                let maximum = task
                    .scope()
                    .limits()
                    .render_bytes
                    .min(task.scope().limits().file_bytes);
                super::edit::validate_edit_bound(maximum)?;
                if input.file.descriptor().byte_length() > maximum {
                    return Err(Error::Invalid("file exceeds text rendering bound".into()));
                }
                let bytes = task.read_file(&input.file).await?;
                let text = String::from_utf8(bytes)
                    .map_err(|_| Error::Invalid("file read requires UTF-8 content".into()))?;
                Value::String(text)
            }
            Self::Write => {
                let input: WriteInput = decode(invocation.arguments)?;
                public_path(&input.path)?;
                let file = task
                    .stage_file_once(
                        invocation.operation_id,
                        &input.path,
                        input.text.as_bytes(),
                        &input.media_type,
                        &input.display_name,
                    )
                    .await?;
                json!({"file":file})
            }
            Self::Edit => {
                let input: EditInput = decode(invocation.arguments)?;
                public_path(input.file.path())?;
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
                json!({"file":file})
            }
        };
        Ok(ToolResult { value })
    }
}

impl ToolExecutor for FileTool {
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

impl ToolProjection for FileTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(serde_json::json!({"kind":"json","value":result.value}))
    }
}
