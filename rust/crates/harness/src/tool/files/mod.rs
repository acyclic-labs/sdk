//! Individually assembled portable file tools using owner-bound task providers.

use super::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult, decode,
};
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

fn assemble(adapter: FileTool) -> Result<Tool> {
    let (name, description, mut input_schema, output_schema, projection_schema) = match adapter {
        FileTool::Read => (
            "acyclic.read_file",
            "Read exact bounded UTF-8 from an authorized immutable file",
            super::schema::input::<ReadFileInput>()?,
            super::schema::output::<String>()?,
            super::schema::ProjectionMode::Full.schema::<String>()?,
        ),
        FileTool::Write => (
            "acyclic.write_file",
            "Write bounded UTF-8 through the original owner-bound content publisher",
            super::schema::input::<WriteFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::ProjectionMode::Full.schema::<FileResult>()?,
        ),
        FileTool::Edit => (
            "acyclic.edit_file",
            "Replace one exact UTF-8 match at an authorized pinned source generation",
            super::schema::input::<EditFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::ProjectionMode::Full.schema::<FileResult>()?,
        ),
        FileTool::Patch { .. } => (
            "acyclic.patch_file",
            "Apply exact V4A update-file diff hunks to one authorized immutable file; @@ headers, exact context and optional EOF; ambiguous context fails",
            super::schema::input::<PatchFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::ProjectionMode::Full.schema::<FileResult>()?,
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
            FileTool::Patch { .. } => "portable-patch-4",
            _ => "portable-5",
        }
        .into(),
        description: description.into(),
        input_schema,
        projection_schema,
        output_schema,
    };
    definition.validate(&mut None)?;
    let projection: Arc<dyn ToolProjection> = match adapter {
        FileTool::Read => Arc::new(ReadFileProjection(super::schema::ProjectionMode::Full)),
        _ => Arc::new(FileResultProjection(super::schema::ProjectionMode::Full)),
    };
    Ok(Tool {
        definition,
        executor: Arc::new(adapter),
        projection,
    })
}

mod inputs;
pub use inputs::{EditFileInput, FileResult, PatchFileInput, ReadFileInput, WriteFileInput};
mod executor;
use executor::FileTool;
mod projection;
pub use projection::{FileResultProjection, ReadFileProjection};
#[cfg(test)]
mod tests;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use super::value;
use super::{edit, patch, schema};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) mod native;
