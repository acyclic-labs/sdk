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

/// Reads a workspace-relative UTF-8 file, optionally by one-based line range,
/// resolved through the task view and returned as `cat -n` numbered text.
pub fn read() -> Result<Tool> {
    FileTool::Read.assemble(super::schema::ProjectionMode::Full)
}

/// Publishes UTF-8 through the task's original owner-bound writer.
pub fn write() -> Result<Tool> {
    FileTool::Write.assemble(super::schema::ProjectionMode::Full)
}

/// Replaces one exact, unambiguous match (or every match with `replace_all`) in
/// a workspace-relative file, published only at the generation it resolved.
pub fn edit() -> Result<Tool> {
    FileTool::Edit.assemble(super::schema::ProjectionMode::Full)
}

/// Explicitly selects exact V4A update-file editing with pinned finite work limits.
pub fn patch_file(maximum_work: u64, maximum_hunks: u32) -> Result<Tool> {
    super::patch::PatchLimits {
        maximum_bytes: 1,
        maximum_work,
        maximum_hunks,
    }
    .validate()?;
    FileTool::Patch {
        maximum_work,
        maximum_hunks,
    }
    .assemble(super::schema::ProjectionMode::Full)
}

mod inputs;
pub use inputs::{
    EditFileInput, FileResult, PatchFileInput, ReadFileInput, ReadFileResult, WriteFileInput,
};
mod executor;
mod lines;
pub(crate) use executor::FileTool;
mod assembly;
mod authorization;
mod projection;
pub use projection::FileProjection;
#[cfg(test)]
mod tests;
use super::value;
use super::{edit, patch, schema, text};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) mod native;
