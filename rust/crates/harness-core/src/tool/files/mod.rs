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

/// Reads a project path using the task's authenticated reader. An optional
/// opaque version pins the source returned by an earlier read or publication.
pub fn read() -> Result<Tool> {
    FileTool::Read.assemble(super::schema::ProjectionMode::Full)
}

/// Publishes UTF-8 through the task's original owner-bound writer.
pub fn write() -> Result<Tool> {
    FileTool::Write.assemble(super::schema::ProjectionMode::Full)
}

/// Replaces one exact, unambiguous match at a project path. An optional opaque
/// version rejects publication if that source generation is no longer current.
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
pub(crate) use inputs::FileView;
pub use inputs::{
    EditFileInput, FileResult, PatchFileInput, ReadFileInput, ReadFileResult, WriteFileInput,
};
mod executor;
mod version;
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

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod rejection_tests;
