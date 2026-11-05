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

/// Edits exact text or notebook cells at a project path. Text replacements require
/// one unambiguous match unless all occurrences are requested. An optional opaque
/// version rejects publication if that source generation is no longer current.
pub fn edit() -> Result<Tool> {
    FileTool::Edit.assemble(super::schema::ProjectionMode::Full)
}

/// Explicitly selects exact V4A update-file editing with pinned finite work limits.
pub fn patch_file(maximum_work: u64, maximum_hunks: u32) -> Result<Tool> {
    super::patch::PatchLimits {
        maximum_bytes: None,
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
mod source;
pub(crate) use inputs::FileView;
pub use inputs::{
    EditFileInput, FileResult, NotebookCell, NotebookEdit, NotebookOutput, PatchFileInput,
    ReadFileInput, ReadFileResult, ReadPage, ReadSelection, WriteFileInput,
};
pub use source::FileReadSource;
mod executor;
mod version;
pub(crate) use executor::FileTool;
pub(crate) use version::encode_version;
mod assembly;
mod authorization;
mod index;
mod lines;
mod notebook;
mod notebook_outputs;
mod notebook_read;
mod projection;
mod read;
mod window;
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
