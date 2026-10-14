//! Pinned, journaled tool calls.
use crate::executor::stock_boundary as boundary;
use crate::wire::ErrorCode::{Conflict, NotFound, Unauthorized, Unsupported};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::{Invalid, Storage};
use crate::{
    Error, NativeCustody, NativeWork, OperationId, Outcome, Result, TaskId,
    conversation::{FileRef, Limits},
    executor::{
        ExecutionEvent, ExecutionJournal, PreparationIo, ToolFailureKind, load_json,
        preparation_digest, preparation_format, replay_execution, stage_json, stock_string_copy,
        validate_json_schema_value, with_history_io,
    },
    runtime::ToolContext,
    tool::{Tool, ToolDefinition, ToolInvocation, ToolRegistry, ToolResult},
    workflow::{MachineCheckpoint, MachineRegistry, ResumableMachine, WorkflowAdmission},
};
use serde_json::Value;
use std::sync::Arc;
mod definition;
pub use definition::ResumableTool;
mod registry;
pub use registry::ResumableToolRegistry;
mod runner;
pub use runner::DurableToolRunner;
#[cfg(test)]
mod tests;
fn preparation_io(work: NativeWork<'_>) -> Result<Option<PreparationIo>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        work.map(PreparationIo::from_native_work).transpose()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        if work.is_some() {
            return Err(Error::Invalid("native tool work is unavailable".into()));
        }
        Ok(None)
    }
}
fn native_work(io: Option<&PreparationIo>) -> NativeWork<'_> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        io.and_then(PreparationIo::original_native_work)
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = io;
        None
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for Value {
    fn diagnostic_label_bytes() -> u64 {
        0
    }
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::Root
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::executor::json::json_value_logical_owned_bytes(self)
    }
}
