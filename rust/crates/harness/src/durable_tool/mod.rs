//! Pinned, journaled tool calls for resumable tasks.

use crate::contract::capability;
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::Limits,
    executor::{
        ExecutionEvent, ExecutionJournal, ToolFailureKind, load_json, replay_execution, stage_json,
    },
    runtime::ToolContext,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, ToolResult},
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineRegistry, MachineStatus, MachineTransition,
        ResumableMachine, WorkflowAdmission, WorkflowJournal,
    },
};
use serde_json::Value;

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for Value {
    fn diagnostic_label_bytes() -> u64 {
        0
    }
    fn json_value_schema() -> crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::Root
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::executor::json::json_value_logical_owned_bytes(self)
    }
}
use std::sync::Arc;

mod definition;
pub use definition::ResumableTool;
mod registry;
pub use registry::ResumableToolRegistry;
mod session;
pub use session::ResumableToolSession;
mod runner;
pub use runner::DurableToolRunner;
#[cfg(test)]
mod tests;
use runner::tool_error;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_session;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use native_session::NativeResumableToolSession;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use native_session::clone_admission;
