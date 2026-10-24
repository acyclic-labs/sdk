//! Pi task lifetime over the existing registered workflow and execution owner.

use crate::pi_root::PiRootIntent;
use crate::{PiScratchAllocation, PiTaskBinding};
use acyclic_harness::{
    OperationId, Outcome,
    conversation::FileRef,
    filesystem::EFFECT_TASK_COMMAND_KIND,
    runtime::TaskDefinition,
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::sync::Arc;

/// Resolves a creator-seeded native intent under the actual admitted child scope before staging its one-shot process effect.
pub(crate) const NATIVE_LAUNCH_TASK_COMMAND_KIND: &str = "acyclic.pi.native.launch.v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiControlInput {
    pub(crate) command: WorkflowCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiControlReply {
    pub(crate) operation_id: OperationId,
    pub(crate) value: Value,
}

/// Immutable admission input.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiTaskInput<B = PiTaskBinding, R = PiRootIntent, C = WorkflowCommand> {
    pub(crate) binding: B,
    pub(crate) execution: PiTaskExecution<R, C>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiTaskExecution<R = PiRootIntent, C = WorkflowCommand> {
    Host { root: R, preparation: C },
    Child { launch: C },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum PiTaskState {
    Preparing {
        command: WorkflowCommand,
    },
    Allocating {
        operation: OperationId,
    },
    Hosting {
        allocation: FileRef,
        #[serde(default)]
        reply: Option<PiControlReply>,
    },
    Controlling {
        allocation: FileRef,
        operation: OperationId,
    },
    Ready {
        command: WorkflowCommand,
    },
    Awaiting {
        operation: OperationId,
    },
    Completed {
        result: FileRef,
    },
    Failed {
        message: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResults {
    commands: Vec<CommandResult>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResult {
    operation_id: OperationId,
    value: Outcome<FileRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlResults {
    commands: Vec<PiControlReply>,
}

#[cfg(not(target_arch = "wasm32"))]
const PI_COMMAND_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES + 54;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) const PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES: u64 =
    PI_COMMAND_DIAGNOSTIC_LABEL_BYTES + 116;

#[cfg(not(target_arch = "wasm32"))]
const CONTROL_INPUT_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES + 29;

#[cfg(not(target_arch = "wasm32"))]
const COMMAND_RESULTS_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES + 147;
#[cfg(not(target_arch = "wasm32"))]
const STATE_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES + 232;
#[cfg(not(target_arch = "wasm32"))]
const CONTROL_RESULTS_LABEL_BYTES: u64 = PI_COMMAND_DIAGNOSTIC_LABEL_BYTES + 73;
pub(crate) struct PiTaskMachine {
    identity: MachineIdentity,
    schema: Value,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: acyclic_fs::NativeAllocation,
}

fn native_validation(
    error: acyclic_harness::Error,
    reason: &'static str,
) -> acyclic_harness::Error {
    match error {
        acyclic_harness::Error::Invalid(_) => transition_error(reason),
        error => error,
    }
}

fn transition_error(reason: &'static str) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

mod codec;
mod definition;
mod execution;
mod state;
use codec::*;
pub(crate) use codec::{control_reply, native_metadata_quota};
use state::*;
#[cfg(test)]
#[path = "../pi_task_tests.rs"]
mod tests;
