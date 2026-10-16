//! One direct tool call uses the existing fenced workflow/tool journal.
use crate::workflow::WorkflowCommand;
use crate::{Error, Outcome, Result, filesystem, runtime, tool};
use std::sync::Arc;
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}
use crate::workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine};
use serde_json::{Value, json};

pub(super) struct DirectTool {
    identity: MachineIdentity,
    schema: Value,
}
impl DirectTool {
    pub(super) fn new() -> Self {
        Self {
            identity: MachineIdentity {
                name: "acyclic.direct_tool".into(),
                version: "1".into(),
                digest: *blake3::hash(include_bytes!("command_machine.rs")).as_bytes(),
            },
            schema: json!({"type":"object","required":["command","sent"],"properties":{"command":{"type":"object"},"sent":{"type":"boolean"}},"additionalProperties":false}),
        }
    }
    pub(super) fn definition(
        self: &Arc<Self>,
    ) -> Result<runtime::TaskDefinition<Value, tool::ToolResult>> {
        runtime::TaskDefinition::resumable(
            self.clone(),
            json!({"type":"object","required":["command"],"properties":{"command":{"type":"object"}},"additionalProperties":false}),
            json!({"type":"object","required":["value"],"properties":{"value":{}},"additionalProperties":false}),
        )
    }
}
impl ResumableMachine for DirectTool {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        let command: WorkflowCommand =
            serde_json::from_value(input["command"].clone()).map_err(storage)?;
        command.validate(&mut None)?;
        if command.kind != filesystem::TOOL_TASK_COMMAND_KIND {
            return Err(Error::Invalid("direct command must be a tool call".into()));
        }
        Ok(json!({"command":command,"sent":false}))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let command: WorkflowCommand =
            serde_json::from_value(state["command"].clone()).map_err(storage)?;
        let sent = state["sent"]
            .as_bool()
            .ok_or_else(|| Error::Invalid("direct command stage absent".into()))?;
        if !sent {
            return Ok(MachineTransition {
                state: json!({"command":command,"sent":true}),
                commands: vec![command],
                status: MachineStatus::Suspended,
            });
        }
        let results = input["commands"]
            .as_array()
            .ok_or_else(|| Error::Invalid("direct command result absent".into()))?;
        let [result] = results.as_slice() else {
            return Err(Error::Invalid("direct command requires one result".into()));
        };
        if result["operation_id"] != json!(command.operation_id) {
            return Err(Error::Conflict(
                "direct command result identity changed".into(),
            ));
        }
        let value: Outcome<Value> =
            serde_json::from_value(result["value"].clone()).map_err(storage)?;
        let status = match value {
            Outcome::Succeeded(result) => MachineStatus::Completed {
                value: serde_json::to_value(tool::ToolResult { value: result }).map_err(storage)?,
            },
            Outcome::Failed { message } => MachineStatus::Failed { message },
            _ => return Err(Error::Indeterminate(command.operation_id)),
        };
        Ok(MachineTransition {
            state: state.clone(),
            commands: vec![],
            status,
        })
    }
}

#[cfg(test)]
#[path = "command_machine_tests.rs"]
mod tests;
