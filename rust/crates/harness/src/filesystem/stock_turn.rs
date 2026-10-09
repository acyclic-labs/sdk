//! The stock turn's deterministic outbox adapter over the existing task worker.

use super::MODEL_TASK_COMMAND_KIND;
use crate::{
    Error, OperationId, Outcome, Result,
    executor::TurnOutput,
    runtime::TaskDefinition,
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// One registered stock turn. Provider work belongs to the existing model
/// command adapter; this machine retains only its immutable command reference.
pub struct StockTurnMachine {
    identity: MachineIdentity,
    state_schema: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready { command: Box<WorkflowCommand> },
    Waiting { operation_id: OperationId },
    Settled,
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
    value: Outcome<TurnOutput>,
}

impl StockTurnMachine {
    /// Creates the portable machine without a provider, journal or task context.
    #[must_use]
    pub fn new() -> Self {
        let command = command_schema();
        // Checkout newline conversion must not change the portable identity.
        let implementation = include_str!("stock_turn.rs").replace("\r\n", "\n");
        Self {
            identity: MachineIdentity {
                name: "acyclic.stock_turn".into(),
                version: "1".into(),
                digest: *blake3::hash(implementation.as_bytes()).as_bytes(),
            },
            state_schema: json!({"oneOf":[
                {"type":"object","required":["phase","command"],"properties":{
                    "phase":{"const":"ready"},"command":command
                },"additionalProperties":false},
                {"type":"object","required":["phase","operation_id"],"properties":{
                    "phase":{"const":"waiting"},"operation_id":{"type":"string","minLength":1}
                },"additionalProperties":false},
                {"type":"object","required":["phase"],"properties":{
                    "phase":{"const":"settled"}
                },"additionalProperties":false}
            ]}),
        }
    }

    /// Creates the typed registered definition for the existing durable host.
    /// The input is one ref-only `acyclic.model.v1` command, never inline media
    /// or a separately captured model/tool execution context.
    pub fn definition(self: &Arc<Self>) -> Result<TaskDefinition<WorkflowCommand, TurnOutput>> {
        self.registered_definition()
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn wire_definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> {
        self.registered_definition()
    }

    fn registered_definition<I, O>(self: &Arc<Self>) -> Result<TaskDefinition<I, O>> {
        TaskDefinition::resumable(self.clone(), command_schema(), output_schema())?
            .requires("model")
    }
}

impl Default for StockTurnMachine {
    fn default() -> Self {
        Self::new()
    }
}

fn command_schema() -> Value {
    json!({
        "type":"object", "required":["operation_id","kind","payload"],
        "properties":{
            "operation_id":{"type":"string","minLength":1},
            "kind":{"const":MODEL_TASK_COMMAND_KIND},
            "payload":{"type":"object"}
        }, "additionalProperties":false
    })
}

fn output_schema() -> Value {
    json!({
        "type":"object", "required":["text","metadata","steps"],
        "properties":{
            "text":{"type":"string"}, "metadata":{},
            "attachments":{"type":"array","items":{"type":"object"}},
            "steps":{"type":"integer","minimum":0,"maximum":u32::MAX}
        }, "additionalProperties":false
    })
}

fn validate_command(command: &WorkflowCommand) -> Result<()> {
    command.validate()?;
    if command.kind != MODEL_TASK_COMMAND_KIND {
        return Err(Error::Invalid(
            "stock turn requires the pinned model command kind".into(),
        ));
    }
    Ok(())
}

impl ResumableMachine for StockTurnMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }

    fn state_schema(&self) -> &Value {
        &self.state_schema
    }

    fn initialize(&self, input: &Value) -> Result<Value> {
        let command: WorkflowCommand = serde_json::from_value(input.clone())
            .map_err(|error| Error::Invalid(format!("invalid stock turn command: {error}")))?;
        validate_command(&command)?;
        serde_json::to_value(State::Ready {
            command: Box::new(command),
        })
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let state: State = serde_json::from_value(state.clone())
            .map_err(|error| Error::Invalid(format!("invalid stock turn state: {error}")))?;
        let (state, commands, status) = match state {
            State::Ready { command } => {
                validate_command(&command)?;
                if !input.is_null() {
                    return Err(Error::Invalid(
                        "stock turn initial trigger must be null".into(),
                    ));
                }
                (
                    State::Waiting {
                        operation_id: command.operation_id,
                    },
                    vec![*command],
                    MachineStatus::Suspended,
                )
            }
            State::Waiting { operation_id } => {
                let results: CommandResults =
                    serde_json::from_value(input.clone()).map_err(|error| {
                        Error::Invalid(format!("invalid stock turn result: {error}"))
                    })?;
                let mut results = results.commands.into_iter();
                let result = results
                    .next()
                    .ok_or_else(|| Error::Invalid("stock turn result is missing".into()))?;
                if results.next().is_some() || result.operation_id != operation_id {
                    return Err(Error::Conflict(
                        "stock turn result belongs to another outbox".into(),
                    ));
                }
                let status = match result.value {
                    Outcome::Succeeded(output) => {
                        for attachment in &output.attachments {
                            attachment.validate()?;
                        }
                        MachineStatus::Completed {
                            value: serde_json::to_value(output)
                                .map_err(|error| Error::Invalid(error.to_string()))?,
                        }
                    }
                    Outcome::Failed { message } => MachineStatus::Failed { message },
                    Outcome::Cancelled | Outcome::Indeterminate { .. } => {
                        return Err(Error::Invalid(
                            "stock command has no terminal model result".into(),
                        ));
                    }
                };
                (State::Settled, Vec::new(), status)
            }
            State::Settled => {
                return Err(Error::Conflict("stock turn is already settled".into()));
            }
        };
        Ok(MachineTransition {
            state: serde_json::to_value(state)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            commands,
            status,
        })
    }
}

#[cfg(test)]
mod tests;
