//! Pi task lifetime over the existing registered workflow and execution owner.

use super::pi_root::PiRootIntent;
use super::{PiScratchAllocation, PiTaskBinding};
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
use serde_json::{Value, json};
use std::sync::Arc;

/// Immutable admission input. Allocation progress stays on the existing plugin
/// preparation/route record; it cannot change the task's admitted selection.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiTaskInput {
    pub(crate) binding: PiTaskBinding,
    pub(crate) execution: PiTaskExecution,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiTaskExecution {
    Host {
        root: PiRootIntent,
        preparation: WorkflowCommand,
    },
    Child {
        launch: WorkflowCommand,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum PiTaskState {
    Preparing { command: WorkflowCommand },
    Allocating { operation: OperationId },
    Hosting { allocation: FileRef },
    Ready { command: WorkflowCommand },
    Awaiting { operation: OperationId },
    Completed { result: FileRef },
    Failed { message: String },
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

pub(crate) struct PiTaskMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl PiTaskMachine {
    pub(crate) fn new() -> Self {
        // Pin deterministic behavior and binding validation, independently of
        // installer, service recovery and test-only changes.
        let mut digest = blake3::Hasher::new();
        for source in [
            include_bytes!("pi_task.rs").as_slice(),
            include_bytes!("pi_binding.rs").as_slice(),
            include_bytes!("pi_root.rs").as_slice(),
        ] {
            digest.update(&(source.len() as u64).to_le_bytes());
            digest.update(source);
        }
        Self {
            identity: MachineIdentity {
                name: "acyclic.pi".to_owned(),
                version: "1".to_owned(),
                digest: *digest.finalize().as_bytes(),
            },
            schema: json!({"oneOf":[
                {
                    "type":"object", "additionalProperties":false, "required":["state","command"],
                    "properties":{"state":{"const":"preparing"},"command":command_schema()}
                },
                {
                    "type":"object", "additionalProperties":false, "required":["state","operation"],
                    "properties":{"state":{"const":"allocating"},"operation":{"type":"string","format":"uuid"}}
                },
                {
                    "type":"object", "additionalProperties":false, "required":["state","allocation"],
                    "properties":{"state":{"const":"hosting"},"allocation":{"type":"object"}}
                },
                {
                    "type":"object", "additionalProperties":false, "required":["state","command"],
                    "properties":{"state":{"const":"ready"},"command":command_schema()}
                },
                {
                    "type":"object", "additionalProperties":false, "required":["state","operation"],
                    "properties":{"state":{"const":"awaiting"},"operation":{"type":"string","format":"uuid"}}
                },
                {
                    "type":"object", "additionalProperties":false, "required":["state","result"],
                    "properties":{"state":{"const":"completed"},"result":{"type":"object"}}
                },
                {
                    "type":"object", "additionalProperties":false, "required":["state","message"],
                    "properties":{"state":{"const":"failed"},"message":{"type":"string"}}
                }
            ]}),
        }
    }

    pub(crate) fn definition(
        self: &Arc<Self>,
    ) -> acyclic_harness::Result<TaskDefinition<PiTaskInput, FileRef>> {
        TaskDefinition::resumable(
            self.clone(),
            json!({
                "type":"object", "additionalProperties":false,
                "required":["binding","execution"],
                "properties":{
                    "binding":{"type":"object"},
                    "execution":{"oneOf":[
                        {"type":"object","additionalProperties":false,"required":["kind","root","preparation"],
                            "properties":{"kind":{"const":"host"},"root":PiRootIntent::schema(),"preparation":command_schema()}},
                        {"type":"object","additionalProperties":false,"required":["kind","launch"],
                            "properties":{"kind":{"const":"child"},"launch":command_schema()}}
                    ]}
                }
            }),
            json!({"type":"object"}),
        )
    }
}

fn command_schema() -> Value {
    json!({
        "type":"object", "additionalProperties":false,
        "required":["operation_id","kind","payload"],
        "properties":{
            "operation_id":{"type":"string","format":"uuid"},
            "kind":{"const":EFFECT_TASK_COMMAND_KIND},
            // FileRef's shared serde constructor validates its complete shape
            // and semantic invariants before this reference can be consumed.
            "payload":{"type":"object"}
        }
    })
}

fn validate_effect_command(command: &WorkflowCommand) -> acyclic_harness::Result<()> {
    command.validate()?;
    if command.kind != EFFECT_TASK_COMMAND_KIND {
        return Err(invalid("Pi task requires the existing effect command"));
    }
    Ok(())
}

impl ResumableMachine for PiTaskMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }

    fn state_schema(&self) -> &Value {
        &self.schema
    }

    fn initialize(&self, input: &Value) -> acyclic_harness::Result<Value> {
        let input: PiTaskInput = decode(input)?;
        input
            .binding
            .validate(
                None,
                input
                    .binding
                    .project_volumes
                    .keys()
                    .map(String::as_str)
                    .collect(),
                true,
            )
            .map_err(acyclic_harness::Error::Invalid)?;
        if !matches!(
            input.binding.scratch.allocation,
            PiScratchAllocation::Preparing
        ) {
            return Err(invalid("Pi admission must precede scratch allocation"));
        }
        let state = match input.execution {
            PiTaskExecution::Host { root, preparation } => {
                root.validate(&input.binding)
                    .map_err(acyclic_harness::Error::Invalid)?;
                validate_effect_command(&preparation)?;
                PiTaskState::Preparing {
                    command: preparation,
                }
            }
            PiTaskExecution::Child { launch } => {
                validate_effect_command(&launch)?;
                PiTaskState::Ready { command: launch }
            }
        };
        encode(&state)
    }

    fn transition(
        &self,
        state: &Value,
        input: &Value,
    ) -> acyclic_harness::Result<MachineTransition> {
        let state: PiTaskState = decode(state)?;
        let (state, commands, status) = match state {
            PiTaskState::Hosting { allocation } if input.is_null() => (
                PiTaskState::Hosting { allocation },
                Vec::new(),
                MachineStatus::Suspended,
            ),
            PiTaskState::Preparing { command } if input.is_null() => {
                validate_effect_command(&command)?;
                (
                    PiTaskState::Allocating {
                        operation: command.operation_id,
                    },
                    vec![command],
                    MachineStatus::Suspended,
                )
            }
            PiTaskState::Ready { command } if input.is_null() => {
                validate_effect_command(&command)?;
                (
                    PiTaskState::Awaiting {
                        operation: command.operation_id,
                    },
                    vec![command],
                    MachineStatus::Suspended,
                )
            }
            PiTaskState::Allocating { operation } | PiTaskState::Awaiting { operation } => {
                let allocating = matches!(state, PiTaskState::Allocating { .. });
                let results: CommandResults = decode(input)?;
                let [result]: [CommandResult; 1] = results
                    .commands
                    .try_into()
                    .map_err(|_| invalid("Pi result differs from its retained effect command"))?;
                if result.operation_id != operation {
                    return Err(invalid(
                        "Pi result differs from its retained effect command",
                    ));
                }
                match result.value {
                    Outcome::Succeeded(result) => {
                        result.validate()?;
                        if allocating {
                            (
                                PiTaskState::Hosting { allocation: result },
                                Vec::new(),
                                MachineStatus::Suspended,
                            )
                        } else {
                            let value = encode(&result)?;
                            (
                                PiTaskState::Completed { result },
                                Vec::new(),
                                MachineStatus::Completed { value },
                            )
                        }
                    }
                    Outcome::Failed { message } => (
                        PiTaskState::Failed {
                            message: message.clone(),
                        },
                        Vec::new(),
                        MachineStatus::Failed { message },
                    ),
                    Outcome::Cancelled | Outcome::Indeterminate { .. } => {
                        return Err(invalid("unsettled Pi effect cannot advance its workflow"));
                    }
                }
            }
            _ => return Err(invalid("Pi task does not accept this transition")),
        };
        Ok(MachineTransition {
            state: encode(&state)?,
            commands,
            status,
        })
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> acyclic_harness::Result<T> {
    serde_json::from_value(value.clone()).map_err(|error| invalid(error.to_string()))
}

fn encode(value: &impl Serialize) -> acyclic_harness::Result<Value> {
    serde_json::to_value(value).map_err(|error| invalid(error.to_string()))
}

fn invalid(message: impl Into<String>) -> acyclic_harness::Error {
    acyclic_harness::Error::Invalid(message.into())
}

#[cfg(test)]
#[path = "pi_task_tests.rs"]
mod tests;
