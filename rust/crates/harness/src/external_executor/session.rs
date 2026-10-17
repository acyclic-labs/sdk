use crate::{
    Error, OperationId, Result,
    runtime::TaskDefinition,
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// A session's single command slot, executed by the existing durable task worker.
/// Request files supply commands; the worker retains their dispatch and results.
pub struct CommandSessionMachine {
    identity: MachineIdentity,
    schema: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Input {
    command: WorkflowCommand,
    terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    executor: Option<crate::conversation::FileRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_context: Option<crate::conversation::FileRef>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready {
        input: Input,
    },
    Waiting {
        operation_id: OperationId,
        terminal: bool,
        #[serde(default)]
        native_process: bool,
    },
    Idle,
    Closed,
}

impl CommandSessionMachine {
    /// Pins the implementation and its durable state schema.
    #[must_use]
    pub fn new() -> Self {
        let implementation = include_str!("session.rs").replace("\r\n", "\n");
        Self {
            identity: MachineIdentity {
                name: "acyclic.command_session".into(),
                version: "1".into(),
                digest: *blake3::hash(implementation.as_bytes()).as_bytes(),
            },
            schema: json!({"oneOf":[
                {"type":"object","required":["stage","input"],"properties":{
                    "stage":{"const":"ready"},"input":input_schema()
                },"additionalProperties":false},
                {"type":"object","required":["stage","operation_id","terminal"],"properties":{
                    "stage":{"const":"waiting"},"operation_id":{"type":"string"},
                    "terminal":{"type":"boolean"},"native_process":{"type":"boolean"}
                },"additionalProperties":false},
                {"type":"object","required":["stage"],"properties":{
                    "stage":{"enum":["idle","closed"]}
                },"additionalProperties":false}
            ]}),
        }
    }

    /// Registers a session without a model provider or a separate executor.
    pub fn definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> {
        TaskDefinition::resumable(self.clone(), input_schema(), output_schema())
    }
}

impl Default for CommandSessionMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumableMachine for CommandSessionMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }

    fn state_schema(&self) -> &Value {
        &self.schema
    }

    fn initialize(&self, input: &Value) -> Result<Value> {
        let input: Input = decode(input)?;
        input.command.validate(&mut None)?;
        encode(State::Ready { input })
    }

    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let mut commands = Vec::new();
        let mut completion = None;
        let state = match decode(state)? {
            State::Ready { input: command } if input.is_null() => dispatch(command, &mut commands)?,
            State::Idle if input.is_null() => State::Idle,
            State::Idle if input.get("native_exit").is_some() => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Finished {
                    native_exit: crate::Outcome<Value>,
                }
                completion = Some(decode::<Finished>(input)?.native_exit);
                State::Closed
            }
            State::Idle => dispatch(decode(input)?, &mut commands)?,
            State::Waiting {
                operation_id,
                terminal,
                native_process,
            } => {
                let result = require_result(input, operation_id)?;
                let rejected = if result.get("Failed").is_some() {
                    matches!(
                        decode::<crate::Outcome<serde::de::IgnoredAny>>(result)?,
                        crate::Outcome::Failed { .. }
                    )
                } else {
                    false
                };
                // Rejection settles this request, including a rejected stop;
                // it cannot turn untrusted terminal intent into task closure.
                if !rejected && (terminal || result.get("native_exit").is_some()) {
                    completion = Some(if let Some(exit) = result.get("native_exit") {
                        decode::<crate::Outcome<Value>>(exit)?
                    } else if !native_process {
                        crate::Outcome::Succeeded(Value::Null)
                    } else {
                        crate::Outcome::Succeeded(result.clone())
                    });
                    State::Closed
                } else {
                    State::Idle
                }
            }
            State::Ready { .. } => {
                return Err(invalid("session must start before receiving input"));
            }
            State::Closed => return Err(invalid("session is closed")),
        };
        let status = if matches!(state, State::Closed) {
            match completion {
                Some(crate::Outcome::Succeeded(value)) => MachineStatus::Completed { value },
                Some(crate::Outcome::Failed { message }) => MachineStatus::Failed { message },
                Some(_) => return Err(invalid("native execution has no settled outcome")),
                None => MachineStatus::Completed { value: Value::Null },
            }
        } else {
            MachineStatus::Suspended
        };
        Ok(MachineTransition {
            state: encode(state)?,
            commands,
            status,
        })
    }
}

fn dispatch(input: Input, commands: &mut Vec<WorkflowCommand>) -> Result<State> {
    input.command.validate(&mut None)?;
    let state = State::Waiting {
        operation_id: input.command.operation_id,
        terminal: input.terminal,
        native_process: input.command.kind == super::launch::PROCESS_COMMAND,
    };
    commands.push(input.command);
    Ok(state)
}

fn require_result(input: &Value, operation_id: OperationId) -> Result<&Value> {
    let envelope = input
        .as_object()
        .filter(|value| value.len() == 1)
        .ok_or_else(|| invalid("session requires one command result"))?;
    let results = envelope
        .get("commands")
        .and_then(Value::as_array)
        .filter(|values| values.len() == 1)
        .ok_or_else(|| invalid("session requires one command result"))?;
    let result = results[0]
        .as_object()
        .filter(|value| value.len() == 2)
        .ok_or_else(|| invalid("session command result is malformed"))?;
    let id: OperationId = decode(
        result
            .get("operation_id")
            .ok_or_else(|| invalid("session result identity is missing"))?,
    )?;
    if id != operation_id {
        return Err(invalid("session result differs from the pending command"));
    }
    result
        .get("value")
        .ok_or_else(|| invalid("session result value is missing"))
}

fn input_schema() -> Value {
    json!({"type":"object","required":["command","terminal"],"properties":{
        "executor":{"type":"object"},
        "native_context":{"type":"object"},
        "command":{"type":"object","required":["operation_id","kind","payload"],"properties":{
            "operation_id":{"type":"string"},"kind":{"type":"string"},"payload":{"type":"object"}
        },"additionalProperties":false},"terminal":{"type":"boolean"}
    },"additionalProperties":false})
}

fn output_schema() -> Value {
    json!({"oneOf":[{"type":"null"},{"type":"object","required":["success","exit_code"],
        "properties":{"success":{"type":"boolean"},"exit_code":{"type":["integer","null"]}}}]})
}

fn decode<'a, T: Deserialize<'a>>(value: &'a Value) -> Result<T> {
    T::deserialize(value).map_err(|error| Error::Invalid(error.to_string()))
}

fn encode(state: State) -> Result<Value> {
    serde_json::to_value(state).map_err(|error| Error::Invalid(error.to_string()))
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        // Session outputs are disconnect/null or native process envelopes;
        // scalars, arrays and malformed native outcomes never match the schema.
        #[test]
        fn output_contract_registers_and_preserves_native_process_metadata(
            success in any::<bool>(), code in any::<Option<i32>>(), text in ".{0,64}",
        ) {
            let machine=Arc::new(CommandSessionMachine::new());
            machine.definition()?;
            let check=|value:&Value|crate::executor::validate_json_schema_value(&output_schema(),value,"session output",&mut None);
            check(&Value::Null)?;
            check(&json!({"success":success,"exit_code":code,"native_metadata":{"text":text}}))?;
            for invalid in [json!(success),json!(text),json!([code]),json!({}),json!({"success":success}),
                json!({"success":text,"exit_code":code}),json!({"success":success,"exit_code":text})] {
                prop_assert!(check(&invalid).is_err());
            }
        }
    }
}
