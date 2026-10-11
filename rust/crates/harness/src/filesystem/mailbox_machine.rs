//! Single-item steering through the canonical workflow checkpoint boundary.

use super::{MAIL_RECEIVE_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND};
use crate::{Error, Outcome, Result, scheduler::InboxItem, workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand}};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Reusable receive/checkpoint/model machine.
pub struct MailboxModelMachine {
    identity: MachineIdentity,
    schema: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    receive: WorkflowCommand,
    model: WorkflowCommand,
    stage: Stage,
    mail: Option<InboxItem>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage { Ready, Receive, Model, Complete }

impl Default for MailboxModelMachine {
    fn default() -> Self {
        Self {
            identity: MachineIdentity {
                name: "acyclic.mailbox_model".into(), version: "1".into(),
                digest: *blake3::hash(include_str!("mailbox_machine.rs").as_bytes()).as_bytes(),
            },
            schema: json!({
                "type":"object", "additionalProperties":false,
                "required":["receive","model","stage","mail"],
                "properties": {
                    "receive":{"type":"object"}, "model":{"type":"object"},
                    "stage":{"enum":["ready","receive","model","complete"]},
                    "mail":{"type":["object","null"]}
                }
            }),
        }
    }
}

fn command_result(input: &Value, command: &WorkflowCommand) -> Result<Value> {
    let results = input["commands"].as_array()
        .ok_or_else(|| Error::Invalid("mailbox machine requires a command result".into()))?;
    let [result] = results.as_slice() else {
        return Err(Error::Invalid("mailbox machine consumes one command result".into()));
    };
    if result["operation_id"] != serde_json::to_value(command.operation_id)
        .map_err(|error| Error::Invalid(error.to_string()))?
    {
        return Err(Error::Conflict("mailbox command result identity differs".into()));
    }
    Ok(result["value"].clone())
}

impl ResumableMachine for MailboxModelMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> {
        let receive: WorkflowCommand = serde_json::from_value(input["receive"].clone())
            .map_err(|error| Error::Invalid(format!("invalid mailbox receive: {error}")))?;
        let model: WorkflowCommand = serde_json::from_value(input["model"].clone())
            .map_err(|error| Error::Invalid(format!("invalid mailbox model: {error}")))?;
        receive.validate()?;
        model.validate()?;
        if receive.kind != MAIL_RECEIVE_TASK_COMMAND_KIND || model.kind != MODEL_TASK_COMMAND_KIND
            || receive.operation_id == model.operation_id
        {
            return Err(Error::Invalid("mailbox machine requires distinct receive and model commands".into()));
        }
        serde_json::to_value(State { receive, model, stage: Stage::Ready, mail: None })
            .map_err(|error| Error::Invalid(error.to_string()))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let mut state: State = serde_json::from_value(state.clone())
            .map_err(|error| Error::Invalid(format!("invalid mailbox state: {error}")))?;
        let (commands, status) = match state.stage {
            Stage::Ready => {
                if !input.is_null() { return Err(Error::Invalid("initial mailbox trigger must be null".into())); }
                state.stage = Stage::Receive;
                (vec![state.receive.clone()], MachineStatus::Suspended)
            }
            Stage::Receive => {
                let receipt: Outcome<InboxItem> = serde_json::from_value(command_result(input, &state.receive)?)
                    .map_err(|error| Error::Invalid(format!("invalid mailbox receipt: {error}")))?;
                let Outcome::Succeeded(item) = receipt else {
                    return Err(Error::Invalid("mailbox receive must succeed before model admission".into()));
                };
                if item.sequence == 0 { return Err(Error::Invalid("mailbox sequence must be positive".into())); }
                state.mail = Some(item);
                state.stage = Stage::Model;
                (vec![state.model.clone()], MachineStatus::Suspended)
            }
            Stage::Model => {
                let result = command_result(input, &state.model)?;
                state.stage = Stage::Complete;
                (Vec::new(), MachineStatus::Completed { value: json!({"mail":state.mail,"model":result}) })
            }
            Stage::Complete => return Err(Error::Conflict("mailbox machine is already complete".into())),
        };
        Ok(MachineTransition {
            state: serde_json::to_value(state).map_err(|error| Error::Invalid(error.to_string()))?,
            commands, status,
        })
    }
}
