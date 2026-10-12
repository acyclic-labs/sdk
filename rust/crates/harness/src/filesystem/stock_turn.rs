//! The stock turn's deterministic outbox adapter over the existing task worker.

use super::MailboxModelMachine;
use crate::{
    Error, Outcome, Result,
    executor::TurnOutput,
    runtime::TaskDefinition,
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use serde_json::{Value, json};
use std::sync::Arc;

/// One registered stock turn.
pub struct StockTurnMachine {
    identity: MachineIdentity,
    mailbox: MailboxModelMachine,
}

impl StockTurnMachine {
    /// Creates the portable machine without a provider, journal or task context.
    #[must_use]
    pub fn new() -> Self {
        let implementation = concat!(include_str!("stock_turn.rs"), include_str!("mailbox_machine.rs")).replace("\r\n", "\n");
        Self {
            identity: MachineIdentity {
                name: "acyclic.stock_turn".into(),
                version: "1".into(),
                digest: *blake3::hash(implementation.as_bytes()).as_bytes(),
            },
            mailbox: MailboxModelMachine::default(),
        }
    }

    /// Creates the typed registered definition for the existing durable host.
    pub fn definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, TurnOutput>> {
        self.registered_definition()
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn wire_definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> {
        self.registered_definition()
    }

    fn registered_definition<I, O>(self: &Arc<Self>) -> Result<TaskDefinition<I, O>> {
        TaskDefinition::resumable(self.clone(), json!({
            "type":"object", "required":["receive","model"],
            "properties":{"receive":{"type":"object"},"model":{"type":"object"}},
            "additionalProperties":false
        }), output_schema())?
            .requires("model")
    }
}

impl Default for StockTurnMachine {
    fn default() -> Self {
        Self::new()
    }
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

impl ResumableMachine for StockTurnMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { self.mailbox.state_schema() }
    fn initialize(&self, input: &Value) -> Result<Value> { self.mailbox.initialize(input) }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if state["stage"] == "receive" {
            let receive: WorkflowCommand = serde_json::from_value(state["receive"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if super::mailbox_machine::command_result(input, &receive)? == json!({"Succeeded":null}) {
                let model = serde_json::from_value(state["model"].clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let mut state = state.clone();
                state["stage"] = json!("model");
                return Ok(MachineTransition { state, commands: vec![model], status: MachineStatus::Suspended });
            }
        }
        let mut transition = self.mailbox.transition(state, input)?;
        if let MachineStatus::Completed { value } = transition.status {
            let outcome: Outcome<TurnOutput> = serde_json::from_value(value["model"].clone())
                .map_err(|error| Error::Invalid(format!("invalid stock turn result: {error}")))?;
            transition.status = match outcome {
                Outcome::Succeeded(output) => {
                    for attachment in &output.attachments { attachment.validate()?; }
                    MachineStatus::Completed { value: serde_json::to_value(output)
                        .map_err(|error| Error::Invalid(error.to_string()))? }
                }
                Outcome::Failed { message } => MachineStatus::Failed { message },
                Outcome::Cancelled | Outcome::Indeterminate { .. } => return Err(Error::Invalid(
                    "stock command has no terminal model result".into(),
                )),
            };
        }
        Ok(transition)
    }
}

#[cfg(test)]
mod tests;
