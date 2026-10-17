//! Repeated mailbox turns under one created child's original admission.
use super::MailboxModelMachine;
use crate::{
    Error, OperationId, Outcome, Result,
    executor::TurnOutput,
    runtime::TaskDefinition,
    scheduler::InboxItem,
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use serde_json::{Value, json};
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub(crate) fn is_child(admission: &crate::runtime::TaskAdmissionRecord) -> bool {
    admission.task.name == "acyclic.child_agent" && admission.task.version == "1"
}

pub(crate) fn mail_cursor(
    transition: &MachineTransition,
    command: &WorkflowCommand,
) -> std::result::Result<u64, &'static str> {
    if transition.state.get("stage").and_then(Value::as_str) != Some("receive")
        || transition.commands.as_slice() != std::slice::from_ref(command)
        || command.kind != super::MAIL_RECEIVE_TASK_COMMAND_KIND
    {
        return Err("child receive is outside its committed outbox");
    }
    match transition.state.get("mail") {
        Some(Value::Null) => Ok(0),
        Some(mail) => mail
            .get("sequence")
            .and_then(Value::as_u64)
            .filter(|sequence| *sequence > 0)
            .ok_or("child mailbox cursor is invalid"),
        None => Err("child mailbox cursor is absent"),
    }
}

/// Projects the exact committed model outcome without losing it at receive.
pub(crate) fn completed_turn(
    child: crate::TaskId,
    record: &crate::workflow::WorkflowRecord,
) -> Result<crate::task_volume::ChildTurnResult> {
    if record.prior.state["stage"].as_str() != Some("model")
        || record.transition.state["stage"].as_str() != Some("receive")
        || record.prior.state["mail"] != record.transition.state["mail"]
    {
        return Err(Error::Conflict(
            "child result is outside its completed model turn".into(),
        ));
    }
    let mail: InboxItem = serde_json::from_value(record.prior.state["mail"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if mail.task_id != child || mail.sequence == 0 {
        return Err(Error::Unauthorized(
            "child result has a foreign mailbox owner".into(),
        ));
    }
    let command: WorkflowCommand = serde_json::from_value(record.prior.state["model"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if command.kind != super::MODEL_TASK_COMMAND_KIND
        || record.transition.status != MachineStatus::Suspended
        || record.transition.commands.len() != 1
        || mail_cursor(&record.transition, &record.transition.commands[0]) != Ok(mail.sequence)
    {
        return Err(Error::Conflict(
            "child result has no next receive outbox".into(),
        ));
    }
    let outcome = super::mailbox_machine::command_result(&record.input, &command)?;
    let result = crate::task_volume::ChildTurnResult {
        task: child,
        turn: mail.sequence,
        outcome: serde_json::from_value(outcome)
            .map_err(|error| Error::Invalid(error.to_string()))?,
    };
    result
        .validate()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(result)
}

/// A child retains its created volumes and admission while waiting for messages.
pub struct ChildAgentMachine {
    identity: MachineIdentity,
    mailbox: MailboxModelMachine,
}

impl ChildAgentMachine {
    /// Creates the deterministic machine without a worker or provider.
    #[must_use]
    pub fn new() -> Self {
        let implementation = concat!(
            include_str!("child_agent.rs"),
            include_str!("mailbox_machine.rs")
        )
        .replace("\r\n", "\n");
        Self {
            identity: MachineIdentity {
                name: "acyclic.child_agent".into(),
                version: "1".into(),
                digest: *blake3::hash(implementation.as_bytes()).as_bytes(),
            },
            mailbox: MailboxModelMachine::default(),
        }
    }

    /// Registers the existing mailbox and model command contracts.
    pub fn definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> {
        TaskDefinition::resumable(
            self.clone(),
            json!({
                "type":"object", "required":["receive","model"],
                "properties":{"receive":{"type":"object"},"model":{"type":"object"}},
                "additionalProperties":false
            }),
            json!({"type":"null"}),
        )?
        .requires("model")
    }
}

impl Default for ChildAgentMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumableMachine for ChildAgentMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        self.mailbox.state_schema()
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        self.mailbox.initialize(input)
    }

    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if state.get("stage").and_then(Value::as_str) == Some("receive") {
            let receive: WorkflowCommand = serde_json::from_value(state["receive"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let receipt: Outcome<InboxItem> =
                serde_json::from_value(super::mailbox_machine::command_result(input, &receive)?)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let Outcome::Succeeded(item) = receipt else {
                return Err(Error::Invalid("child receive did not succeed".into()));
            };
            let previous: Option<InboxItem> = serde_json::from_value(state["mail"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if previous
                .as_ref()
                .map_or(0, |mail| mail.sequence)
                .checked_add(1)
                != Some(item.sequence)
                || previous
                    .as_ref()
                    .is_some_and(|mail| mail.task_id != item.task_id)
            {
                return Err(Error::Conflict(
                    "child mailbox sequence or owner changed".into(),
                ));
            }
        }
        let mut transition = self.mailbox.transition(state, input)?;
        if let MachineStatus::Completed { value } = &transition.status {
            let outcome: Outcome<TurnOutput> = serde_json::from_value(value["model"].clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match outcome {
                Outcome::Succeeded(output) => {
                    for attachment in output.attachments {
                        attachment.validate(&mut None)?;
                    }
                }
                Outcome::Indeterminate { .. } => {
                    return Err(Error::Invalid("uncertain child turn cannot advance".into()));
                }
                Outcome::Failed { .. } | Outcome::Cancelled => {}
            }
            let decode = |name| -> Result<WorkflowCommand> {
                serde_json::from_value(transition.state[name].clone())
                    .map_err(|error| Error::Invalid(error.to_string()))
            };
            let mut receive = decode("receive")?;
            let mut model = decode("model")?;
            receive.operation_id = next_operation(b"receive", receive.operation_id);
            model.operation_id = next_operation(b"model", model.operation_id);
            transition.state["receive"] = serde_json::to_value(&receive)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            transition.state["model"] =
                serde_json::to_value(model).map_err(|error| Error::Invalid(error.to_string()))?;
            transition.state["stage"] = json!("receive");
            transition.commands = vec![receive];
            transition.status = MachineStatus::Suspended;
        }
        Ok(transition)
    }
}

fn next_operation(kind: &[u8], previous: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.child-agent.turn/v1\0");
    hash.update(kind);
    hash.update(&previous.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
