//! Native names select existing workflow commands; they introduce no executor.
use super::{ExternalAction, ExternalRequest};
use crate::{Error, Result, filesystem::*};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

impl ExternalRequest {
    pub(super) fn task_command(&self) -> Result<Option<(&'static str, Value)>> {
        if matches!(self.action, ExternalAction::Join { .. }) {
            return Ok(Some((
                TASK_JOIN_COMMAND_KIND,
                serde_json::to_value(TaskJoinCommand {
                    child: self.child_attachment("")?.task_id(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            )));
        }
        let ExternalAction::RunTool {
            name, arguments, ..
        } = &self.action
        else {
            return Ok(None);
        };
        let (kind, payload) = match name.as_str() {
            "acyclic_spawn" => (
                TASK_ADMIT_COMMAND_KIND,
                canonical::<TaskAdmitCommand>(arguments)?,
            ),
            "acyclic_send" | "acyclic_steer" => (
                MAIL_SEND_TASK_COMMAND_KIND,
                canonical::<MailSendTaskCommand>(arguments)?,
            ),
            "acyclic_receive" => (
                MAIL_RECEIVE_TASK_COMMAND_KIND,
                canonical::<MailReceiveTaskCommand>(arguments)?,
            ),
            "acyclic_wait" => (
                TASK_JOIN_COMMAND_KIND,
                canonical::<TaskJoinCommand>(arguments)?,
            ),
            "acyclic_cancel" => (
                TASK_CANCEL_COMMAND_KIND,
                canonical::<TaskCancelCommand>(arguments)?,
            ),
            "acyclic_inspect" => (
                TASK_INSPECT_COMMAND_KIND,
                canonical::<TaskInspectCommand>(arguments)?,
            ),
            _ => return Ok(None),
        };
        Ok(Some((kind, payload)))
    }
}

fn canonical<T: DeserializeOwned + Serialize>(value: &Value) -> Result<Value> {
    let decoded = T::deserialize(value).map_err(|error| Error::Invalid(error.to_string()))?;
    serde_json::to_value(decoded).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(feature = "test-support")]
pub(crate) fn fuzz_command(request: &ExternalRequest) {
    if let Ok(completion) = request.child_completion() {
        assert!(matches!(request.action, ExternalAction::Join { .. }));
        assert_eq!(
            completion.task_id(),
            request.child_attachment("").unwrap().task_id()
        );
        let task = crate::task_volume::Component::new(completion.task_id().to_string()).unwrap();
        match completion.to_file(task.clone()) {
            Ok(file) => assert_eq!(
                ExternalRequest::from_file(&task, &file).unwrap(),
                completion
            ),
            Err(crate::task_volume::Error::LimitExceeded) => {}
            Err(error) => panic!("invalid child completion: {error}"),
        }
    }
    if let Ok(Some((kind, payload))) = request.task_command() {
        if kind == TASK_ADMIT_COMMAND_KIND {
            if let Ok(input) = <TaskAdmitCommand as serde::Deserialize>::deserialize(&payload) {
                if let Ok(Some(command)) = input.initial_command(None) {
                    let encoded = serde_json::to_value(&command).unwrap();
                    assert_eq!(input.input["command"], encoded);
                }
            }
        }
        let mut replay = request.clone();
        if let ExternalAction::RunTool { arguments, .. } = &mut replay.action {
            *arguments = payload.clone();
        }
        assert_eq!(replay.task_command().unwrap(), Some((kind, payload)));
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
