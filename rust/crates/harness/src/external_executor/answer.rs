use super::{ExternalHost, HookAction, hooks, volume};
use crate::{Error, Result};
use serde_json::Value;

/// Decodes a native hook answer, requiring an explicit tool authorization.
/// A malformed result file must never become the host's implicit permission.
pub fn hook_answer(host: ExternalHost, event: &str, bytes: &[u8]) -> Result<Value> {
    validate(host, event, volume::decode(bytes).map_err(|_| invalid())?)
}

pub(super) fn validate(host: ExternalHost, event: &str, answer: Value) -> Result<Value> {
    let spec = hooks(host)
        .find(|spec| spec.event == event)
        .ok_or_else(invalid)?;
    if !answer.is_object() {
        return Err(invalid());
    }
    if answer.get("Failed").is_some() {
        return match serde::Deserialize::deserialize(&answer) {
            Ok(crate::Outcome::<Value>::Failed { message }) => Err(Error::Invalid(message)),
            _ => Err(invalid()),
        };
    }
    if spec.action == HookAction::RunTool {
        let input = if host == ExternalHost::Pi {
            if answer.get("block").and_then(Value::as_bool).is_none()
                || answer
                    .get("reason")
                    .is_some_and(|reason| !reason.is_string())
            {
                return Err(invalid());
            }
            answer.get("updatedInput")
        } else {
            let output = answer.get("hookSpecificOutput").ok_or_else(invalid)?;
            if output.get("hookEventName").and_then(Value::as_str) != Some("PreToolUse")
                || !matches!(
                    output.get("permissionDecision").and_then(Value::as_str),
                    Some("allow" | "deny" | "ask")
                )
                || output
                    .get("permissionDecisionReason")
                    .is_some_and(|reason| !reason.is_string())
            {
                return Err(invalid());
            }
            output.get("updatedInput")
        };
        if input.is_some_and(|input| !input.is_object()) {
            return Err(invalid());
        }
    }
    Ok(answer)
}

fn invalid() -> Error {
    Error::Invalid("invalid native hook answer or missing explicit tool decision".into())
}

/// Acknowledgement records observation only; the native host owns this effect.
pub(super) fn host_tool(
    host: ExternalHost,
    event: &str,
    request: &super::ExternalRequest,
    answer: Value,
) -> Result<Value> {
    if answer.get("task") != Some(&serde_json::json!(request.task_id()))
        || answer.get("operation_id") != Some(&serde_json::json!(request.operation_id))
    {
        return Err(invalid());
    }
    validate(
        host,
        event,
        if host == ExternalHost::Pi {
            serde_json::json!({"block":false})
        } else {
            serde_json::json!({"hookSpecificOutput":{
                "hookEventName":"PreToolUse", "permissionDecision":"allow"
            }})
        },
    )
}
