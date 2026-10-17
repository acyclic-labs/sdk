use super::{ChildSession, ExternalAction, ExternalHost, ExternalRequest, HookAction, hooks};
use crate::{Error, OperationId, Result};
use serde::Deserialize;
use serde_json::{Map, Value};

/// Converts a hook without retaining native host state or unrelated input fields.
pub fn request(host: ExternalHost, event: &str, input: Value) -> Result<ExternalRequest> {
    let spec = hooks(host)
        .find(|spec| spec.event == event)
        .ok_or_else(|| Error::Invalid(format!("unknown {} hook: {event}", host.name())))?;
    let Value::Object(mut input) = input else {
        return Err(Error::Invalid(
            "external hook input must be an object".into(),
        ));
    };
    let session_id = text(&mut input, &["session_id", "sessionId"])?;
    let turn_id = optional_text(&mut input, &["turn_id", "turnId"])?;
    let caller = optional_text(
        &mut input,
        match spec.action {
            HookAction::Fork | HookAction::Join => &["parent_agent_id", "parentAgentId"],
            _ => &["agent_id", "agentId", "thread_id", "threadId"],
        },
    )?;
    let action = match spec.action {
        HookAction::Attach => ExternalAction::Attach {
            workspace: text(&mut input, &["cwd"])?,
        },
        HookAction::RunTool => tool(&mut input)?,
        HookAction::CancelTool => ExternalAction::CancelTool {
            call_id: text(&mut input, &["tool_use_id", "toolCallId"])?,
        },
        HookAction::ToolFinished => {
            let failed = failed(&mut input, host == ExternalHost::Pi)?;
            let result = match (host, event) {
                (_, "PostToolUseFailure") => {
                    let Some(Value::String(error)) = input.remove("error") else {
                        return Err(Error::Invalid(
                            "external tool failure must include its error text".into(),
                        ));
                    };
                    Some(Value::String(error))
                }
                (ExternalHost::Pi, _) => {
                    let Some(content @ Value::Array(_)) = input.remove("content") else {
                        return Err(Error::Invalid("Pi tool result must include content".into()));
                    };
                    let mut result = Map::new();
                    result.insert("content".into(), content);
                    for field in ["details", "structuredContent", "usage"] {
                        if let Some(value) = input.remove(field) {
                            result.insert(field.into(), value);
                        }
                    }
                    Some(Value::Object(result))
                }
                _ => value(&mut input, &["tool_response", "result"])?,
            };
            ExternalAction::ToolFinished {
                call_id: text(&mut input, &["tool_use_id", "toolCallId"])?,
                failed: event == "PostToolUseFailure" || failed,
                result,
            }
        }
        HookAction::Fork | HookAction::Join => {
            let child_id = text(&mut input, &["agent_id", "agentId"])?;
            let child_session = child_session(&mut input, host, &session_id, &child_id)?;
            if spec.action == HookAction::Fork {
                ExternalAction::Fork {
                    child_id,
                    child_session,
                    kind: optional_text(&mut input, &["agent_type", "agentType"])?,
                }
            } else {
                ExternalAction::Join {
                    child_id,
                    child_session,
                }
            }
        }
        HookAction::Detach => ExternalAction::Detach,
    };
    let operation_id = operation(host, &session_id, &caller, &action);
    // Session attachment is independent of whichever turn the host has open.
    let turn_id = if matches!(action, ExternalAction::Attach { .. }) {
        None
    } else {
        turn_id
    };
    Ok(ExternalRequest {
        operation_id,
        host,
        session_id,
        turn_id,
        caller,
        action,
    })
}

fn child_session(
    input: &mut Map<String, Value>,
    host: ExternalHost,
    session: &str,
    child: &str,
) -> Result<ChildSession> {
    let target = match value(input, &["child_session", "childSession"])? {
        Some(value) => {
            ChildSession::deserialize(value).map_err(|error| Error::Invalid(error.to_string()))?
        }
        None => ChildSession {
            host,
            session_id: session.into(),
            caller: Some(child.into()),
        },
    };
    if !target.valid() {
        return Err(Error::Invalid("invalid native child session".into()));
    }
    Ok(target)
}

fn value(input: &mut Map<String, Value>, fields: &[&str]) -> Result<Option<Value>> {
    let mut found = None;
    for field in fields {
        if let Some(value) = input.remove(*field) {
            if found.is_some() {
                return Err(Error::Invalid(format!(
                    "duplicate external hook field: {field}"
                )));
            }
            found = Some(value);
        }
    }
    Ok(found)
}

fn optional_text(input: &mut Map<String, Value>, fields: &[&str]) -> Result<Option<String>> {
    match value(input, fields)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if super::request::valid_text(&text) => Ok(Some(text)),
        _ => Err(Error::Invalid("invalid external hook text field".into())),
    }
}

fn text(input: &mut Map<String, Value>, fields: &[&str]) -> Result<String> {
    optional_text(input, fields)?
        .ok_or_else(|| Error::Invalid("required external hook field is missing".into()))
}

fn failed(input: &mut Map<String, Value>, required: bool) -> Result<bool> {
    match input.remove("isError") {
        None if !required => Ok(false),
        Some(Value::Bool(false)) => Ok(false),
        Some(Value::Bool(true)) => Ok(true),
        _ => Err(Error::Invalid(
            "external tool failure flag must be a boolean".into(),
        )),
    }
}

pub(super) fn operation(
    host: ExternalHost,
    session: &str,
    caller: &Option<String>,
    action: &ExternalAction,
) -> OperationId {
    let (kind, invocation) = match action {
        ExternalAction::Attach { .. } => ("attach", ""),
        ExternalAction::RunTool { call_id, .. } | ExternalAction::HostTool { call_id, .. } => {
            ("tool", call_id.as_str())
        }
        ExternalAction::CancelTool { call_id } => ("tool-cancel", call_id.as_str()),
        ExternalAction::ToolFinished { call_id, .. } => ("tool-finished", call_id.as_str()),
        ExternalAction::Fork { child_id, .. } => {
            return child_start_operation(host, session, caller, child_id);
        }
        ExternalAction::Join { child_id, .. } => ("join", child_id.as_str()),
        ExternalAction::Detach => ("detach", ""),
    };
    identity(host, session, caller, kind, invocation)
}

pub(super) fn child_start_operation(
    host: ExternalHost,
    session: &str,
    caller: &Option<String>,
    label: &str,
) -> OperationId {
    identity(host, session, caller, "fork", label)
}

fn identity(
    host: ExternalHost,
    session: &str,
    caller: &Option<String>,
    kind: &str,
    invocation: &str,
) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.external-executor.request/v1\0");
    for field in [
        host.name(),
        session,
        caller.as_deref().unwrap_or(""),
        kind,
        invocation,
    ] {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field.as_bytes());
    }
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn tool(input: &mut Map<String, Value>) -> Result<ExternalAction> {
    let host_executed = match input.remove("hostExecuted") {
        None | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        _ => {
            return Err(Error::Invalid(
                "host execution flag must be a boolean".into(),
            ));
        }
    };
    let call_id = text(input, &["tool_use_id", "toolCallId"])?;
    let name = text(input, &["tool_name", "toolName"])?;
    let arguments = value(input, &["tool_input", "input"])?
        .ok_or_else(|| Error::Invalid("external tool arguments are missing".into()))?;
    Ok(if host_executed {
        ExternalAction::HostTool {
            call_id,
            name,
            arguments,
        }
    } else {
        ExternalAction::RunTool {
            call_id,
            name,
            arguments,
        }
    })
}
