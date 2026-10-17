//! A native handoff names an immutable request file, never a second execution.
use super::{ExternalAction, ExternalHost, ExternalRequest};
use crate::{
    Error, Result,
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
};
use acyclic_stream::StreamProvider;
use serde_json::{Value, json};

const FIELD: &str = "_acyclic";

pub(super) fn normalize_tool(host: ExternalHost, event: &str, input: &mut Value) -> Result<bool> {
    if host == ExternalHost::Pi || event != "PreToolUse" {
        return Ok(false);
    }
    let name_field = if input.get("tool_name").is_some() {
        "tool_name"
    } else {
        "toolName"
    };
    let Some(name) = input
        .get(name_field)
        .and_then(Value::as_str)
        .and_then(|name| {
            name.strip_prefix("mcp__acyclic__")
                .or_else(|| name.strip_prefix("mcp__plugin_acyclic_acyclic__"))
        })
    else {
        return Ok(false);
    };
    let name = name.to_owned();
    let object = input.as_object_mut().ok_or_else(invalid)?;
    object.insert(name_field.into(), Value::String(name));
    let arguments_field = if object.contains_key("tool_input") {
        "tool_input"
    } else {
        "input"
    };
    if let Some(arguments) = object
        .get_mut(arguments_field)
        .and_then(Value::as_object_mut)
    {
        arguments.remove(FIELD);
    }
    Ok(true)
}

pub(super) fn answer(request: ExternalRequest, task: Component, result: &Value) -> Result<Value> {
    if !result
        .as_object()
        .is_some_and(|object| object.len() == 1 && object.contains_key("Succeeded"))
    {
        return Err(Error::Invalid(
            result
                .pointer("/Failed/message")
                .and_then(Value::as_str)
                .unwrap_or("harness tool has no successful result")
                .into(),
        ));
    }
    let path = request.request_key(task).map_err(|_| invalid())?.path();
    let ExternalAction::RunTool { arguments, .. } = request.action else {
        return Err(invalid());
    };
    let Value::Object(mut arguments) = arguments else {
        return Err(invalid());
    };
    arguments.insert(FIELD.into(), Value::String(path));
    let mut answer =
        json!({"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow"}});
    answer["hookSpecificOutput"]["updatedInput"] = Value::Object(arguments);
    Ok(answer)
}

pub(crate) fn file_key(arguments: &Value) -> Result<FileKey> {
    FileKey::parse(reference(arguments)?.1).map_err(|_| invalid())
}

pub(super) fn workspace_key(key: &str) -> Result<()> {
    if key.len() == 64
        && key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(invalid())
    }
}

pub(super) fn reference(arguments: &Value) -> Result<(Option<&str>, &str)> {
    let path = arguments
        .get(FIELD)
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    match path.strip_prefix("workspaces/") {
        Some(path) => {
            let (workspace, path) = path.split_once('/').ok_or_else(invalid)?;
            workspace_key(workspace)?;
            Ok((Some(workspace), path))
        }
        None => Ok((None, path)),
    }
}

pub(super) fn qualify(answer: &mut Value, workspace: &str) -> Result<()> {
    workspace_key(workspace)?;
    let field = answer
        .pointer_mut("/hookSpecificOutput/updatedInput/_acyclic")
        .ok_or_else(invalid)?;
    let path = field.as_str().ok_or_else(invalid)?;
    *field = Value::String(format!("workspaces/{workspace}/{path}"));
    Ok(())
}

#[cfg(feature = "test-support")]
pub(super) fn fuzz(bytes: &[u8]) {
    if let Ok(arguments) = serde_json::from_slice::<Value>(bytes) {
        if let Ok((workspace, path)) = reference(&arguments) {
            if let Some(workspace) = workspace {
                workspace_key(workspace).unwrap();
            }
            if let Ok(key) = file_key(&arguments) {
                assert_eq!(key.path(), path);
            }
        }
    }
}

/// Checks a native handoff against its original immutable request file.
/// The result client can only follow that request's result, never execute it.
/// Qualified references must match the authenticated provider's workspace.
pub fn native_tool_request(
    name: &str,
    arguments: Value,
    file: &FileValue,
    workspace: Option<&str>,
) -> Result<ExternalRequest> {
    let key = bound_key(&arguments, workspace)?;
    verify_request(name, arguments, file, &key)
}

pub(crate) fn bound_key(arguments: &Value, workspace: Option<&str>) -> Result<FileKey> {
    let (scope, path) = reference(arguments)?;
    if scope.is_some() && scope != workspace {
        return Err(invalid());
    }
    FileKey::parse(path).map_err(|_| invalid())
}

fn verify_request(
    name: &str,
    arguments: Value,
    file: &FileValue,
    key: &FileKey,
) -> Result<ExternalRequest> {
    let FileKey::Task {
        task,
        file: TaskFile::Request(_),
    } = key
    else {
        return Err(invalid());
    };
    if &file.key != key {
        return Err(invalid());
    }
    let request = ExternalRequest::from_file(task, file).map_err(|_| invalid())?;
    let Value::Object(mut arguments) = arguments else {
        return Err(invalid());
    };
    arguments.remove(FIELD);
    let ExternalAction::RunTool {
        name: original,
        arguments: input,
        ..
    } = &request.action
    else {
        return Err(invalid());
    };
    if original != name || *input != Value::Object(arguments) {
        return Err(invalid());
    }
    Ok(request)
}

pub(crate) async fn read_result<P: StreamProvider>(
    name: &str,
    arguments: Value,
    requests: &TaskVolume<P>,
    results: &TaskVolume<P>,
    workspace: Option<&str>,
) -> Result<Value> {
    let query = Query::File(bound_key(&arguments, workspace)?);
    let source = requests
        .snapshot(&query, 1)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let file = source.files.first().ok_or_else(invalid)?;
    let Query::File(key) = &query else {
        return Err(invalid());
    };
    let request = verify_request(name, arguments, file, key)?;
    let FileKey::Task { task, .. } = &file.key else {
        return Err(invalid());
    };
    request
        .follow_result(results, task.clone())
        .await
        .map_err(|e| Error::Storage(e.to_string()))
}

pub(super) fn schema(mut schema: Value) -> Value {
    if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        properties.insert(
            FIELD.into(),
            json!({"type":"string","description":"Filled by the native hook; omit this field."}),
        );
    }
    schema
}

fn invalid() -> Error {
    Error::Unauthorized("MCP arguments differ from their native request".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn near_record_limit_tool_uses_a_constant_size_file_reference()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let request = super::super::request(
            ExternalHost::Codex,
            "PreToolUse",
            json!({
                "session_id":"boundary","tool_use_id":"call","tool_name":"bash",
                "tool_input":{"command":"x".repeat(acyclic_stream::MAX_RECORD_BYTES-2048)},
            }),
        )?;
        let task = Component::new(request.task_id().to_string())?;
        let file = request.to_file(task.clone())?;
        assert!(file.bytes.len() > acyclic_stream::MAX_RECORD_BYTES - 2048);
        let mut answer = answer(request.clone(), task, &json!({"Succeeded":{}}))?;
        let workspace = "0".repeat(64);
        qualify(&mut answer, &workspace)?;
        let arguments = answer["hookSpecificOutput"]["updatedInput"].clone();
        assert!(serde_json::to_vec(&arguments[FIELD])?.len() < 256);
        assert_eq!(
            native_tool_request("bash", arguments, &file, Some(&workspace))?,
            request
        );
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // A qualified file keeps the exact task key, request and arguments;
        // routing rejects noncanonical workspace components before any I/O.
        #[test]
        fn qualified_native_references_preserve_the_request(workspace in any::<[u8;32]>(), command in ".{0,32}") {
            let workspace = hex::encode(workspace);
            let request = super::super::request(ExternalHost::Codex,"PreToolUse",json!({
                "session_id":"qualified","tool_use_id":"call","tool_name":"bash","tool_input":{"command":command},
            }))?;
            let task = Component::new(request.task_id().to_string())?;
            let file = request.to_file(task.clone())?;
            let mut answer = answer(request.clone(),task,&json!({"Succeeded":null}))?;
            qualify(&mut answer,&workspace)?;
            let arguments = &answer["hookSpecificOutput"]["updatedInput"];
            let (scope,path) = reference(arguments)?;
            prop_assert_eq!(scope,Some(workspace.as_str()));
            prop_assert_eq!(path,file.key.path());
            prop_assert_eq!(native_tool_request("bash",arguments.clone(),&file,Some(&workspace))?,request);
            prop_assert!(native_tool_request("bash",arguments.clone(),&file,None).is_err());
            let mut other = workspace.clone();
            other.replace_range(..1, if workspace.starts_with('a') { "b" } else { "a" });
            prop_assert!(native_tool_request("bash",arguments.clone(),&file,Some(&other)).is_err());
            for invalid in ["..", "a/b", "owner", "X", ""] {
                let mut arguments = arguments.clone();
                arguments[FIELD] = json!(format!("workspaces/{invalid}/{path}"));
                prop_assert!(file_key(&arguments).is_err());
            }
        }
        // Native spelling aliases must select the same harness request and
        // immutable result; a foreign namespace must remain host-executed.
        #[test]
        fn native_namespace_aliases_preserve_the_request(
            session in "[a-z]{1,16}", command in ".{0,64}",
            camel_name in any::<bool>(), camel_input in any::<bool>(),
        ) {
            for host in [ExternalHost::Codex,ExternalHost::ClaudeCode] {
                for prefix in ["mcp__acyclic__", "mcp__plugin_acyclic_acyclic__"] {
                    let mut input = json!({"session_id":session,"tool_use_id":"call"});
                    input[if camel_name {"toolName"} else {"tool_name"}] = json!(format!("{prefix}bash"));
                    input[if camel_input {"input"} else {"tool_input"}] = json!({"command":command,"_acyclic":"forged"});
                    let (request,mcp) = super::super::native::prepare_hook(host,"PreToolUse",input.clone())?;
                    prop_assert!(mcp);
                    let canonical = super::super::request(host,"PreToolUse",json!({
                        "session_id":session,"tool_use_id":"call","tool_name":"bash","tool_input":{"command":command}
                    }))?;
                    prop_assert_eq!(&request,&canonical);
                    let task = Component::new(request.task_id().to_string())?;
                    let file = request.to_file(task.clone())?;
                    let answer = answer(request.clone(),task,&json!({"Succeeded":null}))?;
                    prop_assert_eq!(native_tool_request("bash",answer["hookSpecificOutput"]["updatedInput"].clone(),&file,None)?,request);
                    input[if camel_name {"toolName"} else {"tool_name"}] = json!("mcp__foreign__bash");
                    let (foreign,mcp) = super::super::native::prepare_hook(host,"PreToolUse",input)?;
                    prop_assert!(!mcp);
                    prop_assert!(matches!(foreign.action,ExternalAction::HostTool {..}), "foreign tools must remain host-executed");
                }
            }
        }

        // Property: handoffs preserve request identity and every argument;
        // changing the tool name, path or arguments cannot change the effect.
        #[test]
        fn handoff_round_trip_binds_every_argument(session in "[a-z]{1,16}",command in ".{1,64}") {
            for host in [ExternalHost::Codex,ExternalHost::ClaudeCode] {
                let request = super::super::request(host,"PreToolUse",json!({
                    "session_id":session,"tool_use_id":"call","tool_name":"bash","tool_input":{"command":command},
                }))?;
                let task = Component::new(request.task_id().to_string())?;
                let file = request.to_file(task.clone())?;
                let handed_off = request.clone();
                let ExternalAction::RunTool {arguments: input,..} = &handed_off.action else {unreachable!()};
                let payload = input["command"].as_str().unwrap().as_ptr();
                let answer = answer(handed_off,task,&json!({"Succeeded":{"stdout":"result"}}))?;
                prop_assert_eq!(answer["hookSpecificOutput"]["updatedInput"]["command"].as_str().unwrap().as_ptr(),payload);
                let arguments = answer["hookSpecificOutput"]["updatedInput"].clone();
                prop_assert_eq!(native_tool_request("bash",arguments.clone(),&file,None)?,request.clone());
                prop_assert!(native_tool_request("write",arguments.clone(),&file,None).is_err());
                let mut changed = arguments.clone();
                changed["command"] = json!({"different":true});
                prop_assert!(native_tool_request("bash",changed,&file,None).is_err());
                let mut changed = arguments;
                changed[FIELD] = json!("tasks/another/requests/another.json");
                prop_assert!(native_tool_request("bash",changed,&file,None).is_err());
            }
        }
    }
}
