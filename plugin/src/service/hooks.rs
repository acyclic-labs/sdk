//! Hooks for the service.
use super::*;

pub(crate) fn native_tool_identity(
    control: &mut ControlPlane,
    host: &str,
    input: &Value,
    cwd: &Path,
) -> Result<(String, String, WorkspaceRootId), String> {
    let agent_id = match hook_optional_id(input, "agent_id", "agentId")? {
        Some(agent_id) => Some(agent_id),
        None => codex_thread_agent(control, host, input)?,
    };
    if let Some(agent_id) = agent_id {
        let route = control
            .state
            .routes
            .get(&agent_id)
            .ok_or_else(|| format!("{host} tool reports an unknown subagent identity"))?
            .clone();
        if !route.lifecycle.accepts_tools() {
            return Err("subagent workspace is sealed after SubagentStop".to_owned());
        }
        let (cwd_caller, _, root_id) = control.route_root_from_cwd(cwd)?;
        let owns_reported_root = cwd_caller == agent_id
            || (cwd_caller == control.state.root_agent_id
                && route.roots.contains_key(&root_key(root_id)));
        if !owns_reported_root {
            return Err(format!(
                "{host} tool identity does not own its reported workspace cwd"
            ));
        }
        let turn_id = if host == "codex" {
            hook_id(input, "turn_id", "turnId")?
        } else {
            hook_optional_id(input, "turn_id", "turnId")?.unwrap_or_else(|| route.turn_id.clone())
        };
        if control.state.root_turns.contains(&turn_id)
            || control
                .state
                .turns
                .get(&turn_id)
                .is_some_and(|bound| bound != &agent_id)
        {
            return Err(format!(
                "{host} tool identity conflicts with its turn binding"
            ));
        }
        if control.state.turns.get(&turn_id) != Some(&agent_id) {
            control
                .state
                .turns
                .insert(turn_id.clone(), agent_id.clone());
            control.persist()?;
        }
        return Ok((agent_id, turn_id, root_id));
    }
    if host == "codex" {
        let turn_id = hook_id(input, "turn_id", "turnId")?;
        let caller = control.resolve_turn(&turn_id)?;
        let root_id = if caller == control.state.root_agent_id {
            let (cwd_caller, _, root_id) = control.route_root_from_cwd(cwd)?;
            if cwd_caller != caller {
                return Err("root tool cwd resolves to a child workspace".to_owned());
            }
            root_id
        } else {
            WorkspaceRootId::from_bytes(
                control
                    .state
                    .routes
                    .get(&caller)
                    .ok_or_else(|| "subagent route is missing".to_owned())?
                    .root_id,
            )
        };
        return Ok((caller, turn_id, root_id));
    }
    let (caller, route, root_id) = control.route_root_from_cwd(cwd)?;
    let turn_id = route.as_ref().map_or_else(
        || format!("{host}:root:{}", control.state.root_session_id),
        |route| route.turn_id.clone(),
    );
    if caller == control.state.root_agent_id {
        control.remember_root_turn(turn_id.clone());
    }
    Ok((caller, turn_id, root_id))
}

/// The spawned subagent a Codex tool event's calling thread is, if any.
///
/// Codex's command hooks carry `agent_id` only for spawned subagents, and a
/// spawned subagent's agent identity is its thread identity. Codex's MCP hook
/// transport cannot template a field that is present only sometimes, so it
/// carries the calling thread (`thread_id`) instead. A thread with a
/// registered route is therefore that agent, and any other thread (the root,
/// or an internal Codex subagent that never sends `SubagentStart`) has no agent
/// identity, exactly as on the command path.
pub(crate) fn codex_thread_agent(
    control: &ControlPlane,
    host: &str,
    input: &Value,
) -> Result<Option<String>, String> {
    if host != "codex" {
        return Ok(None);
    }
    Ok(hook_optional_id(input, "thread_id", "threadId")?
        .filter(|thread| control.state.routes.contains_key(thread)))
}

pub(crate) async fn dispatch_native_session_hook(
    control: &mut ControlPlane,
    host: &str,
    event: &str,
    input: Value,
    cwd: &Path,
) -> Result<Value, String> {
    let session_id = hook_id(&input, "session_id", "sessionId")?;
    let hook_cwd = hook_path(&input, "cwd").unwrap_or_else(|| cwd.to_path_buf());
    match event {
        "UserPromptSubmit" | "userPromptSubmitted" => {
            let (caller, _, _) = control.route_root_from_cwd(&hook_cwd)?;
            if caller != control.state.root_agent_id {
                return Err("root prompt hook originated inside a child mount".to_owned());
            }
            let turn_id = if host == "codex" {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .ok_or_else(|| "Codex prompt hook lacks a stable turn identity".to_owned())?
            } else {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .or(hook_optional_id(&input, "prompt_id", "promptId")?)
                    .unwrap_or_else(|| format!("{host}:root:{session_id}"))
            };
            control.user_prompt(json!({
                "session_id": session_id,
                "turn_id": turn_id
            }))
        }
        "PreToolUse" | "preToolUse" => {
            let (_, turn_id, root_id) = native_tool_identity(control, host, &input, &hook_cwd)?;
            let mut tool_name = hook_string(&input, "tool_name", "toolName")?;
            if host == "copilot" && tool_name == "task" {
                tool_name = "Agent".to_owned();
            }
            let tool_input = input
                .get("tool_input")
                .or_else(|| input.get("toolArgs"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let tool_use_id = native_hook_tool_id(&input, &session_id, &tool_name, &tool_input)?;
            // Like the request's own save, its authority writes survive a crash
            // of the service at once and a power loss once the next request
            // begins (see `ControlPlane::make_durable`).
            let output = acyclic_fs::deferring_authority_durability(control.pre_tool(json!({
                "session_id": session_id,
                "turn_id": turn_id,
                "tool_use_id": tool_use_id,
                "tool_name": tool_name,
                "tool_input": tool_input,
                "_caller_root_id": hex::encode(root_id.into_bytes())
            })))
            .await?;
            if host == "copilot" {
                let updated = output.pointer("/hookSpecificOutput/updatedInput").cloned();
                Ok(json!({
                    "permissionDecision": "allow",
                    "permissionDecisionReason": "Acyclic routed this tool to its workspace context",
                    "modifiedArgs": updated
                }))
            } else {
                Ok(output)
            }
        }
        "PostToolUse" | "postToolUse" | "PostToolUseFailure" | "postToolUseFailure" => {
            let (_, turn_id, _) = native_tool_identity(control, host, &input, &hook_cwd)?;
            let mut tool_name = hook_string(&input, "tool_name", "toolName")?;
            if host == "copilot" && tool_name == "task" {
                tool_name = "Agent".to_owned();
            }
            let tool_input = input
                .get("tool_input")
                .or_else(|| input.get("toolArgs"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let tool_use_id = native_hook_tool_id(&input, &session_id, &tool_name, &tool_input)?;
            acyclic_fs::deferring_authority_durability(control.post_tool(json!({
                "session_id": session_id,
                "turn_id": turn_id,
                "tool_use_id": tool_use_id,
                "tool_name": tool_name
            })))
            .await
        }
        "SubagentStart" | "subagentStart" => {
            let agent_id = hook_optional_id(&input, "agent_id", "agentId")?
                .or(hook_optional_id(&input, "agent_name", "agentName")?)
                .ok_or_else(|| "subagent start lacks a stable agent identity".to_owned())?;
            let turn_id = if host == "codex" {
                hook_id(&input, "turn_id", "turnId")?
            } else {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .unwrap_or_else(|| format!("{host}:agent:{agent_id}"))
            };
            let parent = control
                .state
                .routes
                .get(&agent_id)
                .map(|route| route.parent_agent_id.clone())
                .or_else(|| {
                    control
                        .state
                        .pending
                        .front()
                        .map(|pending| pending.parent_agent_id.clone())
                })
                .ok_or_else(|| {
                    "subagent start has no serialized spawn or resume identity".to_owned()
                })?;
            control
                .subagent_start(json!({
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "agent_id": agent_id,
                    "_authenticated_parent_agent_id": parent,
                    "agent_type": hook_optional_string(&input, "agent_type", "agentType").unwrap_or_else(|| "subagent".to_owned())
                }))
                .await
        }
        "SubagentStop" | "subagentStop" => {
            let agent_id = hook_optional_id(&input, "agent_id", "agentId")?
                .or(hook_optional_id(&input, "agent_name", "agentName")?)
                .ok_or_else(|| "subagent stop lacks a stable agent identity".to_owned())?;
            let turn_id = if host == "codex" {
                hook_id(&input, "turn_id", "turnId")?
            } else {
                hook_optional_id(&input, "turn_id", "turnId")?
                    .unwrap_or_else(|| format!("{host}:agent:{agent_id}"))
            };
            control
                .subagent_stop(json!({
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "agent_id": agent_id
                }))
                .await
        }
        "Stop" | "agentStop" => Ok(json!({})),
        "SessionStart" | "sessionStart" | "SessionEnd" | "sessionEnd" => {
            Err("session boundary hook reached a workspace actor".to_owned())
        }
        _ => Err(format!("unsupported {host} lifecycle event '{event}'")),
    }
}

pub(crate) fn hook_optional_string(input: &Value, snake: &str, camel: &str) -> Option<String> {
    input
        .get(snake)
        .or_else(|| input.get(camel))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub(crate) fn hook_string(input: &Value, snake: &str, camel: &str) -> Result<String, String> {
    hook_optional_string(input, snake, camel)
        .ok_or_else(|| format!("native hook is missing '{snake}'"))
}

pub(crate) fn hook_optional_id(
    input: &Value,
    snake: &str,
    camel: &str,
) -> Result<Option<String>, String> {
    hook_optional_string(input, snake, camel)
        .map(|value| validate_hook_id(snake, value))
        .transpose()
}

pub(crate) fn hook_id(input: &Value, snake: &str, camel: &str) -> Result<String, String> {
    hook_optional_id(input, snake, camel)?
        .ok_or_else(|| format!("native hook is missing '{snake}'"))
}

pub(crate) fn validate_hook_id(field: &str, value: String) -> Result<String, String> {
    const MAXIMUM_HOOK_ID_BYTES: usize = 512;
    if value.is_empty()
        || value.len() > MAXIMUM_HOOK_ID_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(format!(
            "native hook '{field}' must be 1-{MAXIMUM_HOOK_ID_BYTES} non-control bytes"
        ));
    }
    Ok(value)
}

pub(crate) fn hook_path(input: &Value, field: &str) -> Option<PathBuf> {
    input.get(field).and_then(Value::as_str).map(PathBuf::from)
}

pub(crate) fn hook_root_path(input: &Value) -> Option<PathBuf> {
    hook_path(input, "cwd").or_else(|| {
        input
            .get("workspace_roots")
            .and_then(Value::as_array)
            .and_then(|roots| roots.first())
            .and_then(Value::as_str)
            .map(PathBuf::from)
    })
}

pub(crate) fn native_tool_id(session_id: &str, tool_name: &str, tool_input: &Value) -> String {
    let mut digest = blake3::Hasher::new();
    digest.update(session_id.as_bytes());
    digest.update(&[0]);
    digest.update(tool_name.as_bytes());
    digest.update(&[0]);
    if let Ok(encoded) = serde_json::to_vec(tool_input) {
        digest.update(&encoded);
    }
    format!("native:{}", digest.finalize().to_hex())
}

pub(crate) fn native_hook_tool_id(
    input: &Value,
    session_id: &str,
    tool_name: &str,
    tool_input: &Value,
) -> Result<String, String> {
    if let Some(id) = hook_optional_id(input, "tool_use_id", "toolUseId")? {
        return Ok(id);
    }
    if is_filesystem_tool(tool_name) {
        return Err(format!(
            "filesystem tool '{tool_name}' is missing a stable tool-use identity"
        ));
    }
    Ok(native_tool_id(session_id, tool_name, tool_input))
}

pub(crate) fn native_hook_is_process_local_noop(host: &str, event: &str, input: &Value) -> bool {
    matches!(host, "codex" | "claude-code" | "copilot" | "cursor")
        && matches!(
            event,
            "PreToolUse"
                | "preToolUse"
                | "PostToolUse"
                | "postToolUse"
                | "PostToolUseFailure"
                | "postToolUseFailure"
        )
        && hook_optional_string(input, "tool_name", "toolName")
            .is_some_and(|tool| is_known_non_filesystem_tool(&tool))
}
