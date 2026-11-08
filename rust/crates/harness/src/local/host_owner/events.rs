//! Session boundaries only. Native subagents and native tools are unmanaged.
use super::agents::Event;
use super::*;

impl HostWorkspaceOwner {
    pub(super) async fn hook(
        &self,
        operation: OperationId,
        host: &str,
        event: &str,
        input: Value,
    ) -> Result<Value> {
        if event == "PermissionRequest" {
            return Ok(self.permission(host, &input));
        }
        if !matches!(
            event,
            "SessionStart" | "Stop" | "SessionEnd" | "NativeSession"
        ) {
            return Ok(json!({}));
        }
        let agent = if let Some(task) = input.get("task") {
            serde_json::from_value::<AgentPath>(task.clone()).map_err(storage)?
        } else {
            let session = input
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("native session identity absent".into()))?;
            self.open(host, session).await?
        };
        // Its boundary commits and capture are serial with the agent's other
        // effects, never with unrelated agents.
        let _lock = self.locks.hold(vec![agent.clone()]).await;
        // A managed boundary is an acknowledgement, not view admission or
        // capture. Only its physical process owner can retire and capture it.
        if agent.depth() != 0 && matches!(event, "Stop" | "SessionEnd") {
            return Ok(json!({}));
        }
        if event == "NativeSession" {
            if let Some(session) = input.get("native_session") {
                let object = session.as_object().ok_or_else(|| {
                    Error::Invalid("native session reference must be an object".into())
                })?;
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Invalid("native session id absent".into()))?;
                if id.is_empty()
                    || id.len() > 256
                    || id.chars().any(char::is_control)
                    || object
                        .keys()
                        .any(|key| !matches!(key.as_str(), "id" | "file"))
                    || object.get("file").is_some_and(|value| {
                        !value.is_null()
                            && !value
                                .as_str()
                                .is_some_and(|file| Path::new(file).is_absolute())
                    })
                {
                    return Err(Error::Invalid("invalid native session reference".into()));
                }
                self.native
                    .request(
                        &agent,
                        operation,
                        &Event::Native {
                            session: session.clone(),
                        },
                    )
                    .await?;
                return Ok(session.clone());
            }
            return self
                .native
                .session(&agent)
                .await?
                .1
                .native
                .ok_or_else(|| Error::NotFound("native session reference absent".into()));
        }
        self.attach(&agent).await?;
        if matches!(event, "Stop" | "SessionEnd") {
            self.view(&agent)?.capture(operation).await?;
            return Ok(json!({}));
        }
        Ok(
            json!({"hookSpecificOutput":{"hookEventName":event,"additionalContext":format!("Harness agent {agent}. Use Acyclic spawn/send/wait/archive for isolated sessions and acyclic fs merge agents/<ref> to merge. Native subagents retain their host behavior and are not harness forks.")}}),
        )
    }

    /// A host's permission prompt for harness spawn, answered by the
    /// configured approval chain in order: a rules
    /// stage allows, denies or escalates; exhausting the chain refuses. A
    /// human, model or parent stage leaves the prompt to the host, whose own
    /// approval then applies. Other tools never reach the chain: their
    /// prompts stay the host's. Fork, merge and commit use the workspace
    /// boundary, not this MCP hook. No agent, file or request beyond the hook.
    pub(super) fn permission(&self, host: &str, input: &Value) -> Value {
        use crate::approvals::{ApprovalRule, Approver, RuleVerdict};
        let Some(tool) = input
            .get("tool_name")
            .and_then(Value::as_str)
            .and_then(|name| gated_tool(host, name))
        else {
            return json!({});
        };
        let arguments = input.get("tool_input").unwrap_or(&Value::Null);
        let mut verdict = RuleVerdict::Deny;
        for approver in &self.local.preset.approval_policy.approvers {
            let Approver::Rules {
                rules, fallback, ..
            } = approver
            else {
                return json!({});
            };
            verdict = ApprovalRule::evaluate(rules, *fallback, tool, arguments);
            if verdict != RuleVerdict::Ask {
                break;
            }
        }
        let decision = if verdict == RuleVerdict::Allow {
            json!({"behavior": "allow"})
        } else {
            json!({"behavior": "deny", "message": format!("the harness approval policy refuses {tool}")})
        };
        json!({"hookSpecificOutput": {"hookEventName": "PermissionRequest", "decision": decision}})
    }

    pub(super) async fn locate(&self, cwd: &Path) -> Result<Value> {
        // The client resolves its own cwd. This owner serves the views a cwd
        // names, and resolving one here would wait on its own callbacks.
        if !cwd.is_absolute()
            || cwd.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::CurDir | std::path::Component::ParentDir
                )
            })
        {
            return Err(Error::Invalid("native cwd is not canonical".into()));
        }
        // A native session enters only after its view has been attached. Cwd
        // discovery observes those held views; it never reconstructs the
        // agent tree or acquires unrelated views as a side effect of a read.
        let mut selected = self.attached()?;
        selected.retain(|(_, view)| cwd.starts_with(view.path()));
        selected.sort_by_key(|(_, view)| std::cmp::Reverse(view.path().components().count()));
        let Some((agent, first)) = selected.first() else {
            return Err(Error::NotFound(
                "no native agent selects this directory".into(),
            ));
        };
        if selected
            .get(1)
            .is_some_and(|(_, second)| second.path() == first.path())
        {
            return Err(Error::Conflict(
                "multiple native sessions select this directory; use --task".into(),
            ));
        }
        if cwd.starts_with(first.path().join(host_workspace::HOST_VIEW_ROOT)) {
            return Err(Error::NotFound(
                "native cwd selects no held agent view".into(),
            ));
        }
        first.verify().await?;
        self.location(agent)
    }
}

/// Only spawn is gated among the tools the native MCP server advertises.
/// Match the host's exact server namespace before interpreting the tool.
fn gated_tool<'a>(host: &str, name: &'a str) -> Option<&'a str> {
    let servers: &[&str] = match host {
        "codex" => &["mcp__acyclic-sessions__", "mcp__acyclic_sessions__"],
        "claude-code" => &["mcp__plugin_acyclic_acyclic-sessions__"],
        _ => &[],
    };
    servers
        .iter()
        .find_map(|server| name.strip_prefix(server))
        .filter(|tool| *tool == "spawn")
}
