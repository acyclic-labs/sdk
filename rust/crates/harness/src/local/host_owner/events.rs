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
        if agent.parent().is_some() && matches!(event, "Stop" | "SessionEnd") {
            return Ok(json!({}));
        }
        self.attach(&agent).await?;
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
        if matches!(event, "Stop" | "SessionEnd") {
            self.view(&agent)?.capture(operation).await?;
            return Ok(json!({}));
        }
        Ok(
            json!({"hookSpecificOutput":{"hookEventName":event,"additionalContext":format!("Harness agent {agent}. Use Acyclic spawn/send/wait/archive for isolated sessions and acyclic fs merge agents/<ref> to merge. Native subagents retain their host behavior and are not harness forks.")}}),
        )
    }

    pub(super) async fn locate(&self, cwd: &Path) -> Result<Value> {
        let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
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
