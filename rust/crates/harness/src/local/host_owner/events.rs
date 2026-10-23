//! Native lifecycle identities live in ordinary task context/tool files.
//! There is no second task tree, process registry, provider or execution loop.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, TaskFile, ToolFile};

fn text<'a>(input: &'a Value, field: &str) -> Result<&'a str> {
    input
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Invalid(format!("native event lacks {field}")))
}

fn identity(prefix: &str, value: &str) -> String {
    format!("{prefix}-{}", blake3::hash(value.as_bytes()).to_hex())
}

#[derive(Clone, Copy)]
struct NativeEvent<'a> {
    operation: OperationId,
    host: &'a str,
    event: &'a str,
    input: &'a Value,
    task: TaskId,
    root: TaskId,
    records: &'a [Value],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeSessionRef {
    id: String,
    file: Option<PathBuf>,
}

impl HostWorkspaceOwner {
    pub(super) async fn hook(
        &mut self,
        operation: OperationId,
        host: &str,
        event: &str,
        input: Value,
    ) -> Result<Value> {
        let session = text(&input, "session_id")?;
        let root_operation = context::publication_operation(
            b"acyclic.native-root/v1\0",
            &serde_json::to_vec(&(host, session)).map_err(storage)?,
        );
        let root = self
            .local
            .admit_host_task(root_operation, host, session)
            .await?;
        self.attach(root).await?;
        if event == "SessionStart" {
            return self.context_answer(root, event);
        }
        let records = self.records(root).await?;
        if host == "claude-code" && event == "WorktreeCreate" {
            return self.create_worktree(root, &input, &records).await;
        }
        if host == "claude-code" && event == "WorktreeRemove" {
            return self.remove_worktree(operation, &input).await;
        }
        if event == "SubagentStart" {
            return self.bind_child(root, host, event, &input, &records).await;
        }
        let task = if host == "pi" && input.get("task").is_some() {
            let task: TaskId =
                serde_json::from_value(input.get("task").unwrap_or(&Value::Null).clone())
                    .map_err(storage)?;
            let mut ancestor = task;
            while ancestor != root {
                ancestor = self.require_native(ancestor).await?.ok_or_else(|| {
                    Error::Unauthorized("native task belongs to another root".into())
                })?;
            }
            task
        } else {
            self.caller(root, session, &input, &records)?
        };
        self.attach(task).await?;
        let frame = NativeEvent {
            operation,
            host,
            event,
            input: &input,
            task,
            root,
            records: &records,
        };
        match event {
            "UserPromptSubmit" => {
                if let Some(turn) = input.get("turn_id").and_then(Value::as_str) {
                    self.put_record(
                        root,
                        &identity("turn", turn),
                        json!({"kind":"turn","turn":turn,"task":task}),
                    )
                    .await?;
                }
                self.context_answer(task, event)
            }
            "PreToolUse" => self.pre_tool(frame).await,
            "PostToolUse" | "PostToolUseFailure" => self.post_tool(frame).await,
            "NativeSession" if host == "pi" => self.native_session(task, &input).await,
            "SubagentStop" | "SessionEnd" => {
                self.quiescent(task).await?;
                let attached = self.attached(task)?;
                attached
                    .workspace
                    .capture(&attached.context, operation)
                    .await?;
                // Native stop can be followed by native resume; retain the task
                // and its lazy view. Cancellation is an explicit shared intent.
                Ok(json!({}))
            }
            _ => Err(Error::Unsupported(format!(
                "native lifecycle event {event}"
            ))),
        }
    }

    async fn native_session(&self, task: TaskId, input: &Value) -> Result<Value> {
        if let Some(session) = input.get("native_session") {
            // This is an opaque native host reference in ordinary task files.
            // The harness never opens or copies the native transcript.
            let session: NativeSessionRef =
                serde_json::from_value(session.clone()).map_err(storage)?;
            if session.id.is_empty()
                || session.id.len() > 256
                || session
                    .file
                    .as_ref()
                    .is_some_and(|path| !path.is_absolute())
            {
                return Err(Error::Invalid("native session reference is invalid".into()));
            }
            let value = serde_json::to_value(session).map_err(storage)?;
            self.put_record(task, "session", value.clone()).await?;
            Ok(value)
        } else {
            self.record(task, "session")
                .await?
                .ok_or_else(|| Error::NotFound("native session reference is absent".into()))
        }
    }

    async fn create_worktree(
        &mut self,
        root: TaskId,
        input: &Value,
        records: &[Value],
    ) -> Result<Value> {
        let name = text(input, "name")?;
        let key = identity("worktree", name);
        let task = if let Some(record) = self.record(root, &key).await? {
            serde_json::from_value(record.get("task").unwrap_or(&Value::Null).clone())
                .map_err(storage)?
        } else {
            let reserved = records
                .iter()
                .filter(|value| value.get("kind").unwrap_or(&Value::Null) == "worktree")
                .map(|value| value.get("task").unwrap_or(&Value::Null).clone())
                .collect::<Vec<_>>();
            let pending = records
                .iter()
                .filter(|value| {
                    value.get("kind").unwrap_or(&Value::Null) == "spawn"
                        && !reserved.contains(value.get("task").unwrap_or(&Value::Null))
                })
                .collect::<Vec<_>>();
            let selected = pending.first().ok_or_else(|| {
                Error::Conflict("native worktree has no prepared child fork".into())
            })?;
            if pending.len() != 1 {
                return Err(Error::Conflict(
                    "native worktree identity is ambiguous; wait for its pending spawn to bind"
                        .into(),
                ));
            }
            let task: TaskId =
                serde_json::from_value(selected.get("task").unwrap_or(&Value::Null).clone())
                    .map_err(storage)?;
            self.put_record(
                root,
                &key,
                json!({"kind":"worktree","task":task,"name":name}),
            )
            .await?;
            task
        };
        self.attach(task).await?;
        self.location(task)
    }

    async fn remove_worktree(&mut self, operation: OperationId, input: &Value) -> Result<Value> {
        let path = Path::new(text(input, "worktree_path")?);
        let task = self
            .views
            .iter()
            .find(|(_, view)| view.workspace.path() == path)
            .map(|(task, _)| *task)
            .ok_or_else(|| Error::Unauthorized("native worktree is not an admitted view".into()))?;
        self.quiescent(task).await?;
        let attached = self.attached(task)?;
        attached
            .workspace
            .capture(&attached.context, operation)
            .await?;
        self.views.remove(&task);
        Ok(json!({}))
    }

    async fn bind_child(
        &mut self,
        root: TaskId,
        host: &str,
        event: &str,
        input: &Value,
        records: &[Value],
    ) -> Result<Value> {
        let agent = text(input, "agent_id")?;
        let key = identity("agent", agent);
        let task = if let Some(record) = self.record(root, &key).await? {
            serde_json::from_value(record.get("task").unwrap_or(&Value::Null).clone())
                .map_err(storage)?
        } else {
            let bound = records
                .iter()
                .filter(|value| value.get("kind").unwrap_or(&Value::Null) == "agent")
                .map(|value| value.get("task").unwrap_or(&Value::Null).clone())
                .collect::<Vec<_>>();
            let pending = records
                .iter()
                .filter(|value| {
                    value.get("kind").unwrap_or(&Value::Null) == "spawn"
                        && !bound.contains(value.get("task").unwrap_or(&Value::Null))
                })
                .collect::<Vec<_>>();
            let selected = self.select_spawn(host, input, records, &pending).await?;
            let task: TaskId =
                serde_json::from_value(selected.get("task").unwrap_or(&Value::Null).clone())
                    .map_err(storage)?;
            self.put_record(
                root,
                &key,
                json!({"kind":"agent","agent":agent,"task":task,"turn":input.get("turn_id")}),
            )
            .await?;
            task
        };
        self.attach(task).await?;
        let parent = self
            .require_native(task)
            .await?
            .ok_or_else(|| Error::Unauthorized("native child has no parent".into()))?;
        self.attached(parent)?
            .workspace
            .name_child(
                &self.attached(parent)?.context,
                &self.attached(task)?.context,
                &self.attached(task)?.workspace,
                agent,
            )
            .await?;
        self.context_answer(task, event)
    }

    async fn select_spawn<'a>(
        &mut self,
        host: &str,
        input: &Value,
        records: &[Value],
        pending: &[&'a Value],
    ) -> Result<&'a Value> {
        let selected = pending
            .first()
            .copied()
            .ok_or_else(|| Error::Conflict("native child has no prepared parent fork".into()))?;
        if host == "claude-code" {
            let cwd = Path::new(text(input, "cwd")?);
            for candidate in pending {
                let task = candidate.get("task").unwrap_or(&Value::Null);
                if !records.iter().any(|record| {
                    record.get("kind") == Some(&json!("worktree"))
                        && record.get("task") == Some(task)
                }) {
                    continue;
                }
                let task: TaskId = serde_json::from_value(task.clone()).map_err(storage)?;
                self.attach(task).await?;
                if self.attached(task)?.workspace.path() == cwd {
                    return Ok(candidate);
                }
            }
            return Err(Error::Conflict(
                "native child cwd does not select its prepared worktree".into(),
            ));
        }
        if pending.iter().any(|value| {
            value.get("parent") != selected.get("parent") || value.get("cut") != selected.get("cut")
        }) {
            return Err(Error::Conflict(
                "native child identity is ambiguous across parent fork points".into(),
            ));
        }
        Ok(selected)
    }

    async fn pre_tool(&mut self, frame: NativeEvent<'_>) -> Result<Value> {
        let NativeEvent {
            host,
            event,
            input,
            task,
            root,
            ..
        } = frame;

        let tool = text(input, "tool_name")?;
        let call = text(input, "tool_use_id")?;
        if matches!(tool, "spawn_agent" | "Agent" | "Task") {
            return self.prepare_spawn(frame, call).await;
        }
        let mut updated = input
            .get("tool_input")
            .cloned()
            .ok_or_else(|| Error::Invalid("native tool has no input".into()))?;
        let original = updated.clone();
        if task != root {
            let cwd = std::fs::canonicalize(text(input, "cwd")?).map_err(storage)?;
            let view =
                std::fs::canonicalize(self.attached(task)?.workspace.path()).map_err(storage)?;
            if !cwd.starts_with(&view) {
                if host != "codex" {
                    return Err(Error::Unauthorized(
                        "native child cwd does not select its task view".into(),
                    ));
                }
                updated = crate::host_hooks::route_tool(
                    tool,
                    updated,
                    self.attached(root)?.workspace.path(),
                    &view,
                )?;
            }
        }
        if writes(tool) && !coordination_command(&original) {
            self.tool_file(task, call, ToolFile::Request, json!({"native_tool":tool}))
                .await?;
        }
        if updated == original {
            return Ok(json!({}));
        }
        // Codex's documented rewrite schema requires `allow`. This
        // selects arguments; its OS sandbox still authorizes effects.
        // Claude accepts updatedInput while retaining its normal
        // permission decision, so it receives no approval override.
        let mut output =
            json!({"hookSpecificOutput":{"hookEventName":event,"updatedInput":updated}});
        if host == "codex"
            && let Some(object) = output
                .get_mut("hookSpecificOutput")
                .and_then(Value::as_object_mut)
        {
            object.insert("permissionDecision".into(), json!("allow"));
        }
        Ok(output)
    }

    async fn post_tool(&mut self, frame: NativeEvent<'_>) -> Result<Value> {
        let NativeEvent {
            operation,
            event,
            input,
            task,
            ..
        } = frame;

        let call = text(input, "tool_use_id")?;
        let key = tool_key(task, call, ToolFile::Request)?;
        let snapshot = self
            .files(task)
            .await?
            .snapshot(&Query::File(key), 1)
            .await
            .map_err(storage)?;
        if !snapshot.files.is_empty() {
            let response = input.get("tool_response").cloned().unwrap_or(Value::Null);
            let active = running_session(&response);
            let finished = if matches!(
                input.get("tool_name").and_then(Value::as_str),
                Some("write_stdin" | "TaskOutput")
            ) && active.is_none()
                && process_finished(&response)
            {
                input
                    .pointer("/tool_input/session_id")
                    .or_else(|| input.pointer("/tool_input/task_id"))
                    .cloned()
            } else {
                None
            };
            self.tool_file(
                task,
                call,
                ToolFile::Result,
                json!({"native_event":event,"active_session":active,"finished_session":finished}),
            )
            .await?;
        }
        match self.quiescent(task).await {
            Err(Error::Conflict(_)) => return Ok(json!({})),
            Err(error) => return Err(error),
            Ok(()) => {}
        }
        {
            let attached = self.attached(task)?;
            attached
                .workspace
                .capture(&attached.context, operation)
                .await?;
        }
        Ok(json!({}))
    }

    async fn prepare_spawn(&mut self, frame: NativeEvent<'_>, call: &str) -> Result<Value> {
        let NativeEvent {
            operation,
            host,
            event,
            input,
            task,
            root,
            records,
        } = frame;

        let key = identity("spawn", call);
        if self.record(root, &key).await?.is_none() {
            self.quiescent(task).await?;
            let parent = self.attached(task)?;
            parent.workspace.capture(&parent.context, operation).await?;
            let cut = parent.workspace.fork_point(&parent.context).await?;
            let bound = records
                .iter()
                .filter(|value| value.get("kind").unwrap_or(&Value::Null) == "agent")
                .map(|value| value.get("task").unwrap_or(&Value::Null).clone())
                .collect::<Vec<_>>();
            if records.iter().any(|value| {
                value.get("kind").unwrap_or(&Value::Null) == "spawn"
                    && !bound.contains(value.get("task").unwrap_or(&Value::Null))
                    && (value.get("parent") != Some(&json!(task))
                        || value.get("cut").and_then(Value::as_str) != Some(cut.as_str()))
            }) {
                return Err(Error::Conflict(
                    "wait for the pending native child to bind before changing its fork point"
                        .into(),
                ));
            }
            let child = self.local.admit_host_child(&parent.context, &key).await?;
            self.attach(child).await?;
            self.put_record(
                root,
                &key,
                json!({"kind":"spawn","task":child,"parent":task,"cut":cut}),
            )
            .await?;
        }
        if host == "claude-code" {
            let mut updated = input
                .get("tool_input")
                .cloned()
                .ok_or_else(|| Error::Invalid("native spawn has no input".into()))?;
            updated
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("native spawn input must be an object".into()))?
                .insert("isolation".into(), json!("worktree"));
            return Ok(
                json!({"hookSpecificOutput":{"hookEventName":event,"updatedInput":updated}}),
            );
        }
        Ok(json!({}))
    }

    fn caller(
        &self,
        root: TaskId,
        session: &str,
        input: &Value,
        records: &[Value],
    ) -> Result<TaskId> {
        if let Some(agent) = input
            .get("agent_id")
            .and_then(Value::as_str)
            .filter(|agent| !agent.is_empty())
            .or_else(|| input.get("thread_id").and_then(Value::as_str))
        {
            if agent == session {
                return Ok(root);
            }
            let record = records
                .iter()
                .find(|value| {
                    value.get("kind").unwrap_or(&Value::Null) == "agent"
                        && value.get("agent").unwrap_or(&Value::Null) == agent
                })
                .ok_or_else(|| {
                    Error::Unauthorized("native calling thread has no admitted child".into())
                })?;
            return serde_json::from_value(record.get("task").unwrap_or(&Value::Null).clone())
                .map_err(storage);
        }
        if let Some(turn) = input.get("turn_id").and_then(Value::as_str)
            && let Some(record) = records.iter().find(|value| {
                value.get("kind").unwrap_or(&Value::Null) == "turn"
                    && value.get("turn").unwrap_or(&Value::Null) == turn
            })
        {
            return serde_json::from_value(record.get("task").unwrap_or(&Value::Null).clone())
                .map_err(storage);
        }
        if let Some(cwd) = input.get("cwd").and_then(Value::as_str)
            && let Some((task, _)) = self.views.iter().find(|(task, attached)| {
                attached.workspace.path() == Path::new(cwd)
                    && (**task == root
                        || records
                            .iter()
                            .any(|record| record.get("task") == Some(&json!(task))))
            })
        {
            return Ok(*task);
        }
        if input.get("agent_id").is_some() {
            return Err(Error::Unauthorized("unbound native agent".into()));
        }
        Ok(root)
    }

    pub(super) async fn locate(&mut self, cwd: &Path) -> Result<Value> {
        let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
        let snapshot = self
            .local
            .task_files
            .snapshot(&Query::TaskOverviews, 4096)
            .await
            .map_err(storage)?;
        let mut live = Vec::new();
        for file in snapshot.files {
            let FileKey::Task { task, .. } = file.key else {
                continue;
            };
            let task: TaskId = serde_json::from_value(json!(task.as_str())).map_err(storage)?;
            if self.require_native(task).await.is_err() {
                continue;
            }
            match self.attach(task).await {
                Ok(()) => live.push(task),
                Err(Error::Unauthorized(_) | Error::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
        }
        let mut selected = self
            .views
            .iter()
            .filter(|(task, attached)| {
                live.contains(task) && cwd.starts_with(attached.workspace.path())
            })
            .collect::<Vec<_>>();
        selected.sort_by_key(|(_, attached)| {
            std::cmp::Reverse(attached.workspace.path().components().count())
        });
        let Some((task, first)) = selected.first() else {
            return Err(Error::NotFound(
                "no native task selects this working directory".into(),
            ));
        };
        if selected
            .get(1)
            .is_some_and(|(_, second)| second.workspace.path() == first.workspace.path())
        {
            return Err(Error::Conflict(
                "multiple native sessions select this directory; use --task <id>".into(),
            ));
        }
        self.location(**task)
    }

    pub(super) async fn quiescent(&self, task: TaskId) -> Result<()> {
        let snapshot = self
            .files(task)
            .await?
            .snapshot(
                &Query::Tools {
                    task: Component::new(task.to_string()).map_err(storage)?,
                    turn: 0,
                },
                4096,
            )
            .await
            .map_err(storage)?;
        for request in &snapshot.files {
            if let FileKey::Task {
                file:
                    TaskFile::Tool {
                        call,
                        file: ToolFile::Request,
                        ..
                    },
                ..
            } = &request.key
            {
                let key = tool_key(task, call.as_str(), ToolFile::Result)?;
                let Some(result) = snapshot.files.iter().find(|file| file.key == key) else {
                    return Err(Error::Conflict(
                        "native writes are still active; wait for their host tool results".into(),
                    ));
                };
                let result: Value = serde_json::from_slice(&result.bytes).map_err(storage)?;
                if let Some(active) = result
                    .get("active_session")
                    .filter(|value| !value.is_null())
                {
                    let completed = snapshot
                        .files
                        .iter()
                        .filter(|file| {
                            matches!(
                                file.key,
                                FileKey::Task {
                                    file: TaskFile::Tool {
                                        file: ToolFile::Result,
                                        ..
                                    },
                                    ..
                                }
                            )
                        })
                        .try_fold(false, |found, file| {
                            let result: Value =
                                serde_json::from_slice(&file.bytes).map_err(storage)?;
                            Ok::<_, Error>(found || result.get("finished_session") == Some(active))
                        })?;
                    if !completed {
                        return Err(Error::Conflict("native background writes are active; wait through the host's process tool".into()));
                    }
                }
            }
        }
        Ok(())
    }

    fn context_answer(&self, task: TaskId, event: &str) -> Result<Value> {
        let path = self.attached(task)?.workspace.path();
        Ok(
            json!({"hookSpecificOutput":{"hookEventName":event,"additionalContext":format!("Task {task}. Working directory: {}. Native tools and permissions remain in force. Use acyclic --task {task} fs merge agents/<ref> to merge a child through the shared harness; acyclic --task {task} agents shows shared files.",path.display())}}),
        )
    }

    pub(super) async fn records(&self, task: TaskId) -> Result<Vec<Value>> {
        let snapshot = self
            .files(task)
            .await?
            .snapshot(
                &Query::Task(Component::new(task.to_string()).map_err(storage)?),
                4096,
            )
            .await
            .map_err(storage)?;
        snapshot.files.into_iter().filter(|file| matches!(&file.key, FileKey::Task { file:TaskFile::Context(name), .. } if name.as_str().starts_with("native-")))
            .map(|file| serde_json::from_slice(&file.bytes).map_err(storage)).collect()
    }

    async fn record(&self, task: TaskId, name: &str) -> Result<Option<Value>> {
        let snapshot = self
            .files(task)
            .await?
            .snapshot(&Query::File(context_key(task, name)?), 1)
            .await
            .map_err(storage)?;
        snapshot
            .files
            .first()
            .map(|file| serde_json::from_slice(&file.bytes).map_err(storage))
            .transpose()
    }

    async fn put_record(&self, task: TaskId, name: &str, value: Value) -> Result<()> {
        self.publish(
            task,
            FileValue::new(
                context_key(task, name)?,
                serde_json::to_vec(&value).map_err(storage)?,
            ),
        )
        .await
    }

    async fn tool_file(
        &self,
        task: TaskId,
        call: &str,
        kind: ToolFile,
        value: Value,
    ) -> Result<()> {
        self.publish(
            task,
            FileValue::new(
                tool_key(task, call, kind)?,
                serde_json::to_vec(&value).map_err(storage)?,
            ),
        )
        .await
    }

    async fn publish(&self, task: TaskId, file: FileValue) -> Result<()> {
        let owner = self
            .attached(task)?
            .context
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("native owner absent".into()))?;
        let files = self.files(task).await?;
        let snapshot = files
            .snapshot(&Query::File(file.key.clone()), 1)
            .await
            .map_err(storage)?;
        crate::task_volume::publish(&files.inner, snapshot.generation, &file, || async {
            let (condition, deadline) = owner.file_publication_condition().await?;
            Ok::<_, Error>((vec![condition], deadline))
        })
        .await?;
        Ok(())
    }
}

fn context_key(task: TaskId, name: &str) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string()).map_err(storage)?,
        file: TaskFile::Context(Component::new(format!("native-{name}")).map_err(storage)?),
    })
}

fn tool_key(task: TaskId, call: &str, file: ToolFile) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string()).map_err(storage)?,
        file: TaskFile::Tool {
            turn: 0,
            call: Component::new(call).map_err(storage)?,
            file,
        },
    })
}

fn writes(tool: &str) -> bool {
    matches!(
        tool,
        "exec_command"
            | "shell_command"
            | "Bash"
            | "bash"
            | "PowerShell"
            | "Write"
            | "Edit"
            | "NotebookEdit"
            | "write"
            | "edit"
            | "apply_patch"
            | "write_stdin"
            | "TaskOutput"
    )
}

pub(super) fn running_session(response: &Value) -> Option<Value> {
    if process_finished(response) {
        return None;
    }
    if let Some(session) = response
        .get("session_id")
        .or_else(|| response.get("process_id"))
        .or_else(|| response.get("background_task_id"))
        .filter(|session| !session.is_null())
    {
        return Some(session.clone());
    }
    if response.get("status").and_then(Value::as_str) == Some("running") {
        return response.get("task_id").cloned();
    }
    let rendered = response
        .as_str()
        .map_or_else(|| response.to_string(), str::to_owned);
    let session = rendered
        .split("Process running with session ID ")
        .nth(1)?
        .split(|c: char| !c.is_ascii_digit())
        .next()?;
    session.parse::<u64>().ok().map(|session| json!(session))
}

pub(super) fn process_finished(response: &Value) -> bool {
    response
        .get("exit_code")
        .is_some_and(|code| !code.is_null())
        || matches!(
            response.pointer("/task/status").and_then(Value::as_str),
            Some("completed" | "failed" | "killed")
        )
        || response.to_string().contains("Process exited with code ")
}

fn coordination_command(input: &Value) -> bool {
    input
        .get("cmd")
        .or_else(|| input.get("command"))
        .and_then(Value::as_str)
        .and_then(|command| crate::tools::parse_command(command).ok())
        .is_some_and(|arguments| {
            let Some(executable) = arguments
                .first()
                .and_then(|value| Path::new(value).file_name())
                .and_then(|name| name.to_str())
            else {
                return false;
            };
            matches!(executable, "acyclic" | "acyclic.exe")
                && arguments.iter().skip(1).all(|value| {
                    !value
                        .chars()
                        .any(|c| matches!(c, ';' | '|' | '&' | '`' | '\n' | '$'))
                })
        })
}
