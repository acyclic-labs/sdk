//! The service's request dispatch and session catalog.

use super::*;

#[derive(Clone)]
pub(crate) struct ServiceResources {
    pub(crate) data: PathBuf,
    pub(crate) fs: LocalFs,
    pub(crate) store: LocalCoreStateStore,
    pub(crate) shared_roots: SharedRootRegistry,
    pub(crate) binary_identity: String,
    pub(crate) instance_id: String,
}

impl ServiceResources {
    pub(crate) async fn open(
        data: PathBuf,
    ) -> Result<(Self, BTreeMap<String, ControlPlane>), String> {
        fs::create_dir_all(data.join("sessions")).map_err(display)?;
        // A binary that has not been identified since it changed is hashed in
        // full; that runs alongside opening the filesystem.
        let identity_data = data.clone();
        let binary_identity = tokio::task::spawn_blocking(move || service_identity(&identity_data));
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(display)?;
        let binary_identity = binary_identity.await.map_err(display)??;
        let resources = Self {
            store: LocalCoreStateStore::open_owned(data.join("core-state")).map_err(display)?,
            shared_roots: SharedRootRegistry::default(),
            binary_identity,
            instance_id: uuid::Uuid::new_v4().to_string(),
            data,
            fs,
        };
        resources
            .shared_roots
            .collection
            .start(resources.fs.clone(), resources.store.clone());
        let mut entries = fs::read_dir(resources.data.join("sessions"))
            .map_err(display)?
            .filter_map(|entry| match entry {
                Ok(entry) => match entry.file_type() {
                    Ok(kind) if kind.is_dir() => Some(Ok(entry.path())),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                },
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(display)?;
        entries.sort_by(|left, right| {
            state_modified(right)
                .cmp(&state_modified(left))
                .then_with(|| right.cmp(left))
        });
        let mut sessions = BTreeMap::new();
        let mut retained_sources = BTreeMap::<PathBuf, ([u8; 16], [u8; 16])>::new();
        for entry in entries {
            let (state, _) = load_state(&entry)?;
            if !state.active {
                continue;
            }
            let mut roots = Vec::with_capacity(state.roots.len());
            let mut stale = false;
            for root in state.roots.values() {
                let canonical = root.path.canonicalize().map_err(display)?;
                if retained_sources.get(&canonical).is_some_and(
                    |(source_identity, native_identity)| {
                        *native_identity == root.native_root_identity
                            && *source_identity != root.source_identity
                    },
                ) {
                    stale = true;
                    break;
                }
                roots.push((canonical, (root.source_identity, root.native_root_identity)));
            }
            if stale {
                fs::remove_dir_all(&entry).map_err(display)?;
                continue;
            }
            let control = resources.open_session_directory(entry).await?;
            if !control.state.root_session_id.is_empty() {
                retained_sources.extend(roots);
                sessions.insert(control.state.root_session_id.clone(), control);
            }
        }
        Ok((resources, sessions))
    }

    pub(crate) fn session_directory(&self, session_id: &str) -> PathBuf {
        self.data
            .join("sessions")
            .join(blake3::hash(session_id.as_bytes()).to_hex().as_str())
    }

    pub(crate) async fn open_session(&self, session_id: &str) -> Result<ControlPlane, String> {
        self.open_session_directory(self.session_directory(session_id))
            .await
    }

    pub(crate) async fn open_session_directory(
        &self,
        directory: PathBuf,
    ) -> Result<ControlPlane, String> {
        ControlPlane::open_with(
            directory,
            self.data.clone(),
            self.fs.clone(),
            self.store.clone(),
            self.shared_roots.clone(),
        )
        .await
    }
}

#[cfg(test)]
pub(crate) struct ServiceControl {
    pub(crate) resources: ServiceResources,
    pub(crate) sessions: BTreeMap<String, ControlPlane>,
}

#[cfg(test)]
impl std::ops::Deref for ServiceControl {
    type Target = ServiceResources;

    fn deref(&self) -> &Self::Target {
        &self.resources
    }
}

#[cfg(test)]
impl ServiceControl {
    pub(crate) async fn open(data: PathBuf) -> Result<Self, String> {
        let (resources, sessions) = ServiceResources::open(data).await?;
        Ok(Self {
            resources,
            sessions,
        })
    }

    pub(crate) async fn create_session(
        &mut self,
        session_id: &str,
    ) -> Result<&mut ControlPlane, String> {
        if !self.sessions.contains_key(session_id) {
            let control = self.resources.open_session(session_id).await?;
            self.sessions.insert(session_id.to_owned(), control);
        }
        self.sessions
            .get_mut(session_id)
            .ok_or_else(|| "Acyclic session registration failed".to_owned())
    }

    pub(crate) fn session_for_cwd(&self, cwd: &Path) -> Result<Option<String>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let mut matches = Vec::new();
        for (session_id, control) in &self.sessions {
            let mut score = control
                .state
                .roots
                .values()
                .filter_map(|root| root.path.canonicalize().ok())
                .filter(|root| cwd.starts_with(root))
                .map(|root| root.components().count())
                .max();
            for route in control
                .state
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
                if let Ok(mount_path) = route.mount_path.canonicalize()
                    && cwd.starts_with(&mount_path)
                {
                    score = Some(score.unwrap_or(0).max(mount_path.components().count()));
                }
            }
            if let Some(score) = score {
                matches.push((score, session_id.clone()));
            }
        }
        matches.sort_by(|left, right| right.cmp(left));
        let Some((score, session_id)) = matches.first() else {
            return Ok(None);
        };
        if matches
            .get(1)
            .is_some_and(|candidate| candidate.0 == *score)
        {
            return Err(
                "cwd belongs to multiple Acyclic sessions; run inside a managed child mount"
                    .to_owned(),
            );
        }
        Ok(Some(session_id.clone()))
    }

    pub(crate) async fn session_for_cwd_or_register(
        &mut self,
        cwd: &Path,
    ) -> Result<String, String> {
        if let Some(session_id) = self.session_for_cwd(cwd)? {
            return Ok(session_id);
        }
        if !self.sessions.is_empty() {
            return Err(
                "cwd is outside every registered Acyclic session; refusing service-assisted filesystem access"
                    .to_owned(),
            );
        }
        let canonical = cwd.canonicalize().map_err(display)?;
        let session_id = format!(
            "cwd:{}",
            blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
        );
        self.create_session(&session_id)
            .await?
            .session_start(json!({
                "session_id": session_id,
                "cwd": canonical,
                "host": "cli"
            }))
            .await?;
        Ok(session_id)
    }

    pub(crate) fn child_mount_for_cwd(
        &self,
        cwd: &Path,
    ) -> Result<Option<(String, String)>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let mut matches = Vec::new();
        for (session_id, control) in &self.sessions {
            for route in control
                .state
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
                if let Ok(mount_path) = route.mount_path.canonicalize()
                    && cwd.starts_with(&mount_path)
                {
                    matches.push((
                        mount_path.components().count(),
                        session_id.clone(),
                        route.agent_id.clone(),
                    ));
                }
            }
            for pending in control
                .state
                .pending
                .iter()
                .filter(|pending| pending.lifecycle == PendingSpawnLifecycle::Prepared)
            {
                if let Ok(mount_path) = pending.mount_path.canonicalize()
                    && cwd.starts_with(&mount_path)
                {
                    matches.push((
                        mount_path.components().count(),
                        session_id.clone(),
                        format!("pending:{}", pending.mount_name()),
                    ));
                }
            }
        }
        matches.sort_by(|left, right| right.cmp(left));
        let Some((depth, session_id, agent_id)) = matches.first() else {
            return Ok(None);
        };
        if matches
            .get(1)
            .is_some_and(|candidate| candidate.0 == *depth)
        {
            return Err("cwd belongs to multiple managed child mounts".to_owned());
        }
        Ok(Some((session_id.clone(), agent_id.clone())))
    }

    pub(crate) async fn shutdown_sessions(&mut self, deactivate: bool) -> Result<(), String> {
        let sessions = self.sessions.keys().cloned().collect::<Vec<_>>();
        let mut failed_sessions = 0_usize;
        let mut first_error = None;
        for session_id in sessions {
            let error = self.close_session(&session_id, deactivate).await.err();
            let session_failed = error.is_some();
            if let Some(error) = error {
                first_error.get_or_insert_with(|| format!("session {session_id}: {error}"));
            }
            failed_sessions += usize::from(session_failed);
        }
        first_error.map_or(Ok(()), |error| {
            Err(format!(
                "Acyclic service shutdown failed for {failed_sessions} session(s); first error: {error}"
            ))
        })
    }

    pub(crate) async fn close_session(
        &mut self,
        session_id: &str,
        deactivate: bool,
    ) -> Result<(), String> {
        let control = self
            .sessions
            .remove(session_id)
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())?;
        let mut terminal_state = control.state.clone();
        let directory = control.data.clone();
        let mut slots = match control.close(deactivate).await {
            Ok(slots) => slots,
            Err(shutdown_error) => {
                let recovered = ControlPlane::open_with(
                    directory,
                    self.data.clone(),
                    self.fs.clone(),
                    self.store.clone(),
                    self.shared_roots.clone(),
                )
                .await;
                return match recovered {
                    Ok(control) => {
                        self.sessions.insert(session_id.to_owned(), control);
                        Err(format!(
                            "Acyclic session shutdown failed and was restored: {shutdown_error}"
                        ))
                    }
                    Err(recovery_error) => Err(format!(
                        "Acyclic session shutdown failed: {shutdown_error}; live recovery failed: {recovery_error}"
                    )),
                };
            }
        };
        if deactivate {
            terminal_state.active = false;
            slots.save(&directory, &terminal_state, Survives::PowerLoss)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn shutdown(mut self) -> Result<(), String> {
        let root_released = self.fs.local_root_release_barrier();
        let result = self.shutdown_sessions(false).await;
        self.shared_roots.collection.stop().await;
        // Publish service shutdown only after its final LocalFs handle has released the durable
        // Stream and Objects roots. A completed async future may otherwise retain `self` until the
        // executor drops the future, allowing an immediate replacement service to race the lock.
        drop(self);
        wait_for_root_release(root_released).await?;
        result
    }

    pub(crate) async fn dispatch_native_hook(
        &mut self,
        host: &str,
        event: &str,
        input: Value,
        cwd: &Path,
    ) -> Result<Value, String> {
        let session_id = hook_id(&input, "session_id", "sessionId")?;
        if matches!(event, "SessionStart" | "sessionStart") {
            let root = hook_root_path(&input).unwrap_or_else(|| cwd.to_path_buf());
            if let Some((owner_session, owner_agent)) = self.child_mount_for_cwd(&root)? {
                return Err(format!(
                    "session {session_id} cannot attach a managed child mount owned by {owner_agent} in session {owner_session} as a physical root"
                ));
            }
            let control = self.create_session(&session_id).await?;
            let output = control
                .session_start(json!({"session_id":session_id,"cwd":root,"host":host}))
                .await?;
            if host == "cursor" {
                return Ok(json!({
                    "additional_context": output
                        .pointer("/hookSpecificOutput/additionalContext")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                }));
            }
            return Ok(output);
        }
        if matches!(event, "SessionEnd" | "sessionEnd") {
            self.close_session(&session_id, true).await?;
            return Ok(json!({}));
        }
        let control = self
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())?;
        dispatch_native_session_hook(control, host, event, input, cwd).await
    }
}

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

#[cfg(test)]
impl ControlRequestDispatcher for ServiceControl {
    async fn dispatch_request(&mut self, request: ControlRequest) -> Result<Value, String> {
        if matches!(request.command, ControlCommand::Ping) {
            return Ok(json!({
                "identity": self.binary_identity,
                "instanceId": self.instance_id,
                "sessions": self.sessions.len(),
            }));
        }
        if matches!(request.command, ControlCommand::Doctor) {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
            let routes = self
                .sessions
                .values()
                .map(|session| session.state.routes.len())
                .sum();
            let leases = self
                .sessions
                .values()
                .map(|session| session.state.leases.len())
                .sum();
            let pending_recovery = self.sessions.values().any(|session| {
                !session.state.pending_discards.is_empty() || !session.state.pending.is_empty()
            });
            return doctor_report(
                &self.data,
                &self.binary_identity,
                &executable.sha256,
                &executable.blake3,
                self.sessions.len(),
                routes,
                leases,
                pending_recovery,
            );
        }
        if matches!(request.command, ControlCommand::Hook) {
            let (host, event) = request
                .name
                .split_once(':')
                .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
            return self
                .dispatch_native_hook(host, event, request.arguments, &request.cwd)
                .await;
        }
        let session_id = self.session_for_cwd_or_register(&request.cwd).await?;
        let control = self
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| "Acyclic session is not registered".to_owned())?;
        dispatch_session_request(control, request).await
    }
}

pub(crate) const SESSION_ACTIVE: u8 = 0;
pub(crate) const SESSION_CLOSING: u8 = 1;
pub(crate) const SESSION_CLOSED: u8 = 2;
pub(crate) const SESSION_CLOSE_FAILED: u8 = 3;
pub(crate) const SESSION_DATA_CAPACITY: usize = 64;
pub(crate) const SESSION_QUEUE_CAPACITY: usize = SESSION_DATA_CAPACITY + 1;
pub(crate) const SERVICE_RUNNING: u8 = 0;
pub(crate) const SERVICE_DRAINING: u8 = 1;
pub(crate) const SERVICE_CLOSED: u8 = 2;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SessionKey {
    pub(crate) session_id: String,
    pub(crate) epoch: u64,
}

pub(crate) struct SessionHandle {
    pub(crate) key: SessionKey,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    pub(crate) admission: Mutex<()>,
    pub(crate) sender: tokio::sync::mpsc::Sender<SessionCommand>,
    pub(crate) data_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) snapshot: Arc<std::sync::RwLock<Option<AdapterState>>>,
    pub(crate) close_result: watch::Sender<Option<Result<(), String>>>,
}

pub(crate) enum SessionCommand {
    Dispatch {
        request: ControlRequest,
        response: tokio::sync::oneshot::Sender<Result<Value, String>>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    Shutdown {
        deactivate: bool,
        response: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    #[cfg(test)]
    Pause {
        entered: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    },
}

impl SessionHandle {
    pub(crate) fn new(key: SessionKey, control: ControlPlane, resources: ServiceResources) -> Self {
        let snapshot = Arc::new(std::sync::RwLock::new(Some(control.state.clone())));
        let actor_snapshot = Arc::clone(&snapshot);
        let state = Arc::new(std::sync::atomic::AtomicU8::new(SESSION_ACTIVE));
        let actor_state = Arc::clone(&state);
        let close_result = watch::channel(None).0;
        let actor_close_result = close_result.clone();
        let data_slots = Arc::new(tokio::sync::Semaphore::new(SESSION_DATA_CAPACITY));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(SESSION_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut control = Some(control);
            while let Some(command) = receiver.recv().await {
                match command {
                    SessionCommand::Dispatch {
                        request,
                        response,
                        _permit,
                    } => {
                        let result = match control.as_mut() {
                            Some(control) => dispatch_session_request(control, request).await,
                            None => Err("Acyclic session actor has no workspace".to_owned()),
                        };
                        // Other sessions see only durable state: an unflushed
                        // save is published once flushed, after the reply.
                        let mut pending = Some((response, result));
                        if control.as_ref().is_some_and(|control| control.unflushed)
                            && let Some((response, result)) = pending.take()
                        {
                            let _ = response.send(result);
                        }
                        let durable = match control.as_mut() {
                            Some(control) => control.make_durable().await.is_ok(),
                            None => true,
                        };
                        if durable {
                            let next = control.as_ref().map(|control| control.state.clone());
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = next;
                            }
                        }
                        if let Some((response, result)) = pending {
                            let _ = response.send(result);
                        }
                    }
                    SessionCommand::Shutdown {
                        deactivate,
                        response,
                    } => {
                        let current = control
                            .take()
                            .ok_or_else(|| "Acyclic session actor has no workspace".to_owned());
                        let result = match current {
                            Err(error) => Err(error),
                            Ok(current) => {
                                let mut terminal_state = current.state.clone();
                                let directory = current.data.clone();
                                match current.close(deactivate).await {
                                    Ok(mut slots) => {
                                        if deactivate {
                                            terminal_state.active = false;
                                            slots.save(
                                                &directory,
                                                &terminal_state,
                                                Survives::PowerLoss,
                                            )
                                        } else {
                                            Ok(())
                                        }
                                    }
                                    Err(shutdown_error) => {
                                        match ControlPlane::open_with(
                                            directory,
                                            resources.data.clone(),
                                            resources.fs.clone(),
                                            resources.store.clone(),
                                            resources.shared_roots.clone(),
                                        )
                                        .await
                                        {
                                            Ok(recovered) => {
                                                control = Some(recovered);
                                                Err(format!(
                                                    "Acyclic session shutdown failed and was restored: {shutdown_error}"
                                                ))
                                            }
                                            Err(recovery_error) => Err(format!(
                                                "Acyclic session shutdown failed: {shutdown_error}; live recovery failed: {recovery_error}"
                                            )),
                                        }
                                    }
                                }
                            }
                        };
                        let _ = actor_close_result.send(Some(result.clone()));
                        if result.is_ok() {
                            actor_state.store(SESSION_CLOSED, std::sync::atomic::Ordering::Release);
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = None;
                            }
                            let _ = response.send(result);
                            break;
                        }
                        actor_state
                            .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                        let next = control.as_ref().map(|control| control.state.clone());
                        if let Ok(mut snapshot) = actor_snapshot.write() {
                            *snapshot = next;
                        }
                        let _ = response.send(result);
                    }
                    #[cfg(test)]
                    SessionCommand::Pause { entered, resume } => {
                        let _ = entered.send(());
                        let _ = resume.await;
                    }
                }
            }
        });
        Self {
            key,
            state,
            admission: Mutex::new(()),
            sender,
            data_slots,
            snapshot,
            close_result,
        }
    }

    pub(crate) fn snapshot(&self) -> Result<Option<AdapterState>, String> {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| "session routing snapshot lock is poisoned".to_owned())
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == SESSION_ACTIVE
    }

    pub(crate) async fn dispatch(&self, request: ControlRequest) -> Result<Value, String> {
        let (response, result) = tokio::sync::oneshot::channel();
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| "session admission lock is poisoned".to_owned())?;
            if !self.is_active() {
                return Err("Acyclic session is closing".to_owned());
            }
            let permit = Arc::clone(&self.data_slots)
                .try_acquire_owned()
                .map_err(|_| "Acyclic session operation queue is full".to_owned())?;
            self.sender
                .try_send(SessionCommand::Dispatch {
                    request,
                    response,
                    _permit: permit,
                })
                .map_err(|error| match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        "Acyclic session operation queue is full".to_owned()
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "Acyclic session actor stopped".to_owned()
                    }
                })?;
        }
        result
            .await
            .map_err(|_| "Acyclic session actor dropped an accepted operation".to_owned())?
    }

    pub(crate) async fn shutdown(&self, deactivate: bool) -> Result<(), String> {
        let (response, result) = tokio::sync::oneshot::channel();
        let mut existing = None;
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| "session admission lock is poisoned".to_owned())?;
            match self.state.load(std::sync::atomic::Ordering::Acquire) {
                SESSION_ACTIVE => self
                    .state
                    .store(SESSION_CLOSING, std::sync::atomic::Ordering::Release),
                SESSION_CLOSING => existing = Some(self.close_result.subscribe()),
                SESSION_CLOSE_FAILED => {
                    self.state
                        .store(SESSION_CLOSING, std::sync::atomic::Ordering::Release);
                    let _ = self.close_result.send(None);
                }
                SESSION_CLOSED => return Ok(()),
                _ => return Err("invalid Acyclic session lifecycle state".to_owned()),
            }
            if existing.is_none()
                && let Err(error) = self.sender.try_send(SessionCommand::Shutdown {
                    deactivate,
                    response,
                })
            {
                let error = match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        "Acyclic session control queue is unexpectedly full during shutdown"
                            .to_owned()
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "Acyclic session actor stopped before shutdown".to_owned()
                    }
                };
                self.state
                    .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                let _ = self.close_result.send(Some(Err(error.clone())));
                return Err(error);
            }
        }
        if let Some(mut existing) = existing {
            if existing.borrow().is_none() {
                existing.changed().await.map_err(display)?;
            }
            return existing
                .borrow()
                .clone()
                .ok_or_else(|| "Acyclic session close completed without a result".to_owned())?;
        }
        result
            .await
            .map_err(|_| "Acyclic session actor dropped shutdown".to_owned())?
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "this deterministic test gate must panic immediately if its actor fixture is broken"
    )]
    pub(crate) async fn pause(&self) -> tokio::sync::oneshot::Sender<()> {
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(SessionCommand::Pause {
                entered,
                resume: paused,
            })
            .expect("session actor");
        accepted.await.expect("actor pause accepted");
        resume
    }
}

pub(crate) struct OpeningSession {
    pub(crate) key: SessionKey,
    pub(crate) cancelled: bool,
    pub(crate) complete: watch::Sender<bool>,
}

#[derive(Default)]
pub(crate) struct SessionCatalog {
    pub(crate) next_epoch: u64,
    pub(crate) opening: BTreeMap<String, OpeningSession>,
    pub(crate) active: BTreeMap<String, Arc<SessionHandle>>,
    pub(crate) draining: BTreeMap<SessionKey, Arc<SessionHandle>>,
    pub(crate) completed: BTreeSet<String>,
}

pub(crate) struct ConcurrentServiceControl {
    pub(crate) resources: ServiceResources,
    pub(crate) lifecycle: std::sync::atomic::AtomicU8,
    pub(crate) shutdown_deactivates: std::sync::atomic::AtomicBool,
    pub(crate) catalog: AsyncMutex<SessionCatalog>,
}

impl std::ops::Deref for ConcurrentServiceControl {
    type Target = ServiceResources;

    fn deref(&self) -> &Self::Target {
        &self.resources
    }
}

impl ConcurrentServiceControl {
    pub(crate) async fn open(data: PathBuf) -> Result<Self, String> {
        let (resources, sessions) = ServiceResources::open(data).await?;
        Ok(Self::from_parts(resources, sessions))
    }

    pub(crate) fn from_parts(
        resources: ServiceResources,
        sessions: BTreeMap<String, ControlPlane>,
    ) -> Self {
        let mut catalog = SessionCatalog::default();
        for (session_id, control) in sessions {
            catalog.next_epoch += 1;
            let key = SessionKey {
                session_id: session_id.clone(),
                epoch: catalog.next_epoch,
            };
            catalog.active.insert(
                session_id,
                Arc::new(SessionHandle::new(key, control, resources.clone())),
            );
        }
        Self {
            resources,
            lifecycle: std::sync::atomic::AtomicU8::new(SERVICE_RUNNING),
            shutdown_deactivates: std::sync::atomic::AtomicBool::new(false),
            catalog: AsyncMutex::new(catalog),
        }
    }

    pub(crate) fn ensure_running(&self) -> Result<(), String> {
        if self.lifecycle.load(std::sync::atomic::Ordering::Acquire) == SERVICE_RUNNING {
            Ok(())
        } else {
            Err("Acyclic service is draining".to_owned())
        }
    }

    pub(crate) async fn session(&self, session_id: &str) -> Result<Arc<SessionHandle>, String> {
        self.catalog
            .lock()
            .await
            .active
            .get(session_id)
            .cloned()
            .ok_or_else(|| "Acyclic native hook session is not registered".to_owned())
    }

    pub(crate) async fn start_session(
        &self,
        session_id: &str,
        host: &str,
        root: PathBuf,
    ) -> Result<(Arc<SessionHandle>, Value), String> {
        let root = root.canonicalize().map_err(display)?;
        if let Some(owner) = self.route_session(&root).await?
            && owner.key.session_id != session_id
        {
            return Err(format!(
                "session {session_id} cannot attach a workspace owned by session {} as a physical root",
                owner.key.session_id
            ));
        }
        loop {
            self.ensure_running()?;
            let (key, wait) = {
                let mut catalog = self.catalog.lock().await;
                if let Some(handle) = catalog.active.get(session_id) {
                    let handle = Arc::clone(handle);
                    drop(catalog);
                    let output = handle
                        .dispatch(ControlRequest {
                            version: 1,
                            command: ControlCommand::Hook,
                            cwd: root.clone(),
                            argv: Vec::new(),
                            name: format!("{host}:SessionStart"),
                            arguments: json!({
                                "session_id": session_id,
                                "cwd": root,
                            }),
                        })
                        .await?;
                    return Ok((handle, output));
                }
                if let Some(opening) = catalog.opening.get(session_id) {
                    (opening.key.clone(), Some(opening.complete.subscribe()))
                } else {
                    catalog.completed.remove(session_id);
                    catalog.next_epoch += 1;
                    let key = SessionKey {
                        session_id: session_id.to_owned(),
                        epoch: catalog.next_epoch,
                    };
                    catalog.opening.insert(
                        session_id.to_owned(),
                        OpeningSession {
                            key: key.clone(),
                            cancelled: false,
                            complete: watch::channel(false).0,
                        },
                    );
                    (key, None)
                }
            };
            if let Some(mut wait) = wait {
                if !*wait.borrow() {
                    wait.changed().await.map_err(display)?;
                }
                continue;
            }

            let mut control = self.resources.open_session(session_id).await?;
            let opened = control
                .session_start(json!({"session_id": session_id, "cwd": root, "host": host}))
                .await;
            let handle = Some(Arc::new(SessionHandle::new(
                key.clone(),
                control,
                self.resources.clone(),
            )));
            let (cancelled, complete) = {
                let mut catalog = self.catalog.lock().await;
                let opening = catalog
                    .opening
                    .remove(session_id)
                    .ok_or_else(|| "Acyclic session opening reservation disappeared".to_owned())?;
                if opening.key != key {
                    return Err("Acyclic session opening epoch changed".to_owned());
                }
                let cancelled = opened.is_err()
                    || opening.cancelled
                    || self.lifecycle.load(std::sync::atomic::Ordering::Acquire) != SERVICE_RUNNING;
                if let Some(handle) = &handle {
                    if cancelled {
                        catalog
                            .draining
                            .insert(handle.key.clone(), Arc::clone(handle));
                    } else {
                        catalog
                            .active
                            .insert(session_id.to_owned(), Arc::clone(handle));
                    }
                }
                (cancelled, opening.complete)
            };
            let output = match opened {
                Ok(output) => output,
                Err(error) => {
                    let cleanup = if let Some(handle) = &handle {
                        let cleanup = handle.shutdown(true).await;
                        if cleanup.is_ok() {
                            let mut catalog = self.catalog.lock().await;
                            catalog.draining.remove(&handle.key);
                            catalog.completed.insert(session_id.to_owned());
                        }
                        cleanup
                    } else {
                        Ok(())
                    };
                    let _ = complete.send(true);
                    return match cleanup {
                        Ok(()) => Err(error),
                        Err(cleanup) => Err(format!(
                            "{error}; failed session-start cleanup was retained: {cleanup}"
                        )),
                    };
                }
            };
            let handle = handle.ok_or_else(|| "Acyclic session did not open".to_owned())?;
            if cancelled {
                let cleanup = handle.shutdown(true).await;
                if cleanup.is_ok() {
                    let mut catalog = self.catalog.lock().await;
                    catalog.draining.remove(&handle.key);
                    catalog.completed.insert(session_id.to_owned());
                }
                let _ = complete.send(true);
                cleanup?;
                return Err("Acyclic session start was cancelled".to_owned());
            }
            let _ = complete.send(true);
            return Ok((handle, output));
        }
    }

    pub(crate) async fn end_session(
        &self,
        session_id: &str,
        deactivate: bool,
    ) -> Result<(), String> {
        let mut cancelled_opening = false;
        loop {
            let wait = {
                let mut catalog = self.catalog.lock().await;
                if let Some(opening) = catalog.opening.get_mut(session_id) {
                    opening.cancelled = true;
                    cancelled_opening = true;
                    Some(opening.complete.subscribe())
                } else {
                    None
                }
            };
            if let Some(mut wait) = wait {
                if !*wait.borrow() {
                    wait.changed().await.map_err(display)?;
                }
                continue;
            }
            break;
        }
        let handle = {
            let mut catalog = self.catalog.lock().await;
            if let Some(handle) = catalog.active.remove(session_id) {
                catalog
                    .draining
                    .insert(handle.key.clone(), Arc::clone(&handle));
                handle
            } else if let Some(handle) = catalog
                .draining
                .values()
                .find(|handle| handle.key.session_id == session_id)
                .cloned()
            {
                handle
            } else {
                if cancelled_opening {
                    return Ok(());
                }
                if catalog.completed.contains(session_id) {
                    return Ok(());
                }
                return Err("Acyclic native hook session is not registered".to_owned());
            }
        };
        let result = handle.shutdown(deactivate).await;
        {
            let mut catalog = self.catalog.lock().await;
            if result.is_ok() {
                catalog.draining.remove(&handle.key);
                catalog.completed.insert(session_id.to_owned());
            }
        }
        if result.is_ok() {
            self.shared_roots.prune().await;
        }
        result
    }

    pub(crate) async fn route_session(
        &self,
        cwd: &Path,
    ) -> Result<Option<Arc<SessionHandle>>, String> {
        let cwd = cwd.canonicalize().map_err(display)?;
        let handles = self.catalog.lock().await;
        let handles = handles
            .active
            .values()
            .chain(handles.draining.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut matches = Vec::new();
        for handle in handles {
            let Some(snapshot) = handle.snapshot()? else {
                continue;
            };
            let mut score = snapshot
                .roots
                .values()
                .filter_map(|root| root.path.canonicalize().ok())
                .filter(|root| cwd.starts_with(root))
                .map(|root| root.components().count())
                .max();
            for route in snapshot
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
                if let Ok(mount) = route.mount_path.canonicalize()
                    && cwd.starts_with(&mount)
                {
                    score = Some(score.unwrap_or(0).max(mount.components().count()));
                }
            }
            if let Some(score) = score {
                matches.push((score, handle));
            }
        }
        matches.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
        let Some((score, handle)) = matches.first() else {
            return Ok(None);
        };
        if matches
            .get(1)
            .is_some_and(|candidate| candidate.0 == *score)
        {
            return Err(
                "cwd belongs to multiple Acyclic sessions; run inside a managed child mount"
                    .to_owned(),
            );
        }
        Ok(Some(Arc::clone(handle)))
    }

    pub(crate) async fn drain_sessions(&self, deactivate: bool) -> Result<(), String> {
        self.lifecycle
            .store(SERVICE_DRAINING, std::sync::atomic::Ordering::Release);
        loop {
            let waits = {
                let mut catalog = self.catalog.lock().await;
                if catalog.opening.is_empty() {
                    break;
                }
                catalog
                    .opening
                    .values_mut()
                    .map(|opening| {
                        opening.cancelled = true;
                        opening.complete.subscribe()
                    })
                    .collect::<Vec<_>>()
            };
            for mut wait in waits {
                if !*wait.borrow() {
                    wait.changed().await.map_err(display)?;
                }
            }
        }
        let handles = {
            let mut catalog = self.catalog.lock().await;
            let active = std::mem::take(&mut catalog.active);
            for handle in active.values() {
                catalog
                    .draining
                    .insert(handle.key.clone(), Arc::clone(handle));
            }
            catalog.draining.values().cloned().collect::<Vec<_>>()
        };
        let mut failed_sessions = 0_usize;
        let mut first_error = None;
        for handle in handles {
            if let Err(error) = handle.shutdown(deactivate).await {
                failed_sessions += 1;
                first_error
                    .get_or_insert_with(|| format!("session {}: {error}", handle.key.session_id));
            } else {
                self.catalog.lock().await.draining.remove(&handle.key);
            }
        }
        if let Some(error) = first_error {
            Err(format!(
                "Acyclic service shutdown failed for {failed_sessions} session(s); first error: {error}"
            ))
        } else {
            self.lifecycle
                .store(SERVICE_CLOSED, std::sync::atomic::Ordering::Release);
            Ok(())
        }
    }

    /// Stops admitting requests ahead of a drain that ends every session, as a
    /// stop request asks; see [`ServiceMarker`].
    pub(crate) fn begin_drain(&self) {
        self.shutdown_deactivates
            .store(true, std::sync::atomic::Ordering::Release);
        self.lifecycle
            .store(SERVICE_DRAINING, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn shutdown(self) -> Result<(), String> {
        let root_released = self.fs.local_root_release_barrier();
        let deactivate = self
            .shutdown_deactivates
            .load(std::sync::atomic::Ordering::Acquire);
        let result = self.drain_sessions(deactivate).await;
        self.shared_roots.collection.stop().await;
        drop(self);
        wait_for_root_release(root_released).await?;
        result
    }
}

impl ConcurrentControlRequestDispatcher for ConcurrentServiceControl {
    async fn dispatch_request(&self, request: ControlRequest) -> Result<Value, String> {
        if request.command == ControlCommand::Ping {
            let catalog = self.catalog.lock().await;
            let active_sessions = catalog.active.len();
            let opening_sessions = catalog.opening.len();
            let draining_sessions = catalog.draining.len();
            return Ok(json!({
                "identity": self.binary_identity,
                "instanceId": self.instance_id,
                "sessions": active_sessions + opening_sessions + draining_sessions,
                "activeSessions": active_sessions,
                "openingSessions": opening_sessions,
                "drainingSessions": draining_sessions,
            }));
        }
        self.ensure_running()?;
        if request.command == ControlCommand::Doctor {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
            let (session_claims, handles) = {
                let catalog = self.catalog.lock().await;
                (
                    catalog.active.len() + catalog.opening.len() + catalog.draining.len(),
                    catalog
                        .active
                        .values()
                        .chain(catalog.draining.values())
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };
            let mut routes = 0;
            let mut leases = 0;
            let mut pending_recovery = false;
            for handle in &handles {
                if let Some(state) = handle.snapshot()? {
                    routes += state.routes.len();
                    leases += state.leases.len();
                    pending_recovery |=
                        !state.pending_discards.is_empty() || !state.pending.is_empty();
                }
            }
            return doctor_report(
                &self.data,
                &self.binary_identity,
                &executable.sha256,
                &executable.blake3,
                session_claims,
                routes,
                leases,
                pending_recovery,
            );
        }
        if request.command == ControlCommand::Hook {
            let (host, event) = request
                .name
                .split_once(':')
                .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
            let session_id = hook_id(&request.arguments, "session_id", "sessionId")?;
            if matches!(event, "SessionStart" | "sessionStart") {
                let root =
                    hook_root_path(&request.arguments).unwrap_or_else(|| request.cwd.clone());
                let (_, output) = self.start_session(&session_id, host, root).await?;
                return Ok(output);
            }
            if matches!(event, "SessionEnd" | "sessionEnd") {
                self.end_session(&session_id, true).await?;
                return Ok(json!({}));
            }
            return self.session(&session_id).await?.dispatch(request).await;
        }
        let handle = if let Some(handle) = self.route_session(&request.cwd).await? {
            handle
        } else {
            let catalog = self.catalog.lock().await;
            if !catalog.active.is_empty()
                || !catalog.opening.is_empty()
                || !catalog.draining.is_empty()
            {
                return Err("cwd is outside every registered Acyclic session; refusing service-assisted filesystem access".to_owned());
            }
            drop(catalog);
            let canonical = request.cwd.canonicalize().map_err(display)?;
            let session_id = format!(
                "cwd:{}",
                blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
            );
            self.start_session(&session_id, "cli", canonical).await?.0
        };
        handle.dispatch(request).await
    }
}

impl ControlRequestDispatcher for ControlPlane {
    async fn dispatch_request(&mut self, request: ControlRequest) -> Result<Value, String> {
        dispatch_session_request(self, request).await
    }
}

pub(crate) async fn dispatch_session_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    control.make_durable().await?;
    if request.command == ControlCommand::Hook {
        let (host, event) = request
            .name
            .split_once(':')
            .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
        return dispatch_native_session_hook(control, host, event, request.arguments, &request.cwd)
            .await;
    }
    let selected_cwd = selected_cli_cwd(&request)?;
    let (invocation_caller, _) = control.route_from_cwd(&request.cwd)?;
    let (selected_caller, _) = control.route_from_cwd(&selected_cwd)?;
    if selected_caller != invocation_caller {
        return Err("acyclic -C cannot cross workspace-context authority boundaries".to_owned());
    }
    let mut request = request;
    request.cwd = selected_cwd;
    request.arguments = Value::Null;
    dispatch_plane_request(control, request).await
}

pub(crate) async fn dispatch_plane_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    match request.command {
        ControlCommand::Ping => Ok(json!({})),
        ControlCommand::Doctor => {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
            doctor_report(
                &control.config_root,
                &executable.service_identity,
                &executable.sha256,
                &executable.blake3,
                1,
                control.state.routes.len(),
                control.state.leases.len(),
                !control.state.pending_discards.is_empty() || !control.state.pending.is_empty(),
            )
        }
        ControlCommand::Agents => {
            parse_read_only_arguments(&request.argv)?;
            let (caller, _) = control.route_from_cwd(&request.cwd)?;
            control.agents_status(&caller).await
        }
        ControlCommand::Discard => {
            let (caller, _) = control.route_from_cwd(&request.cwd)?;
            let reference = request
                .argv
                .first()
                .and_then(|value| value.strip_prefix("agents/"))
                .ok_or_else(|| "discard requires agents/<ref>".to_owned())?;
            control
                .agent_discard_as(&caller, json!({"agent": reference}))
                .await
        }
        ControlCommand::Git => {
            if request.argv.is_empty() {
                return Err("acyclic git requires a Git-style subcommand".to_owned());
            }
            let (caller, route, root_id) = control.route_root_from_cwd(&request.cwd)?;
            if let [command, option] = request.argv.as_slice()
                && command == "merge"
                && matches!(option.as_str(), "--continue" | "--abort")
            {
                let abort = option == "--abort";
                let agent_conflict = control
                    .pending_conflict_for_parent(control.context_for_agent(&caller)?)
                    .await?
                    .is_some();
                return if route.is_some() || agent_conflict {
                    control.agent_merge_transition(&caller, abort).await
                } else {
                    control.root_git_tool(root_id, request.argv).await
                };
            }
            if request.argv.first().is_some_and(|command| command == "add")
                && let Some(operation_id) = control
                    .pending_conflict_for_parent(control.context_for_agent(&caller)?)
                    .await?
            {
                control.sync_agent(&caller).await?;
                let paths = request
                    .argv
                    .iter()
                    .skip(1)
                    .filter(|argument| argument.as_str() != "--")
                    .filter(|argument| !argument.starts_with('-'))
                    .cloned()
                    .collect::<Vec<_>>();
                let paths = if paths.is_empty()
                    && request
                        .argv
                        .iter()
                        .skip(1)
                        .any(|argument| argument == "-A" || argument == "--all")
                {
                    vec![".".to_owned()]
                } else {
                    paths
                };
                if paths.is_empty() {
                    return Err("acyclic git add requires a conflicted path or -A".to_owned());
                }
                control
                    .publication_coordinator()
                    .await?
                    .declare_conflicted_paths(
                        operation_id,
                        control.context_for_agent(&caller)?,
                        root_id,
                        &paths,
                    )
                    .await
                    .map_err(display)?;
                return Ok(json!({"status":"resolved","automaticStaging":true}));
            }
            if request
                .argv
                .first()
                .is_some_and(|command| command == "merge")
                && let Some(reference) = request
                    .argv
                    .get(1)
                    .and_then(|value| value.strip_prefix("agents/"))
            {
                return control
                    .agent_merge_as(&caller, json!({"agent": reference}))
                    .await;
            }
            if request
                .argv
                .first()
                .is_some_and(|command| command == "diff")
                && let Some(reference) = request
                    .argv
                    .get(1)
                    .and_then(|value| value.strip_prefix("agents/"))
            {
                let mut paths = request.argv.get(2..).unwrap_or_default();
                if paths.first().is_some_and(|argument| argument == "--") {
                    paths = paths.get(1..).unwrap_or_default();
                }
                if paths.len() > 1 {
                    return Err("acyclic git diff agents/<ref> accepts at most one path".to_owned());
                }
                return control
                    .agent_changes_as(
                        &caller,
                        json!({
                            "agent": reference,
                            "path": paths.first(),
                            "_caller_root_id": hex::encode(root_id.into_bytes())
                        }),
                    )
                    .await;
            }
            match route {
                Some(route) => {
                    control
                        .git_tool(&route.agent_id, &route, root_id, request.argv)
                        .await
                }
                None => control.root_git_tool(root_id, request.argv).await,
            }
        }
        ControlCommand::Hook => {
            Err("control command is invalid for a workspace session".to_owned())
        }
    }
}

pub(crate) fn uncorrelated_control_response(error: &str) -> Vec<u8> {
    encode_control_response(&json!({"version":2,"ok":false,"error":error}), None)
}

pub(crate) fn invalid_control_request_response(
    request: &[u8],
    error: &serde_json::Error,
) -> Vec<u8> {
    let message = format!("invalid Acyclic control request: {error}");
    let request_id = serde_json::from_slice::<Value>(request)
        .ok()
        .and_then(|value| value.get("requestId")?.as_str().map(str::to_owned))
        .and_then(|value| control_protocol::RequestId::from_wire(value).ok());
    match request_id {
        Some(request_id) => control_response_for(&request_id, Err(message)),
        None => uncorrelated_control_response(&message),
    }
}

/// Encodes the response for one request, bounded to one control message.
pub(crate) fn control_response_for(
    request_id: &control_protocol::RequestId,
    result: Result<Value, String>,
) -> Vec<u8> {
    let response = match result {
        Ok(result) => {
            json!({"version":2,"requestId":request_id.as_str(),"ok":true,"result":result})
        }
        Err(error) => {
            json!({"version":2,"requestId":request_id.as_str(),"ok":false,"error":error})
        }
    };
    encode_control_response(&response, Some(request_id))
}

pub(crate) struct BoundedJsonBuffer {
    pub(crate) bytes: Vec<u8>,
}

impl BoundedJsonBuffer {
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(8 * 1024),
        }
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(buffer.len()) >= MAXIMUM_CONTROL_MESSAGE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Acyclic control response exceeds the 4 MiB bound",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Encodes `response` in under one control message (leaving room for the
/// frame's newline), or, when it does not fit, the error that replaces it.
pub(crate) fn encode_control_response(
    response: &Value,
    request_id: Option<&control_protocol::RequestId>,
) -> Vec<u8> {
    let mut bounded = BoundedJsonBuffer::new();
    if serde_json::to_writer(&mut bounded, response).is_ok() {
        return bounded.bytes;
    }
    // Request identities are validated ASCII identifiers, so they need no
    // escaping.
    let correlation = request_id.map_or_else(String::new, |request_id| {
        format!(r#""requestId":"{}","#, request_id.as_str())
    });
    format!(
        r#"{{"version":2,{correlation}"ok":false,"error":"Acyclic control response exceeds the 4 MiB bound"}}"#
    )
    .into_bytes()
}
