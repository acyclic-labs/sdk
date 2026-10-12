//! The service's request dispatch and session catalog.
//! Trusted CLI infrastructure is configured under the service lock before the
//! finite native bootstrap. Protected session IO then uses the held SDK private
//! root and original work; native response custody extends through transport IO.

use super::*;
use acyclic_harness::Error;

pub(crate) fn session_directory(data: &Path, session_id: &str) -> PathBuf {
    data.join("sessions")
        .join(blake3::hash(session_id.as_bytes()).to_hex().as_str())
}

#[derive(Clone)]
pub(crate) struct ServiceResources {
    pub(crate) data: PathBuf,
    pub(crate) fs: LocalFs,
    pub(crate) store: LocalCoreStateStore,
    pub(crate) shared_roots: SharedRootRegistry,
    pub(crate) binary_identity: String,
    pub(crate) instance_id: String,
    pub(crate) private_state_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
}

pub(crate) type ServiceConfiguredInfrastructure = (LocalFs, LocalCoreStateStore, String, String);

impl ServiceResources {
    // The trusted, locked CLI bootstrap configures daemon-lifetime backends
    // before finite native work. This is not a Root IO/allocation receipt.
    pub(crate) async fn configure(
        data: &Path,
    ) -> acyclic_harness::Result<ServiceConfiguredInfrastructure> {
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(|error| Error::Storage(display(error)))?;
        acyclic_fs::initialize_native_mount_services()
            .map_err(|error| Error::Storage(display(error)))?;
        let identity_data = data.to_path_buf();
        let binary_identity = tokio::task::spawn_blocking(move || service_identity(&identity_data));
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(display(error)))?;
        let binary_identity = binary_identity
            .await
            .map_err(|error| Error::Storage(display(error)))?
            .map_err(Error::Storage)?;
        let store = LocalCoreStateStore::open_owned(data.join("core-state"))
            .map_err(|error| Error::Storage(display(error)))?;
        Ok((fs, store, binary_identity, uuid::Uuid::new_v4().to_string()))
    }

    pub(crate) async fn open(
        data: PathBuf,
        configured: ServiceConfiguredInfrastructure,
        private_state_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
        original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Self, BTreeMap<String, SessionController>)> {
        original_bootstrap_work
            .create_directories_with_native_work(&private_state_root, Path::new("sessions"))?;
        let (fs, store, binary_identity, instance_id) = configured;
        let resources = Self {
            store,
            shared_roots: SharedRootRegistry::default(),
            binary_identity,
            instance_id,
            data,
            fs,
            private_state_root,
        };
        resources
            .shared_roots
            .collection
            .start(resources.fs.clone(), resources.store.clone());
        let mut entries = original_bootstrap_work.read_private_state_directory_with_native_work(
            &resources.private_state_root,
            Path::new("sessions"),
        )?;
        for entry in entries.0.iter_mut().filter(|entry| entry.is_directory) {
            let relative = original_bootstrap_work
                .path(Path::new("sessions"), Some(Path::new(&entry.name)))?;
            let mut modified: Option<std::time::SystemTime> = None;
            for slot in ADAPTER_STATE_SLOTS {
                let path = original_bootstrap_work.path(&relative.0, Some(Path::new(slot)))?;
                match original_bootstrap_work
                    .state_path_modified_with_native_work(&resources.private_state_root, &path.0)
                {
                    Ok(Some(value)) => {
                        modified = Some(modified.map_or(value, |old| old.max(value)))
                    }
                    Ok(None) | Err(acyclic_fs::NativeTreeMaterializationError::Io(_)) => {}
                    Err(cause) => {
                        return Err(Error::NativeWork {
                            cause,
                            custody: None,
                        });
                    }
                }
            }
            entry.modified = modified.unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        }
        original_bootstrap_work.sort_by_with_native_work(&mut entries.0, |left, right| {
            if left.is_directory != right.is_directory {
                return Ok(!left.is_directory);
            }
            if !left.is_directory {
                return Ok(false);
            }
            let order = left.modified.cmp(&right.modified);
            if order != std::cmp::Ordering::Equal {
                return Ok(order == std::cmp::Ordering::Less);
            }
            Ok(left.name < right.name)
        })?;
        let mut sessions = BTreeMap::new();
        let mut retained_sources = BTreeMap::<PathBuf, ([u8; 16], [u8; 16])>::new();
        for row in entries.0.iter().filter(|entry| entry.is_directory) {
            let relative =
                original_bootstrap_work.path(Path::new("sessions"), Some(Path::new(&row.name)))?;
            let (directory_custody, entry) = {
                let (path, custody) =
                    original_bootstrap_work.path(&resources.data, Some(&relative.0))?;
                (custody, path)
            };
            let loaded = crate::adapter_state::load_state_with_native_work(
                &resources.private_state_root,
                &relative.0,
                original_bootstrap_work,
            )?;
            let state = &loaded.0.0;
            if !state.active {
                continue;
            }
            if matches!(state.task_binding, PluginTaskBinding::Pi(_)) {
                let control = SessionController::open(entry, &resources, original_bootstrap_work)
                    .await?
                    .retain_original_input(directory_custody)?;
                let (session_id, key_custody) =
                    original_bootstrap_work.display(&state.root_session_id)?;
                let control = control.retain_original_input(key_custody)?;
                sessions.insert(session_id, control);
                continue;
            }
            let mut roots = Vec::with_capacity(state.roots.len());
            let mut stale = false;
            for root in state.roots.values() {
                let creation = root.created().map_err(Error::Invalid)?;
                let canonical = root
                    .path
                    .canonicalize()
                    .map_err(|error| Error::Storage(display(error)))?;
                if retained_sources.get(&canonical).is_some_and(
                    |(source_identity, native_identity)| {
                        *native_identity == creation.native_root_identity
                            && *source_identity != creation.source_identity
                    },
                ) {
                    stale = true;
                    break;
                }
                roots.push((
                    canonical,
                    (creation.source_identity, creation.native_root_identity),
                ));
            }
            if stale {
                fs::remove_dir_all(&entry).map_err(|error| Error::Storage(display(error)))?;
                continue;
            }
            let control = resources
                .open_session_directory(entry)
                .await
                .map_err(Error::Storage)?;
            if !control.state.root_session_id.is_empty() {
                retained_sources.extend(roots);
                let (session_id, key_custody) =
                    original_bootstrap_work.display(&control.state.root_session_id)?;
                let control = SessionController::from(control)
                    .retain_original_input(directory_custody)?
                    .retain_original_input(key_custody)?;
                sessions.insert(session_id, control);
            }
        }
        Ok((resources, sessions))
    }

    pub(crate) fn session_directory(&self, session_id: &str) -> PathBuf {
        session_directory(&self.data, session_id)
    }

    pub(crate) fn original_session_directory_matches(
        &self,
        relative_directory: &Path,
        session_id: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<bool> {
        let items_examined =
            (session_id.len() as u64)
                .checked_add(64)
                .ok_or(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "original session identity inspection count overflows",
                })?;
        work.charge(acyclic_fs::WorkCounters {
            items_examined,
            ..Default::default()
        })?;
        let key = blake3::hash(session_id.as_bytes()).to_hex();
        Ok(relative_directory.parent() == Some(Path::new("sessions"))
            && relative_directory.file_name() == Some(std::ffi::OsStr::new(key.as_str())))
    }

    pub(crate) fn private_session_data<'a>(
        &self,
        directory: &'a Path,
    ) -> acyclic_harness::Result<&'a Path> {
        let relative = directory
            .strip_prefix(&self.data)
            .map_err(|_| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "private session directory is outside the configured service root",
            })?;
        let mut components = relative.components();
        let Some(std::path::Component::Normal(namespace)) = components.next() else {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "private session directory has no configured sessions namespace",
            });
        };
        let key = match components.next() {
            Some(std::path::Component::Normal(key)) => key.to_str(),
            _ => None,
        };
        if namespace != "sessions"
            || !key.is_some_and(|key| {
                key.len() == 64
                    && key
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            || components.next().is_some()
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "private session directory is not an original service session key",
            });
        }
        Ok(relative)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(crate) enum SessionCloseState {
    Closed,
    Failed,
}

/// Readonly service metadata, never a native allocation or authority proof.
pub(crate) enum SessionSnapshot {
    HostManaged(AdapterState),
    Pi {
        original_task_id: acyclic_harness::TaskId,
        pending_root_registration: bool,
        roots_created: bool,
        scratch_created: bool,
        routes: usize,
        leases: usize,
        pending_recovery: bool,
    },
}

impl SessionSnapshot {
    fn observe(control: &SessionController) -> Self {
        let state = control.state();
        match &state.task_binding {
            PluginTaskBinding::Pi(binding) => Self::Pi {
                original_task_id: binding.task_id,
                pending_root_registration: state.pending_root_registration,
                roots_created: state
                    .roots
                    .values()
                    .all(|root| matches!(root.allocation, RootAllocation::Created(_))),
                scratch_created: matches!(
                    binding.scratch.allocation,
                    PiScratchAllocation::Created(_)
                ),
                routes: state.routes.len(),
                leases: state.leases.len(),
                pending_recovery: !state.pending_discards.is_empty() || !state.pending.is_empty(),
            },
            _ => Self::HostManaged(state.clone()),
        }
    }
}

pub(crate) struct SessionHandle {
    pub(crate) key: SessionKey,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    pub(crate) admission: Mutex<()>,
    pub(crate) sender: tokio::sync::mpsc::Sender<SessionCommand>,
    pub(crate) data_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) snapshot: Arc<std::sync::RwLock<Option<SessionSnapshot>>>,
    pub(crate) close_result: watch::Sender<Option<SessionCloseState>>,
}

pub(crate) enum SessionCommand {
    Dispatch {
        request: ControlRequest,
        original_native_work: Option<acyclic_fs::NativeMaterializationWork>,
        response: tokio::sync::oneshot::Sender<(
            acyclic_harness::Result<ControlDispatchResponse>,
            Option<acyclic_fs::NativeMaterializationWork>,
        )>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    Shutdown {
        deactivate: bool,
        original_native_work: Option<acyclic_fs::NativeMaterializationWork>,
        response: tokio::sync::oneshot::Sender<(
            acyclic_harness::Result<SessionCloseState>,
            Option<acyclic_fs::NativeMaterializationWork>,
        )>,
    },
    #[cfg(test)]
    Pause {
        entered: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    },
}

impl SessionHandle {
    pub(crate) fn new(
        key: SessionKey,
        control: impl Into<SessionController>,
        resources: (ServiceResources, Option<acyclic_fs::NativeAllocation>),
    ) -> Self {
        let control = control.into();
        let snapshot = Arc::new(std::sync::RwLock::new(Some(SessionSnapshot::observe(
            &control,
        ))));
        let actor_snapshot = Arc::clone(&snapshot);
        let state = Arc::new(std::sync::atomic::AtomicU8::new(SESSION_ACTIVE));
        let actor_state = Arc::clone(&state);
        let close_result = watch::channel(None).0;
        let actor_close_result = close_result.clone();
        let data_slots = Arc::new(tokio::sync::Semaphore::new(SESSION_DATA_CAPACITY));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(SESSION_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut undelivered_close_failure: Option<Error> = None;
            let mut control = Some(control);
            while let Some(command) = receiver.recv().await {
                match command {
                    SessionCommand::Dispatch {
                        request,
                        mut original_native_work,
                        response,
                        _permit,
                    } => {
                        let result = match control.as_mut() {
                            Some(control) => {
                                control.dispatch(request, &mut original_native_work).await
                            }
                            None => Err(Error::Invalid(
                                "Acyclic session actor has no workspace".to_owned(),
                            )),
                        };
                        // Other sessions see only durable state: an unflushed
                        // save is published once flushed, after the reply.
                        let mut pending = Some((response, (result, original_native_work)));
                        if control.as_ref().is_some_and(SessionController::unflushed)
                            && let Some((response, result)) = pending.take()
                        {
                            let _ = response.send(result);
                        }
                        let durable = match control.as_mut() {
                            Some(control) => control.make_durable().await.is_ok(),
                            None => true,
                        };
                        if durable {
                            let next = control.as_ref().map(SessionSnapshot::observe);
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
                        mut original_native_work,
                        response,
                    } => {
                        let result = async {
                            if original_native_work.is_none()
                                && control.as_ref().is_some_and(|current| {
                                    matches!(current, SessionController::Pi(_))
                                })
                            {
                                let cancellation = CancellationToken::new();
                                original_native_work = Some(
                                    acyclic_fs::NativeMaterializationWork::new(
                                        pi_root_provider::allocation_budget(),
                                        &cancellation,
                                    )
                                    .map_err(
                                        |(cause, work)| Error::NativeConstruction { cause, work },
                                    )?,
                                );
                            }
                            let current = control.as_ref().ok_or_else(|| {
                                Error::Invalid("Acyclic session actor has no workspace".to_owned())
                            })?;
                            let native = matches!(current, SessionController::Pi(_));
                            let mut terminal_state = if native {
                                None
                            } else {
                                Some(current.state().clone())
                            };
                            let mut directory = match (native, original_native_work.as_ref()) {
                                (true, Some(work)) => {
                                    let copied = work.path(current.directory(), None)?;
                                    (copied.0, Some(copied.1))
                                }
                                (true, None) => {
                                    return Err(Error::NativeBoundary {
                                        code: acyclic_harness::wire::ErrorCode::Storage,
                                        reason: "Pi close requires its original native query span",
                                    });
                                }
                                (false, _) => (current.directory().to_path_buf(), None),
                            };
                            let current = control.take().ok_or_else(|| {
                                Error::Invalid("Acyclic session actor has no workspace".to_owned())
                            })?;
                            match current
                                .close(deactivate, original_native_work.as_ref())
                                .await
                            {
                                Ok(mut slots) => {
                                    if deactivate
                                        && let Some(terminal_state) = terminal_state.as_mut()
                                    {
                                        terminal_state.active = false;
                                        slots
                                            .save(&directory.0, terminal_state, Survives::PowerLoss)
                                            .map_err(Error::Storage)?;
                                    }
                                    Ok(SessionCloseState::Closed)
                                }
                                Err(shutdown_error) => {
                                    let recovered = match original_native_work.as_ref() {
                                        Some(work) => {
                                            SessionController::open(directory.0, &resources.0, work)
                                                .await
                                        }
                                        None => resources
                                            .0
                                            .open_session_directory(directory.0)
                                            .await
                                            .map(Into::into)
                                            .map_err(Error::Storage),
                                    };
                                    if let Ok(recovered) = recovered {
                                        let recovered = match directory.1.take() {
                                            Some(custody) => {
                                                recovered.retain_original_input(custody)
                                            }
                                            None => Ok(recovered),
                                        };
                                        if let Ok(recovered) = recovered {
                                            control = Some(recovered);
                                        }
                                    }
                                    Err(shutdown_error)
                                }
                            }
                        }
                        .await;
                        let status = if result.is_ok() {
                            SessionCloseState::Closed
                        } else {
                            SessionCloseState::Failed
                        };
                        let _ = actor_close_result.send(Some(status));
                        if result.is_ok() {
                            actor_state.store(SESSION_CLOSED, std::sync::atomic::Ordering::Release);
                            if let Ok(mut snapshot) = actor_snapshot.write() {
                                *snapshot = None;
                            }
                            let _ = response.send((result, original_native_work));
                            break;
                        }
                        actor_state
                            .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                        let next = control.as_ref().map(SessionSnapshot::observe);
                        if let Ok(mut snapshot) = actor_snapshot.write() {
                            *snapshot = next;
                        }
                        if let Err((Err(error), _)) = response.send((result, original_native_work))
                        {
                            if undelivered_close_failure.is_none() {
                                undelivered_close_failure = Some(error);
                            }
                        }
                    }
                    #[cfg(test)]
                    SessionCommand::Pause { entered, resume } => {
                        let _ = entered.send(());
                        let _ = resume.await;
                    }
                }
            }
            drop(control);
            // Capture the whole pair: resource bodies outlive controller
            // recovery and always drop before their original allocation.
            drop(resources);
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

    pub(crate) fn with_snapshot<T>(
        &self,
        read: impl FnOnce(Option<&SessionSnapshot>) -> T,
    ) -> Result<T, String> {
        self.snapshot
            .read()
            .map(|snapshot| read(snapshot.as_ref()))
            .map_err(|_| "session routing snapshot lock is poisoned".to_owned())
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == SESSION_ACTIVE
    }

    pub(crate) async fn dispatch(
        &self,
        request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        let (response, result) = tokio::sync::oneshot::channel();
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| Error::Storage("session admission lock is poisoned".to_owned()))?;
            if !self.is_active() {
                return Err(Error::Invalid("Acyclic session is closing".to_owned()));
            }
            let permit = Arc::clone(&self.data_slots)
                .try_acquire_owned()
                .map_err(|_| {
                    Error::Invalid("Acyclic session operation queue is full".to_owned())
                })?;
            self.sender
                .try_send(SessionCommand::Dispatch {
                    request,
                    original_native_work: original_native_work.clone(),
                    response,
                    _permit: permit,
                })
                .map_err(|error| {
                    Error::Invalid(match error {
                        tokio::sync::mpsc::error::TrySendError::Full(_) => {
                            "Acyclic session operation queue is full".to_owned()
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            "Acyclic session actor stopped".to_owned()
                        }
                    })
                })?;
        }
        let (result, returned_work) = result.await.map_err(|_| {
            Error::Storage("Acyclic session actor dropped an accepted operation".to_owned())
        })?;
        *original_native_work = returned_work;
        result
    }

    pub(crate) async fn shutdown(
        &self,
        deactivate: bool,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<SessionCloseState> {
        let (response, result) = tokio::sync::oneshot::channel();
        let mut existing = None;
        {
            let _admission = self
                .admission
                .lock()
                .map_err(|_| Error::Storage("session admission lock is poisoned".to_owned()))?;
            match self.state.load(std::sync::atomic::Ordering::Acquire) {
                SESSION_ACTIVE => self
                    .state
                    .store(SESSION_CLOSING, std::sync::atomic::Ordering::Release),
                SESSION_CLOSING => existing = Some(self.close_result.subscribe()),
                SESSION_CLOSE_FAILED => return Ok(SessionCloseState::Failed),
                SESSION_CLOSED => return Ok(SessionCloseState::Closed),
                _ => {
                    return Err(Error::Invalid(
                        "invalid Acyclic session lifecycle state".to_owned(),
                    ));
                }
            }
            if existing.is_none()
                && let Err(error) = self.sender.try_send(SessionCommand::Shutdown {
                    deactivate,
                    original_native_work: original_native_work.clone(),
                    response,
                })
            {
                let error = Error::Invalid(match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        "Acyclic session control queue is unexpectedly full during shutdown"
                            .to_owned()
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        "Acyclic session actor stopped before shutdown".to_owned()
                    }
                });
                self.state
                    .store(SESSION_CLOSE_FAILED, std::sync::atomic::Ordering::Release);
                let _ = self.close_result.send(Some(SessionCloseState::Failed));
                return Err(error);
            }
        }
        if let Some(mut existing) = existing {
            if existing.borrow().is_none() {
                existing
                    .changed()
                    .await
                    .map_err(|error| Error::Storage(display(error)))?;
            }
            return existing.borrow().as_ref().copied().ok_or_else(|| {
                Error::Storage("Acyclic session close completed without a status".to_owned())
            });
        }
        let (result, returned_work) = result
            .await
            .map_err(|_| Error::Storage("Acyclic session actor dropped shutdown".to_owned()))?;
        *original_native_work = returned_work;
        result
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
    pub(crate) async fn open(
        data: PathBuf,
        configured: ServiceConfiguredInfrastructure,
        private_state_root: acyclic_fs::materializer::NativeHostPrivateStateRoot,
        original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let (resources, sessions) = ServiceResources::open(
            data,
            configured,
            private_state_root,
            original_bootstrap_work,
        )
        .await?;
        Self::from_parts(resources, sessions, original_bootstrap_work)
    }

    pub(crate) fn from_parts(
        resources: ServiceResources,
        sessions: BTreeMap<String, SessionController>,
        original_bootstrap_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        let mut catalog = SessionCatalog::default();
        for (session_id, control) in sessions {
            catalog.next_epoch += 1;
            let key = SessionKey {
                session_id: session_id.clone(),
                epoch: catalog.next_epoch,
            };
            let actor_resources = pi_root_provider::service_resources_with_native_work(
                &resources,
                original_bootstrap_work,
            )?;
            catalog.active.insert(
                session_id,
                Arc::new(SessionHandle::new(
                    key,
                    control,
                    (actor_resources.0, Some(actor_resources.1)),
                )),
            );
        }
        Ok(Self {
            resources,
            lifecycle: std::sync::atomic::AtomicU8::new(SERVICE_RUNNING),
            shutdown_deactivates: std::sync::atomic::AtomicBool::new(false),
            catalog: AsyncMutex::new(catalog),
        })
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
        mut input: Value,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<(Arc<SessionHandle>, ControlDispatchResponse)> {
        let root = root
            .canonicalize()
            .map_err(|error| Error::Storage(display(error)))?;
        let arguments = input.as_object_mut().ok_or_else(|| {
            Error::Invalid("session bootstrap input must be an object".to_owned())
        })?;
        arguments.insert("session_id".to_owned(), json!(session_id));
        arguments.insert("cwd".to_owned(), json!(root));
        if let Some(owner) = self.route_session(&root).await.map_err(Error::Storage)?
            && owner.key.session_id != session_id
        {
            return Err(Error::Invalid(format!(
                "session {session_id} cannot attach a workspace owned by session {} as a physical root",
                owner.key.session_id
            )));
        }
        loop {
            self.ensure_running().map_err(Error::Invalid)?;
            let (key, wait) = {
                let mut catalog = self.catalog.lock().await;
                if let Some(handle) = catalog.active.get(session_id) {
                    let handle = Arc::clone(handle);
                    drop(catalog);
                    let output = handle
                        .dispatch(
                            ControlRequest {
                                version: 1,
                                command: ControlCommand::Hook,
                                cwd: root.clone(),
                                argv: Vec::new(),
                                name: format!("{host}:SessionStart"),
                                arguments: input.clone(),
                            },
                            original_native_work,
                        )
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
                    wait.changed()
                        .await
                        .map_err(|error| Error::Storage(display(error)))?;
                }
                continue;
            }

            let opening_result = async {
                if host == "pi" {
                    if original_native_work.is_none() {
                        let cancellation = CancellationToken::new();
                        *original_native_work = Some(
                            acyclic_fs::NativeMaterializationWork::new(
                                pi_root_provider::allocation_budget(),
                                &cancellation,
                            )
                            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?,
                        );
                    }
                    let work =
                        original_native_work
                            .as_ref()
                            .ok_or_else(|| Error::NativeBoundary {
                                code: acyclic_harness::wire::ErrorCode::Storage,
                                reason: "Pi session bootstrap lost its original query work",
                            })?;
                    let actor_resources = pi_root_provider::service_resources_with_native_work(
                        &self.resources,
                        work,
                    )?;
                    let native_runtime = input
                        .get("runtime")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|error| Error::Invalid(display(error)))?;
                    let mut control = pi_registration::register(
                        &self.resources,
                        session_id,
                        root.clone(),
                        native_runtime,
                        work,
                    )
                    .await?;
                    let opened = control
                        .dispatch(
                            ControlRequest {
                                version: 1,
                                command: ControlCommand::Hook,
                                cwd: root.clone(),
                                argv: Vec::new(),
                                name: "pi:SessionStart".to_owned(),
                                arguments: input.clone(),
                            },
                            original_native_work,
                        )
                        .await;
                    Ok::<_, Error>((
                        control,
                        opened,
                        (actor_resources.0, Some(actor_resources.1)),
                    ))
                } else {
                    let mut control = self
                        .resources
                        .open_session_directory(self.resources.session_directory(session_id))
                        .await
                        .map_err(Error::Storage)?;
                    let opened = control
                        .session_start(json!({"session_id": session_id, "cwd": root, "host": host}))
                        .await
                        .map(|value| (value, None))
                        .map_err(Error::Storage);
                    Ok((control.into(), opened, (self.resources.clone(), None)))
                }
            }
            .await;
            let (control, opened, actor_resources) = match opening_result {
                Ok(opened) => opened,
                Err(error) => {
                    let mut catalog = self.catalog.lock().await;
                    let opening = catalog.opening.remove(session_id).ok_or_else(|| {
                        Error::Storage("Acyclic session opening reservation disappeared".to_owned())
                    })?;
                    if opening.key != key {
                        return Err(Error::Storage(
                            "Acyclic session opening epoch changed".to_owned(),
                        ));
                    }
                    opening.complete.send_replace(true);
                    return Err(error);
                }
            };
            let handle = Some(Arc::new(SessionHandle::new(
                key.clone(),
                control,
                actor_resources,
            )));
            let (cancelled, complete) = {
                let mut catalog = self.catalog.lock().await;
                let opening = catalog.opening.remove(session_id).ok_or_else(|| {
                    Error::Storage("Acyclic session opening reservation disappeared".to_owned())
                })?;
                if opening.key != key {
                    return Err(Error::Storage(
                        "Acyclic session opening epoch changed".to_owned(),
                    ));
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
                        let cleanup = handle.shutdown(true, original_native_work).await;
                        if cleanup
                            .as_ref()
                            .is_ok_and(|status| *status == SessionCloseState::Closed)
                        {
                            let mut catalog = self.catalog.lock().await;
                            catalog.draining.remove(&handle.key);
                            catalog.completed.insert(session_id.to_owned());
                        }
                        cleanup
                    } else {
                        Ok(SessionCloseState::Closed)
                    };
                    let _ = complete.send(true);
                    return match cleanup {
                        Ok(_) => Err(error),
                        Err(_) => Err(error),
                    };
                }
            };
            let handle =
                handle.ok_or_else(|| Error::Storage("Acyclic session did not open".to_owned()))?;
            if cancelled {
                let cleanup = handle.shutdown(true, original_native_work).await;
                if cleanup
                    .as_ref()
                    .is_ok_and(|status| *status == SessionCloseState::Closed)
                {
                    let mut catalog = self.catalog.lock().await;
                    catalog.draining.remove(&handle.key);
                    catalog.completed.insert(session_id.to_owned());
                }
                let _ = complete.send(true);
                let _ = cleanup?;
                return Err(Error::Invalid(
                    "Acyclic session start was cancelled".to_owned(),
                ));
            }
            let _ = complete.send(true);
            return Ok((handle, output));
        }
    }

    pub(crate) async fn end_session(
        &self,
        session_id: &str,
        deactivate: bool,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<SessionCloseState> {
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
                    wait.changed()
                        .await
                        .map_err(|error| Error::Storage(display(error)))?;
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
                    return Ok(SessionCloseState::Closed);
                }
                if catalog.completed.contains(session_id) {
                    return Ok(SessionCloseState::Closed);
                }
                return Err(Error::Invalid(
                    "Acyclic native hook session is not registered".to_owned(),
                ));
            }
        };
        let result = handle.shutdown(deactivate, original_native_work).await;
        {
            let mut catalog = self.catalog.lock().await;
            if result
                .as_ref()
                .is_ok_and(|status| *status == SessionCloseState::Closed)
            {
                catalog.draining.remove(&handle.key);
                catalog.completed.insert(session_id.to_owned());
            }
        }
        if result
            .as_ref()
            .is_ok_and(|status| *status == SessionCloseState::Closed)
        {
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
            let score = handle.with_snapshot(|snapshot| {
                let Some(SessionSnapshot::HostManaged(snapshot)) = snapshot else {
                    return Ok::<_, String>(None);
                };
                snapshot.require_created_roots()?;
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
                Ok(score)
            })??;
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

    pub(crate) async fn drain_sessions(
        &self,
        deactivate: bool,
    ) -> acyclic_harness::Result<SessionCloseState> {
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
                    wait.changed()
                        .await
                        .map_err(|error| Error::Storage(display(error)))?;
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
        let mut status = SessionCloseState::Closed;
        let mut first_error = None;
        for handle in handles {
            match handle.shutdown(deactivate, &mut None).await {
                Ok(SessionCloseState::Closed) => {
                    self.catalog.lock().await.draining.remove(&handle.key);
                }
                Ok(SessionCloseState::Failed) => status = SessionCloseState::Failed,
                Err(error) => {
                    status = SessionCloseState::Failed;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            if status == SessionCloseState::Closed {
                self.lifecycle
                    .store(SERVICE_CLOSED, std::sync::atomic::Ordering::Release);
            }
            Ok(status)
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

    pub(crate) async fn shutdown(self) -> acyclic_harness::Result<SessionCloseState> {
        let root_released = self.fs.local_root_release_barrier();
        let deactivate = self
            .shutdown_deactivates
            .load(std::sync::atomic::Ordering::Acquire);
        let result = self.drain_sessions(deactivate).await;
        self.shared_roots.collection.stop().await;
        drop(self);
        let released = wait_for_root_release(root_released).await;
        match result {
            Err(error) => Err(error),
            Ok(status) => {
                released.map_err(Error::Storage)?;
                Ok(status)
            }
        }
    }
}

impl ConcurrentControlRequestDispatcher for ConcurrentServiceControl {
    async fn dispatch_request(
        &self,
        request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        if request.command == ControlCommand::Ping {
            let catalog = self.catalog.lock().await;
            let active_sessions = catalog.active.len();
            let opening_sessions = catalog.opening.len();
            let draining_sessions = catalog.draining.len();
            return Ok((
                json!({
                    "identity": self.binary_identity,
                    "instanceId": self.instance_id,
                    "sessions": active_sessions + opening_sessions + draining_sessions,
                    "activeSessions": active_sessions,
                    "openingSessions": opening_sessions,
                    "drainingSessions": draining_sessions,
                }),
                None,
            ));
        }
        self.ensure_running().map_err(Error::Invalid)?;
        if request.command == ControlCommand::Doctor {
            parse_read_only_arguments(&request.argv).map_err(Error::Invalid)?;
            let executable = executable_digests_async().await.map_err(Error::Storage)?;
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
                handle
                    .with_snapshot(|snapshot| match snapshot {
                        Some(SessionSnapshot::HostManaged(state)) => {
                            routes += state.routes.len();
                            leases += state.leases.len();
                            pending_recovery |=
                                !state.pending_discards.is_empty() || !state.pending.is_empty();
                        }
                        Some(SessionSnapshot::Pi {
                            routes: count_routes,
                            leases: count_leases,
                            pending_recovery: pending,
                            ..
                        }) => {
                            routes += count_routes;
                            leases += count_leases;
                            pending_recovery |= pending;
                        }
                        None => {}
                    })
                    .map_err(Error::Storage)?;
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
            )
            .map(|value| (value, None))
            .map_err(Error::Storage);
        }
        if request.command == ControlCommand::Hook {
            let (host, event) = request.name.split_once(':').ok_or_else(|| {
                Error::Invalid("native hook request has no host/event pair".to_owned())
            })?;
            let session_id =
                hook_id(&request.arguments, "session_id", "sessionId").map_err(Error::Invalid)?;
            if host == "pi"
                && let Some(caller) = request.arguments.get("caller")
            {
                let caller: acyclic_harness::core::Scope =
                    serde_json::from_value(caller.clone())
                        .map_err(|error| Error::Invalid(display(error)))?;
                let root_session = pi_caller::routing_session(&caller).map_err(Error::Invalid)?;
                // Routing is not authentication: only the existing original
                // actor verifies this proof and its admitted native child.
                // A recursive child can never bootstrap another root budget.
                return self
                    .session(&root_session)
                    .await
                    .map_err(Error::Invalid)?
                    .dispatch(request, original_native_work)
                    .await;
            }
            if matches!(event, "SessionStart" | "sessionStart") {
                let root =
                    hook_root_path(&request.arguments).unwrap_or_else(|| request.cwd.clone());
                let (_, output) = self
                    .start_session(
                        &session_id,
                        host,
                        root,
                        request.arguments.clone(),
                        original_native_work,
                    )
                    .await?;
                return Ok(output);
            }
            if matches!(event, "SessionEnd" | "sessionEnd") {
                let status = self
                    .end_session(&session_id, true, original_native_work)
                    .await?;
                return Ok((
                    match status {
                        SessionCloseState::Closed => json!({}),
                        SessionCloseState::Failed => json!({"closed": false, "state": "failed"}),
                    },
                    None,
                ));
            }
            return self
                .session(&session_id)
                .await
                .map_err(Error::Invalid)?
                .dispatch(request, original_native_work)
                .await;
        }
        let handle = if let Some(handle) = self
            .route_session(&request.cwd)
            .await
            .map_err(Error::Storage)?
        {
            handle
        } else {
            let catalog = self.catalog.lock().await;
            if !catalog.active.is_empty()
                || !catalog.opening.is_empty()
                || !catalog.draining.is_empty()
            {
                return Err(Error::Invalid("cwd is outside every registered Acyclic session; refusing service-assisted filesystem access".to_owned()));
            }
            drop(catalog);
            let canonical = request
                .cwd
                .canonicalize()
                .map_err(|error| Error::Storage(display(error)))?;
            let session_id = format!(
                "cwd:{}",
                blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
            );
            self.start_session(
                &session_id,
                "cli",
                canonical,
                json!({}),
                original_native_work,
            )
            .await?
            .0
        };
        handle.dispatch(request, original_native_work).await
    }
}

impl ControlRequestDispatcher for ControlPlane {
    async fn dispatch_request(
        &mut self,
        request: ControlRequest,
        _original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        dispatch_session_request(self, request)
            .await
            .map(|value| (value, None))
            .map_err(Error::Storage)
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
            if request.argv.as_slice() == ["merge", "--continue"] {
                return control.agent_merge_transition(&caller, false).await;
            }
            if request.argv.as_slice() == ["merge", "--abort"] {
                return control.agent_merge_transition(&caller, true).await;
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

pub(crate) fn uncorrelated_control_response(
    error: &str,
) -> acyclic_harness::Result<ControlEncodedResponse> {
    Ok((
        encode_control_response(&json!({"version":2,"ok":false,"error":error}), None),
        None,
        None,
    ))
}

pub(crate) fn invalid_control_request_response(
    request: &[u8],
    error: &serde_json::Error,
) -> acyclic_harness::Result<ControlEncodedResponse> {
    let message = format!("invalid Acyclic control request: {error}");
    let request_id = serde_json::from_slice::<Value>(request)
        .ok()
        .and_then(|value| value.get("requestId")?.as_str().map(str::to_owned))
        .and_then(|value| control_protocol::RequestId::from_wire(value).ok());
    match request_id {
        Some(request_id) => control_response_for(&request_id, Err(Error::Invalid(message)), None),
        None => uncorrelated_control_response(&message),
    }
}

pub(crate) fn serialize_control_error<S: serde::Serializer>(
    error: &Error,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(error)
}

/// Encodes the response for one request, bounded to one control message.
pub(crate) fn control_response_for(
    request_id: &control_protocol::RequestId,
    result: acyclic_harness::Result<ControlDispatchResponse>,
    original_native_work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> acyclic_harness::Result<ControlEncodedResponse> {
    obs::record_outcome(&result);
    #[derive(Serialize)]
    struct Response<'a> {
        version: u8,
        #[serde(rename = "requestId")]
        request_id: &'a str,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<&'a Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<ErrorMessage<'a>>,
    }
    struct ErrorMessage<'a>(&'a Error);
    impl Serialize for ErrorMessage<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serialize_control_error(self.0, serializer)
        }
    }
    // Declaration order also retires the payload before its SDK custody on
    // unwind. The original failure remains owned until actual transport send.
    let (failure, source_allocation, payload) = match result {
        Ok((payload, allocation)) => (None, allocation, Some(payload)),
        Err(error) => (Some(error), None, None),
    };
    let response = Response {
        version: 2,
        request_id: request_id.as_str(),
        ok: failure.is_none(),
        result: payload.as_ref(),
        error: failure.as_ref().map(ErrorMessage),
    };
    let encoded = match original_native_work {
        Some(work) => {
            // This is the original query span, not an execution-payload guard's
            // possibly different ledger. Failed admission sends no new frame.
            match service_process::encode_native_control_value(&response, work) {
                Ok((bytes, allocation)) => (bytes, Some(allocation)),
                Err(encoding_error) => {
                    // Close without an unfunded frame, but never replace an
                    // execution failure with a later reporting failure.
                    drop(payload);
                    drop(source_allocation);
                    return Err(failure.unwrap_or(encoding_error));
                }
            }
        }
        None => {
            if source_allocation.is_some() {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "native response has no original control-query work",
                });
            }
            if failure.as_ref().is_some_and(|error| {
                !matches!(
                    error,
                    Error::NotFound(_)
                        | Error::Conflict(_)
                        | Error::Unsupported(_)
                        | Error::Invalid(_)
                        | Error::Unauthorized(_)
                        | Error::Storage(_)
                        | Error::InteractionRejected(_)
                        | Error::Indeterminate(_)
                )
            }) {
                return Err(failure.expect("native failure is present"));
            }
            (encode_control_response(&response, Some(request_id)), None)
        }
    };
    drop(payload);
    drop(source_allocation);
    Ok((encoded.0, encoded.1, failure))
}

pub(crate) struct BoundedJsonBuffer {
    pub(crate) bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJsonBuffer {
    pub(crate) fn new() -> Self {
        // Control frames retain one byte for their trailing newline.
        Self::with_limit(MAXIMUM_CONTROL_MESSAGE_BYTES - 1)
    }

    pub(crate) fn with_limit(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(8 * 1024)),
            maximum,
        }
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "JSON exceeds its configured byte bound",
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
pub(crate) fn encode_control_response<T: Serialize + ?Sized>(
    response: &T,
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
