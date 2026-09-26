//! Host installation and uninstallation.

use super::*;

pub(crate) fn install_command(arguments: &[String]) -> Result<(), String> {
    let (host, project) = parse_install_arguments(arguments)?;
    if host == "--detected" {
        let mut installed = 0;
        for (binary, target) in [
            ("codex", "codex"),
            ("claude", "claude-code"),
            ("cursor", "cursor"),
            ("copilot", "copilot"),
            ("opencode", "opencode"),
        ] {
            if executable_on_path(binary) {
                install_host(target, false)?;
                installed += 1;
            }
        }
        if installed == 0 {
            return Err("no supported local coding-agent executable was detected".to_owned());
        }
        return Ok(());
    }
    install_host(host, project)
}

pub(crate) fn parse_install_arguments(arguments: &[String]) -> Result<(&str, bool), String> {
    match arguments {
        [host] if host == "--detected" => Ok((host, false)),
        [host] if !host.starts_with('-') => Ok((host, false)),
        [host, project] if !host.starts_with('-') && project == "--project" => Ok((host, true)),
        _ => Err("usage: acyclic install HOST [--project] | acyclic install --detected".to_owned()),
    }
}

pub(crate) async fn uninstall_command(arguments: &[String]) -> Result<(), String> {
    let (host, purge) = parse_uninstall_arguments(arguments)?;
    let data = default_data_directory();
    drop(drain_service(&data, None).await?);
    uninstall_host(host)?;
    if purge {
        purge_durable_state(&data).await?;
        println!("purged Acyclic durable state");
    } else {
        println!("preserved Acyclic durable state; pass --purge to remove it explicitly");
    }
    Ok(())
}

pub(crate) fn parse_uninstall_arguments(arguments: &[String]) -> Result<(&str, bool), String> {
    match arguments {
        [host] if !host.starts_with('-') => Ok((host, false)),
        [host, purge] if !host.starts_with('-') && purge == "--purge" => Ok((host, true)),
        _ => Err("usage: acyclic uninstall HOST [--purge]".to_owned()),
    }
}

pub(crate) fn parse_read_only_arguments(arguments: &[String]) -> Result<bool, String> {
    match arguments {
        [] => Ok(false),
        [json] if json == "--json" => Ok(true),
        _ => Err("usage: acyclic doctor [--json] | acyclic agents [--json]".to_owned()),
    }
}

pub(crate) async fn purge_durable_state(data: &Path) -> Result<(), String> {
    if !data.exists() {
        return Ok(());
    }
    let mut drain_fence = drain_service(data, None).await?;
    drain_fence.prepare_purge();
    let parent = data
        .parent()
        .ok_or_else(|| "durable state path has no parent".to_owned())?;
    remove_tree_checked(parent, data)
}

/// Takes the service lock when no service holds it, and clears the identity
/// the last service published. A service holds its lock for as long as it
/// runs, from before it opens its endpoint, so a free lock proves that none
/// is running or starting.
pub(crate) fn claim_stopped_service(
    data: &Path,
    expected_identity: Option<&str>,
) -> Result<Option<ServiceLock>, String> {
    let Some(lock) = acquire_service_lock(data)? else {
        return Ok(None);
    };
    if let Some(expected) = expected_identity {
        let marker = ServiceMarker::read(data).ok_or_else(|| {
            "cannot authenticate stale service: it published no identity".to_owned()
        })?;
        if marker.instance_id != expected {
            return Err("refusing to clean a replacement Acyclic service".to_owned());
        }
    }
    match fs::remove_file(ServiceMarker::path(data)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(display(error)),
    }
    Ok(Some(lock))
}

/// Stops the running service, if any, through the contract every binary
/// understands (see [`ServiceMarker`]), and returns its lock. A service is
/// named by its instance ID; `expected_identity` refuses any other.
pub(crate) async fn drain_service(
    data: &Path,
    expected_identity: Option<&str>,
) -> Result<ServiceLock, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    // A service publishes its marker once its endpoint opens, moments after
    // it takes the lock.
    let marker = loop {
        if let Some(lock) = claim_stopped_service(data, expected_identity)? {
            return Ok(lock);
        }
        if let Some(marker) = ServiceMarker::read(data) {
            break marker;
        }
        if std::time::Instant::now() >= deadline {
            return Err(
                "Acyclic service lock is held without a published identity; state was preserved"
                    .to_owned(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    if expected_identity.is_some_and(|expected| expected != marker.instance_id) {
        return Err("refusing to drain a replacement Acyclic service".to_owned());
    }
    let drain_id = uuid::Uuid::new_v4().to_string();
    request_service_stop(data, &marker.instance_id, &drain_id)?;
    for _ in 0..250 {
        if let Some(lock) = acquire_service_lock(data)? {
            verify_service_drain_completion(data, &marker.instance_id)?;
            return Ok(lock);
        }
        // A replacement that took the lock first holds what this drain was
        // for; waiting would only time out.
        if ServiceMarker::read(data).is_some_and(|next| next.instance_id != marker.instance_id) {
            return Err(
                "a replacement Acyclic service started before this drain could take its place"
                    .to_owned(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Err("Acyclic service did not drain; durable state and executable were preserved".to_owned())
}

pub(crate) async fn service_status(data: &Path) -> Result<Value, String> {
    let marker_identity = ServiceMarker::read(data).map(|marker| marker.instance_id);
    let ping = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    let reachable_identity = match send_control_request(data, &ping).await {
        Ok(response) => Some(
            response
                .get("instanceId")
                .or_else(|| response.get("identity"))
                .and_then(Value::as_str)
                .ok_or_else(|| "reachable service omitted its identity".to_owned())?
                .to_owned(),
        ),
        Err(ControlRequestError::Unavailable(_)) => None,
        Err(error) => return Err(format!("cannot inspect the Acyclic service: {error}")),
    };
    let lock = acquire_service_lock(data)?;
    let lock_acquirable = lock.is_some();
    drop(lock);
    Ok(json!({
        "version": 1,
        "markerIdentity": marker_identity,
        "reachableIdentity": reachable_identity,
        "lockAcquirable": lock_acquirable,
    }))
}

pub(crate) fn install_host(host: &str, project: bool) -> Result<(), String> {
    if host == "codex" && project {
        return Err("Codex plugin installation is per-user; omit --project".to_owned());
    }
    if project
        && !matches!(
            host,
            "codex" | "claude-code" | "cursor" | "opencode" | "vscode"
        )
    {
        return Err(format!("'{host}' has no project-scoped adapter"));
    }
    if host == "codex" && !project {
        return install_codex_plugin();
    }
    let executable = current_executable()?;
    if host == "claude-code" {
        let path = install_claude_hooks(&executable, project)?;
        println!(
            "installed claude-code lifecycle hooks at {}",
            path.display()
        );
        return Ok(());
    }
    if host == "copilot" {
        if project {
            return Err("Copilot CLI installation is per-user; omit --project".to_owned());
        }
        let path = install_copilot_hooks(&executable)?;
        println!(
            "installed Copilot CLI lifecycle hooks at {}",
            path.display()
        );
        println!(
            "capability: provisional because Copilot subagentStart omits a stable child id and general-purpose subagents emit no lifecycle events"
        );
        return Ok(());
    }
    if host == "cursor" {
        let path = install_cursor_hooks(&executable, project)?;
        println!(
            "installed Cursor root lifecycle hooks at {}",
            path.display()
        );
        println!("capability: provisional/CLI-only; native subagent isolation is not claimed");
        return Ok(());
    }
    if host == "opencode" {
        let path = install_opencode_guidance(project)?;
        println!("installed OpenCode guidance at {}", path.display());
        println!("capability: provisional/CLI-only; native subagent isolation is not claimed");
        return Ok(());
    }
    let (path, shape) = host_config(host, project)?;
    merge_mcp_config(&path, shape, &executable)?;
    println!(
        "installed {host} {} adapter at {}",
        if project { "project" } else { "per-user" },
        path.display()
    );
    Ok(())
}

pub(crate) fn uninstall_host(host: &str) -> Result<(), String> {
    if host == "codex" {
        let manifest_root = plugin_root()?;
        let ownership_path = codex_ownership_path();
        let ownership = read_codex_ownership(&ownership_path)?;
        let marketplaces = codex_json(&["plugin", "marketplace", "list", "--json"])?;
        let configured = marketplaces
            .get("marketplaces")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|entry| entry.get("name").and_then(Value::as_str) == Some("acyclic"));
        if let Some(configured) = configured {
            let configured_root = configured
                .get("root")
                .and_then(Value::as_str)
                .ok_or_else(|| "configured Acyclic marketplace has no root".to_owned())?;
            let owned = Path::new(configured_root)
                .canonicalize()
                .ok()
                .zip(manifest_root.canonicalize().ok())
                .is_some_and(|(configured, package)| configured == package);
            if !owned {
                return Err(format!(
                    "refusing to remove the Acyclic marketplace owned by {configured_root}"
                ));
            }
        }
        let plugin_was_installed = installed_codex_plugin()?.is_some();
        let Some(ownership) = ownership else {
            if configured.is_some() || plugin_was_installed {
                return Err(
                    "refusing to remove Codex integration without an Acyclic ownership record"
                        .to_owned(),
                );
            }
            return Ok(());
        };
        let owned_root = ownership
            .marketplace_root
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(owned, package)| owned == package);
        if ownership.version != 1 || !owned_root {
            return Err(
                "Codex integration ownership does not match this Acyclic package".to_owned(),
            );
        }
        if plugin_was_installed && !ownership.plugin_was_installed {
            run_codex(&["plugin", "remove", "acyclic@acyclic", "--json"])?;
        }
        if ownership.added_marketplace && configured.is_some() {
            run_codex(&["plugin", "marketplace", "remove", "acyclic"])?;
        }
        // Restore configuration last. Every preceding step is idempotent, so
        // an interrupted uninstall can resume from the durable ownership record.
        restore_codex_config(&ownership)?;
        fs::remove_file(&ownership_path).map_err(display)?;
        println!("removed Acyclic-owned Codex integration and preserved prior state");
        return Ok(());
    }
    if host == "claude-code" {
        let user = home_directory()?.join(".claude/settings.json");
        let project = env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json");
        let same_settings = same_existing_path(&user, &project);
        remove_claude_hooks(false)?;
        if !same_settings {
            remove_claude_hooks(true)?;
        }
        println!("removed Acyclic-owned Claude Code lifecycle hooks");
        return Ok(());
    }
    if host == "copilot" {
        let path = home_directory()?.join(".copilot/hooks/acyclic.json");
        remove_copilot_hooks_at(&path)?;
        println!(
            "removed Acyclic-owned Copilot CLI hooks from {}",
            path.display()
        );
        return Ok(());
    }
    if host == "cursor" {
        for project in [false, true] {
            remove_cursor_hooks(project)?;
        }
        println!("removed Acyclic-owned Cursor hooks");
        return Ok(());
    }
    if host == "opencode" {
        for project in [false, true] {
            remove_opencode_guidance(project)?;
        }
        println!("removed Acyclic-owned OpenCode guidance");
        return Ok(());
    }
    if host == "vscode" {
        let (path, shape) = host_config(host, true)?;
        remove_mcp_config(&path, shape)?;
        println!(
            "removed Acyclic-owned {host} configuration from {}",
            path.display()
        );
        return Ok(());
    }
    let (path, shape) = host_config(host, false)?;
    remove_mcp_config(&path, shape)?;
    println!(
        "removed Acyclic-owned {host} configuration from {}",
        path.display()
    );
    Ok(())
}

pub(crate) fn same_existing_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    left.canonicalize()
        .ok()
        .zip(right.canonicalize().ok())
        .is_some_and(|(left, right)| left == right)
}

pub(crate) fn remove_copilot_hooks_at(path: &Path) -> Result<(), String> {
    remove_owned_json(path, "copilot-hooks", "Copilot hooks")
}

pub(crate) fn install_claude_hooks(executable: &Path, project: bool) -> Result<PathBuf, String> {
    let path = if project {
        env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json")
    } else {
        home_directory()?.join(".claude/settings.json")
    };
    install_owned_json_normalized(
        &path,
        "claude-hooks",
        "Claude Code hooks",
        normalize_claude_hook_document,
        |prior| {
            let mut document = prior
                .cloned()
                .unwrap_or_else(|| json!({}))
                .as_object()
                .cloned()
                .ok_or_else(|| format!("{} must contain a JSON object", path.display()))?;
            let hooks = object_entry(&mut document, "hooks")?;
            for event in [
                "SessionStart",
                "PreToolUse",
                "PostToolUse",
                "PostToolUseFailure",
                "SubagentStart",
                "SubagentStop",
                "SessionEnd",
            ] {
                let entries = hooks
                    .entry(event.to_owned())
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .ok_or_else(|| format!("Claude Code hooks.{event} must be an array"))?;
                let mut group = serde_json::Map::new();
                if matches!(event, "PreToolUse" | "PostToolUse" | "PostToolUseFailure") {
                    group.insert("matcher".to_owned(), Value::String(".*".to_owned()));
                }
                // Exec form: Claude Code spawns the executable directly
                // instead of through bash, Git Bash or PowerShell.
                group.insert(
                    "hooks".to_owned(),
                    json!([{
                        "type": "command",
                        "command": executable,
                        "args": ["__hook", "claude-code", event],
                        "timeout": claude_hook_timeout(event),
                        "statusMessage": "Acyclic is routing the workspace"
                    }]),
                );
                entries.push(Value::Object(group));
            }
            Ok(Value::Object(document))
        },
    )?;
    Ok(path)
}

pub(crate) fn normalize_claude_hook_document(
    prior: Option<&Value>,
) -> Result<Option<Value>, String> {
    let Some(prior) = prior else {
        return Ok(None);
    };
    let mut document = prior
        .as_object()
        .cloned()
        .ok_or_else(|| "Claude Code settings must contain a JSON object".to_owned())?;
    let Some(hooks) = document.get_mut("hooks") else {
        return Ok(Some(Value::Object(document)));
    };
    let hooks = hooks
        .as_object_mut()
        .ok_or_else(|| "Claude Code settings hooks must contain a JSON object".to_owned())?;
    for event in [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "SubagentStart",
        "SubagentStop",
        "SessionEnd",
    ] {
        let Some(entries) = hooks.get_mut(event) else {
            continue;
        };
        let entries = entries
            .as_array_mut()
            .ok_or_else(|| format!("Claude Code hooks.{event} must be an array"))?;
        entries.retain(|entry| !is_acyclic_claude_hook_group(event, entry));
        if entries.is_empty() {
            hooks.remove(event);
        }
    }
    if hooks.is_empty() {
        document.remove("hooks");
    }
    Ok(Some(Value::Object(document)))
}

pub(crate) fn is_acyclic_claude_hook_group(event: &str, group: &Value) -> bool {
    let Some(group) = group.as_object() else {
        return false;
    };
    let expected_fields = if matches!(event, "PreToolUse" | "PostToolUse" | "PostToolUseFailure") {
        2
    } else {
        1
    };
    if group.len() != expected_fields
        || expected_fields == 2 && group.get("matcher").and_then(Value::as_str) != Some(".*")
    {
        return false;
    }
    let Some(hook) = group
        .get("hooks")
        .and_then(Value::as_array)
        .filter(|hooks| hooks.len() == 1)
        .and_then(|hooks| hooks.first())
        .and_then(Value::as_object)
    else {
        return false;
    };
    if hook.len() != 5
        || hook.get("type").and_then(Value::as_str) != Some("command")
        || hook.get("args") != Some(&json!(["__hook", "claude-code", event]))
        || hook.get("timeout").and_then(Value::as_u64) != Some(claude_hook_timeout(event))
        || hook.get("statusMessage").and_then(Value::as_str)
            != Some("Acyclic is routing the workspace")
    {
        return false;
    }
    hook.get("command")
        .and_then(Value::as_str)
        .map(Path::new)
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
        })
}

pub(crate) fn claude_hook_timeout(event: &str) -> u64 {
    // Claude bounds SessionEnd separately from ordinary hooks. Keep teardown
    // within its terminal-hook budget so the process cannot exit while mounts
    // and service session ownership remain live.
    if event == "SessionEnd" { 3 } else { 120 }
}

pub(crate) fn remove_claude_hooks(project: bool) -> Result<(), String> {
    let path = if project {
        env::current_dir()
            .map_err(display)?
            .join(".claude/settings.json")
    } else {
        home_directory()?.join(".claude/settings.json")
    };
    remove_owned_json(&path, "claude-hooks", "Claude Code hooks")
}

pub(crate) fn install_copilot_hooks(executable: &Path) -> Result<PathBuf, String> {
    let path = home_directory()?.join(".copilot/hooks/acyclic.json");
    install_copilot_hooks_at(executable, &path)?;
    Ok(path)
}

pub(crate) fn install_copilot_hooks_at(executable: &Path, path: &Path) -> Result<(), String> {
    let command = executable.to_string_lossy();
    let mut hooks = serde_json::Map::new();
    for event in [
        "sessionStart",
        "preToolUse",
        "postToolUse",
        "subagentStart",
        "subagentStop",
        "sessionEnd",
    ] {
        hooks.insert(
            event.to_owned(),
            json!([{
                "type": "command",
                "exec": command,
                "args": ["__hook", "copilot", event],
                "timeout": 120
            }]),
        );
    }
    let installed = json!({"version":1,"hooks":Value::Object(hooks)});
    install_owned_json(path, "copilot-hooks", "Copilot hooks", |_| Ok(installed))
}

pub(crate) fn cursor_hooks_path(project: bool) -> Result<PathBuf, String> {
    Ok(if project {
        env::current_dir()
            .map_err(display)?
            .join(".cursor/hooks.json")
    } else {
        home_directory()?.join(".cursor/hooks.json")
    })
}

pub(crate) fn install_cursor_hooks(executable: &Path, project: bool) -> Result<PathBuf, String> {
    let path = cursor_hooks_path(project)?;
    install_owned_json(&path, "cursor-hooks", "Cursor hooks", |prior| {
        let mut document = prior
            .cloned()
            .unwrap_or_else(|| json!({}))
            .as_object()
            .cloned()
            .ok_or_else(|| format!("{} must contain a JSON object", path.display()))?;
        document.insert("version".to_owned(), json!(1));
        let hooks = object_entry(&mut document, "hooks")?;
        for event in ["sessionStart", "sessionEnd"] {
            let entries = hooks
                .entry(event.to_owned())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .ok_or_else(|| format!("Cursor hooks.{event} must be an array"))?;
            entries.push(json!({
                "command": format!("{} __hook cursor {event}", shell_command_path(executable)),
                "timeout": 120
            }));
        }
        Ok(Value::Object(document))
    })?;
    Ok(path)
}

pub(crate) fn remove_cursor_hooks(project: bool) -> Result<(), String> {
    let path = cursor_hooks_path(project)?;
    remove_owned_json(&path, "cursor-hooks", "Cursor hooks")
}

pub(crate) const OPENCODE_GUIDANCE_START: &str = "<!-- acyclic:start -->";
pub(crate) const OPENCODE_GUIDANCE_END: &str = "<!-- acyclic:end -->";

pub(crate) fn opencode_guidance_path(project: bool) -> Result<PathBuf, String> {
    Ok(if project {
        env::current_dir().map_err(display)?.join("AGENTS.md")
    } else {
        config_directory()?.join("opencode/AGENTS.md")
    })
}

pub(crate) fn install_opencode_guidance(project: bool) -> Result<PathBuf, String> {
    let path = opencode_guidance_path(project)?;
    let existing = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(display(error)),
    };
    let without = remove_marked_block(&existing, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END);
    let block = format!(
        "{OPENCODE_GUIDANCE_START}\n{}\n{OPENCODE_GUIDANCE_END}",
        guidance_for("opencode")
    );
    let next = if without.trim().is_empty() {
        format!("{block}\n")
    } else {
        format!("{}\n\n{block}\n", without.trim_end())
    };
    write_text_with_backup(&path, &next)?;
    Ok(path)
}

pub(crate) fn remove_opencode_guidance(project: bool) -> Result<(), String> {
    let path = opencode_guidance_path(project)?;
    let existing = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(display(error)),
    };
    let next = remove_marked_block(&existing, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END);
    write_text_with_backup(&path, next.trim_end())
}

pub(crate) fn remove_marked_block(input: &str, start: &str, end: &str) -> String {
    let Some((before, marked)) = input.split_once(start) else {
        return input.to_owned();
    };
    let Some((_, after)) = marked.split_once(end) else {
        return input.to_owned();
    };
    let mut output = String::with_capacity(input.len());
    output.push_str(before);
    output.push_str(after);
    output.trim().to_owned()
}

pub(crate) fn shell_command_path(path: &Path) -> String {
    let path = path.to_string_lossy();
    if cfg!(windows) {
        format!("\"{}\"", path.replace('"', "\"\""))
    } else {
        format!("'{}'", path.replace('\'', "'\\''"))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum McpConfigShape {
    Standard,
    VsCode,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct McpConfigOwnership {
    pub(crate) version: u32,
    pub(crate) config_path: PathBuf,
    pub(crate) section: String,
    pub(crate) prior: Option<Value>,
    pub(crate) installed: Value,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct CodexPluginOwnership {
    pub(crate) version: u32,
    pub(crate) marketplace_root: PathBuf,
    pub(crate) added_marketplace: bool,
    pub(crate) plugin_was_installed: bool,
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) prior_default_permissions: Option<String>,
}

pub(crate) struct CodexConfigPlan {
    pub(crate) path: PathBuf,
    pub(crate) prior_default_permissions: Option<String>,
    pub(crate) installed: String,
}

pub(crate) fn codex_ownership_path() -> PathBuf {
    default_data_directory().join("install-ownership/codex-plugin.json")
}

pub(crate) fn read_codex_ownership(path: &Path) -> Result<Option<CodexPluginOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn write_codex_ownership(
    path: &Path,
    ownership: &CodexPluginOwnership,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(ownership).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

pub(crate) fn codex_config_path() -> Result<PathBuf, String> {
    Ok(env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or(home_directory()?.join(".codex"))
        .join("config.toml"))
}

pub(crate) fn codex_plugin_configuration_matches(
    ownership: &CodexPluginOwnership,
    packaged_root: &Path,
) -> bool {
    let Some(config_path) = ownership.config_path.as_ref() else {
        return false;
    };
    let Ok(document) = fs::read_to_string(config_path)
        .map_err(display)
        .and_then(|text| text.parse::<toml_edit::DocumentMut>().map_err(display))
    else {
        return false;
    };
    let marketplace = document
        .get("marketplaces")
        .and_then(toml_edit::Item::as_table)
        .and_then(|marketplaces| marketplaces.get("acyclic"))
        .and_then(toml_edit::Item::as_table);
    let configured_root = marketplace
        .and_then(|marketplace| marketplace.get("source"))
        .and_then(toml_edit::Item::as_str)
        .map(Path::new);
    let source_is_local = marketplace
        .and_then(|marketplace| marketplace.get("source_type"))
        .and_then(toml_edit::Item::as_str)
        == Some("local");
    let enabled = document
        .get("plugins")
        .and_then(toml_edit::Item::as_table)
        .and_then(|plugins| plugins.get("acyclic@acyclic"))
        .and_then(toml_edit::Item::as_table)
        .and_then(|plugin| plugin.get("enabled"))
        .and_then(toml_edit::Item::as_bool)
        == Some(true);
    source_is_local
        && enabled
        && configured_root
            .and_then(|configured| configured.canonicalize().ok())
            .zip(packaged_root.canonicalize().ok())
            .is_some_and(|(configured, packaged)| configured == packaged)
}

pub(crate) fn read_optional_text(path: &Path) -> Result<Option<String>, String> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn toml_table<'a>(
    parent: &'a mut toml_edit::Table,
    key: &str,
) -> Result<&'a mut toml_edit::Table, String> {
    parent
        .entry(key)
        .or_insert_with(|| toml_edit::Item::Table(toml_edit::Table::new()))
        .as_table_mut()
        .ok_or_else(|| format!("Codex configuration field '{key}' must be a table"))
}

pub(crate) fn codex_permission_profile(
    data: &Path,
    base: &str,
) -> Result<toml_edit::Table, String> {
    let mut profile = toml_edit::Table::new();
    profile.insert("extends", toml_edit::value(base));
    let roots = toml_table(&mut profile, "workspace_roots")?;
    roots.insert(
        data.join("w").to_string_lossy().as_ref(),
        toml_edit::value(true),
    );
    #[cfg(unix)]
    {
        let runtime = unix_control_runtime_directory();
        roots.insert(runtime.to_string_lossy().as_ref(), toml_edit::value(true));
    }
    #[cfg(windows)]
    {
        let filesystem = toml_table(&mut profile, "filesystem")?;
        filesystem.insert(":minimal", toml_edit::value("read"));
        toml_table(filesystem, ":workspace_roots")?.insert(".", toml_edit::value("write"));
    }
    Ok(profile)
}

pub(crate) fn codex_profile_matches(
    profile: &toml_edit::Table,
    data: &Path,
    base_profile: &str,
) -> bool {
    let roots = profile
        .get("workspace_roots")
        .and_then(toml_edit::Item::as_table);
    #[cfg(unix)]
    {
        let runtime = unix_control_runtime_directory();
        profile.len() == 2
            && profile.get("extends").and_then(toml_edit::Item::as_str) == Some(base_profile)
            && roots.is_some_and(|roots| {
                roots.len() == 2
                    && roots
                        .get(data.join("w").to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
                    && roots
                        .get(runtime.to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
            })
    }
    #[cfg(not(unix))]
    {
        let filesystem = profile
            .get("filesystem")
            .and_then(toml_edit::Item::as_table);
        profile.len() == 3
            && profile.get("extends").and_then(toml_edit::Item::as_str) == Some(base_profile)
            && roots.is_some_and(|roots| {
                roots.len() == 1
                    && roots
                        .get(data.join("w").to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
            })
            && filesystem.is_some_and(|filesystem| {
                filesystem.len() == 2
                    && filesystem.get(":minimal").and_then(toml_edit::Item::as_str) == Some("read")
                    && filesystem
                        .get(":workspace_roots")
                        .and_then(toml_edit::Item::as_table)
                        .is_some_and(|roots| {
                            roots.len() == 1
                                && roots.get(".").and_then(toml_edit::Item::as_str) == Some("write")
                        })
            })
    }
}

pub(crate) fn plan_codex_config(
    ownership: &CodexPluginOwnership,
) -> Result<CodexConfigPlan, String> {
    let path = codex_config_path()?;
    let mut document = read_optional_text(&path)?
        .as_deref()
        .unwrap_or("")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("invalid Codex configuration at {}: {error}", path.display()))?;
    let root = document.as_table_mut();
    let base_profile = ownership
        .prior_default_permissions
        .as_deref()
        .unwrap_or(":workspace");
    if let Some(owned_path) = ownership.config_path.as_ref() {
        let installed_profile = root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .and_then(|permissions| permissions.get("acyclic"))
            .and_then(toml_edit::Item::as_table);
        let owned = owned_path == &path
            && root
                .get("default_permissions")
                .and_then(toml_edit::Item::as_str)
                == Some("acyclic")
            && installed_profile.is_some_and(|profile| {
                codex_profile_matches(profile, &default_data_directory(), base_profile)
            });
        if !owned {
            return Err(format!(
                "Codex configuration ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        return Ok(CodexConfigPlan {
            path,
            prior_default_permissions: ownership.prior_default_permissions.clone(),
            installed: document.to_string(),
        });
    }
    if ownership.prior_default_permissions.is_some() {
        return Err("Codex configuration ownership record is incomplete".to_owned());
    }
    let prior_default_permissions = root
        .get("default_permissions")
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "Codex default_permissions must be a string".to_owned())
        })
        .transpose()?;
    let expected_profile = codex_permission_profile(
        &default_data_directory(),
        prior_default_permissions.as_deref().unwrap_or(":workspace"),
    )?;
    if toml_table(root, "permissions")?.contains_key("acyclic") {
        return Err(format!(
            "refusing to replace the unowned 'acyclic' permission profile in {}",
            path.display()
        ));
    }
    root.insert("default_permissions", toml_edit::value("acyclic"));
    toml_table(root, "permissions")?.insert("acyclic", toml_edit::Item::Table(expected_profile));
    Ok(CodexConfigPlan {
        path,
        prior_default_permissions,
        installed: document.to_string(),
    })
}

pub(crate) fn apply_codex_config(plan: &CodexConfigPlan) -> Result<(), String> {
    write_text_with_backup(&plan.path, &plan.installed)
}

pub(crate) fn restore_codex_config(ownership: &CodexPluginOwnership) -> Result<(), String> {
    let Some(path) = ownership.config_path.as_ref() else {
        return Ok(());
    };
    let mut document = read_optional_text(path)?
        .as_deref()
        .unwrap_or("")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("invalid Codex configuration at {}: {error}", path.display()))?;
    let root = document.as_table_mut();
    let base_profile = ownership
        .prior_default_permissions
        .as_deref()
        .unwrap_or(":workspace");
    let owned = root
        .get("default_permissions")
        .and_then(toml_edit::Item::as_str)
        == Some("acyclic")
        && root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .and_then(|permissions| permissions.get("acyclic"))
            .and_then(toml_edit::Item::as_table)
            .is_some_and(|profile| {
                codex_profile_matches(profile, &default_data_directory(), base_profile)
            });
    if !owned {
        let restored_default = match ownership.prior_default_permissions.as_deref() {
            Some(prior) => {
                root.get("default_permissions")
                    .and_then(toml_edit::Item::as_str)
                    == Some(prior)
            }
            None => !root.contains_key("default_permissions"),
        };
        let profile_absent = root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .is_none_or(|permissions| !permissions.contains_key("acyclic"));
        if restored_default && profile_absent {
            return Ok(());
        }
        return Err(format!(
            "the Acyclic Codex permission profile at {} was modified; leaving it untouched",
            path.display()
        ));
    }
    match ownership.prior_default_permissions.as_deref() {
        Some(prior) => {
            root.insert("default_permissions", toml_edit::value(prior));
        }
        None => {
            root.remove("default_permissions");
        }
    }
    let permissions = toml_table(root, "permissions")?;
    permissions.remove("acyclic");
    write_text_with_backup(path, &document.to_string())
}

pub(crate) fn mcp_ownership_path(path: &Path, section: &str) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-mcp-config-ownership-v1\0");
    hasher.update(path.as_os_str().to_string_lossy().as_bytes());
    hasher.update(&[0]);
    hasher.update(section.as_bytes());
    #[cfg(test)]
    return path.with_extension(format!(
        "{}.acyclic-owner",
        hasher
            .finalize()
            .to_hex()
            .chars()
            .take(16)
            .collect::<String>()
    ));
    #[cfg(not(test))]
    default_data_directory()
        .join("install-ownership")
        .join(format!(
            "{}.json",
            hasher
                .finalize()
                .to_hex()
                .chars()
                .take(32)
                .collect::<String>()
        ))
}

pub(crate) fn read_mcp_ownership(path: &Path) -> Result<Option<McpConfigOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn write_mcp_ownership(
    path: &Path,
    ownership: &McpConfigOwnership,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(ownership).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

pub(crate) fn read_optional_json(path: &Path, label: &str) -> Result<Option<Value>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            format!(
                "refusing to overwrite invalid {label} at {}: {error}",
                path.display()
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn install_owned_json(
    path: &Path,
    section: &str,
    label: &str,
    build: impl FnOnce(Option<&Value>) -> Result<Value, String>,
) -> Result<(), String> {
    install_owned_json_normalized(path, section, label, |prior| Ok(prior.cloned()), build)
}

pub(crate) fn install_owned_json_normalized(
    path: &Path,
    section: &str,
    label: &str,
    normalize: impl FnOnce(Option<&Value>) -> Result<Option<Value>, String>,
    build: impl FnOnce(Option<&Value>) -> Result<Value, String>,
) -> Result<(), String> {
    let ownership_path = mcp_ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let raw_prior = match read_mcp_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == section
                && (current == ownership.prior
                    || current.as_ref() == Some(&ownership.installed)) =>
        {
            ownership.prior
        }
        Some(_) => {
            return Err(format!(
                "{label} ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        None => current,
    };
    let prior = normalize(raw_prior.as_ref())?;
    let installed = build(prior.as_ref())?;
    write_mcp_ownership(
        &ownership_path,
        &McpConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: section.to_owned(),
            prior,
            installed: installed.clone(),
        },
    )?;
    write_json_with_backup(path, &installed)
}

pub(crate) fn remove_owned_json(path: &Path, section: &str, label: &str) -> Result<(), String> {
    let ownership_path = mcp_ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
        if current.is_some() {
            return Err(format!(
                "refusing to remove {label} without an Acyclic ownership record from {}",
                path.display()
            ));
        }
        return Ok(());
    };
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != section
        || (current != ownership.prior && current.as_ref() != Some(&ownership.installed))
    {
        return Err(format!(
            "the Acyclic {label} at {} were modified; leaving them untouched",
            path.display()
        ));
    }
    if current != ownership.prior {
        match &ownership.prior {
            Some(prior) => write_json_with_backup(path, prior)?,
            None => match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(display(error)),
            },
        }
    }
    fs::remove_file(&ownership_path).map_err(display)
}

pub(crate) fn host_config(host: &str, project: bool) -> Result<(PathBuf, McpConfigShape), String> {
    let cwd = env::current_dir().map_err(display)?;
    let home = home_directory()?;
    let result = match (host, project) {
        ("vscode", true) => (cwd.join(".mcp.json"), McpConfigShape::VsCode),
        ("vscode", false) => {
            return Err("VS Code installation is project-scoped; pass --project".to_owned());
        }
        ("copilot", false) => (
            home.join(".copilot/mcp-config.json"),
            McpConfigShape::Standard,
        ),
        ("claude-desktop", false) => (claude_desktop_config()?, McpConfigShape::Standard),
        ("copilot-cloud", _) => {
            return Err("Copilot cloud is job-local; install the binary in the job and invoke its MCP bridge explicitly".to_owned());
        }
        ("pydantic-ai", _) => {
            return Err("Pydantic AI requires explicit SDK integration and has no host configuration to mutate".to_owned());
        }
        _ => return Err(format!("unsupported Acyclic host '{host}'")),
    };
    Ok(result)
}

pub(crate) fn merge_mcp_config(
    path: &Path,
    shape: McpConfigShape,
    executable: &Path,
) -> Result<(), String> {
    let mut document = read_json_object(path)?;
    let command = executable.to_string_lossy().into_owned();
    let (section, installed) = match shape {
        McpConfigShape::Standard => (
            "mcpServers",
            json!({"command":command,"args":["__mcp-commandless"]}),
        ),
        McpConfigShape::VsCode => (
            "servers",
            json!({"type":"stdio","command":command,"args":["__mcp-commandless"]}),
        ),
    };
    let ownership_path = mcp_ownership_path(path, section);
    let entries = object_entry(&mut document, section)?;
    let current = entries.get("acyclic").cloned();
    let ownership = match read_mcp_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == section
                && ownership.installed == installed
                && (current == ownership.prior || current == Some(installed.clone())) =>
        {
            ownership
        }
        Some(_) => {
            return Err(format!(
                "Acyclic does not own the current '{}' entry in {}; resolve it manually",
                section,
                path.display()
            ));
        }
        None if current.is_none() => McpConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: section.to_owned(),
            prior: None,
            installed: installed.clone(),
        },
        None => {
            return Err(format!(
                "refusing to replace the unowned 'acyclic' entry in {}",
                path.display()
            ));
        }
    };
    write_mcp_ownership(&ownership_path, &ownership)?;
    entries.insert("acyclic".to_owned(), installed);
    if let Err(error) = write_json_with_backup(path, &Value::Object(document)) {
        return Err(format!("{error}; ownership intent retained for recovery"));
    }
    Ok(())
}

pub(crate) fn remove_mcp_config(path: &Path, shape: McpConfigShape) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let mut document = read_json_object(path)?;
    let key = match shape {
        McpConfigShape::Standard => "mcpServers",
        McpConfigShape::VsCode => "servers",
    };
    let ownership_path = mcp_ownership_path(path, key);
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
        return Err(format!(
            "refusing to remove an Acyclic entry without an ownership record from {}",
            path.display()
        ));
    };
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != key
        || document
            .get(key)
            .and_then(Value::as_object)
            .and_then(|entries| entries.get("acyclic"))
            != Some(&ownership.installed)
    {
        return Err(format!(
            "the Acyclic entry in {} was modified; leaving it untouched",
            path.display()
        ));
    }
    if let Some(Value::Object(entries)) = document.get_mut(key) {
        match ownership.prior {
            Some(prior) => {
                entries.insert("acyclic".to_owned(), prior);
            }
            None => {
                entries.remove("acyclic");
            }
        }
    }
    write_json_with_backup(path, &Value::Object(document))?;
    fs::remove_file(&ownership_path).map_err(display)
}

pub(crate) fn install_codex_plugin() -> Result<(), String> {
    let manifest_root = plugin_root()?;
    let marketplace = manifest_root.join(".agents/plugins/marketplace.json");
    if !marketplace.is_file() {
        return Err(format!(
            "Codex marketplace manifest is missing at {}",
            marketplace.display()
        ));
    }
    let marketplaces = codex_json(&["plugin", "marketplace", "list", "--json"])?;
    let configured = marketplaces
        .get("marketplaces")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|entry| entry.get("name").and_then(Value::as_str) == Some("acyclic"));
    if let Some(configured) = configured {
        let configured_root = configured
            .get("root")
            .and_then(Value::as_str)
            .ok_or_else(|| "configured Acyclic marketplace has no root".to_owned())?;
        let same = Path::new(configured_root)
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(configured, package)| configured == package);
        if !same {
            return Err(format!(
                "refusing to replace the Acyclic marketplace owned by {configured_root}"
            ));
        }
    }
    let installed_before = installed_codex_plugin()?;
    let plugin_was_installed = installed_before.is_some();
    if plugin_was_installed && configured.is_none() {
        return Err(
            "Codex reports an installed Acyclic plugin without its marketplace; refusing mutation"
                .to_owned(),
        );
    }
    if let Some(installed) = installed_before.as_ref() {
        validate_existing_codex_plugin(installed)?;
    }
    let added_marketplace = configured.is_none();
    let ownership_path = codex_ownership_path();
    let existing_ownership = read_codex_ownership(&ownership_path)?;
    let mut ownership = if let Some(ownership) = existing_ownership.as_ref() {
        let same = ownership
            .marketplace_root
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(owned, package)| owned == package);
        if ownership.version != 1 || !same {
            return Err("existing Codex integration ownership is inconsistent".to_owned());
        }
        CodexPluginOwnership {
            version: ownership.version,
            marketplace_root: ownership.marketplace_root.clone(),
            added_marketplace: ownership.added_marketplace,
            plugin_was_installed: ownership.plugin_was_installed,
            config_path: ownership.config_path.clone(),
            prior_default_permissions: ownership.prior_default_permissions.clone(),
        }
    } else {
        CodexPluginOwnership {
            version: 1,
            marketplace_root: manifest_root.clone(),
            added_marketplace,
            plugin_was_installed,
            config_path: None,
            prior_default_permissions: None,
        }
    };
    write_codex_ownership(&ownership_path, &ownership)?;
    let install = (|| {
        if added_marketplace {
            run_codex(&[
                "plugin",
                "marketplace",
                "add",
                manifest_root.to_string_lossy().as_ref(),
            ])?;
        }
        if !plugin_was_installed {
            run_codex(&["plugin", "add", "acyclic@acyclic", "--json"])?;
        }
        let installed = codex_json(&["plugin", "list", "--marketplace", "acyclic", "--json"])?;
        if !installed
            .get("installed")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|entry| {
                entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic")
                    && entry.get("version").and_then(Value::as_str)
                        == Some(env!("CARGO_PKG_VERSION"))
                    && entry.get("installed").and_then(Value::as_bool) == Some(true)
                    && entry.get("enabled").and_then(Value::as_bool) == Some(true)
            })
        {
            return Err("Codex did not activate the expected Acyclic release".to_owned());
        }
        let plan = plan_codex_config(&ownership)?;
        ownership.config_path = Some(plan.path.clone());
        ownership
            .prior_default_permissions
            .clone_from(&plan.prior_default_permissions);
        write_codex_ownership(&ownership_path, &ownership)?;
        apply_codex_config(&plan)?;
        Ok(())
    })();
    if let Err(error) = install {
        let _ = restore_codex_config(&ownership);
        if !plugin_was_installed {
            let _ = run_codex(&["plugin", "remove", "acyclic@acyclic", "--json"]);
        }
        if added_marketplace {
            let _ = run_codex(&["plugin", "marketplace", "remove", "acyclic"]);
        }
        match existing_ownership.as_ref() {
            Some(existing) => {
                let _ = write_codex_ownership(&ownership_path, existing);
            }
            None => {
                let _ = fs::remove_file(&ownership_path);
            }
        }
        return Err(format!("Codex plugin installation rolled back: {error}"));
    }
    println!("installed Acyclic through the Codex plugin manager");
    Ok(())
}

pub(crate) fn run_codex(arguments: &[&str]) -> Result<(), String> {
    let status = std::process::Command::new("codex")
        .args(arguments)
        .status()
        .map_err(|error| format!("cannot run Codex plugin manager: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Codex plugin manager exited with {status}"))
    }
}

pub(crate) fn installed_codex_plugin() -> Result<Option<Value>, String> {
    let plugins = codex_json(&["plugin", "list", "--json"])?;
    let installed = plugins
        .get("installed")
        .and_then(Value::as_array)
        .ok_or_else(|| "Codex plugin list has no installed array".to_owned())?;
    let matches = installed
        .iter()
        .filter(|entry| entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic"))
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [plugin] => Ok(Some(plugin.clone())),
        _ => Err("Codex reported duplicate installed Acyclic plugins".to_owned()),
    }
}

pub(crate) fn validate_existing_codex_plugin(installed: &Value) -> Result<(), String> {
    let version = installed
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let enabled = installed
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if version == env!("CARGO_PKG_VERSION") && enabled {
        Ok(())
    } else {
        Err(format!(
            "Codex already has Acyclic {version} (enabled={enabled}); refusing to replace prior plugin state. Update the local package and restart Codex, or explicitly remove the old plugin before retrying"
        ))
    }
}

pub(crate) fn plugin_root() -> Result<PathBuf, String> {
    let executable = current_executable()?;
    let installed = installed_plugin_root()?;
    discover_plugin_root(&executable, &installed)
        .ok_or_else(|| "cannot locate the installed Acyclic plugin root".to_owned())
}

pub(crate) fn discover_plugin_root(executable: &Path, installed: &Path) -> Option<PathBuf> {
    executable
        .ancestors()
        .take(4)
        .chain(std::iter::once(installed))
        .find(|root| {
            root.join("plugin.json").is_file()
                && root.join(".agents/plugins/marketplace.json").is_file()
        })
        .map(Path::to_path_buf)
}

pub(crate) fn installed_plugin_root() -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("Acyclic/plugin"))
            .ok_or_else(|| "LOCALAPPDATA is unavailable".to_owned())
    }
    #[cfg(not(windows))]
    {
        let data = env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".local/share"));
        Ok(data.join("acyclic/plugin"))
    }
}

pub(crate) fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    if !path.exists() {
        return Ok(serde_json::Map::new());
    }
    serde_json::from_slice::<Value>(&fs::read(path).map_err(display)?)
        .map_err(display)?
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{} must contain a JSON object", path.display()))
}

pub(crate) fn object_entry<'a>(
    document: &'a mut serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a mut serde_json::Map<String, Value>, String> {
    document
        .entry(key.to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("configuration field '{key}' must be an object"))
}

pub(crate) fn write_json_with_backup(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

pub(crate) fn write_text_with_backup(path: &Path, value: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("txt")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(&temporary, value.as_bytes()).map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
    )
    .map_err(display)
}

pub(crate) fn executable_on_path(name: &str) -> bool {
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| {
            directory.join(name).is_file()
                || cfg!(windows) && directory.join(format!("{name}.exe")).is_file()
        })
    })
}

pub(crate) fn home_directory() -> Result<PathBuf, String> {
    env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| "cannot locate the per-user configuration directory".to_owned())
}

pub(crate) fn config_directory() -> Result<PathBuf, String> {
    if cfg!(windows) {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| "APPDATA is unavailable".to_owned())
    } else {
        Ok(env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".config")))
    }
}

pub(crate) fn claude_desktop_config() -> Result<PathBuf, String> {
    if cfg!(target_os = "macos") {
        Ok(home_directory()?.join("Library/Application Support/Claude/claude_desktop_config.json"))
    } else {
        Ok(config_directory()?.join("Claude/claude_desktop_config.json"))
    }
}
