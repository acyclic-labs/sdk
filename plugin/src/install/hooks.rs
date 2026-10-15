//! Host hooks and guidance.

use super::*;

pub(crate) fn install_host(host: &str, project: bool) -> Result<(), String> {
    if host == "codex" && project {
        return Err("Codex plugin installation is per-user; omit --project".to_owned());
    }
    if project
        && !matches!(
            host,
            "codex" | "claude-code" | "cursor" | "opencode" | "vscode" | "pi"
        )
    {
        return Err(format!("'{host}' has no project-scoped adapter"));
    }
    if host == "codex" && !project {
        return install_codex_plugin();
    }
    let executable = current_executable()?;
    if host == "pi" {
        let settings = pi_settings_path(project)?;
        install_pi_at(
            &settings,
            &default_data_directory().join("pi-extension"),
            &executable,
        )?;
        println!("installed Pi extension at {}", settings.display());
        println!(
            "capability: diagnostics only; recursive workspace execution is not yet qualified"
        );
        return Ok(());
    }
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
    if host == "pi" {
        for project in [false, true] {
            let settings = pi_settings_path(project)?;
            if read_mcp_ownership(&mcp_ownership_path(&settings, "pi-extension"))?.is_some() {
                remove_owned_json(&settings, "pi-extension", "Pi settings")?;
            }
        }
        println!("removed Acyclic-owned Pi configuration; retained pinned extension assets");
        return Ok(());
    }
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
