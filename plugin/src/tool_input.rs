//! Tool-input path validation and rewriting.

use super::*;

pub(crate) async fn wait_for_root_release(
    barrier: Option<acyclic_native_runtime::OwnershipReleaseBarrier>,
) -> Result<(), String> {
    let Some(barrier) = barrier else {
        return Ok(());
    };
    tokio::task::spawn_blocking(move || barrier.wait())
        .await
        .map_err(|error| format!("filesystem release barrier failed: {error}"))
}

pub(crate) fn rewrite_tool_input(
    tool_name: &str,
    mut input: Value,
    root: &Path,
    child: &Path,
) -> Result<Value, String> {
    let root_text = root.to_string_lossy();
    let child_text = child.to_string_lossy();
    reject_original_root_references(&input, None, &root_text)?;
    rewrite_strings(&mut input, &root_text, &child_text);
    rewrite_relative_tool_paths(&mut input, None, child)?;
    if matches!(tool_name, "Bash" | "PowerShell") {
        let command_key = if input.get("command").is_some() {
            "command"
        } else {
            "cmd"
        };
        let command = input
            .get(command_key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{tool_name} tool input lacks a command string"))?;
        let command = shell_command_in_directory(tool_name, command, child);
        let object = input
            .as_object_mut()
            .ok_or_else(|| format!("{tool_name} tool input must be an object"))?;
        object.insert(command_key.to_owned(), Value::String(command));
        if object.contains_key("workdir") {
            object.insert(
                "workdir".to_owned(),
                Value::String(child.to_string_lossy().into_owned()),
            );
        }
    } else if is_exec_command_tool(tool_name) {
        if let Some(object) = input.as_object_mut() {
            object.insert("workdir".to_owned(), Value::String(child_text.into_owned()));
        }
    } else if tool_name == "apply_patch" {
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| "apply_patch lacks a command string".to_owned())?
            .to_owned();
        let rewritten = command
            .lines()
            .map(|line| rewrite_patch_header(line, child))
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        input
            .as_object_mut()
            .ok_or_else(|| "apply_patch input must be an object".to_owned())?
            .insert("command".to_owned(), Value::String(rewritten));
    }
    validate_tool_paths(tool_name, &input, child)?;
    Ok(input)
}

pub(crate) fn shell_command_in_directory(
    tool_name: &str,
    command: &str,
    directory: &Path,
) -> String {
    let directory = directory.to_string_lossy();
    if tool_name == "PowerShell" {
        format!(
            "Set-Location -LiteralPath '{}';\n{command}",
            directory.replace('\'', "''")
        )
    } else {
        let directory = if cfg!(windows) {
            directory.replace('\\', "/")
        } else {
            directory.into_owned()
        };
        format!(
            "cd -- '{}' &&\n{command}",
            directory.replace('\'', "'\"'\"'")
        )
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn pre_tool_update(updated: Value) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": updated
        }
    })
}

pub(crate) fn is_acyclic_cli_invocation(tool_name: &str, input: &Value) -> Result<bool, String> {
    if !(matches!(tool_name, "Bash" | "PowerShell") || is_exec_command_tool(tool_name)) {
        return Ok(false);
    }
    let command = input
        .get("cmd")
        .or_else(|| input.get("command"))
        .and_then(Value::as_str)
        .ok_or_else(|| "shell tool input lacks a string command".to_owned())?;
    let Some(program) = shell_program(command) else {
        return Ok(false);
    };
    let acyclic = Path::new(&program)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
        });
    if !acyclic {
        return Ok(false);
    }
    let argv = split_standalone_command(command)?;
    Ok(argv.first().is_some_and(|program| {
        Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
            })
    }))
}

pub(crate) fn shell_program(command: &str) -> Option<String> {
    let mut program = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in command.trim_start().chars() {
        if escaped {
            program.push(character);
            escaped = false;
            continue;
        }
        match quote {
            Some(expected) if character == expected => quote = None,
            _ if character == '\\' => escaped = true,
            None if matches!(character, '\'' | '"') => quote = Some(character),
            None if character.is_whitespace() || ";|&<>$`()".contains(character) => break,
            Some(_) | None => program.push(character),
        }
    }
    (!program.is_empty() && quote.is_none() && !escaped).then_some(program)
}

pub(crate) fn split_standalone_command(command: &str) -> Result<Vec<String>, String> {
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        match quote {
            Some('\'') => {
                if character == '\'' {
                    quote = None;
                } else {
                    current.push(character);
                }
            }
            Some('"') => match character {
                '"' => quote = None,
                '\\' => escaped = true,
                '$' | '`' => {
                    return Err("shell expansion is not allowed in an Acyclic command".to_owned());
                }
                _ => current.push(character),
            },
            Some(_) => unreachable!(),
            None => match character {
                '\'' | '"' => quote = Some(character),
                '\\' => escaped = true,
                ' ' | '\t' => {
                    if !current.is_empty() {
                        arguments.push(std::mem::take(&mut current));
                    }
                }
                '\r' | '\n' | ';' | '|' | '&' | '<' | '>' | '$' | '`' | '(' | ')' => {
                    return Err(
                        "Acyclic commands cannot be composed with shell operators".to_owned()
                    );
                }
                _ => current.push(character),
            },
        }
    }
    if quote.is_some() || escaped {
        return Err("Acyclic command has an unterminated quote or escape".to_owned());
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    Ok(arguments)
}

pub(crate) fn validate_tool_paths(
    tool_name: &str,
    input: &Value,
    child: &Path,
) -> Result<(), String> {
    if (matches!(tool_name, "Bash" | "PowerShell") || is_exec_command_tool(tool_name))
        && let Some(command) = input
            .get("cmd")
            .or_else(|| input.get("command"))
            .and_then(Value::as_str)
    {
        validate_shell_paths(command, child)?;
    }
    validate_structured_paths(input, None, child)
}

pub(crate) fn is_exec_command_tool(tool_name: &str) -> bool {
    tool_name == "exec_command" || tool_name.ends_with("__exec_command")
}

pub(crate) fn validate_structured_paths(
    value: &Value,
    key: Option<&str>,
    child: &Path,
) -> Result<(), String> {
    match value {
        Value::String(path) if key.is_some_and(is_path_field) => validate_path(path, child),
        Value::Array(values) => {
            for value in values {
                validate_structured_paths(value, key, child)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                validate_structured_paths(value, Some(key), child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn is_path_field(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "file"
            | "file_path"
            | "directory"
            | "destination"
            | "source"
            | "cwd"
            | "workdir"
            | "referenced_image_paths"
    )
}

pub(crate) fn validate_path(path: &str, child: &Path) -> Result<(), String> {
    let path = Path::new(path);
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err("parent traversal is denied in an isolated agent workspace".to_owned());
    }
    if path.is_absolute() && !path.starts_with(child) {
        return Err(format!(
            "absolute path '{}' escapes the isolated agent workspace",
            path.display()
        ));
    }
    let child_root = child.canonicalize().map_err(display)?;
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        child.join(path)
    };
    let mut existing = candidate.as_path();
    while fs::symlink_metadata(existing).is_err() {
        existing = existing
            .parent()
            .ok_or_else(|| "path has no existing isolated ancestor".to_owned())?;
    }
    let resolved = existing.canonicalize().map_err(display)?;
    if !resolved.starts_with(&child_root) {
        return Err(format!(
            "path '{}' resolves through a link outside the isolated agent workspace",
            path.display()
        ));
    }
    Ok(())
}

pub(crate) fn workspace_path_argument(mount: &Path, argument: &str) -> Result<String, String> {
    let argument = Path::new(argument);
    let relative = if argument.is_absolute() {
        argument
            .strip_prefix(mount)
            .map_err(|_| "path is outside the managed workspace".to_owned())?
    } else {
        argument
    };
    let mut result = String::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| "compatibility path is not valid Unicode".to_owned())?;
                result.push('/');
                result.push_str(name);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("path escapes the managed workspace".to_owned());
            }
        }
    }
    if result.is_empty() {
        return Err("path must name a file inside the managed workspace".to_owned());
    }
    Ok(result)
}

pub(crate) fn validate_shell_paths(command: &str, child: &Path) -> Result<(), String> {
    if command.contains(['$', '`'])
        || cfg!(target_os = "windows") && command.contains('%')
        || command
            .split_whitespace()
            .any(|token| token.starts_with('~'))
    {
        return Err(
            "shell expansion is denied in an isolated agent workspace; use paths relative to the workspace"
                .to_owned(),
        );
    }
    for token in command.split(|character: char| {
        character.is_whitespace()
            || matches!(
                character,
                '\'' | '"' | '(' | ')' | '[' | ']' | '{' | '}' | ';' | ',' | '<' | '>'
            )
    }) {
        let token = token.trim_matches(|character: char| matches!(character, '`' | '&' | '|'));
        if token.is_empty() {
            continue;
        }
        if token.starts_with('-') {
            if let Some((_, value)) = token.split_once('=')
                && (Path::new(value).is_absolute()
                    || value.starts_with('.')
                    || value.contains('/')
                    || value.contains('\\'))
            {
                validate_path(value, child)?;
            }
            continue;
        }
        validate_path(token, child)?;
    }
    Ok(())
}

pub(crate) fn reject_original_root_references(
    value: &Value,
    key: Option<&str>,
    root: &str,
) -> Result<(), String> {
    match value {
        Value::String(text) if !matches!(key, Some("workdir" | "cwd")) => {
            let contains = if cfg!(target_os = "windows") {
                text.to_lowercase().contains(&root.to_lowercase())
            } else {
                text.contains(root)
            };
            if contains {
                return Err(
                    "hard-coded parent checkout paths are denied in an isolated agent workspace"
                        .to_owned(),
                );
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                reject_original_root_references(value, key, root)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                reject_original_root_references(value, Some(key), root)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) async fn open_lazy_source(
    root: &Path,
    reference: Option<SourceReference>,
    observer: Arc<dyn DemandDirectoryObserver>,
) -> Result<Arc<LocalLazySource>, String> {
    let limits = VolumeLimits::default();
    let source = match reference {
        Some(reference) => {
            NativeDemandSource::open_with_reference_and_observer(
                root,
                native_filesystem_profile(),
                limits,
                reference,
                observer,
            )
            .await
        }
        None => {
            NativeDemandSource::open_with_observer(
                root,
                native_filesystem_profile(),
                limits,
                observer,
            )
            .await
        }
    }
    .map_err(display)?;
    Ok(Arc::new(FilteredDemandSource::new(
        source,
        reserved_root_paths()?,
    )))
}

/// Host subtrees owned by version control or SDK state, never workspace
/// content. The order is the watcher fence placement preference: an existing
/// `.git` hosts fence cookies, and only a root without one gains `.acyclic-sdk`.
pub(crate) const RESERVED_ROOT_NAMES: [&str; 2] = [".acyclic-sdk", ".git"];

pub(crate) fn reserved_root_paths() -> Result<Vec<NamespacePath>, String> {
    let limits = VolumeLimits::default();
    RESERVED_ROOT_NAMES
        .iter()
        .map(|name| {
            let path = PortablePath::parse(&format!("/{name}"), limits).map_err(display)?;
            NamespacePath::from_portable_in_profile(&path, native_filesystem_profile(), limits)
                .map_err(display)
        })
        .collect()
}

pub(crate) fn reserved_ignore_rules() -> String {
    RESERVED_ROOT_NAMES
        .iter()
        .map(|name| format!("{name}/\n"))
        .collect()
}

pub(crate) fn open_native_watcher(root: &Path) -> Result<Arc<Mutex<NativeWatch>>, String> {
    let mut watcher = NativeWatch::open_with_profile(
        root,
        native_filesystem_profile(),
        NativeWatchOptions {
            limits: VolumeLimits::default(),
            maximum_queued_changes: 65_536,
            // Linux inotify has no constant-size recursive subscription.
            // NativeDemandSource registers only demanded directories instead.
            recursive: !cfg!(target_os = "linux"),
        },
    )
    .map_err(display)?;
    watcher
        .exclude_subtrees(&reserved_root_paths()?)
        .map_err(display)?;
    watcher.accept_lazy_baseline().map_err(display)?;
    Ok(Arc::new(Mutex::new(watcher)))
}

pub(crate) const fn native_filesystem_profile() -> FilesystemProfile {
    if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    }
}

pub(crate) fn verify_native_root_identity(
    source: &LocalLazySource,
    expected: [u8; 16],
    path: &Path,
) -> Result<(), String> {
    if expected == [0; 16] {
        return Err(format!(
            "native root identity is absent for {}; fresh Acyclic state is required",
            path.display()
        ));
    }
    let observed = source.inner().root_identity().to_bytes();
    if observed != expected {
        return Err(format!(
            "native root identity changed for {}; refusing to reuse its workspace",
            path.display()
        ));
    }
    Ok(())
}

pub(crate) async fn merge_drivers_for(
    workspace: &LocalLazyWorkspace,
) -> Result<Arc<MergeDriverRegistry>, String> {
    let attributes = match workspace.read("/.gitattributes", 1024 * 1024).await {
        Ok(bytes) => String::from_utf8(bytes.to_vec())
            .map_err(|_| ".gitattributes must be UTF-8".to_owned())?,
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .map_err(display)?;
    registry.set_default("text").map_err(display)?;
    if !attributes.trim().is_empty() {
        registry.set_git_attributes(&attributes).map_err(display)?;
    }
    Ok(Arc::new(registry))
}

pub(crate) fn agent_change_filter(
    path: &str,
    route: &Route,
    root: &RouteRoot,
    profile: FilesystemProfile,
) -> Result<NamespacePath, String> {
    let candidate = Path::new(path);
    let relative = if candidate.is_absolute() {
        candidate
            .strip_prefix(route.mount_path.join(root.mount_name()))
            .map_err(|_| "the selected path is outside the inspected workspace root".to_owned())?
            .to_path_buf()
    } else {
        candidate.to_path_buf()
    };
    let mut portable = String::from("/");
    for component in relative.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(name) => {
                if portable.len() > 1 {
                    portable.push('/');
                }
                portable.push_str(
                    name.to_str()
                        .ok_or_else(|| "change filter path is not UTF-8".to_owned())?,
                );
            }
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("change filter path must remain inside one workspace root".to_owned());
            }
        }
    }
    let limits = VolumeLimits::default();
    let portable = PortablePath::parse(&portable, limits).map_err(display)?;
    NamespacePath::from_portable_in_profile(&portable, profile, limits).map_err(display)
}

pub(crate) fn namespace_path_text(path: &NamespacePath) -> Result<String, String> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let components = path
        .components()
        .iter()
        .map(|name| {
            name.unicode_text()
                .map(|value| value.into_owned())
                .ok_or_else(|| "non-Unicode path cannot be presented to an agent".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("/{}", components.join("/")))
}

pub(crate) fn is_filesystem_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "Bash"
            | "PowerShell"
            | "exec_command"
            | "apply_patch"
            | "Edit"
            | "Write"
            | "Read"
            | "view_image"
            | "image_gen__imagegen"
    ) || tool_name.ends_with("__exec_command")
        || tool_name.starts_with("mcp__filesystem__")
        || tool_name.starts_with("mcp__acyclic__")
}

pub(crate) fn is_known_non_filesystem_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "web__run"
            | "web.run"
            | "webrun"
            | "collaboration.followup_task"
            | "collaboration.interrupt_agent"
            | "collaboration.list_agents"
            | "collaboration.send_message"
            | "collaboration.wait_agent"
            | "collaborationfollowup_task"
            | "collaborationinterrupt_agent"
            | "collaborationlist_agents"
            | "collaborationsend_message"
            | "collaborationwait_agent"
            | "followup_task"
            | "interrupt_agent"
            | "list_agents"
            | "send_message"
            | "wait_agent"
            | "get_goal"
            | "functions.get_goal"
            | "functionsget_goal"
            | "update_goal"
            | "functions.update_goal"
            | "functionsupdate_goal"
            | "request_user_input"
            | "functions.request_user_input"
            | "functionsrequest_user_input"
            | "mcp__codex_app__attach_artifact"
            | "mcp__codex_app__automation_update"
            | "mcp__codex_app__capture_screen_context"
            | "mcp__codex_app__consume_usage_reset"
            | "mcp__codex_app__create_sidebar_section"
            | "mcp__codex_app__delete_sidebar_section"
            | "mcp__codex_app__end_realtime_voice_call"
            | "mcp__codex_app__get_handoff_status"
            | "mcp__codex_app__get_usage_limits"
            | "mcp__codex_app__list_archived_threads"
            | "mcp__codex_app__list_artifacts"
            | "mcp__codex_app__list_projects"
            | "mcp__codex_app__list_threads"
            | "mcp__codex_app__load_workspace_dependencies"
            | "mcp__codex_app__move_project_to_sidebar_section"
            | "mcp__codex_app__move_thread_to_sidebar_section"
            | "mcp__codex_app__navigate_to_codex_page"
            | "mcp__codex_app__read_thread"
            | "mcp__codex_app__read_thread_terminal"
            | "mcp__codex_app__remove_artifact"
            | "mcp__codex_app__rename_sidebar_section"
            | "mcp__codex_app__reorder_section"
            | "mcp__codex_app__reorder_sidebar_projects"
            | "mcp__codex_app__reorder_sidebar_sections"
            | "mcp__codex_app__send_message_to_thread"
            | "mcp__codex_app__set_thread_archived"
            | "mcp__codex_app__set_thread_title"
            | "mcp__codex_app__share_thread"
            | "mcp__codex_app__wait_threads"
    )
}

pub(crate) fn is_spawn_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "spawn_agent" | "Agent" | "collaboration.spawn_agent" | "collaborationspawn_agent"
    )
}

pub(crate) fn normalize_tool_input(input: Value) -> Result<Value, String> {
    match input {
        Value::String(text) => serde_json::from_str(&text)
            .map_err(|error| format!("hook tool_input is not valid JSON: {error}")),
        value => Ok(value),
    }
}

pub(crate) fn rewrite_strings(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::String(text) => *text = text.replace(from, to),
        Value::Array(values) => {
            for value in values {
                rewrite_strings(value, from, to);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                rewrite_strings(value, from, to);
            }
        }
        _ => {}
    }
}

pub(crate) fn rewrite_relative_tool_paths(
    value: &mut Value,
    key: Option<&str>,
    child: &Path,
) -> Result<(), String> {
    match value {
        Value::String(path) if key.is_some_and(is_path_field) => {
            let path = Path::new(path);
            if path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            {
                return Err("parent traversal is denied in an isolated agent workspace".to_owned());
            }
            if path.is_relative() {
                *value = Value::String(child.join(path).to_string_lossy().into_owned());
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                rewrite_relative_tool_paths(value, key, child)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                rewrite_relative_tool_paths(value, Some(key), child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn rewrite_patch_header(line: &str, child: &Path) -> Result<String, String> {
    for prefix in [
        "*** Add File: ",
        "*** Update File: ",
        "*** Delete File: ",
        "*** Move to: ",
        "*** Copy to: ",
    ] {
        if let Some(path) = line.strip_prefix(prefix) {
            let path = Path::new(path);
            if path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            {
                return Err("parent traversal is denied in an isolated agent patch".to_owned());
            }
            validate_path(
                path.to_str()
                    .ok_or_else(|| "patch path is not valid Unicode".to_owned())?,
                child,
            )?;
            if path.is_absolute() {
                if !path.starts_with(child) {
                    return Err(format!(
                        "absolute patch path '{}' escapes the isolated agent workspace",
                        path.display()
                    ));
                }
                return Ok(line.to_owned());
            }
            return Ok(format!("{prefix}{}", child.join(path).display()));
        }
    }
    Ok(line.to_owned())
}

pub(crate) fn subagent_context(path: &Path) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "SubagentStart",
            "additionalContext": format!("Your workspace mount is {}. {AGENT_GUIDANCE}", path.display())
        }
    })
}
