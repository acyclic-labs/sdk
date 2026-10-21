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

/// A parent checkout root and the child's fork view of the same tree.
pub(crate) struct RootView {
    /// The parent's checkout root in each spelling a host may use for it,
    /// longest first so a spelling never matches inside a longer one.
    spellings: Vec<String>,
    /// The child's fork view of that root.
    view: PathBuf,
    /// The view in comparable form: text already naming the view is kept,
    /// even where the view lies under the root it maps.
    view_spelling: String,
}

impl RootView {
    pub(crate) fn new(root: &Path, view: PathBuf) -> Self {
        let mut spellings = vec![comparable_path(&root.to_string_lossy())];
        if let Ok(canonical) = root.canonicalize() {
            spellings.push(comparable_path(&canonical.to_string_lossy()));
        }
        spellings.sort_by_key(|spelling| std::cmp::Reverse(spelling.len()));
        spellings.dedup();
        let view_spelling = comparable_path(&view.to_string_lossy());
        Self {
            spellings,
            view,
            view_spelling,
        }
    }

    /// The child's fork view of this root.
    pub(crate) fn view(&self) -> &Path {
        &self.view
    }
}

/// Every fork view of one child and the view its relative paths resolve in.
pub(crate) struct ChildScope<'a> {
    pub(crate) views: &'a [RootView],
    pub(crate) selected: &'a Path,
}

/// Maps one path a child names into its fork views and decides access to it,
/// returning the root view the path lies in and its path there. A path under
/// a parent root names the same relative path in the child's view of that
/// root, a relative path resolves in the selected view, and the access
/// decision then holds the path to the view it was mapped into. Mapping and
/// validation are one step, so they cannot choose different roots.
pub(crate) fn route_path<'a>(
    path: &str,
    scope: &ChildScope<'a>,
) -> Result<(&'a RootView, PathBuf), String> {
    let mapped = map_root_references(path, scope.views);
    let mapped = Path::new(&mapped);
    if mapped
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err("parent traversal is denied in an isolated agent workspace".to_owned());
    }
    let (view, candidate) = if mapped.is_absolute() {
        let view = scope
            .views
            .iter()
            .filter(|view| mapped.starts_with(view.view()))
            .max_by_key(|view| view.view().components().count())
            .ok_or_else(|| {
                format!(
                    "absolute path '{}' escapes the isolated agent workspace",
                    mapped.display()
                )
            })?;
        (view, mapped.to_path_buf())
    } else {
        let view = scope
            .views
            .iter()
            .find(|view| view.view() == scope.selected)
            .ok_or_else(|| "the selected root has no view in this workspace".to_owned())?;
        (view, scope.selected.join(mapped))
    };
    let view_root = view.view().canonicalize().map_err(display)?;
    let mut existing = candidate.as_path();
    while fs::symlink_metadata(existing).is_err() {
        existing = existing
            .parent()
            .ok_or_else(|| "path has no existing isolated ancestor".to_owned())?;
    }
    let resolved = existing.canonicalize().map_err(display)?;
    if !resolved.starts_with(&view_root) {
        return Err(format!(
            "path '{}' resolves through a link outside the isolated agent workspace",
            mapped.display()
        ));
    }
    Ok((view, candidate))
}

/// The Windows verbatim path prefix.
const VERBATIM_PREFIX: &str = r"\\?\";

/// A path spelling in comparable form: on Windows without the verbatim
/// prefix, with one separator and ASCII case folded.
fn comparable_path(path: &str) -> String {
    if cfg!(windows) {
        comparable_text(path.strip_prefix(VERBATIM_PREFIX).unwrap_or(path))
    } else {
        path.to_owned()
    }
}

/// `text` with each character in comparable form at its own byte offset:
/// ASCII case folding and separator unification preserve lengths, so a
/// match in the comparable text is a match at the same offsets in `text`.
fn comparable_text(text: &str) -> String {
    if cfg!(windows) {
        text.replace('/', "\\").to_ascii_lowercase()
    } else {
        text.to_owned()
    }
}

fn is_path_separator(character: char) -> bool {
    character == '/' || cfg!(windows) && character == '\\'
}

/// Whether `character` continues a path component, so a root spelling that
/// borders it is part of a longer name rather than the root itself.
fn continues_name(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '-' | '.' | '~')
}

/// Rewrites each absolute path under a parent checkout root in `text` to the
/// same relative path in the child's fork view of that root. Paths outside
/// every root are left for the ordinary access decision.
pub(crate) fn map_root_references(text: &str, views: &[RootView]) -> String {
    let comparable = comparable_text(text);
    let mut mapped = String::with_capacity(text.len());
    let mut copied = 0;
    let mut index = 0;
    while let Some(character) = text[index..].chars().next() {
        let at_boundary = text[..index]
            .chars()
            .next_back()
            .is_none_or(|before| !continues_name(before) && !is_path_separator(before));
        // A verbatim prefix belongs to the path it introduces.
        let body = if cfg!(windows) && comparable[index..].starts_with(VERBATIM_PREFIX) {
            index + VERBATIM_PREFIX.len()
        } else {
            index
        };
        let names = |spelling: &str| {
            let end = body + spelling.len();
            let bounded = text
                .get(end..)
                .and_then(|rest| rest.chars().next())
                .is_none_or(|after| is_path_separator(after) || !continues_name(after));
            (at_boundary && bounded && comparable.get(body..end) == Some(spelling)).then_some(end)
        };
        if let Some(end) = views.iter().find_map(|view| names(&view.view_spelling)) {
            index = end;
        } else if let Some((end, view)) = views.iter().find_map(|view| {
            view.spellings
                .iter()
                .find_map(|spelling| names(spelling).map(|end| (end, view)))
        }) {
            mapped.push_str(&text[copied..index]);
            mapped.push_str(&view.view().to_string_lossy());
            copied = end;
            index = end;
        } else {
            index += character.len_utf8();
        }
    }
    mapped.push_str(&text[copied..]);
    mapped
}

/// Keys whose strings are file content or edit text, never paths: they reach
/// the workspace verbatim.
fn is_content_field(key: &str) -> bool {
    matches!(
        key,
        "content" | "old_string" | "new_string" | "new_source" | "old_str" | "new_str"
    )
}

/// Maps parent-root references in every non-content string of a tool input.
/// An apply_patch command maps only its file headers: its body is content.
fn map_tool_input_roots(tool_name: &str, value: &mut Value, key: Option<&str>, views: &[RootView]) {
    match value {
        Value::String(text) if key.is_none_or(|key| !is_content_field(key)) => {
            *text = if tool_name == "apply_patch" && key == Some("command") {
                text.split('\n')
                    .map(|line| match patch_header_path(line) {
                        Some((prefix, path)) => {
                            format!("{prefix}{}", map_root_references(path, views))
                        }
                        None => line.to_owned(),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                map_root_references(text, views)
            };
        }
        Value::Array(values) => {
            for value in values {
                map_tool_input_roots(tool_name, value, key, views);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                map_tool_input_roots(tool_name, value, Some(key), views);
            }
        }
        _ => {}
    }
}

const PATCH_HEADER_PREFIXES: [&str; 5] = [
    "*** Add File: ",
    "*** Update File: ",
    "*** Delete File: ",
    "*** Move to: ",
    "*** Copy to: ",
];

fn patch_header_path(line: &str) -> Option<(&'static str, &str)> {
    PATCH_HEADER_PREFIXES
        .iter()
        .find_map(|prefix| line.strip_prefix(prefix).map(|path| (*prefix, path)))
}

/// Routes a child's tool input into its fork view: an absolute path under a
/// parent checkout root becomes the same relative path in the child's view
/// of that root, a relative path resolves in `child`, and every path is then
/// held to the child's workspace.
pub(crate) fn rewrite_tool_input(
    tool_name: &str,
    mut input: Value,
    views: &[RootView],
    child: &Path,
) -> Result<Value, String> {
    let scope = ChildScope {
        views,
        selected: child,
    };
    let child_text = child.to_string_lossy();
    map_tool_input_roots(tool_name, &mut input, None, views);
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
            .map(|line| rewrite_patch_header(line, &scope))
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        input
            .as_object_mut()
            .ok_or_else(|| "apply_patch input must be an object".to_owned())?
            .insert("command".to_owned(), Value::String(rewritten));
    }
    validate_tool_paths(tool_name, &input, &scope)?;
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

pub(crate) fn pre_tool_update(updated: &Value) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": updated
        }
    })
}

// The npm package links `acyclic-plugin` to the executable; agents following
// guidance from earlier releases, and hooks naming the executable by path, run
// it as `acyclic`.
fn is_acyclic_command_name(name: &str) -> bool {
    [
        "acyclic",
        "acyclic.exe",
        "acyclic.cmd",
        "acyclic.ps1",
        "acyclic-plugin",
        "acyclic-plugin.exe",
        "acyclic-plugin.cmd",
        "acyclic-plugin.ps1",
    ]
    .iter()
    .any(|command| name.eq_ignore_ascii_case(command))
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
        .is_some_and(is_acyclic_command_name);
    if !acyclic {
        return Ok(false);
    }
    let argv = split_standalone_command(command)?;
    Ok(argv.first().is_some_and(|program| {
        Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_acyclic_command_name)
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
    scope: &ChildScope<'_>,
) -> Result<(), String> {
    if (matches!(tool_name, "Bash" | "PowerShell") || is_exec_command_tool(tool_name))
        && let Some(command) = input
            .get("cmd")
            .or_else(|| input.get("command"))
            .and_then(Value::as_str)
    {
        validate_shell_paths(command, scope)?;
    }
    validate_structured_paths(input, None, scope)
}

pub(crate) fn is_exec_command_tool(tool_name: &str) -> bool {
    tool_name == "exec_command" || tool_name.ends_with("__exec_command")
}

pub(crate) fn validate_structured_paths(
    value: &Value,
    key: Option<&str>,
    scope: &ChildScope<'_>,
) -> Result<(), String> {
    match value {
        Value::String(path) if key.is_some_and(is_path_field) => validate_path(path, scope),
        Value::Array(values) => {
            for value in values {
                validate_structured_paths(value, key, scope)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                validate_structured_paths(value, Some(key), scope)?;
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
            | "notebook_path"
            | "referenced_image_paths"
    )
}

pub(crate) fn validate_path(path: &str, scope: &ChildScope<'_>) -> Result<(), String> {
    route_path(path, scope).map(drop)
}

pub(crate) fn validate_shell_paths(command: &str, scope: &ChildScope<'_>) -> Result<(), String> {
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
                validate_path(value, scope)?;
            }
            continue;
        }
        validate_path(token, scope)?;
    }
    Ok(())
}

/// Host subtrees owned by version control or SDK state, never workspace
/// content. The order is the watcher fence placement preference: an existing
/// `.git` hosts fence cookies, and only a root without one gains `.acyclic-sdk`.
pub(crate) const RESERVED_ROOT_NAMES: [&str; 2] = [".acyclic-sdk", ".git"];

pub(crate) fn reserved_root_paths() -> Result<Vec<NamespacePath>, String> {
    RESERVED_ROOT_NAMES
        .iter()
        .map(|name| native_namespace_path(&format!("/{name}")))
        .collect()
}

/// The host's namespace path for the portable absolute `path`.
pub(crate) fn native_namespace_path(path: &str) -> Result<NamespacePath, String> {
    let limits = VolumeLimits::default();
    let path = PortablePath::parse(path, limits).map_err(display)?;
    NamespacePath::from_portable_in_profile(&path, native_filesystem_profile(), limits)
        .map_err(display)
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
    let observed = source.root_identity().to_bytes();
    if observed != expected {
        return Err(format!(
            "native root identity changed for {}; refusing to reuse its workspace",
            path.display()
        ));
    }
    Ok(())
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
            name.unicode_text(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .ok()
            .map(|receipt| receipt.value)
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

pub(crate) fn rewrite_patch_header(line: &str, scope: &ChildScope<'_>) -> Result<String, String> {
    let Some((prefix, path)) = patch_header_path(line) else {
        return Ok(line.to_owned());
    };
    let (_, routed) = route_path(path, scope)?;
    Ok(format!("{prefix}{}", routed.display()))
}

pub(crate) fn subagent_context(path: &Path) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "SubagentStart",
            "additionalContext": format!("Your workspace mount is {}. {AGENT_GUIDANCE}", path.display())
        }
    })
}
