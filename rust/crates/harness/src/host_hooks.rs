//! Native lifecycle hook manifests. Hosts retain their tools, permissions and loop.
//!
//! Ported selectively from #515 (ed9b04d8d, d28c15805, 4f63a3dad).
//! This module describes hooks; it never executes tools or starts an agent.

use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

/// First supported Codex release, including native MCP lifecycle hooks.
pub const MINIMUM_CODEX_VERSION: &str = "0.164.0";

/// Refuses unsupported or unidentifiable hosts before installation mutates files.
pub fn check_codex_version(output: &str) -> crate::Result<()> {
    let version = output.trim().strip_prefix("codex-cli ").ok_or_else(|| {
        crate::Error::Invalid(format!(
            "cannot identify Codex; requires Codex >= {MINIMUM_CODEX_VERSION}"
        ))
    })?;
    let mut parts = version.split('.');
    let numbers: [Option<u64>; 3] = std::array::from_fn(|_| parts.next()?.parse().ok());
    if !matches!(numbers, [Some(major), Some(minor), Some(patch)]
        if [major, minor, patch] >= [0, 164, 0])
        || parts.next().is_some()
    {
        return Err(crate::Error::Unsupported(format!(
            "Codex {version} is unsupported; requires Codex >= {MINIMUM_CODEX_VERSION}; upgrade Codex before installing or using Acyclic"
        )));
    }
    Ok(())
}

/// Claude lifecycle boundaries used for workspace coordination, and the
/// spawn permission prompt the harness approval chain answers.
pub const CLAUDE_EVENTS: [&str; 4] = ["SessionStart", "Stop", "SessionEnd", "PermissionRequest"];

/// Only the plugin's registered spawn prompt reaches the harness.
/// Native tools and other coordination prompts retain Claude's permissions.
const CLAUDE_PERMISSION_MATCHER: &str = "^mcp__plugin_acyclic_acyclic-sessions__spawn$";

/// Command hooks for an installed executable, quoted as one shell argument.
///
/// Claude reads the command string, not a separate `args` property. These
/// hooks observe native execution boundaries; they do not replace host tools.
#[must_use]
pub fn claude_hooks(executable: &str) -> BTreeMap<&'static str, Vec<Value>> {
    #[cfg(windows)]
    let executable = executable.replace('\\', "/");
    command_hooks(&format!("'{}'", executable.replace('\'', "'\"'\"'")))
}

fn command_hooks(executable: &str) -> BTreeMap<&'static str, Vec<Value>> {
    CLAUDE_EVENTS
        .into_iter()
        .map(|event| {
            let mut group = Map::new();
            if event == "PermissionRequest" {
                group.insert("matcher".into(), json!(CLAUDE_PERMISSION_MATCHER));
            }
            group.insert(
                "hooks".into(),
                json!([{
                    "type": "command",
                    "command": format!("{executable} __hook claude-code {event}"),
                    "timeout": if event == "SessionEnd" { 3 } else { 120 },
                    "statusMessage": "Acyclic is routing the workspace"
                }]),
            );
            (event, vec![Value::Object(group)])
        })
        .collect()
}

/// Executable of an owned installed hook, for ownership and diagnostics.
/// Ownership follows the reserved command and metadata, independently of its
/// permission selector, so editing a selector cannot leave an owned hook behind.
/// Foreign groups, additional commands and split argument fields are rejected.
#[must_use]
pub fn claude_hook_executable(event: &str, group: &Value) -> Option<String> {
    let command = group
        .get("hooks")?
        .as_array()?
        .first()?
        .get("command")?
        .as_str()?;
    let mut arguments = crate::tools::parse_command(command).ok()?;
    if arguments.len() != 4 || arguments.get(1..)? != ["__hook", "claude-code", event] {
        return None;
    }
    let executable = arguments.remove(0);
    let name = std::path::Path::new(&executable).file_name()?.to_str()?;
    if !name.eq_ignore_ascii_case("acyclic") && !name.eq_ignore_ascii_case("acyclic.exe") {
        return None;
    }
    let hooks = claude_hooks(&executable);
    let expected = hooks.get(event)?.first()?.as_object()?;
    let actual = group.as_object()?;
    (actual.len() == expected.len()
        && expected.iter().all(|(key, value)| {
            if event == "PermissionRequest" && key == "matcher" {
                actual.get(key).is_some_and(Value::is_string)
            } else {
                actual.get(key) == Some(value)
            }
        }))
    .then_some(executable)
}
