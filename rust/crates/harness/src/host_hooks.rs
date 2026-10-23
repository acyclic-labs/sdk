//! Native lifecycle hook manifests. Hosts retain their tools, permissions and loop.
//!
//! Ported selectively from #515 (ed9b04d8d, d28c15805, 4f63a3dad).
//! This module describes hooks; it never executes tools or starts an agent.

use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

mod routing;
pub use routing::route_tool;

/// First supported Codex release, including native MCP lifecycle hooks.
pub const MINIMUM_CODEX_VERSION: &str = "0.164.0";

/// Refuses unsupported or unidentifiable hosts before installation mutates files.
pub fn check_codex_version(output: &str) -> crate::Result<()> {
    let version = output.trim().strip_prefix("codex-cli ").ok_or_else(|| {
        crate::Error::Invalid(format!(
            "cannot identify Codex; requires Codex >= {MINIMUM_CODEX_VERSION}"
        ))
    })?;
    let numbers = version
        .split('.')
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>();
    if !numbers
        .is_ok_and(|numbers| numbers.len() == 3 && numbers.as_slice() >= [0, 164, 0].as_slice())
    {
        return Err(crate::Error::Unsupported(format!(
            "Codex {version} is unsupported; requires Codex >= {MINIMUM_CODEX_VERSION}; upgrade Codex before installing or using Acyclic"
        )));
    }
    Ok(())
}

/// Claude lifecycle boundaries used for workspace coordination.
pub const CLAUDE_EVENTS: [&str; 9] = [
    "SessionStart",
    "PreToolUse",
    "PostToolUse",
    "PostToolUseFailure",
    "SubagentStart",
    "SubagentStop",
    "WorktreeCreate",
    "WorktreeRemove",
    "SessionEnd",
];

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
            group.insert(
                "hooks".into(),
                json!([{
                    "type": "command",
                    "command": format!("{executable} __hook claude-code {event}"),
                    "timeout": if event == "SessionEnd" { 3 } else { 120 },
                    "statusMessage": "Acyclic is routing the workspace"
                }]),
            );
            if matches!(event, "PreToolUse" | "PostToolUse" | "PostToolUseFailure") {
                group.insert("matcher".into(), json!(".*"));
            }
            (event, vec![Value::Object(group)])
        })
        .collect()
}

/// Claude plugin manifest sharing the installed hook vocabulary.
#[must_use]
pub fn claude_manifest(version: &str) -> Value {
    json!({
        "name": "acyclic",
        "version": version,
        "description": "Coordinate native subagent workspaces with the shared harness.",
        "author": {"name": "Acyclic Labs"},
        "license": "Apache-2.0",
        "hooks": command_hooks("\"${CLAUDE_PLUGIN_ROOT}/../bin/acyclic\"")
    })
}

/// Executable of an exactly matching installed hook, for ownership and diagnostics.
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
    (hooks.get(event)?.first()? == group).then_some(executable)
}
