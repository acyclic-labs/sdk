use super::{ExternalHost, HookAction, hooks};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

/// Generates Claude's command hooks from the shared executor catalog.
pub fn claude_hooks(executable: &str) -> BTreeMap<&'static str, Vec<Value>> {
    #[cfg(windows)]
    let executable = executable.replace('\\', "/");
    command_hooks(&format!("'{}'", executable.replace('\'', "'\"'\"'")))
}

fn command_hooks(executable: &str) -> BTreeMap<&'static str, Vec<Value>> {
    hooks(ExternalHost::ClaudeCode)
        .map(|spec| {
            let mut group = Map::new();
            group.insert(
                "hooks".into(),
                json!([{
                    "type": "command",
                    "command": format!("{executable} __hook claude-code {}", spec.event),
                    "timeout": spec.timeout_seconds(),
                    "statusMessage": "Acyclic is routing the workspace"
                }]),
            );
            if matches!(spec.action, HookAction::RunTool | HookAction::ToolFinished) {
                group.insert("matcher".into(), json!(".*"));
            }
            (spec.event, vec![Value::Object(group)])
        })
        .collect()
}

/// Generates the Claude plugin manifest shipped alongside the other host assets.
#[must_use]
pub fn claude_manifest(version: &str) -> Value {
    json!({
        "name": "acyclic",
        "version": version,
        "description": "Run tools and fork subagents through Acyclic harness actors.",
        "author": {"name":"Acyclic Labs"},
        "license": "Apache-2.0",
        "mcpServers": "./.mcp.json",
        "hooks": command_hooks("\"${CLAUDE_PLUGIN_ROOT}/../bin/acyclic\"")
    })
}

/// Recognizes exactly the installed hook shape before removing an owned entry.
pub fn owns_claude_hook(event: &str, group: &Value) -> bool {
    let Some(spec) = hooks(ExternalHost::ClaudeCode).find(|spec| spec.event == event) else {
        return false;
    };
    let Some(group) = group.as_object() else {
        return false;
    };
    let matched = matches!(spec.action, HookAction::RunTool | HookAction::ToolFinished);
    if group.len() != if matched { 2 } else { 1 }
        || matched && group.get("matcher").and_then(Value::as_str) != Some(".*")
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
    if hook.get("type").and_then(Value::as_str) != Some("command")
        || hook.get("timeout").and_then(Value::as_u64) != Some(spec.timeout_seconds())
        || hook.get("statusMessage").and_then(Value::as_str)
            != Some("Acyclic is routing the workspace")
    {
        return false;
    }
    let Some(command) = hook.get("command").and_then(Value::as_str) else {
        return false;
    };
    // Remove the former invalid command/args entries on reinstall as well.
    // They are never emitted or executed by the new client.
    let executable = if hook.contains_key("args") {
        if hook.len() != 5 || hook.get("args") != Some(&json!(["__hook", "claude-code", event])) {
            return false;
        }
        command.to_owned()
    } else {
        if hook.len() != 4 {
            return false;
        }
        let Ok(mut arguments) = crate::tools::parse_command(command) else {
            return false;
        };
        if arguments.len() != 4 || arguments[1..] != ["__hook", "claude-code", event] {
            return false;
        }
        arguments.remove(0)
    };
    std::path::Path::new(&executable)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("acyclic") || name.eq_ignore_ascii_case("acyclic.exe")
        })
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Every executable remains exactly one shell argument. Hook metadata
        // cannot add a second command or turn a foreign group into an owned one.
        #[test]
        fn claude_command_arguments_round_trip(directory in r"[^\x00]{0,80}") {
            let executable = format!("{directory}/acyclic");
            #[cfg(windows)]
            let expected = executable.replace('\\', "/");
            #[cfg(not(windows))]
            let expected = executable.clone();
            for (event, groups) in claude_hooks(&executable) {
                let group = &groups[0];
                let hook = &group["hooks"][0];
                prop_assert!(hook.get("args").is_none());
                prop_assert_eq!(crate::tools::parse_command(hook["command"].as_str().unwrap())?,
                    vec![expected.clone(), "__hook".into(), "claude-code".into(), event.into()]);
                prop_assert!(owns_claude_hook(event, group));
                let mut changed = group.clone();
                changed["hooks"][0]["foreign"] = json!(true);
                prop_assert!(!owns_claude_hook(event, &changed));
                changed = group.clone();
                changed["hooks"][0]["command"] = json!(format!("'foreign' __hook claude-code {event}"));
                prop_assert!(!owns_claude_hook(event, &changed));
            }
        }
    }

    #[test]
    fn formerly_split_hook_arguments_are_removed_but_never_emitted() {
        for (event, groups) in claude_hooks("/owned/acyclic") {
            let mut old = groups[0].clone();
            old["hooks"][0]["command"] = json!("/owned/acyclic");
            old["hooks"][0]["args"] = json!(["__hook", "claude-code", event]);
            assert!(owns_claude_hook(event, &old));
            old["hooks"][0]["args"][2] = json!("another-event");
            assert!(!owns_claude_hook(event, &old));
        }
    }
}
