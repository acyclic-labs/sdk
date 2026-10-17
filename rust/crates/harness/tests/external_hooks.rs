//! Native host hook fixtures over the shared external-executor request API.
use acyclic_harness::external_executor::*;
#[cfg(any(feature = "filesystem", feature = "hooks"))]
use acyclic_harness::task_volume;
use acyclic_harness::{Error, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
#[path = "external_hooks/local.rs"]
mod local;

#[path = "external_hooks/answers.rs"]
mod answers;
#[path = "../src/external_executor/append.rs"]
#[cfg(all(
    any(feature = "filesystem", feature = "hooks"),
    not(target_arch = "wasm32")
))]
mod append;
#[path = "external_hooks/append_model.rs"]
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
mod append_model;
#[path = "external_hooks/attachment_property.rs"]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod attachment_property;
#[path = "external_hooks/boundary_stream.rs"]
#[cfg(all(
    any(feature = "filesystem", feature = "hooks"),
    not(target_arch = "wasm32")
))]
mod boundary_stream;
#[path = "external_hooks/intake.rs"]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod intake;
#[path = "external_hooks/intake_model.rs"]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod intake_model;
#[path = "external_hooks/lifecycle_recovery.rs"]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod lifecycle_recovery;
#[path = "external_hooks/publication_model.rs"]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod publication_model;
#[path = "external_hooks/recovery_model.rs"]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod recovery_model;

#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
#[path = "external_hooks/client.rs"]
mod client;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
#[path = "external_hooks/follow_model.rs"]
mod follow_model;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "external_hooks/native_children.rs"]
mod native_children;
#[cfg(not(target_arch = "wasm32"))]
#[path = "external_hooks/properties.rs"]
mod properties;
#[path = "external_hooks/session.rs"]
#[cfg(not(target_arch = "wasm32"))]
mod session;
#[path = "external_hooks/session_runtime.rs"]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod session_runtime;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "external_hooks/task_commands.rs"]
mod task_commands;
#[path = "external_hooks/tool_input.rs"]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod tool_input;
#[path = "external_hooks/volume.rs"]
mod volume;

#[derive(Deserialize)]
struct Scenario {
    host: ExternalHost,
    event: String,
    input: Value,
    action: Value,
}

#[test]
fn host_events_create_the_same_task_request_actions() -> Result<()> {
    let covered = assert_scenarios(include_str!("external_hooks.json"), "session")?;
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        for spec in hooks(host) {
            assert!(covered.contains(&(host.name(), spec.event.to_owned())));
        }
    }
    Ok(())
}

#[test]
fn declared_child_sessions_preserve_the_parent_hook_identity() -> Result<()> {
    let covered = assert_scenarios(include_str!("external_child_sessions.json"), "parent")?;
    assert_eq!(
        covered,
        HashSet::from([
            ("pi", "subagent_start".to_owned()),
            ("pi", "subagent_stop".to_owned()),
        ])
    );
    Ok(())
}

fn assert_scenarios(bytes: &str, session: &str) -> Result<HashSet<(&'static str, String)>> {
    let scenarios: Vec<Scenario> =
        serde_json::from_str(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut operations = HashSet::new();
    let mut covered = HashSet::new();
    for scenario in scenarios {
        assert!(covered.insert((scenario.host.name(), scenario.event.clone())));
        let actual = request(scenario.host, &scenario.event, scenario.input)?;
        assert_eq!(actual.session_id, session);
        assert_eq!(
            serde_json::to_value(&actual.action).map_err(|error| invalid(&error))?,
            scenario.action
        );
        assert!(
            operations.insert(actual.operation_id),
            "hook actions must have distinct identities"
        );
        let bytes = serde_json::to_vec(&actual).map_err(|error| invalid(&error))?;
        assert_eq!(
            serde_json::from_slice::<ExternalRequest>(&bytes).map_err(|error| invalid(&error))?,
            actual
        );
    }
    Ok(covered)
}

#[test]
fn retry_identity_does_not_turn_changed_arguments_into_another_operation() -> Result<()> {
    let input = json!({"session_id":"session","tool_use_id":"call","tool_name":"Bash","tool_input":{"command":"pwd"}});
    let original = request(ExternalHost::ClaudeCode, "PreToolUse", input.clone())?;
    assert_eq!(
        request(ExternalHost::ClaudeCode, "PreToolUse", input.clone())?,
        original
    );
    let mut changed = input.clone();
    set(&mut changed, "tool_input", json!({"command":"ls"}))?;
    let changed = request(ExternalHost::ClaudeCode, "PreToolUse", changed)?;
    assert_eq!(changed.operation_id, original.operation_id);
    assert_ne!(changed.action, original.action);
    let mut other_session = input.clone();
    set(&mut other_session, "session_id", json!("other"))?;
    assert_ne!(
        request(ExternalHost::ClaudeCode, "PreToolUse", other_session)?.operation_id,
        original.operation_id
    );
    let mut child = input;
    set(&mut child, "agent_id", json!("child"))?;
    assert_ne!(
        request(ExternalHost::ClaudeCode, "PreToolUse", child)?.operation_id,
        original.operation_id
    );
    Ok(())
}

#[test]
fn malformed_hooks_never_acquire_a_request_identity() {
    for (event, input) in [
        ("SessionStart", Value::Null),
        ("SessionStart", json!({"cwd":"/project"})),
        ("SessionStart", json!({"session_id":"","cwd":"/project"})),
        (
            "SessionStart",
            json!({"session_id":"session","sessionId":"other","cwd":"/project"}),
        ),
        (
            "PreToolUse",
            json!({"session_id":"session","tool_name":"Bash","tool_input":{}}),
        ),
        (
            "PreToolUse",
            json!({"session_id":"session","tool_name":"Bash","tool_use_id":"call"}),
        ),
        (
            "PostToolUse",
            json!({"session_id":"session","tool_use_id":"call","isError":"false"}),
        ),
        (
            "PostToolUseFailure",
            json!({"session_id":"session","tool_use_id":"call","isError":"false"}),
        ),
        (
            "PostToolUseFailure",
            json!({"session_id":"session","tool_use_id":"call"}),
        ),
        (
            "PostToolUseFailure",
            json!({"session_id":"session","tool_use_id":"call","error":{"message":"failed"}}),
        ),
        ("SubagentStart", json!({"session_id":"session"})),
        (
            "SubagentStop",
            json!({"session_id":"session","agent_id":"\0"}),
        ),
        ("unknown", json!({"session_id":"session"})),
    ] {
        assert!(
            request(ExternalHost::ClaudeCode, event, input).is_err(),
            "{event}"
        );
    }
    for input in [
        json!({"sessionId":"session","toolCallId":"call","isError":false}),
        json!({"sessionId":"session","toolCallId":"call","content":[]}),
        json!({"sessionId":"session","toolCallId":"call","isError":"false","content":[]}),
        json!({"sessionId":"session","toolCallId":"call","isError":false,"content":"output"}),
    ] {
        assert!(request(ExternalHost::Pi, "tool_result", input).is_err());
    }
}

#[test]
fn native_metadata_and_credentials_are_not_copied_into_requests() -> Result<()> {
    let actual = request(
        ExternalHost::Codex,
        "SessionStart",
        json!({
            "session_id":"session", "cwd":"/project", "environment":{"API_KEY":"private-secret"},
            "transcript_path":"/private/transcript", "model":"native-model"
        }),
    )?;
    let encoded = serde_json::to_string(&actual).map_err(|error| invalid(&error))?;
    for private in [
        "private-secret",
        "transcript",
        "native-model",
        "environment",
    ] {
        assert!(!encoded.contains(private));
    }
    Ok(())
}

#[test]
fn claude_manifest_drives_catalog_hooks_and_the_result_client() -> Result<()> {
    let manifest: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../plugin/claude/.claude-plugin/plugin.json"
    )))
    .map_err(|error| invalid(&error))?;
    let configured = manifest
        .get("hooks")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Invalid("Claude hooks missing".into()))?;
    assert_eq!(manifest, claude_manifest(env!("CARGO_PKG_VERSION")));
    assert_eq!(configured.len(), hooks(ExternalHost::ClaudeCode).count());
    for spec in hooks(ExternalHost::ClaudeCode) {
        let group = configured
            .get(spec.event)
            .and_then(Value::as_array)
            .and_then(|groups| groups.first())
            .ok_or_else(|| Error::Invalid(format!("{} hook group missing", spec.event)))?;
        let handler = group
            .pointer("/hooks/0")
            .ok_or_else(|| Error::Invalid(format!("{} handler missing", spec.event)))?;
        assert_eq!(handler.get("type").and_then(Value::as_str), Some("command"));
        assert_eq!(
            handler.get("command").and_then(Value::as_str),
            Some(
                format!(
                    "\"${{CLAUDE_PLUGIN_ROOT}}/../bin/acyclic\" __hook claude-code {}",
                    spec.event
                )
                .as_str()
            )
        );
        assert_eq!(
            acyclic_harness::tools::parse_command(handler["command"].as_str().unwrap())?,
            [
                "${CLAUDE_PLUGIN_ROOT}/../bin/acyclic",
                "__hook",
                "claude-code",
                spec.event
            ]
        );
        assert!(handler.get("args").is_none());
        assert_eq!(
            handler.get("timeout").and_then(Value::as_u64),
            Some(if spec.action == HookAction::Detach {
                3
            } else {
                120
            })
        );
        if matches!(spec.action, HookAction::RunTool | HookAction::ToolFinished) {
            assert_eq!(group.get("matcher").and_then(Value::as_str), Some(".*"));
        }
    }
    assert_eq!(manifest.get("mcpServers"), Some(&json!("./.mcp.json")));
    Ok(())
}

fn invalid(error: &serde_json::Error) -> Error {
    Error::Invalid(error.to_string())
}

fn set(value: &mut Value, field: &str, replacement: Value) -> Result<()> {
    value
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("hook fixture must be an object".into()))?
        .insert(field.into(), replacement);
    Ok(())
}
