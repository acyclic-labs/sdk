//! Native manifest and installer contracts, without a provider or agent runtime.
use acyclic_harness::host_hooks::{
    CLAUDE_EVENTS, MINIMUM_CODEX_VERSION, check_codex_version, claude_hook_executable, claude_hooks,
};
use proptest::prelude::*;
use serde_json::json;

#[test]
fn codex_minimum_version_is_enforced_without_compatibility_fallback() {
    let supported = include_str!("../fixtures/hosts/codex-0.164.0.stdout");
    let obsolete = include_str!("../fixtures/hosts/codex-0.146.0.stdout");
    assert!(check_codex_version(supported).is_ok(), "{supported}");
    let error = check_codex_version(obsolete).unwrap_err().to_string();
    assert!(error.contains(MINIMUM_CODEX_VERSION), "{error}");

    // Deliberate corruptions of captured stdout, not purported host fixtures.
    let version = supported.trim_end();
    for output in [
        format!("{version}-alpha"),
        version.rsplit_once('.').unwrap().0.to_owned(),
        format!("{version}.1"),
        format!("{version} invalid"),
        version.strip_prefix("codex-cli ").unwrap().to_owned(),
        version.replacen("codex-cli", "other", 1),
        String::new(),
    ] {
        let error = check_codex_version(&output).unwrap_err().to_string();
        assert!(error.contains(MINIMUM_CODEX_VERSION), "{error}");
    }
}

fn packaged_manifest() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../../plugin/claude/.claude-plugin/plugin.json"
    ))
    .unwrap()
}

#[test]
fn installed_permission_selector_matches_only_the_registered_spawn() {
    // Original native init output and its source/digest are retained in this capture.
    let capture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../plugin/tests/hosts/fixtures/native-pair-initialization.json"
    ))
    .unwrap();
    let installed = capture["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["host"] == "claude-code" && record["mode"] == "installed")
        .unwrap();
    let init: serde_json::Value = serde_json::from_str(
        installed["stdout"]
            .as_str()
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let tools = init["tools"].as_array().unwrap();
    let server = init["mcp_servers"][0]["name"].as_str().unwrap();
    let spawn = format!("mcp__{}__spawn", server.replace(':', "_"));
    assert!(tools.iter().any(|tool| tool == &spawn));

    let hooks = claude_hooks("/owned/acyclic");
    let selector = hooks["PermissionRequest"][0]["matcher"].as_str().unwrap();
    assert_eq!(
        packaged_manifest()["hooks"]["PermissionRequest"][0]["matcher"],
        selector
    );
    let pattern = regex_lite::Regex::new(selector).unwrap();
    let selected: Vec<_> = tools
        .iter()
        .map(|tool| tool.as_str().unwrap())
        .filter(|tool| pattern.is_match(tool))
        .collect();
    assert_eq!(selected, vec![spawn.as_str()]);

    // Deliberate corruptions of the captured identity, not native output fixtures.
    for foreign in [
        spawn.replace("plugin_acyclic_", ""),
        spawn.replace("plugin_acyclic_", "plugin_parent_"),
        format!("{spawn}_suffix"),
        format!("prefix_{spawn}"),
    ] {
        assert!(!pattern.is_match(&foreign), "{foreign}");
    }
    let broad = regex_lite::Regex::new(&format!("mcp__{}__.*", server.replace(':', "_"))).unwrap();
    assert!(
        tools
            .iter()
            .filter(|tool| broad.is_match(tool.as_str().unwrap()))
            .count()
            > selected.len()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn installed_commands_preserve_one_executable_argument(directory in r"[^\x00]{0,80}") {
        let executable = format!("{directory}/acyclic");
        #[cfg(windows)]
        let expected = executable.replace('\\', "/");
        #[cfg(not(windows))]
        let expected = executable;
        for (event, groups) in claude_hooks(&expected) {
            let group = groups.first().expect("owned hook group");
            let hook = group.pointer("/hooks/0").expect("owned hook");
            prop_assert!(hook.get("args").is_none());
            prop_assert_eq!(acyclic_harness::tools::parse_command(hook.get("command").and_then(serde_json::Value::as_str).expect("hook command"))?,
                vec![expected.clone(), "__hook".into(), "claude-code".into(), event.into()]);
            prop_assert_eq!(claude_hook_executable(event, group), Some(expected.clone()));
            let mut foreign = group.clone();
            foreign.pointer_mut("/hooks/0").and_then(serde_json::Value::as_object_mut).expect("hook object").insert("permissionDecision".into(), json!("allow"));
            prop_assert!(claude_hook_executable(event, &foreign).is_none());
        }
    }
}

#[test]
fn ownership_rejects_split_arguments_foreign_hooks_and_changed_lifecycle() {
    for (event, groups) in claude_hooks("/owned/acyclic") {
        let mut split = groups.first().expect("owned hook group").clone();
        split
            .pointer_mut("/hooks/0")
            .and_then(serde_json::Value::as_object_mut)
            .expect("hook object")
            .insert("command".into(), json!("/owned/acyclic"));
        split
            .pointer_mut("/hooks/0")
            .and_then(serde_json::Value::as_object_mut)
            .expect("hook object")
            .insert("args".into(), json!(["__hook", "claude-code", event]));
        assert!(claude_hook_executable(event, &split).is_none());
        let mut foreign = groups.first().expect("owned hook group").clone();
        foreign
            .pointer_mut("/hooks/0")
            .and_then(serde_json::Value::as_object_mut)
            .expect("hook object")
            .insert(
                "command".into(),
                json!(format!("'foreign' __hook claude-code {event}")),
            );
        assert!(claude_hook_executable(event, &foreign).is_none());
        let mut changed = groups.first().expect("owned hook group").clone();
        changed
            .pointer_mut("/hooks/0")
            .and_then(serde_json::Value::as_object_mut)
            .expect("hook object")
            .insert(
                "command".into(),
                json!("'/owned/acyclic' __hook claude-code AnotherEvent"),
            );
        assert!(claude_hook_executable(event, &changed).is_none());
        assert!(
            claude_hook_executable("AnotherEvent", groups.first().expect("owned hook group"))
                .is_none()
        );
    }
}

#[cfg(unix)]
#[test]
fn native_shell_receives_arguments_from_both_manifest_and_installer() {
    use std::os::unix::fs::PermissionsExt as _;
    let temporary = tempfile::tempdir().unwrap();
    let package = temporary.path().join("package with spaces and 'quotes'");
    let bin = package.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(package.join("claude")).unwrap();
    let executable = bin.join("acyclic");
    std::fs::write(&executable, "#!/bin/sh\nprintf '%s\\n' \"$@\"\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let manifest = packaged_manifest();
    let installed = claude_hooks(executable.to_str().unwrap());
    for event in CLAUDE_EVENTS {
        for hook in [
            manifest
                .get("hooks")
                .and_then(|hooks| hooks.get(event))
                .and_then(|groups| groups.pointer("/0/hooks/0"))
                .expect("manifest hook"),
            installed
                .get(event)
                .and_then(|groups| groups.first())
                .and_then(|group| group.pointer("/hooks/0"))
                .expect("installed hook"),
        ] {
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(hook["command"].as_str().unwrap())
                .env("CLAUDE_PLUGIN_ROOT", package.join("claude"))
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("__hook\nclaude-code\n{event}\n")
            );
        }
    }
}
