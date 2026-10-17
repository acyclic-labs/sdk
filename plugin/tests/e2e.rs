//! Native hook processes are clients, and cannot create a second task owner.
use acyclic_harness::external_executor::ExternalHost;
use serde::Deserialize;
use serde_json::Value;
use std::{
    io::Write as _,
    process::{Command, Stdio},
};

#[path = "activation/mod.rs"]
mod activation;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Deserialize)]
struct Scenario {
    host: ExternalHost,
    event: String,
    input: Value,
}

#[cfg(unix)]
#[test]
fn claude_command_hooks_deliver_arguments_through_the_real_shell() -> Result {
    let directory = tempfile::tempdir()?;
    let quoted = directory
        .path()
        .join("owned ' $(never-run) `native` directory");
    std::fs::create_dir(&quoted)?;
    let executable = quoted.join("acyclic");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_acyclic"), &executable)?;
    let missing = directory.path().join("missing-binding.json");
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!(
        "../../rust/crates/harness/tests/external_hooks.json"
    ))?;
    let groups = acyclic_harness::external_executor::claude_hooks(executable.to_str().unwrap());
    for scenario in scenarios
        .into_iter()
        .filter(|scenario| scenario.host == ExternalHost::ClaudeCode)
    {
        let command = groups[scenario.event.as_str()][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        let mut child = Command::new("sh")
            .args(["-c", command])
            .current_dir(directory.path())
            .env("ACYCLIC_HARNESS_BINDING", &missing)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("hook stdin")?
            .write_all(&serde_json::to_vec(&scenario.input)?)?;
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(2));
        let answer: Value = serde_json::from_slice(&output.stdout)?;
        if scenario.event == "PreToolUse" {
            assert_eq!(answer["hookSpecificOutput"]["permissionDecision"], "deny");
        } else {
            assert!(
                answer["systemMessage"].is_string(),
                "hook did not receive its event arguments: {answer}"
            );
        }
    }
    assert!(!directory.path().join("never-run").exists());
    Ok(())
}

#[test]
fn invalid_platform_binding_fails_closed_without_starting_a_plugin_host() -> Result {
    let directory = tempfile::tempdir()?;
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!(
        "../../rust/crates/harness/tests/external_hooks.json"
    ))?;
    for scenario in scenarios {
        let mut child = Command::new(env!("CARGO_BIN_EXE_acyclic"))
            .args(["__hook", scenario.host.name(), &scenario.event])
            .current_dir(directory.path())
            .env(
                "ACYCLIC_HARNESS_BINDING",
                directory.path().join("missing-binding.json"),
            )
            .env("HOME", directory.path())
            .env("USERPROFILE", directory.path())
            .env("XDG_STATE_HOME", directory.path())
            .env("LOCALAPPDATA", directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("missing hook stdin")?
            .write_all(&serde_json::to_vec(&scenario.input)?)?;
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(2));
        let answer: Value = serde_json::from_slice(&output.stdout)?;
        if scenario.event == "PreToolUse" {
            assert_eq!(
                answer.pointer("/hookSpecificOutput/permissionDecision"),
                Some(&Value::String("deny".into()))
            );
        } else if scenario.event == "tool_call" {
            assert_eq!(answer.get("block"), Some(&Value::Bool(true)));
        } else {
            assert!(
                answer
                    .get("systemMessage")
                    .and_then(Value::as_str)
                    .is_some_and(|message| !message.is_empty())
            );
        }
        assert_eq!(
            std::fs::read_dir(directory.path())?.count(),
            0,
            "a hook may not initialize an independent host"
        );
    }
    Ok(())
}
