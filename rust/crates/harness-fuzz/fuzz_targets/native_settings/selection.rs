//! Whole-entry source selection, before declaration validation.

use super::{Files, NativeSettings, SettingsConfig, write};
use acyclic_harness_core::mcp::McpTransport;
use serde_json::{Value, json};

fn workspace(
    files: &Files,
    configured: &std::path::Path,
    trusted: bool,
) -> acyclic_harness_core::Result<NativeSettings> {
    futures::executor::block_on(NativeSettings::load(
        SettingsConfig::new(configured, &files.home)
            .codex_home(&files.codex)
            .trust_project_mcp(trusted),
    ))
}

fn selected(snapshot: &NativeSettings, expected: &str) {
    let mut servers = snapshot.mcp.servers();
    let (name, server) = servers
        .next()
        .expect("winning declaration must remain present");
    assert_eq!(name, "selected");
    assert!(
        servers.next().is_none(),
        "disabled declarations must stay absent"
    );
    let McpTransport::Stdio { command, args, .. } = &server.transport else {
        panic!("winning declaration changed transport");
    };
    assert_eq!(command, expected);
    assert!(
        args.is_empty(),
        "whole-entry overrides must discard lower fields"
    );
}

fn scope(files: &Files, role: usize, value: &Value) {
    let (path, bytes) = match role {
        0 => (
            files.home.join(".claude.json"),
            serde_json::to_vec(&json!({"mcpServers":value})).unwrap(),
        ),
        1 => (
            files.codex.join("config.toml"),
            toml::to_string(&json!({"mcp_servers":value}))
                .unwrap()
                .into_bytes(),
        ),
        2 => (
            files.workspace.join(".mcp.json"),
            serde_json::to_vec(&json!({"mcpServers":value})).unwrap(),
        ),
        _ => (
            files.workspace.join(".codex/config.toml"),
            toml::to_string(&json!({"mcp_servers":value}))
                .unwrap()
                .into_bytes(),
        ),
    };
    write(&path, &bytes);
}

pub(super) fn check() {
    let invalid = json!({"selected":{"command":42,"args":["discard"]},"disabled":{"command":42}});
    let valid = json!({"selected":{"command":"winner"},"disabled":{"enabled":false,"command":42}});
    for winner in 0..4 {
        let files = Files::new();
        for role in 0..winner {
            scope(&files, role, &invalid);
        }
        scope(&files, winner, &valid);
        let snapshot = files
            .load(true)
            .expect("winning scope must replace lower invalid declarations");
        selected(&snapshot, "winner");
        scope(&files, winner, &invalid);
        assert!(
            files.load(true).is_err(),
            "invalid winning declarations must be refused"
        );
    }
    for canonical_key in [false, true] {
        let files = Files::new();
        let configured = files.workspace.join("..").join("workspace");
        let canonical = std::fs::canonicalize(&files.workspace).unwrap();
        let key = if canonical_key {
            &canonical
        } else {
            &configured
        };
        for role in 0..4 {
            scope(&files, role, &invalid);
        }
        let home = files.home.join(".claude.json");
        let mut document = json!({"mcpServers":invalid,"projects":{
            "unrelated-project":{"mcpServers":{"selected":{"command":42}}},
            "another-project":42,
            (key.to_string_lossy().as_ref()):{"mcpServers":valid}
        }});
        write(&home, &serde_json::to_vec(&document).unwrap());
        for trusted in [false, true] {
            let snapshot = workspace(&files, &configured, trusted)
                .expect("selected workspace entries must retain their winning declaration");
            selected(&snapshot, "winner");
        }
        document["projects"][key.to_string_lossy().as_ref()]["mcpServers"] = invalid.clone();
        write(&home, &serde_json::to_vec(&document).unwrap());
        assert!(
            workspace(&files, &configured, true).is_err(),
            "invalid selected workspace entries must be refused"
        );
    }
}
