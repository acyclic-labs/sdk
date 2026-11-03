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

fn selected(snapshot: &acyclic_harness_core::mcp::McpConfig, expected: &str) {
    let mut servers = snapshot.servers();
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

pub(super) fn inactive(bytes: &[u8]) {
    let files = Files::new();
    write(
        &files.home.join(".claude/settings.json"),
        br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#,
    );
    let role = usize::from(bytes.first().copied().unwrap_or(0) % 4);
    let invalid = Value::String(String::from_utf8_lossy(bytes).into_owned());
    let path = match role {
        0 => files.home.join(".claude.json"),
        1 => files.codex.join("config.toml"),
        2 => files.workspace.join(".mcp.json"),
        _ => files.workspace.join(".codex/config.toml"),
    };
    let document = if role == 0 {
        serde_json::to_vec(&invalid).unwrap()
    } else if role == 2 {
        serde_json::to_vec(&json!({"mcpServers":invalid})).unwrap()
    } else {
        toml::to_string(&json!({"mcp_servers":invalid}))
            .unwrap()
            .into_bytes()
    };
    write(&path, &document);
    for load_mcp in [false, true] {
        let mut config = files.config(true, true, u32::MAX.into());
        config.load_mcp = load_mcp;
        let result = futures::executor::block_on(NativeSettings::load(config));
        if load_mcp {
            assert!(
                result.is_err(),
                "active invalid MCP sections must be refused"
            );
        } else {
            let snapshot = result.expect("inactive MCP sections must not block hooks");
            assert!(
                !snapshot.hooks.is_empty(),
                "selected hooks must remain present"
            );
            assert!(snapshot.mcp.server_names().next().is_none());
        }
    }
}

pub(super) fn partial(bytes: &[u8]) {
    // Refine the original MCP startup property through both public queries.
    let files = Files::new();
    let number = i64::from_le_bytes(
        bytes
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or_default(),
    );
    let invalid = match bytes.first().copied().unwrap_or(0) % 4 {
        0 => number.to_string(),
        1 => "false".into(),
        2 => "'invalid'".into(),
        _ => "[]".into(),
    };
    write(
        &files.codex.join("config.toml"),
        format!("hooks = {invalid}\n[mcp_servers.selected]\ncommand='winner'\n").as_bytes(),
    );
    let config = std::sync::Arc::new(files.config(true, true, 512));
    for document in [&b"{}"[..], &b"{invalid JSON"[..]] {
        for path in [
            files.home.join(".claude/settings.json"),
            files.workspace.join(".claude/settings.json"),
        ] {
            write(&path, document);
        }
        let snapshot = futures::executor::block_on(NativeSettings::load_mcp(config.clone()))
            .expect("MCP-only startup must ignore hook-only sections and sources");
        selected(&snapshot, "winner");
        assert!(
            futures::executor::block_on(NativeSettings::load_hooks(config.clone())).is_err(),
            "hook-only startup must refuse selected invalid hooks"
        );
    }
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
        selected(&snapshot.mcp, "winner");
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
            selected(&snapshot.mcp, "winner");
        }
        document["projects"][key.to_string_lossy().as_ref()]["mcpServers"] = invalid.clone();
        write(&home, &serde_json::to_vec(&document).unwrap());
        assert!(
            workspace(&files, &configured, true).is_err(),
            "invalid selected workspace entries must be refused"
        );
    }
}
