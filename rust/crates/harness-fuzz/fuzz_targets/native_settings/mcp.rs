//! MCP declaration admission through the original native file loader.

use super::{Files, NativeSettings, write};
use acyclic_harness_core::mcp::McpTransport;
use serde_json::{Value, json};

fn load(declarations: Value, codex: bool) -> acyclic_harness_core::Result<NativeSettings> {
    let files = Files::new();
    let (path, bytes) = if codex {
        (
            files.codex.join("config.toml"),
            toml::to_string(&json!({"mcp_servers":declarations}))
                .unwrap()
                .into_bytes(),
        )
    } else {
        (
            files.home.join(".claude.json"),
            serde_json::to_vec(&json!({"mcpServers":declarations})).unwrap(),
        )
    };
    write(&path, &bytes);
    files.load_bounded(true, u64::from(u32::MAX))
}

pub(super) fn check(bytes: &[u8]) {
    // Arbitrary caller arguments remain data; no process or connection opens.
    let argument = String::from_utf8_lossy(bytes);
    let declarations = json!({"selected":{"command":"server","args":[argument]}});
    let result = load(declarations.clone(), false);
    if argument.contains('\0') {
        assert!(
            result.is_err(),
            "process arguments with NUL must be refused"
        );
    } else {
        let snapshot = result.unwrap();
        let (_, server) = snapshot.mcp.servers().next().unwrap();
        let McpTransport::Stdio { args, .. } = &server.transport else {
            panic!("process declaration changed transport");
        };
        assert_eq!(
            args,
            &[argument.as_ref()],
            "caller arguments must stay intact"
        );
    }
    let mut disabled = declarations;
    disabled["selected"]["enabled"] = false.into();
    // Disabled lower-priority declarations are deliberately not normalized.
    disabled["selected"]["unknown"] = true.into();
    assert!(
        load(disabled, false)
            .unwrap()
            .mcp
            .server_names()
            .next()
            .is_none(),
        "disabled declarations must remain absent without validation"
    );

    static ADMISSION: std::sync::Once = std::sync::Once::new();
    ADMISSION.call_once(|| {
        let process = json!({"command":"server", "args":["with spaces"],
            "env":{"KEY":"caller-value"}, "env_vars":["OTHER"], "cwd":"workspace",
            "startup_timeout_sec":1.5, "tool_timeout_sec":2,
            "enabled_tools":["read"], "disabled_tools":["write"], "required":true});
        for codex in [false, true] {
            let snapshot = load(json!({"selected":process}), codex).unwrap();
            let (_, server) = snapshot.mcp.servers().next().unwrap();
            assert_eq!(
                (server.startup_timeout_ms, server.tool_timeout_ms),
                (1500, 2000)
            );
            assert_eq!(
                server.enabled_tools.as_deref(),
                Some(["read".into()].as_slice())
            );
            assert_eq!(server.disabled_tools, ["write"]);
            assert!(server.required);
            let McpTransport::Stdio {
                command,
                args,
                env,
                env_vars,
                cwd,
            } = &server.transport
            else {
                panic!("process declaration changed transport");
            };
            assert_eq!(command, "server");
            assert_eq!(args, &["with spaces"]);
            assert_eq!(env.get("KEY").map(String::as_str), Some("caller-value"));
            assert_eq!(env_vars, &["OTHER"]);
            assert_eq!(cwd.as_deref(), Some(std::path::Path::new("workspace")));
            for kind in [None, Some("http"), Some("streamable-http")] {
                let mut value = json!({"url":"https://example.test/mcp",
                    "http_headers":{"X-Key":"caller-value"},
                    "env_http_headers":{"X-Other":"KEY"}, "bearer_token_env_var":"TOKEN"});
                if let Some(kind) = kind {
                    value["type"] = kind.into();
                }
                let result = load(json!({"selected":value}), codex);
                if !codex && kind.is_none() {
                    assert!(
                        result.is_err(),
                        "Claude HTTP requires an explicit transport"
                    );
                    continue;
                }
                let snapshot = result.unwrap();
                let (_, server) = snapshot.mcp.servers().next().unwrap();
                assert_eq!(
                    (server.startup_timeout_ms, server.tool_timeout_ms),
                    (10_000, 60_000)
                );
                let McpTransport::Http {
                    url,
                    headers,
                    env_headers,
                    bearer_token_env_var,
                } = &server.transport
                else {
                    panic!("HTTP declaration changed transport");
                };
                assert_eq!(url, "https://example.test/mcp");
                assert_eq!(
                    headers.get("X-Key").map(String::as_str),
                    Some("caller-value")
                );
                assert_eq!(env_headers.get("X-Other").map(String::as_str), Some("KEY"));
                assert_eq!(bearer_token_env_var.as_deref(), Some("TOKEN"));
            }
            for invalid in [
                json!({}),
                json!({"command":""}),
                json!({"command":" "}),
                json!({"command":"a\u{0}b"}),
                json!({"command":"server","args":["a\u{0}b"]}),
                json!({"command":"server","env":{"BAD=KEY":"x"}}),
                json!({"command":"server","env_vars":[""]}),
                json!({"command":"server","env":{"KEY":"a\u{0}b"}}),
                json!({"command":"server","startup_timeout_sec":0}),
                json!({"command":"server","tool_timeout_sec":-1}),
                json!({"command":"server","tool_timeout_sec":0.0001}),
                json!({"command":"server","tool_timeout_sec":1e20}),
                json!({"type":"sse","url":"https://example.test"}),
                json!({"type":"http","command":"server","url":"https://example.test"}),
                json!({"type":"http","url":" "}),
                json!({"type":"stdio","command":"server","headers":{"X":"value"}}),
                json!({"command":"server","unknown":true}),
                json!({"enabled":"false"}),
            ] {
                assert!(
                    load(json!({"selected":invalid}), codex).is_err(),
                    "invalid selected MCP declaration must be refused"
                );
            }
            for name in ["", "bad.name", "bad/name", "bad name", "é"] {
                assert!(
                    load(json!({(name):{"command":"server"}}), codex).is_err(),
                    "invalid selected MCP names must be refused"
                );
            }
        }
        // JSON also admits null; TOML has no null value to serialize.
        assert!(load(json!({"selected":null}), false).is_err());
    });
}
