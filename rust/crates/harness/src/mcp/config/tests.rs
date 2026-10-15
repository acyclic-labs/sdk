use super::*;
use serde_json::{Value, json};

fn imports(value: Value) -> Result<ImportedServer> {
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

fn parse(value: Value, dialect: Dialect) -> Result<McpConfig> {
    let mut config = McpConfig::default();
    config.insert("server".into(), imports(value)?, dialect)?;
    Ok(config)
}

#[test]
fn normalizes_both_transports_and_keeps_credentials_host_owned() -> Result<()> {
    let config = parse(
        json!({"command":"server", "args":["with spaces"],
        "env":{"KEY":"host-secret"}, "env_vars":["OTHER"], "cwd":"workspace",
        "startup_timeout_sec":1.5, "tool_timeout_sec":2,
        "enabled_tools":["read"], "disabled_tools":["write"], "required":true}),
        Dialect::Claude,
    )?;
    let (_, server) = config
        .servers()
        .next()
        .ok_or_else(|| Error::Invalid("missing server".into()))?;
    assert_eq!(server.startup_timeout_ms, 1500);
    assert_eq!(server.tool_timeout_ms, 2000);
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
        panic!("expected stdio")
    };
    assert_eq!(command, "server");
    assert_eq!(args, &["with spaces"]);
    assert_eq!(env.get("KEY").map(String::as_str), Some("host-secret"));
    assert_eq!(env_vars, &["OTHER"]);
    assert_eq!(cwd.as_deref(), Some(PathBuf::from("workspace").as_path()));

    for (dialect, kind) in [
        (Dialect::Claude, Some("http")),
        (Dialect::Claude, Some("streamable-http")),
        (Dialect::Codex, None),
    ] {
        let mut value = json!({"url":"https://example.test/mcp", "http_headers":{"X-Key":"secret"},
            "env_http_headers":{"X-Other":"KEY"}, "bearer_token_env_var":"TOKEN"});
        if let Some(kind) = kind {
            value["type"] = kind.into();
        }
        let config = parse(value, dialect)?;
        let (_, server) = config
            .servers()
            .next()
            .ok_or_else(|| Error::Invalid("missing HTTP server".into()))?;
        assert_eq!(server.startup_timeout_ms, 10_000);
        assert_eq!(server.tool_timeout_ms, 60_000);
        let McpTransport::Http {
            url,
            headers,
            env_headers,
            bearer_token_env_var,
        } = &server.transport
        else {
            panic!("expected HTTP")
        };
        assert_eq!(url, "https://example.test/mcp");
        assert_eq!(headers.get("X-Key").map(String::as_str), Some("secret"));
        assert_eq!(env_headers.get("X-Other").map(String::as_str), Some("KEY"));
        assert_eq!(bearer_token_env_var.as_deref(), Some("TOKEN"));
    }
    Ok(())
}

#[test]
fn rejects_invalid_and_conflicting_declarations_before_connection() {
    for value in [
        json!({}),
        json!({"url":"https://example.test"}),
        json!({"command":""}),
        json!({"command":"a\0b"}),
        json!({"command":"server", "args":["a\0b"]}),
        json!({"command":"server", "env":{"BAD=KEY":"x"}}),
        json!({"command":"server", "env_vars":[""]}),
        json!({"command":"server", "env":{"KEY":"a\0b"}}),
        json!({"command":"server", "startup_timeout_sec":0}),
        json!({"command":"server", "tool_timeout_sec":-1}),
        json!({"command":"server", "tool_timeout_sec":0.0001}),
        json!({"command":"server", "tool_timeout_sec":1e20}),
        json!({"type":"sse", "url":"https://example.test"}),
        json!({"type":"http", "command":"server", "url":"https://example.test"}),
        json!({"type":"http", "url":" "}),
        json!({"type":"stdio", "command":"server", "headers":{"X":"value"}}),
        json!({"command":"server", "unknown":true}),
        json!({"enabled":"false"}),
        json!(null),
    ] {
        assert!(parse(value.clone(), Dialect::Claude).is_err(), "{value}");
    }
    for name in ["", "bad.name", "bad/name", "bad name", "é"] {
        assert!(
            McpConfig::default()
                .insert(
                    name.into(),
                    ImportedServer {
                        command: Some("server".into()),
                        ..Default::default()
                    },
                    Dialect::Claude
                )
                .is_err()
        );
    }
    assert!(milliseconds(f64::NAN).is_err());
    assert!(milliseconds(f64::INFINITY).is_err());
}

#[test]
fn whole_entry_overrides_and_disabling_preserve_scope_precedence() -> Result<()> {
    let mut config = parse(
        json!({"command":"lower", "args":["discard"]}),
        Dialect::Claude,
    )?;
    config.insert(
        "server".into(),
        imports(json!({"url":"https://example.test"}))?,
        Dialect::Codex,
    )?;
    assert!(matches!(
        config.servers().next().map(|(_, spec)| &spec.transport),
        Some(McpTransport::Http { .. })
    ));
    config.insert(
        "server".into(),
        imports(json!({"enabled":false}))?,
        Dialect::Codex,
    )?;
    assert_eq!(config.server_names().count(), 0);
    config.insert(
        "server".into(),
        imports(json!({"command":"last"}))?,
        Dialect::Claude,
    )?;
    assert_eq!(config.server_names().collect::<Vec<_>>(), ["server"]);
    Ok(())
}
