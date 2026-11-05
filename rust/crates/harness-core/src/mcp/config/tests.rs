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
    for name in ["", "bad.name", "bad/name", "bad name", "Ã©"] {
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
