use super::*;

#[cfg(feature = "mcp-stdio")]
#[tokio::test]
async fn default_settings_match_original_and_canonical_workspace_keys() -> Result<()> {
    use crate::native_settings::SettingsConfig;
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir(&home).map_err(super::super::storage)?;
    let configured = workspace.join("..").join("workspace");
    let canonical = std::fs::canonicalize(&configured).map_err(super::super::storage)?;
    assert_ne!(configured, canonical);
    for key in [&configured, &canonical] {
        let document = json!({"projects":{key.to_string_lossy().as_ref():{"mcpServers":{
            "selected":{"command":root.path().join("missing-executable")}
        }}}});
        std::fs::write(home.join(".claude.json"), document.to_string())
            .map_err(super::super::storage)?;
        let harness = LocalHarness::open(
            crate::test_support::local_harness_config(&configured, (model()?, text()))?
                .settings(SettingsConfig::new(&configured, &home).codex_home(home.join(".codex"))),
        )
        .await?;
        let diagnostics = harness.mcp_diagnostics();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].server, "selected");
        assert_eq!(
            diagnostics[0].message,
            "process not_found: MCP executable was not found in its configured search path"
        );
    }
    Ok(())
}

#[cfg(feature = "mcp-stdio")]
#[tokio::test]
async fn configured_server_failures_are_visible_and_required_failures_precede_owner_creation()
-> Result<()> {
    for required in [false, true] {
        let root = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(root.path()).await?;
        let document = json!({"mcpServers":{"missing":{
            "command":root.path().join("missing-mcp-executable"), "required":required,
        }}});
        std::fs::write(root.path().join(".claude.json"), document.to_string())
            .map_err(super::super::storage)?;
        let config = volume_config(root.path(), project, text())?.settings(
            crate::native_settings::SettingsConfig::new(root.path(), root.path())
                .codex_home(root.path().join("codex")),
        );
        let owner = super::super::owner::state_directory(&config.project)?.join("owner.json");
        let opened = LocalHarness::open(config).await;
        if required {
            assert!(
                matches!(opened, Err(Error::Process { kind: "not_found", message })
                if message == "MCP executable was not found in its configured search path")
            );
            assert!(!owner.exists());
        } else {
            let harness = opened?;
            assert_eq!(harness.mcp_diagnostics().len(), 1);
            assert_eq!(harness.mcp_diagnostics()[0].server, "missing");
            assert_eq!(
                harness.mcp_diagnostics()[0].message,
                "process not_found: MCP executable was not found in its configured search path"
            );
            assert!(harness.tools().get("read", None, None)?.is_some());
        }
    }
    Ok(())
}

#[cfg(feature = "mcp-stdio")]
#[tokio::test]
async fn project_servers_require_explicit_settings_trust() -> Result<()> {
    for trust in [false, true] {
        let root = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(root.path()).await?;
        let document = json!({"mcpServers":{"project":{
            "command":root.path().join("missing-project-mcp-executable"),
        }}});
        std::fs::write(root.path().join(".mcp.json"), document.to_string())
            .map_err(super::super::storage)?;
        let config = volume_config(root.path(), project, text())?.settings(
            crate::native_settings::SettingsConfig::new(root.path(), root.path())
                .codex_home(root.path().join("codex"))
                .trust_project_mcp(trust),
        );
        let harness = LocalHarness::open(config).await?;
        assert_eq!(harness.mcp_diagnostics().len(), usize::from(trust));
        if trust {
            assert_eq!(harness.mcp_diagnostics()[0].server, "project");
            assert_eq!(
                harness.mcp_diagnostics()[0].message,
                "process not_found: MCP executable was not found in its configured search path"
            );
        }
    }
    Ok(())
}

#[cfg(feature = "mcp-http")]
#[tokio::test]
async fn configured_http_server_initializes_pages_installs_and_calls_selected_tools() -> Result<()>
{
    use crate::native_settings::SettingsConfig;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    const CATALOG_BYTES: u32 = 1024;
    const QUERY_BYTES: usize = 2048;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(Error::process_io)?;
    let address = listener.local_addr().map_err(Error::process_io)?;
    let service = tokio::spawn(async move {
        let mut methods = Vec::new();
        for step in 0..5 {
            let (mut socket, _) = listener.accept().await.map_err(Error::process_io)?;
            let mut bytes = Vec::new();
            let mut buffer = [0; 1024];
            let (header_end, body_bytes) = loop {
                let count = socket.read(&mut buffer).await.map_err(Error::process_io)?;
                if count == 0 || bytes.len() + count > 8192 {
                    return Err(Error::Invalid(
                        "HTTP fixture request incomplete or too large".into(),
                    ));
                }
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..end])
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|length| length.trim().parse::<usize>().ok())
                        })
                        .ok_or_else(|| {
                            Error::Invalid("HTTP fixture content length absent".into())
                        })?;
                    if bytes.len() >= end + 4 + length {
                        break (end, length);
                    }
                }
            };
            let headers = std::str::from_utf8(&bytes[..header_end])
                .map_err(|error| Error::Invalid(error.to_string()))?
                .to_ascii_lowercase();
            assert!(headers.contains("x-auth: host-secret"));
            assert_eq!(
                headers.contains("mcp-session-id: configured-session"),
                step > 0
            );
            let request: Value =
                serde_json::from_slice(&bytes[header_end + 4..header_end + 4 + body_bytes])
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            methods.push(request["method"].as_str().unwrap_or_default().to_owned());
            let result = match step {
                0 => {
                    assert_eq!(request["method"], "initialize");
                    json!({"protocolVersion":crate::mcp::PROTOCOL_VERSION,"capabilities":{"tools":{}},
                        "serverInfo":{"name":"fixture","version":"1"}})
                }
                1 => {
                    assert_eq!(request["method"], "notifications/initialized");
                    assert!(request.get("id").is_none());
                    socket.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await.map_err(Error::process_io)?;
                    socket.shutdown().await.map_err(Error::process_io)?;
                    continue;
                }
                2 | 3 => {
                    assert_eq!(request["method"], "tools/list");
                    if step == 2 {
                        assert!(request["params"].get("cursor").is_none());
                        json!({"tools":[{"name":"read","inputSchema":{"type":"object"}},
                            {"name":"write","inputSchema":{"type":"object"}}],"nextCursor":"next"})
                    } else {
                        assert_eq!(request["params"]["cursor"], "next");
                        json!({"tools":[{"name":"other","inputSchema":{"type":"object"}}]})
                    }
                }
                _ => {
                    assert_eq!(request["method"], "tools/call");
                    assert!(body_bytes > CATALOG_BYTES as usize);
                    assert_eq!(
                        request["params"],
                        json!({"name":"read","arguments":{"query":"q".repeat(QUERY_BYTES)}})
                    );
                    json!({"content":[{"type":"text","text":"found".repeat(512)}]})
                }
            };
            let result = json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string();
            if step == 4 {
                assert!(result.len() > CATALOG_BYTES as usize);
            }
            let (media, body) = if step == 3 {
                ("text/event-stream", format!("data: {result}\n\n"))
            } else {
                ("application/json", result)
            };
            if step == 4 {
                assert!(
                    body.len() > CATALOG_BYTES as usize,
                    "tool output must exceed the catalog bound"
                );
            }
            let session = if step == 0 {
                "Mcp-Session-Id: configured-session\r\n"
            } else {
                ""
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {media}\r\n{session}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .map_err(Error::process_io)?;
            socket.shutdown().await.map_err(Error::process_io)?;
        }
        Ok::<_, Error>(methods)
    });
    let root = tempfile::tempdir().map_err(Error::process_io)?;
    let document = json!({"mcpServers":{"configured":{"type":"http","url":format!("http://{address}/mcp"),
        "headers":{"X-Auth":"host-secret"},"enabled_tools":["read","write"],"disabled_tools":["write"],
        "startup_timeout_sec":1,"tool_timeout_sec":1,"required":true}}});
    std::fs::write(root.path().join(".claude.json"), document.to_string())
        .map_err(Error::process_io)?;
    let project = volume(root.path()).await?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "configured-call".into(),
                "mcp.configured.read".into(),
                json!({"query":"q".repeat(QUERY_BYTES)}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("configured MCP completed".into()),
    ]);
    let harness = LocalHarness::open(
        volume_config(root.path(), project, provider.clone())?
            .settings(
                SettingsConfig::new(root.path(), root.path()).codex_home(root.path().join("codex")),
            )
            .mcp_limits(crate::mcp::McpClientLimits {
                catalog_bytes: CATALOG_BYTES,
                tool_bytes: 8192,
                ..Default::default()
            })
            .tool(tool("test.custom")),
    )
    .await?;
    assert!(harness.mcp_diagnostics().is_empty());
    for name in ["read", "test.custom", "mcp.configured.read"] {
        assert!(harness.tools().get(name, None, None)?.is_some());
    }
    for name in ["mcp.configured.write", "mcp.configured.other"] {
        assert!(harness.tools().get(name, None, None)?.is_none());
    }
    assert_eq!(
        harness.run("use configured MCP").await?.text,
        "configured MCP completed"
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(provider.remaining(), 0);
    assert!(requests.iter().all(|request| {
        request
            .tools
            .iter()
            .any(|tool| tool.name == "mcp.configured.read")
    }));
    let rendered = serde_json::to_string(&requests[1].messages).map_err(super::super::storage)?;
    assert!(rendered.contains(&"found".repeat(512)));
    assert_eq!(
        service
            .await
            .map_err(|error| Error::Invalid(error.to_string()))??,
        [
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/list",
            "tools/call"
        ]
    );
    Ok(())
}
