use super::*;
use crate::{
    OperationId,
    mcp::{
        McpToolTransport,
        config::{Dialect, ImportedServer},
    },
    runtime::ToolContext,
    tool::{Tool, ToolDefinition, ToolInvocation},
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Remote(AtomicUsize);

impl McpToolTransport for Remote {
    fn call<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        arguments: Value,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<rmcp::model::CallToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(rmcp::model::CallToolResult::success(vec![
                rmcp::model::ContentBlock::text(arguments.to_string()),
            ]))
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<rmcp::model::CallToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn server(value: Value, dialect: Dialect) -> Result<McpServer> {
    let imported: ImportedServer =
        serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut config = McpConfig::default();
    config.insert("server".into(), imported, dialect)?;
    config
        .servers
        .remove("server")
        .ok_or_else(|| Error::Invalid("server disabled".into()))
}

fn advertised(names: &[&str]) -> Vec<McpToolDefinition> {
    names
        .iter()
        .map(|name| McpToolDefinition {
            name: (*name).into(),
            title: None,
            description: String::new(),
            input_schema: json!({"type":"object"}),
            output_schema: None,
        })
        .collect()
}

#[tokio::test]
async fn installation_is_additive_filtered_and_uses_the_retained_connection() -> Result<()> {
    let remote = Arc::new(Remote::default());
    let mut tools = ToolRegistry::default();
    let payload = "x".repeat(131_072);
    let pointer = payload.as_ptr();
    tools.register(
        (
            Tool {
                hook_names: &["Bash"],
                definition: ToolDefinition {
                    name: "bash".into(),
                    revision: "1".into(),
                    description: payload,
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({}),
                    projection_schema: crate::tool::json_projection_schema(json!({})),
                },
                executor: Arc::new(crate::mcp::remote::RemoteTool {
                    server: "existing".into(),
                    name: "bash".into(),
                    output_schema: None,
                    transport: remote.clone(),
                }),
                projection: Arc::new(JsonToolProjection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let config = McpConfig {
        servers: BTreeMap::from([(
            "configured".into(),
            server(
                json!({
                    "command":"server", "enabled_tools":["read", "write"], "disabled_tools":["write"]
                }),
                Dialect::Codex,
            )?,
        )]),
    };
    let connection = remote.clone();
    let installed = config
        .install_with(tools, McpClientLimits::default(), move |_| {
            let transport = connection.clone();
            Box::pin(async move {
                Ok(Connected {
                    transport,
                    tools: advertised(&["read", "write", "other"]),
                })
            })
        })
        .await?;
    assert!(installed.diagnostics.is_empty());
    assert_eq!(
        installed
            .tools
            .definitions(None)?
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["bash", "mcp.configured.read"]
    );
    let original = installed
        .tools
        .get("bash", None, None)?
        .expect("existing tool");
    assert_eq!(original.definition.description.as_ptr(), pointer);
    assert_eq!(original.hook_names, &["Bash"]);
    let tool = installed
        .tools
        .get("mcp.configured.read", None, None)?
        .expect("MCP tool");
    let result = tool
        .executor
        .execute(
            ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "call".into(),
                name: "mcp.configured.read".into(),
                arguments: json!({"x":1}),
            },
            None,
        )
        .await?;
    assert_eq!(result.value["content"][0]["text"], "{\"x\":1}");
    assert_eq!(remote.0.load(Ordering::SeqCst), 1);
    assert!(installed.tools.clone().extend(installed.tools).is_err());
    Ok(())
}

#[tokio::test]
async fn initialization_is_concurrent_and_optional_diagnostics_keep_config_order() -> Result<()> {
    let mut config = McpConfig::default();
    for name in ["a", "b", "c"] {
        config.servers.insert(
            name.into(),
            server(json!({"command":name}), Dialect::Codex)?,
        );
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let result = config
        .install_with(
            ToolRegistry::default(),
            McpClientLimits::default(),
            move |_| {
                let barrier = barrier.clone();
                Box::pin(async move {
                    barrier.wait().await;
                    Err(Error::Invalid("startup rejected".into()))
                })
            },
        )
        .await?;
    assert_eq!(
        result
            .diagnostics
            .iter()
            .map(|failure| failure.server.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert!(result.tools.definitions(None)?.is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn startup_deadlines_and_required_failures_are_bounded_without_retries() -> Result<()> {
    for required in [false, true] {
        let config = McpConfig {
            servers: BTreeMap::from([(
                "hung".into(),
                server(
                    json!({
                        "command":"server", "required":required, "startup_timeout_sec":0.001
                    }),
                    Dialect::Codex,
                )?,
            )]),
        };
        let calls = AtomicUsize::new(0);
        let result = config
            .install_with(ToolRegistry::default(), McpClientLimits::default(), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::pending())
            })
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        match result {
            Ok(installed) => {
                assert!(!required);
                assert_eq!(installed.diagnostics[0].server, "hung");
            }
            Err(Error::Process {
                kind: "timeout", ..
            }) => assert!(required),
            _ => panic!("unexpected startup decision"),
        }
    }
    Ok(())
}

fn page(names: &[&str], cursor: Option<&str>) -> rmcp::model::ListToolsResult {
    let mut page = rmcp::model::ListToolsResult::default();
    page.tools = names
        .iter()
        .map(|name| {
            let mut tool = rmcp::model::Tool::default();
            tool.name = (*name).to_owned().into();
            tool.input_schema =
                Arc::new(serde_json::from_value(json!({"type":"object"})).expect("schema"));
            tool
        })
        .collect();
    page.next_cursor = cursor.map(str::to_owned);
    page
}

#[tokio::test]
async fn discovery_moves_schemas_and_rejects_cursor_count_and_byte_overruns() -> Result<()> {
    let first = page(&["one"], Some("next"));
    let schema_pointer = &first.tools[0].input_schema["type"] as *const Value;
    let pages = Mutex::new(VecDeque::from([first, page(&["two"], None)]));
    let mut requests = Vec::new();
    let cursors = Mutex::new(&mut requests);
    let tools = native::discover(McpClientLimits::default(), |cursor| {
        cursors.lock().expect("cursor lock").push(cursor);
        std::future::ready(Ok(pages
            .lock()
            .expect("pages lock")
            .pop_front()
            .expect("next page")))
    })
    .await?;
    assert_eq!(
        &tools[0].input_schema["type"] as *const Value,
        schema_pointer
    );
    assert_eq!(requests, [None, Some("next".into())]);
    for (pages, limits) in [
        (
            vec![page(&["a"], Some("x")), page(&["b"], Some("x"))],
            McpClientLimits::default(),
        ),
        (vec![page(&[], Some(""))], McpClientLimits::default()),
        (
            vec![page(&[], Some("x"))],
            McpClientLimits {
                pages_per_server: 1,
                ..Default::default()
            },
        ),
        (
            vec![page(&["a", "b"], None)],
            McpClientLimits {
                tools_per_server: 1,
                ..Default::default()
            },
        ),
        (
            vec![page(&["a"], None)],
            McpClientLimits {
                catalog_bytes: 1,
                ..Default::default()
            },
        ),
    ] {
        let pages = Mutex::new(VecDeque::from(pages));
        assert!(
            native::discover(limits, |_| std::future::ready(Ok(pages
                .lock()
                .expect("pages lock")
                .pop_front()
                .expect("next page"))))
            .await
            .is_err()
        );
    }
    Ok(())
}

#[cfg(feature = "mcp-stdio")]
#[test]
fn stdio_configuration_preserves_argv_scope_environment_and_project_root() -> Result<()> {
    let workspace = std::env::current_dir().map_err(Error::process_io)?;
    let environment = |name: &str| match name {
        "PROGRAM" => Some("server".into()),
        "TOKEN" => Some("secret".into()),
        _ => None,
    };
    let claude = server(
        json!({"command":"${PROGRAM}", "args":["with spaces", "${MISSING:-fallback}"],
        "env":{"TOKEN":"${TOKEN}"}, "cwd":"child"}),
        Dialect::Claude,
    )?;
    let command = native::command_for(&claude, &workspace, environment)?;
    assert_eq!(command.get_program(), "server");
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        ["with spaces", "fallback"]
    );
    assert_eq!(
        command.get_current_dir(),
        Some(workspace.join("child").as_path())
    );
    let env = command.get_envs().collect::<BTreeMap<_, _>>();
    assert_eq!(
        env.get(std::ffi::OsStr::new("TOKEN")).copied().flatten(),
        Some(std::ffi::OsStr::new("secret"))
    );
    assert_eq!(
        env.get(std::ffi::OsStr::new("CLAUDE_PROJECT_DIR"))
            .copied()
            .flatten(),
        Some(workspace.as_os_str())
    );
    let codex = server(
        json!({"command":"${PROGRAM}", "env_vars":["TOKEN"], "env":{"TOKEN":"literal"}}),
        Dialect::Codex,
    )?;
    let command = native::command_for(&codex, &workspace, environment)?;
    assert_eq!(command.get_program(), "${PROGRAM}");
    assert_eq!(
        command
            .get_envs()
            .find(|(key, _)| *key == "TOKEN")
            .and_then(|(_, value)| value),
        Some(std::ffi::OsStr::new("literal"))
    );
    assert!(native::command_for(&claude, &workspace, |_| None).is_err());
    Ok(())
}

#[cfg(feature = "mcp-http")]
#[test]
fn http_credentials_resolve_without_serializing_or_disclosing_values() -> Result<()> {
    let environment = |name: &str| (name == "TOKEN").then(|| "secret".into());
    let headers = native::headers_for(
        &BTreeMap::from([("X-Static".into(), "${MISSING:-fallback}".into())]),
        &BTreeMap::from([("X-Env".into(), "TOKEN".into())]),
        Some("TOKEN"),
        true,
        environment,
    )?;
    assert_eq!(headers["x-static"], "fallback");
    assert_eq!(headers["x-env"], "secret");
    assert_eq!(headers["authorization"], "Bearer secret");
    assert!(headers["authorization"].is_sensitive());
    assert!(headers["x-env"].is_sensitive());
    assert!(headers["x-static"].is_sensitive());
    for (name, value, bearer) in [
        ("Bad Header", "value", None),
        ("X-Key", "secret\nvalue", None),
        ("Authorization", "secret", Some("TOKEN")),
    ] {
        let error = native::headers_for(
            &BTreeMap::from([(name.into(), value.into())]),
            &BTreeMap::new(),
            bearer,
            false,
            environment,
        )
        .err()
        .expect("invalid headers");
        assert!(!error.to_string().contains("secret"));
    }
    assert!(
        native::headers_for(
            &BTreeMap::new(),
            &BTreeMap::new(),
            Some("MISSING"),
            false,
            environment
        )
        .is_err()
    );
    Ok(())
}

#[cfg(feature = "mcp-http")]
#[tokio::test]
async fn configured_http_server_initializes_pages_installs_and_calls_selected_tools() -> Result<()>
{
    use crate::native_settings::{NativeSettings, SettingsConfig};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
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
                    assert_eq!(
                        request["params"],
                        json!({"name":"read","arguments":{"query":"needle"}})
                    );
                    json!({"content":[{"type":"text","text":"found"}]})
                }
            };
            let result = json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string();
            let (media, body) = if step == 3 {
                ("text/event-stream", format!("data: {result}\n\n"))
            } else {
                ("application/json", result)
            };
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
    let settings = NativeSettings::load(
        SettingsConfig::new(root.path(), root.path()).codex_home(root.path().join("codex")),
    )
    .await?;
    let installed = settings
        .mcp
        .install(
            ToolRegistry::default(),
            root.path(),
            McpClientLimits::default(),
        )
        .await?;
    assert!(installed.diagnostics.is_empty());
    assert_eq!(
        installed
            .tools
            .definitions(None)?
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["mcp.configured.read"]
    );
    let tool = installed
        .tools
        .get("mcp.configured.read", None, None)?
        .expect("installed MCP tool");
    let result = tool
        .executor
        .execute(
            ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "call".into(),
                name: "mcp.configured.read".into(),
                arguments: json!({"query":"needle"}),
            },
            None,
        )
        .await?;
    assert_eq!(result.value["content"][0]["text"], "found");
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
