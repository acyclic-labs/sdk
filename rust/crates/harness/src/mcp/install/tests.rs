use super::*;
use crate::{
    OperationId,
    conversation::Limits,
    mcp::{
        McpToolTransport,
        config::{Dialect, ImportedServer},
    },
    runtime::{RuntimeScope, ToolContext},
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
pub(super) struct Remote(AtomicUsize);

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

pub(super) fn server(value: Value, dialect: Dialect) -> Result<McpServer> {
    let imported: ImportedServer =
        serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut config = McpConfig::default();
    config.insert("server".into(), imported, dialect)?;
    config
        .servers
        .remove("server")
        .ok_or_else(|| Error::Invalid("server disabled".into()))
}

pub(super) fn advertised(names: &[&str]) -> Vec<McpToolDefinition> {
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
                    binding: [1; 32],
                })
            })
        })
        .await?;
    assert!(installed.diagnostics.is_empty());
    assert_eq!(
        installed.capabilities.iter().collect::<Vec<_>>(),
        ["mcp:call:configured", "tool:call:mcp.configured.read"]
    );
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
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "call".into(),
        name: "mcp.configured.read".into(),
        arguments: json!({"x":1}),
    };
    let scope = RuntimeScope::new(installed.capabilities, Limits::default())?;
    tool.executor
        .authorize(Some(&scope), None, &invocation, None)?;
    let denied = RuntimeScope::new(
        Capabilities::new(["tool:call:mcp.configured.read"]),
        Limits::default(),
    )?;
    assert!(matches!(
        tool.executor
            .authorize(Some(&denied), None, &invocation, None),
        Err(Error::Unauthorized(_))
    ));
    let result = tool.executor.execute(invocation, None).await?;
    assert_eq!(result.value["content"][0]["text"], "{\"x\":1}");
    assert_eq!(remote.0.load(Ordering::SeqCst), 1);
    assert!(installed.tools.clone().extend(installed.tools).is_err());
    Ok(())
}

#[tokio::test]
async fn reinstall_restores_saved_tool_revisions_and_approval_digests() -> Result<()> {
    async fn install(
        binding: [u8; 32],
        reverse: bool,
        changed_schema: bool,
    ) -> Result<McpInstallation> {
        let config = McpConfig {
            servers: BTreeMap::from([(
                "configured".into(),
                server(json!({"command":"server"}), Dialect::Codex)?,
            )]),
        };
        config.install_with(ToolRegistry::default(), McpClientLimits::default(), move |_| {
            Box::pin(async move {
                let mut tools = advertised(&["read", "write"]);
                if changed_schema {
                    tools[0].input_schema = json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"}}});
                }
                if reverse {
                    tools.reverse();
                }
                Ok(Connected { transport: Arc::new(Remote::default()), tools, binding })
            })
        }).await
    }
    let first = install([1; 32], false, false).await?;
    let saved = first
        .tools
        .get("mcp.configured.read", None, None)?
        .expect("initial tool")
        .definition
        .clone();
    let pending_approval = saved.digest()?;
    drop(first);
    let reopened = install([1; 32], true, false).await?;
    let restored = reopened
        .tools
        .get(&saved.name, Some(&saved.revision), None)?
        .expect("saved revision after restart");
    assert_eq!(restored.definition, saved);
    assert_eq!(restored.definition.digest()?, pending_approval);
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "approved-call".into(),
        name: saved.name.clone(),
        arguments: json!({}),
    };
    let scope = RuntimeScope::new(reopened.capabilities, Limits::default())?;
    restored
        .executor
        .authorize(Some(&scope), None, &invocation, None)?;
    restored.executor.execute(invocation, None).await?;
    for changed in [
        install([2; 32], false, false).await?,
        install([1; 32], false, true).await?,
    ] {
        assert!(
            changed
                .tools
                .get(&saved.name, Some(&saved.revision), None)?
                .is_none()
        );
        assert_ne!(
            changed
                .tools
                .get(&saved.name, None, None)?
                .expect("changed tool")
                .definition
                .digest()?,
            pending_approval
        );
    }
    Ok(())
}

#[tokio::test]
async fn registration_uses_the_remaining_startup_deadline_off_the_runtime_thread() -> Result<()> {
    for _ in 0..8 {
        let caller = std::thread::current().id();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let (started, worker) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let active = Arc::new(AtomicUsize::new(0));
        let worker_active = active.clone();
        let connection = Arc::new(Remote::default());
        let worker_connection = connection.clone();
        let task = tokio::spawn(register_before(deadline, move |check| {
            let _connection = worker_connection;
            worker_active.fetch_add(1, Ordering::SeqCst);
            let _ = started.send(std::thread::current().id());
            let result = wait
                .recv()
                .map_err(|error| Error::Invalid(error.to_string()));
            worker_active.fetch_sub(1, Ordering::SeqCst);
            check()?;
            result
        }));
        assert_ne!(
            worker
                .await
                .map_err(|error| Error::Invalid(error.to_string()))?,
            caller
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "timeout must await worker retirement");
        release
            .send(())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(matches!(
            task.await
                .map_err(|error| Error::Invalid(error.to_string()))?,
            Err(Error::Process {
                kind: "timeout",
                ..
            })
        ));
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&connection), 1);
        tokio::time::resume();
    }
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
    assert!(result.capabilities.iter().next().is_none());
    Ok(())
}

#[tokio::test]
async fn no_selected_tools_grant_no_authority_and_invalid_limits_never_connect() -> Result<()> {
    let configuration = || {
        Ok::<_, Error>(McpConfig {
            servers: BTreeMap::from([(
                "configured".into(),
                server(
                    json!({"command":"server", "disabled_tools":["read"]}),
                    Dialect::Codex,
                )?,
            )]),
        })
    };
    let installed = configuration()?
        .install_with(ToolRegistry::default(), McpClientLimits::default(), |_| {
            Box::pin(async {
                Ok(Connected {
                    transport: Arc::new(Remote::default()),
                    tools: advertised(&["read"]),
                    binding: [1; 32],
                })
            })
        })
        .await?;
    assert!(installed.tools.definitions(None)?.is_empty());
    assert!(installed.capabilities.iter().next().is_none());
    assert!(installed.diagnostics.is_empty());
    for field in 0..5 {
        let mut limits = McpClientLimits::default();
        match field {
            0 => limits.servers = 0,
            1 => limits.tools_per_server = 0,
            2 => limits.pages_per_server = 0,
            3 => limits.catalog_bytes = 0,
            _ => limits.tool_bytes = 0,
        }
        assert!(matches!(
            configuration()?
                .install_with(ToolRegistry::default(), limits, |_| panic!(
                    "connected before admission"
                ))
                .await,
            Err(Error::Invalid(_))
        ));
    }
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
    let executable = std::env::current_exe().map_err(Error::process_io)?;
    let environment = |name: &str| match name {
        "PROGRAM" => Some(executable.as_os_str().to_owned()),
        "TOKEN" => Some("secret".into()),
        _ => None,
    };
    let claude = server(
        json!({"command":"${PROGRAM}", "args":["with spaces", "${MISSING:-fallback}"],
        "env":{"TOKEN":"${TOKEN}"}, "cwd":"child"}),
        Dialect::Claude,
    )?;
    let command = native::command_for(&claude, &workspace, environment)?;
    assert_eq!(command.get_program(), executable.as_os_str());
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
        json!({"command":executable, "args":["${PROGRAM}"], "env_vars":["TOKEN"], "env":{"TOKEN":"literal"}}),
        Dialect::Codex,
    )?;
    let command = native::command_for(&codex, &workspace, environment)?;
    assert_eq!(command.get_program(), executable.as_os_str());
    assert_eq!(command.get_args().collect::<Vec<_>>(), ["${PROGRAM}"]);
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

#[cfg(feature = "mcp-stdio")]
#[test]
fn resolved_stdio_binding_tracks_arguments_environment_directory_and_dialect() -> Result<()> {
    let root = std::env::current_dir().map_err(Error::process_io)?;
    let executable = std::env::current_exe().map_err(Error::process_io)?;
    let alternate = tempfile::tempdir_in(executable.parent().expect("executable directory"))
        .map_err(Error::process_io)?;
    let other = alternate
        .path()
        .join(executable.file_name().expect("test executable"));
    std::fs::hard_link(&executable, &other).map_err(Error::process_io)?;
    let executable = executable.to_str().expect("test path UTF-8");
    let other = other.to_str().expect("alternate path UTF-8");
    let config = server(
        json!({"command":"${PROGRAM}", "args":["${ARGUMENT}"], "env":{"KEY":"${TOKEN}"}}),
        Dialect::Claude,
    )?;
    let command = |program: &str, argument: &str, token: &str, directory: &Path| {
        native::command_for(&config, directory, |name| match name {
            "PROGRAM" => Some(program.into()),
            "ARGUMENT" => Some(argument.into()),
            "TOKEN" => Some(token.into()),
            _ => None,
        })
    };
    let original = command(executable, "argument", "secret", &root)?;
    let binding = native::command_binding(&original, &config);
    assert_eq!(
        binding,
        native::command_binding(&command(executable, "argument", "secret", &root)?, &config)
    );
    for changed in [
        command(other, "argument", "secret", &root)?,
        command(executable, "different", "secret", &root)?,
        command(executable, "argument", "new-secret", &root)?,
        command(executable, "argument", "secret", &root.join("child"))?,
    ] {
        assert_ne!(binding, native::command_binding(&changed, &config));
    }
    let mut codex = config;
    codex.dialect = Dialect::Codex;
    assert_ne!(binding, native::command_binding(&original, &codex));
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
#[test]
fn http_binding_tracks_resolved_endpoint_and_credentials_but_not_header_order() -> Result<()> {
    use reqwest::header::{HeaderMap, HeaderValue};
    let mut first = HeaderMap::new();
    first.insert("x-first", HeaderValue::from_static("one"));
    first.insert("x-second", HeaderValue::from_static("secret"));
    let mut reordered = HeaderMap::new();
    reordered.insert("x-second", HeaderValue::from_static("secret"));
    reordered.insert("x-first", HeaderValue::from_static("one"));
    let binding = native::http_binding("https://server/mcp", &first);
    assert_eq!(
        binding,
        native::http_binding("https://server/mcp", &reordered)
    );
    assert_ne!(binding, native::http_binding("https://other/mcp", &first));
    reordered.insert("x-second", HeaderValue::from_static("new-secret"));
    assert_ne!(
        binding,
        native::http_binding("https://server/mcp", &reordered)
    );
    Ok(())
}

#[cfg(all(unix, feature = "mcp-stdio"))]
#[tokio::test]
async fn configured_stdio_server_inherits_its_workspace_and_retains_one_session() -> Result<()> {
    use crate::native_settings::{NativeSettings, SettingsConfig};
    let root = tempfile::tempdir().map_err(Error::process_io)?;
    let cwd = root.path().join("child");
    std::fs::create_dir(&cwd).map_err(Error::process_io)?;
    let script = root.path().join("server.sh");
    std::fs::write(
        &script,
        r#"printf '%s\n' "$MCP_TEST_VALUE" "$CLAUDE_PROJECT_DIR" > environment
IFS= read -r input
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}'
IFS= read -r input
IFS= read -r input
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}}'
IFS= read -r input
printf '%s\n' "$input" > invocation
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"configured stdio"}]}}'
IFS= read -r input
"#.replace("configured stdio", &"x".repeat(2048)),
    )
    .map_err(Error::process_io)?;
    let document = json!({"mcpServers":{"configured":{"command":"sh","args":[script],
        "env":{"MCP_TEST_VALUE":"literal ' $ spaces"},"cwd":"child","required":true,
        "startup_timeout_sec":1,"tool_timeout_sec":1}}});
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
            McpClientLimits {
                catalog_bytes: 1024,
                tool_bytes: 8192,
                ..Default::default()
            },
        )
        .await?;
    assert!(installed.diagnostics.is_empty());
    assert_eq!(
        std::fs::read_to_string(cwd.join("environment")).map_err(Error::process_io)?,
        format!("literal ' $ spaces\n{}\n", root.path().display())
    );
    let tool = installed
        .tools
        .get("mcp.configured.echo", None, None)?
        .expect("installed stdio tool");
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "configured-call".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"payload":"y".repeat(2048)}),
    };
    let scope = RuntimeScope::new(installed.capabilities, Limits::default())?;
    tool.executor
        .authorize(Some(&scope), None, &invocation, None)?;
    let result = tool.executor.execute(invocation, None).await?;
    assert_eq!(result.value["content"][0]["text"], "x".repeat(2048));
    let dispatched: Value =
        serde_json::from_slice(&std::fs::read(cwd.join("invocation")).map_err(Error::process_io)?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        dispatched["params"]["arguments"]["payload"],
        "y".repeat(2048)
    );
    Ok(())
}
