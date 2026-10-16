use super::*;
use crate::{
    mcp::{McpCatalog, McpDiscoveryPolicy, McpSchemaExposure},
    tool::{JsonToolProjection, ToolInvocation, ToolProjection, ToolRegistry},
};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
use tokio_util::codec::{Decoder as _, Encoder as _};

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

#[tokio::test(start_paused = true)]
async fn deadlines_cancel_each_remote_request_once_and_preserve_the_connection() -> Result<()> {
    let (transport, server) = fixture("timeout").await?;
    for _ in 0..8 {
        let operation = OperationId::new();
        assert!(
            matches!(transport.rpc(operation, "tools/list", json!({}), None).await,
            Err(Error::Indeterminate(id)) if id == operation)
        );
        assert!(!transport.service.is_closed());
    }
    transport
        .rpc(
            OperationId::new(),
            "tools/list",
            json!({"cursor":"recover"}),
            None,
        )
        .await?;
    drop(transport);
    let methods = server.await.map_err(invalid)??;
    assert_eq!(
        methods
            .iter()
            .filter(|method| *method == "notifications/cancelled")
            .count(),
        8
    );
    assert_eq!(
        methods
            .iter()
            .filter(|method| *method == "tools/list")
            .count(),
        9
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn blocked_cancellation_delivery_closes_the_connection_within_the_cleanup_bound() -> Result<()>
{
    let (transport, server) = fixture("stalled-cancel").await?;
    let operation = OperationId::new();
    assert!(
        matches!(transport.rpc(operation, "tools/list", json!({}), None).await,
        Err(Error::Indeterminate(id)) if id == operation)
    );
    assert!(transport.service.is_closed());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn cleanup_runs_off_the_async_runtime_thread() -> Result<()> {
    struct Probe(Option<tokio::sync::oneshot::Sender<std::thread::ThreadId>>);
    impl Drop for Probe {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(std::thread::current().id());
            }
        }
    }
    let caller = std::thread::current().id();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    drop_on_worker(&tokio::runtime::Handle::current(), Probe(Some(sender)));
    assert_ne!(receiver.await.map_err(invalid)?, caller);
    Ok(())
}

#[tokio::test]
async fn cancelling_initialization_reaps_the_owned_child_without_blocking_the_runtime() -> Result<()>
{
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(Error::process_io)?;
    let address = listener.local_addr().map_err(Error::process_io)?;
    let executable = std::env::current_exe().map_err(Error::process_io)?;
    let arguments = format!(
        "--exact mcp::stdio::native::tests::native_child_is_initialized_once_and_kept_until_the_transport_drops --nocapture --skip mcp-child:{address}"
    );
    // The test runner's banner is not an MCP protocol message.
    #[cfg(windows)]
    let command = acyclic_native_runtime::shell_command(
        &format!("call \"{}\" {arguments} >NUL", executable.display()),
        Some("cmd"),
    );
    #[cfg(not(windows))]
    let command = acyclic_native_runtime::shell_command(
        &format!("\"{}\" {arguments} >/dev/null", executable.display()),
        Some("sh"),
    );
    let mut future = Box::pin(StdioMcpTransport::spawn(command, 4096, 5000));
    let (mut stream, _) = tokio::select! {
        accepted = listener.accept() => accepted.map_err(Error::process_io)?,
        _ = &mut future => return Err(invalid("MCP child exited before initialization handshake")),
    };
    assert_eq!(stream.read_u8().await.map_err(Error::process_io)?, 1);
    drop(future);
    let result = tokio::time::timeout(Duration::from_millis(500), stream.read_u8())
        .await
        .map_err(invalid)?;
    assert!(
        matches!(result, Err(error) if matches!(error.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset))
    );
    Ok(())
}

async fn fixture(
    mode: &'static str,
) -> Result<(
    StdioMcpTransport,
    tokio::task::JoinHandle<Result<Vec<String>>>,
)> {
    let (client, server) = tokio::io::duplex(if mode == "stalled-cancel" { 1 } else { 1024 });
    let (read, mut write) = tokio::io::split(server);
    let server = tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(read).lines();
        let mut methods = Vec::new();
        let mut calls = 0;
        let mut pending = None;
        let mut unanswered = None;
        while let Some(line) = lines.next_line().await.map_err(Error::process_io)? {
            let mut request: Value = serde_json::from_str(&line).map_err(invalid)?;
            let method = request
                .get("method")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("fixture method missing"))?
                .to_owned();
            let id = request.get_mut("id").map(Value::take);
            methods.push(method.clone());
            let Some(id) = id else {
                if method == "notifications/cancelled" {
                    assert_eq!(
                        unanswered.take().as_ref(),
                        request
                            .get("params")
                            .and_then(|params| params.get("requestId"))
                    );
                }
                continue;
            };
            if method != "initialize" {
                assert!(
                    unanswered.is_none(),
                    "previous server work was not cancelled"
                );
                if mode == "stalled-cancel" {
                    std::future::pending::<()>().await;
                }
            }
            let result = match method.as_str() {
                "initialize" => json!({
                    "protocolVersion":"2025-11-25", "capabilities":{"tools":{}},
                    "serverInfo":{"name":"fixture", "version":"1"}
                }),
                "tools/list" if mode == "reverse" => {
                    json!({"tools":[],"nextCursor":request["params"]["cursor"]})
                }
                "tools/list" => json!({"tools":[{"name":"echo", "description":"Count calls",
                    "inputSchema":{"type":"object"}}]}),
                "tools/call" => {
                    calls += 1;
                    json!({"content":[{"type":"text","text":calls.to_string()}]})
                }
                _ => return Err(invalid("unexpected fixture method")),
            };
            let recovered = request["params"]["cursor"] == "recover";
            let bytes =
                if method == "initialize" || matches!(mode, "success" | "reverse") || recovered {
                    serde_json::to_vec(&json!({"jsonrpc":"2.0", "id":id, "result":result}))
                        .map_err(invalid)?
                } else {
                    match mode {
                        "oversize" => serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,
                        "result":{"tools":[{"name":"large","description":"x".repeat(8192),
                        "inputSchema":{"type":"object"}}]}}))
                        .map_err(invalid)?,
                        "malformed" => b"not json".to_vec(),
                        "rpc-error" => serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,
                        "error":{"code":-32602,"message":"invalid fixture cursor"}}))
                        .map_err(invalid)?,
                        "wrong-id" => {
                            unanswered = Some(id);
                            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":999,"result":result}))
                                .map_err(invalid)?
                        }
                        "timeout" => {
                            unanswered = Some(id);
                            continue;
                        }
                        "closed" => break,
                        _ => return Err(invalid("unknown fixture mode")),
                    }
                };
            let first = if mode == "reverse" && method != "initialize" {
                if pending.is_none() {
                    pending = Some(bytes);
                    continue;
                }
                pending.take()
            } else {
                None
            };
            for bytes in std::iter::once(bytes).chain(first) {
                if let Err(error) = write.write_all(&bytes).await {
                    if mode == "oversize" {
                        return Ok(methods);
                    }
                    return Err(Error::process_io(error));
                }
                write.write_all(b"\n").await.map_err(Error::process_io)?;
            }
            write.flush().await.map_err(Error::process_io)?;
        }
        Ok(methods)
    });
    let (read, write) = tokio::io::split(client);
    let client =
        StdioMcpTransport::bind(read, write, 4096, Duration::from_millis(50), None).await?;
    Ok((client, server))
}

#[tokio::test(start_paused = true)]
async fn a_correlated_server_error_preserves_its_diagnostic_without_replay() -> Result<()> {
    let (transport, server) = fixture("rpc-error").await?;
    let operation = OperationId::from_bytes([7; 16]);
    assert!(
        matches!(transport.rpc(operation, "tools/list", json!({}), None).await,
        Err(Error::Invalid(message)) if message.contains("invalid fixture cursor"))
    );
    assert!(transport.reconcile(operation, None).await?.is_none());
    drop(transport);
    let methods = server.await.map_err(invalid)??;
    assert_eq!(
        methods,
        ["initialize", "notifications/initialized", "tools/list"]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn concurrent_requests_receive_their_own_out_of_order_responses() -> Result<()> {
    let (transport, server) = fixture("reverse").await?;
    let (first, second) = tokio::join!(
        transport.rpc(
            OperationId::from_bytes([5; 16]),
            "tools/list",
            json!({"cursor":"first"}),
            None
        ),
        transport.rpc(
            OperationId::from_bytes([6; 16]),
            "tools/list",
            json!({"cursor":"second"}),
            None
        ),
    );
    assert_eq!(first?["nextCursor"], "first");
    assert_eq!(second?["nextCursor"], "second");
    drop(transport);
    let methods = server.await.map_err(invalid)??;
    assert_eq!(
        methods,
        [
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/list"
        ]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn one_session_discovers_installs_calls_and_reconciles_without_replay() -> Result<()> {
    let (transport, server) = fixture("success").await?;
    let transport = Arc::new(transport);
    let listed = transport
        .list_tools(OperationId::from_bytes([1; 16]), None)
        .await?;
    let catalog = McpCatalog {
        server: "configured".into(),
        revision: "1".into(),
        schema_exposure: McpSchemaExposure::Eager,
        discovery: McpDiscoveryPolicy::Disabled,
        projection_schema: crate::tool::json_projection_schema(json!({})),
        tools: listed.tools.into_iter().map(Into::into).collect(),
    };
    let remote: Arc<dyn McpToolTransport> = transport.clone();
    let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
    let mut registry = ToolRegistry::default();
    catalog.install(&mut registry, None, &remote, &projection, 8, 16_384)?;
    assert_eq!(registry.definitions(None)?[0].name, "mcp.configured.echo");
    let tool = registry
        .get("mcp.configured.echo", None, None)?
        .ok_or_else(|| invalid("installed tool missing"))?;
    let invocation = ToolInvocation::for_model_call(
        OperationId::from_bytes([2; 16]),
        0,
        "call".into(),
        "mcp.configured.echo".into(),
        json!({}),
        None,
    )?;
    let result = tool.executor.execute(invocation, None).await?;
    assert_eq!(result.value["content"][0]["text"], "1");
    let operation = OperationId::from_bytes([3; 16]);
    let result = transport.call(operation, "echo", json!({}), None).await?;
    assert_eq!(
        serde_json::to_value(result).map_err(invalid)?["content"][0]["text"],
        "2"
    );
    assert!(transport.reconcile(operation, None).await?.is_none());
    assert!(matches!(
        transport
            .rpc(
                operation,
                "tools/call",
                json!({"name":"echo","task":{}}),
                None
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    drop(registry);
    drop(remote);
    drop(transport);
    let methods = server.await.map_err(invalid)??;
    assert_eq!(
        methods,
        [
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call",
            "tools/call"
        ]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn lost_invalid_oversize_and_late_responses_never_redispatch() -> Result<()> {
    for mode in ["closed", "malformed", "oversize", "wrong-id", "timeout"] {
        let (transport, server) = fixture(mode).await?;
        let operation = OperationId::from_bytes([9; 16]);
        assert!(
            matches!(transport.rpc(operation, "tools/list", json!({}), None).await,
            Err(Error::Indeterminate(id)) if id == operation),
            "{mode}"
        );
        assert!(transport.reconcile(operation, None).await?.is_none());
        drop(transport);
        let methods = server.await.map_err(invalid)??;
        assert_eq!(
            methods
                .iter()
                .filter(|method| method.as_str() == "tools/list")
                .count(),
            1,
            "{mode}"
        );
    }
    Ok(())
}

#[test]
fn frame_limits_apply_both_directions_without_retaining_a_partial_request() -> Result<()> {
    let mut codec = BoundedCodec::<Value>::new(crate::sync::Arc::new(AtomicU32::new(16)));
    let mut bytes = BytesMut::from(b"prior\n".as_slice());
    assert!(codec.encode(json!("x".repeat(32)), &mut bytes).is_err());
    assert_eq!(bytes.as_ref(), b"prior\n");
    bytes.clear();
    codec
        .encode(json!("x".repeat(14)), &mut bytes)
        .map_err(Error::process_io)?;
    assert_eq!(bytes.len(), 17);
    assert_eq!(
        codec.decode(&mut bytes).map_err(Error::process_io)?,
        Some(json!("x".repeat(14)))
    );
    bytes.extend_from_slice(&[b'x'; 17]);
    assert!(codec.decode(&mut bytes).is_err());
    Ok(())
}

// Property: arbitrary admitted JSON survives every generated chunking and a bound
// transition; rejected encoding never changes an already buffered prefix.
proptest::proptest! {
    #[test]
    fn codec_round_trips_chunked_json_and_rejects_atomically(
        text in ".{0,512}", number in proptest::prelude::any::<i64>(), chunk_size in 1usize..64,
    ) {
        let value = json!({"text":text,"number":number});
        let size = serde_json::to_vec(&value).unwrap().len() as u32;
        let bound = crate::sync::Arc::new(AtomicU32::new(1));
        let mut codec = BoundedCodec::<Value>::new(crate::sync::Arc::clone(&bound));
        bound.store(size, Ordering::Release);
        let mut encoded = BytesMut::new();
        codec.encode(value.clone(), &mut encoded).unwrap();
        let mut pending = BytesMut::new();
        let mut decoded = None;
        for chunk in encoded.chunks(chunk_size) {
            pending.extend_from_slice(chunk);
            if let Some(message) = codec.decode(&mut pending).unwrap() {
                proptest::prop_assert!(decoded.is_none());
                decoded = Some(message);
            }
        }
        proptest::prop_assert_eq!(decoded, Some(value.clone()));
        bound.store(size - 1, Ordering::Release);
        let prefix = BytesMut::from(b"prior\n".as_slice());
        let mut rejected = prefix.clone();
        proptest::prop_assert!(codec.encode(value, &mut rejected).is_err());
        proptest::prop_assert_eq!(rejected, prefix);
    }
}

#[tokio::test]
async fn native_child_is_initialized_once_and_kept_until_the_transport_drops() -> Result<()> {
    if let Some(address) =
        std::env::args().find_map(|arg| arg.strip_prefix("mcp-child:").map(str::to_owned))
    {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(address).map_err(Error::process_io)?;
        stream.write_all(&[1]).map_err(Error::process_io)?;
        stream.read_exact(&mut [0]).map_err(Error::process_io)?;
        return Err(invalid("MCP fixture was released instead of terminated"));
    }
    let directory = tempfile::tempdir().map_err(Error::process_io)?;
    let initialize = r#"{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}"#;
    let tools = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
    #[cfg(windows)]
    let (filename, script) = (
        "server.cmd",
        format!(
            "@echo off\nset /p input=\necho {initialize}\nset /p input=\nset /p input=\necho {tools}\nset /p input=\n"
        ),
    );
    #[cfg(not(windows))]
    let (filename, script) = (
        "server.sh",
        format!(
            "IFS= read -r input\nprintf '%s\\n' '{initialize}'\nIFS= read -r input\nIFS= read -r input\nprintf '%s\\n' '{tools}'\nIFS= read -r input\n"
        ),
    );
    let path = directory.path().join(filename);
    std::fs::write(&path, script).map_err(Error::process_io)?;
    #[cfg(windows)]
    let command =
        acyclic_native_runtime::shell_command(&format!("call \"{}\"", path.display()), Some("cmd"));
    #[cfg(not(windows))]
    let command =
        acyclic_native_runtime::shell_command(&format!(". \"{}\"", path.display()), Some("sh"));
    let transport = StdioMcpTransport::spawn(command, 4096, 5_000).await?;
    let tools = transport
        .rpc(
            OperationId::from_bytes([4; 16]),
            "tools/list",
            json!({}),
            None,
        )
        .await?;
    assert_eq!(tools["tools"], json!([]));
    assert!(!transport.service.is_closed());
    drop(transport);
    Ok(())
}

#[tokio::test]
async fn invalid_bounds_and_spawn_failures_are_explicit() -> Result<()> {
    for (bytes, timeout) in [(0, 1), (1, 0)] {
        assert!(matches!(
            StdioMcpTransport::spawn(Command::new("not-used"), bytes, timeout).await,
            Err(Error::Invalid(_))
        ));
    }
    let directory = tempfile::tempdir().map_err(Error::process_io)?;
    let command = Command::new(directory.path().join("nonexistent-mcp-server"));
    assert!(matches!(
        StdioMcpTransport::spawn(command, 4096, 5_000).await,
        Err(Error::Process {
            kind: "not_found",
            ..
        })
    ));
    Ok(())
}
