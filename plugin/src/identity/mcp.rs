//! Model-visible identity tools over newline-delimited stdio JSON-RPC.

use super::*;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

const PROTOCOL: &str = "2025-11-25";

struct Server {
    identity: Identity,
    credentials: PathBuf,
    pending: Option<(DeviceCode, tokio::time::Instant)>,
    initialized: bool,
    login_cancelled: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl Server {
    fn new(options: Options) -> Result<Self, Error> {
        Ok(Self {
            identity: Identity::new(&options.endpoint)?,
            credentials: options.credentials,
            pending: None,
            initialized: false,
            login_cancelled: None,
        })
    }

    async fn request(&mut self, request: &Value) -> Option<Value> {
        let id = request.get("id").cloned();
        let method = request.get("method").and_then(Value::as_str);
        if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
            return Some(rpc_error(
                &id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request",
            ));
        }
        let id = id?;
        if !id.is_string() && !id.is_number() {
            return Some(rpc_error(&Value::Null, -32600, "Invalid request ID"));
        }
        let result = match method {
            Some("initialize") => {
                if request
                    .pointer("/params/protocolVersion")
                    .and_then(Value::as_str)
                    .is_none()
                {
                    return Some(rpc_error(&id, -32602, "Missing protocol version"));
                }
                self.initialized = true;
                json!({"protocolVersion": PROTOCOL, "capabilities": {"tools": {}},
                    "serverInfo": {"name": "acyclic-identity", "version": env!("CARGO_PKG_VERSION")}})
            }
            Some("ping") => json!({}),
            _ if !self.initialized => return Some(rpc_error(&id, -32000, "Initialize first")),
            Some("tools/list") => json!({"tools": tools()}),
            Some("tools/call") => {
                let name = request
                    .pointer("/params/name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !tools()
                    .iter()
                    .any(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
                {
                    return Some(rpc_error(&id, -32602, "Unknown identity tool"));
                }
                if request
                    .pointer("/params/arguments")
                    .is_some_and(|arguments| {
                        arguments
                            .as_object()
                            .is_none_or(|object| !object.is_empty())
                    })
                {
                    return Some(rpc_error(&id, -32602, "Identity tools take no arguments"));
                }
                let result = self.call(name).await;
                let (value, is_error) = match result {
                    Ok(value) => (value, false),
                    Err(error) => (error.json(), true),
                };
                let text = value.to_string();
                let structured = if value.is_object() {
                    value
                } else {
                    json!({"result": value})
                };
                json!({"content": [{"type": "text", "text": text}],
                    "structuredContent": structured, "isError": is_error})
            }
            _ => return Some(rpc_error(&id, -32601, "Method not found")),
        };
        Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
    }

    async fn call(&mut self, name: &str) -> Result<Value, Error> {
        if name == "identity_login" {
            let store = Store::open(&self.credentials)?;
            let device = self.identity.device_code("mcp", &store).await?;
            let result = json!({"user_code": device.user_code,
                "verification_uri": device.verification_uri,
                "verification_uri_complete": device.verification_uri_complete,
                "expires_in": device.expires_in});
            self.pending = Some((device, tokio::time::Instant::now()));
            return Ok(result);
        }
        let store = Store::open(&self.credentials)?;
        match name {
            "identity_login_complete" => {
                let cancellation = self.login_cancelled.take();
                let cancelled = async move {
                    match cancellation {
                        Some(receiver) => {
                            let _ = receiver.await;
                        }
                        None => std::future::pending().await,
                    }
                };
                let (device, started) = self
                    .pending
                    .as_mut()
                    .ok_or_else(|| Error::new("login_required", "call identity_login first"))?;
                let session = match self.identity.login(device, *started, cancelled).await {
                    Ok(session) => session,
                    Err(error) => {
                        if !error.status.is_some_and(|status| status.is_server_error())
                            && !matches!(
                                error.code.as_str(),
                                "request_cancelled"
                                    | "transport_error"
                                    | "server_error"
                                    | "temporarily_unavailable"
                                    | "unavailable"
                            )
                        {
                            self.pending = None;
                        }
                        return Err(error);
                    }
                };
                let session = store.write(&self.identity.endpoint, session);
                self.pending = None;
                let session = session?;
                Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
            }
            "identity_whoami" => self.identity.get(&store, "/v1/identity/session").await,
            "identity_orgs" => self.identity.get(&store, "/v1/identity/orgs").await,
            "identity_refresh" => {
                let session = store.read(&self.identity.endpoint)?;
                let session = self.identity.refresh(&store, &session).await?;
                Ok(json!({"authenticated": true, "expires_at": session.expires_at}))
            }
            _ => Err(Error::new("unknown_tool", "unknown identity tool")),
        }
    }
}

fn rpc_error(id: &Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn tools() -> Vec<Value> {
    [
        ("identity_login", "Start device login. Show the returned URL and code to the user, then call identity_login_complete."),
        ("identity_login_complete", "Wait for device approval and save the session started by identity_login."),
        ("identity_whoami", "Show the current signed-in identity; refresh expired access automatically."),
        ("identity_orgs", "List organizations visible to the signed-in identity."),
        ("identity_refresh", "Rotate the stored session refresh token."),
    ].iter().map(|(name, description)| json!({"name": name, "description": description,
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false},
        "annotations": {"readOnlyHint": false, "destructiveHint": false,
            "idempotentHint": false, "openWorldHint": true}})).collect()
}

pub(super) async fn serve(arguments: &[String]) -> Result<(), io::Error> {
    let options = Options::parse(arguments).map_err(io::Error::other)?;
    let server = Server::new(options).map_err(io::Error::other)?;
    transport(server, tokio::io::stdin(), tokio::io::stdout()).await
}

async fn read_frame(
    input: &mut (impl tokio::io::AsyncBufRead + Unpin),
    line: &mut Vec<u8>,
) -> io::Result<Option<Vec<u8>>> {
    // Keep partial bytes across select cancellation and bound each frame before parsing.
    let count = tokio::io::AsyncReadExt::take(input, (1024 * 1024 + 1 - line.len()) as u64)
        .read_until(b'\n', line)
        .await?;
    if line.len() > 1024 * 1024 {
        return Err(io::Error::other("MCP frame exceeds 1 MiB"));
    }
    if count == 0 && line.is_empty() {
        return Ok(None);
    }
    Ok(Some(std::mem::take(line)))
}

async fn send_response(
    output: &mut (impl tokio::io::AsyncWrite + Unpin),
    response: &Value,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(response)?;
    bytes.push(b'\n');
    output.write_all(&bytes).await?;
    output.flush().await
}

async fn transport(
    mut server: Server,
    input: impl tokio::io::AsyncRead + Unpin,
    mut output: impl tokio::io::AsyncWrite + Unpin,
) -> Result<(), io::Error> {
    let mut input = tokio::io::BufReader::new(input);
    let mut line = Vec::new();
    while let Some(frame) = read_frame(&mut input, &mut line).await? {
        let Ok(request) = serde_json::from_slice::<Value>(&frame) else {
            send_response(&mut output, &rpc_error(&Value::Null, -32700, "Parse error")).await?;
            continue;
        };
        let login_completion = request.pointer("/params/name").and_then(Value::as_str)
            == Some("identity_login_complete")
            && request.get("method").and_then(Value::as_str) == Some("tools/call");
        let finish_on_cancel = login_completion
            || (request.get("method").and_then(Value::as_str) == Some("tools/call")
                && matches!(
                    request.pointer("/params/name").and_then(Value::as_str),
                    Some("identity_refresh" | "identity_whoami" | "identity_orgs")
                ));
        let mut cancellation = if login_completion {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            server.login_cancelled = Some(receiver);
            Some(sender)
        } else {
            None
        };
        let mut cancelled = false;
        let operation = server.request(&request);
        tokio::pin!(operation);
        let mut completed = false;
        let outcome: io::Result<bool> = async {
        loop {
            tokio::select! {
                biased;
                response = &mut operation => {
                    completed = true;
                    if !cancelled && let Some(response) = response {
                        send_response(&mut output, &response).await?;
                    }
                    break;
                }
                frame = read_frame(&mut input, &mut line) => {
                    let Some(frame) = frame? else {
                        return Ok(true);
                    };
                    let Ok(incoming) = serde_json::from_slice::<Value>(&frame) else {
                            send_response(&mut output, &rpc_error(&Value::Null, -32700, "Parse error")).await?;
                            continue;
                    };
                    let method = incoming.get("method").and_then(Value::as_str);
                    let id = incoming.get("id");
                    if incoming.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
                        send_response(&mut output, &rpc_error(id.unwrap_or(&Value::Null), -32600, "Invalid Request")).await?;
                    } else if id.is_none() {
                        if method == Some("notifications/cancelled")
                            && incoming.pointer("/params/requestId").is_some()
                            && incoming.pointer("/params/requestId") == request.get("id") {
                            if !cancelled {
                                if let Some(sender) = cancellation.take() { let _ = sender.send(()); }
                                cancelled = true;
                                send_response(&mut output, &rpc_error(request.get("id").unwrap_or(&Value::Null), -32800, "Request cancelled")).await?;
                            }
                            if !finish_on_cancel { break; }
                        }
                    } else if let Some(id) = id {
                        let response = if !id.is_string() && !id.is_number() {
                            rpc_error(&Value::Null, -32600, "Invalid request ID")
                        } else if method == Some("ping") {
                            json!({"jsonrpc": "2.0", "id": id, "result": {}})
                        } else {
                            rpc_error(id, -32000, "Identity operation in progress")
                        };
                        send_response(&mut output, &response).await?;
                    }
                }
            }
        }
        Ok(false)
        }.await;
        if !matches!(outcome, Ok(false)) {
            if let Some(sender) = cancellation.take() {
                let _ = sender.send(());
            }
            if finish_on_cancel && !completed {
                let _ = (&mut operation).await;
            }
        }
        match outcome {
            Ok(false) => {}
            Ok(true) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "transport tests fail immediately on invalid fixtures"
    )]

    use super::*;

    fn server(path: PathBuf) -> Server {
        Server::new(Options {
            command: "identity-mcp".to_owned(),
            json: true,
            endpoint: "https://example.test".to_owned(),
            credentials: path,
        })
        .expect("server")
    }

    #[tokio::test]
    async fn initialization_tools_notifications_and_errors() {
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        let before = server
            .request(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
            .await
            .expect("response");
        assert_eq!(before.pointer("/error/code"), Some(&json!(-32000)));
        let initialized = server
            .request(
                &json!({"jsonrpc": "2.0", "id": "init", "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"}}),
            )
            .await
            .expect("initialize");
        assert_eq!(
            initialized.pointer("/result/protocolVersion"),
            Some(&json!("2025-11-25"))
        );
        assert!(
            server
                .request(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
                .await
                .is_none()
        );
        let listed = server
            .request(&json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
            .await
            .expect("list");
        assert_eq!(
            listed
                .pointer("/result/tools")
                .and_then(Value::as_array)
                .expect("tools")
                .len(),
            5
        );
        let unknown = server
            .request(&json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "unknown"}}))
            .await
            .expect("unknown");
        assert_eq!(unknown.pointer("/error/code"), Some(&json!(-32602)));
        let bad = server
            .request(&json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": "identity_whoami", "arguments": {"token": "untrusted"}}}))
            .await
            .expect("bad args");
        assert_eq!(bad.pointer("/error/code"), Some(&json!(-32602)));
    }

    #[tokio::test]
    async fn transport_recovers_after_parse_error_and_emits_only_responses() {
        let directory = tempfile::tempdir().expect("directory");
        let input = b"not json\n{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"ping\"}\n";
        let mut output = Vec::new();
        transport(
            server(directory.path().join("identity.json")),
            input.as_slice(),
            &mut output,
        )
        .await
        .expect("transport");
        let output = String::from_utf8(output).expect("UTF-8");
        let responses = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("JSON"))
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert_eq!(
            responses
                .first()
                .and_then(|response| response.pointer("/error/code")),
            Some(&json!(-32700))
        );
        assert_eq!(
            responses.last().and_then(|response| response.get("id")),
            Some(&json!(8))
        );
    }

    fn pending(server: &mut Server) {
        let device = serde_json::from_value(json!({"device_code": "device", "user_code": "ABCD",
            "verification_uri": "https://example.test", "expires_in": 60, "interval": 1}))
        .expect("device");
        server.pending = Some((device, tokio::time::Instant::now()));
        server.initialized = true;
    }

    #[tokio::test]
    async fn non_json_gateway_failures_retain_approval_until_success() {
        use axum::response::IntoResponse as _;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let router = axum::Router::new().fallback(move || {
            let calls = calls.clone();
            async move {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => (StatusCode::SERVICE_UNAVAILABLE, "<h1>Unavailable</h1>").into_response(),
                    1 => (StatusCode::SERVICE_UNAVAILABLE, "").into_response(),
                    _ => (
                        StatusCode::OK,
                        axum::Json(json!({"access_token": "access", "refresh_token": "refresh",
                        "token_type": "Bearer", "expires_in": 900})),
                    )
                        .into_response(),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        for _ in 0..2 {
            let error = server
                .call("identity_login_complete")
                .await
                .expect_err("transient");
            assert_eq!(error.code, "invalid_response");
            assert_eq!(error.status, Some(StatusCode::SERVICE_UNAVAILABLE));
            assert!(server.pending.is_some());
        }
        assert_eq!(
            server
                .call("identity_login_complete")
                .await
                .expect("retry")
                .get("authenticated"),
            Some(&json!(true))
        );
        assert!(server.pending.is_none());
        assert_eq!(count.load(Ordering::SeqCst), 3);
        task.abort();
    }

    #[tokio::test]
    async fn slowdown_survives_cancelled_completion_and_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let second_poll = std::sync::Arc::new(tokio::sync::Notify::new());
        let calls = count.clone();
        let notified = second_poll.clone();
        let router = axum::Router::new().fallback(move || {
            let calls = calls.clone();
            let notified = notified.clone();
            async move {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => (
                        StatusCode::BAD_REQUEST,
                        axum::Json(json!({"error": "slow_down"})),
                    ),
                    1 => {
                        notified.notify_one();
                        (
                            StatusCode::BAD_REQUEST,
                            axum::Json(json!({"error": "authorization_pending"})),
                        )
                    }
                    _ => (
                        StatusCode::OK,
                        axum::Json(json!({"access_token": "access",
                        "refresh_token": "refresh", "token_type": "Bearer", "expires_in": 900})),
                    ),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve");
        });
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        server.identity = Identity::new(&endpoint).expect("identity");
        pending(&mut server);
        {
            let completion = server.call("identity_login_complete");
            tokio::pin!(completion);
            tokio::select! {
                result = &mut completion => assert!(result.is_err(), "completion must still be polling"),
                () = second_poll.notified() => {}
            }
        }
        assert_eq!(server.pending.as_ref().expect("pending").0.interval, 6);
        let retry = tokio::time::Instant::now();
        assert_eq!(
            server
                .call("identity_login_complete")
                .await
                .expect("retry")
                .get("authenticated"),
            Some(&json!(true))
        );
        assert!(retry.elapsed() >= Duration::from_secs(6));
        assert_eq!(count.load(Ordering::SeqCst), 3);
        task.abort();
    }

    #[tokio::test]
    async fn login_transport_answers_ping_and_cancellation_and_retains_approval() {
        let directory = tempfile::tempdir().expect("directory");
        let mut server = server(directory.path().join("identity.json"));
        pending(&mut server);
        let (mut client, connection) = tokio::io::duplex(4096);
        let (input, output) = tokio::io::split(connection);
        let task = tokio::spawn(transport(server, input, output));
        client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"identity_login_complete\"}}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n").await.expect("requests");
        let mut client = tokio::io::BufReader::new(client);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_millis(500), client.read_line(&mut line))
            .await
            .expect("responsive ping")
            .expect("read");
        assert_eq!(
            serde_json::from_str::<Value>(&line)
                .expect("response")
                .get("id"),
            Some(&json!(2))
        );
        client.get_mut().write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}\n").await.expect("cancel");
        line.clear();
        client
            .read_line(&mut line)
            .await
            .expect("cancelled response");
        assert_eq!(
            serde_json::from_str::<Value>(&line)
                .expect("response")
                .pointer("/error/code"),
            Some(&json!(-32800))
        );
        client.get_mut().write_all(b"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"identity_login_complete\"}}\n{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"ping\"}\n").await.expect("retry");
        line.clear();
        tokio::time::timeout(Duration::from_millis(500), client.read_line(&mut line))
            .await
            .expect("retry still polling")
            .expect("read");
        assert_eq!(
            serde_json::from_str::<Value>(&line)
                .expect("response")
                .get("id"),
            Some(&json!(4))
        );
        client.get_mut().shutdown().await.expect("EOF");
        task.await.expect("transport task").expect("transport");
    }

    struct CountedInput<R> {
        input: R,
        count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        failed: std::sync::Arc<tokio::sync::Notify>,
    }

    impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CountedInput<R> {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            let before = buffer.filled().len();
            let result = std::pin::Pin::new(&mut self.input).poll_read(context, buffer);
            let read = buffer.filled().len() - before;
            if self
                .count
                .fetch_add(read, std::sync::atomic::Ordering::Relaxed)
                + read
                > 1024 * 1024
            {
                self.failed.notify_one();
            }
            result
        }
    }

    struct BrokenOutput<W> {
        output: W,
        broken: std::sync::Arc<std::sync::atomic::AtomicBool>,
        failed: std::sync::Arc<tokio::sync::Notify>,
    }

    impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for BrokenOutput<W> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            if self.broken.load(std::sync::atomic::Ordering::Relaxed) {
                self.failed.notify_one();
                return std::task::Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            std::pin::Pin::new(&mut self.output).poll_write(context, bytes)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.output).poll_flush(context)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.output).poll_shutdown(context)
        }
    }

    #[tokio::test]
    async fn transport_errors_drain_consumed_device_and_refresh_exchanges() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        for login in [false, true] {
            for fault in ["oversized", "ping", "cancel"] {
                let consumed = Arc::new(tokio::sync::Notify::new());
                let released = Arc::new(tokio::sync::Notify::new());
                let notify = consumed.clone();
                let release = released.clone();
                let router = axum::Router::new().fallback(move || {
                    let notify = notify.clone();
                    let release = release.clone();
                    async move {
                        notify.notify_one();
                        release.notified().await;
                        axum::Json(json!({"access_token": "issued", "refresh_token": "rotated",
                            "token_type": "Bearer", "expires_in": 900}))
                    }
                });
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind");
                let endpoint = format!("http://{}", listener.local_addr().expect("address"));
                let http = tokio::spawn(async move {
                    axum::serve(listener, router).await.expect("serve");
                });
                let directory = tempfile::tempdir().expect("directory");
                let path = directory.path().join("identity.json");
                let mut server = server(path.clone());
                server.identity = Identity::new(&endpoint).expect("identity");
                pending(&mut server);
                if !login {
                    Store::open(&path)
                        .expect("store")
                        .write(
                            &endpoint,
                            Session {
                                access_token: "expired".to_owned(),
                                refresh_token: "original".to_owned(),
                                expires_at: 0,
                            },
                        )
                        .expect("seed");
                }
                let (mut client, connection) = tokio::io::duplex(64 * 1024);
                let (input, output) = tokio::io::split(connection);
                let count = Arc::new(AtomicUsize::new(0));
                let broken = Arc::new(AtomicBool::new(false));
                let failed = Arc::new(tokio::sync::Notify::new());
                let task = tokio::spawn(transport(
                    server,
                    CountedInput {
                        input,
                        count: count.clone(),
                        failed: failed.clone(),
                    },
                    BrokenOutput {
                        output,
                        broken: broken.clone(),
                        failed: failed.clone(),
                    },
                ));
                let name = if login {
                    "identity_login_complete"
                } else {
                    "identity_refresh"
                };
                client
                    .write_all(
                        format!(
                            "{}\n",
                            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": name}})
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("request");
                tokio::time::timeout(Duration::from_secs(3), consumed.notified())
                    .await
                    .expect("consumed");
                if fault == "oversized" {
                    count.store(0, Ordering::Relaxed);
                    client
                        .write_all(&vec![b'x'; 1024 * 1024 + 1])
                        .await
                        .expect("oversized frame");
                } else {
                    broken.store(true, Ordering::Relaxed);
                    let request = if fault == "ping" {
                        json!({"jsonrpc": "2.0", "id": 2, "method": "ping"})
                    } else {
                        json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}})
                    };
                    client
                        .write_all(format!("{request}\n").as_bytes())
                        .await
                        .expect("control");
                }
                tokio::time::timeout(Duration::from_secs(3), failed.notified())
                    .await
                    .expect("transport fault reached");
                assert!(
                    !task.is_finished(),
                    "{name}/{fault} must drain before returning the transport error"
                );
                released.notify_one();
                let error = tokio::time::timeout(Duration::from_secs(3), task)
                    .await
                    .expect("drained")
                    .expect("transport task")
                    .expect_err("transport error");
                assert_eq!(
                    error.kind(),
                    if fault == "oversized" {
                        io::ErrorKind::Other
                    } else {
                        io::ErrorKind::BrokenPipe
                    }
                );
                assert_eq!(
                    Store::open(&path)
                        .expect("unlocked store")
                        .read(&endpoint)
                        .expect("saved")
                        .refresh_token,
                    "rotated"
                );
                http.abort();
            }
        }
    }
}
