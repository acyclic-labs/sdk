//! Optional native HTTP I/O, without request replay or a second receipt journal.

use super::{McpHttpProvider, McpHttpRequest, McpHttpResponse, body::bounded};
use crate::{Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::time::Duration;

fn exchange_failure(
    operation: OperationId,
    read: bool,
    code: crate::wire::ErrorCode,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    let decision = if read {
        Error::Boundary {
            code,
            reason: "MCP HTTP read failed",
        }
    } else {
        // A failed POST may have reached the server. Its transport cause never
        // authorizes replay or changes the original uncertain operation.
        Error::Indeterminate(operation)
    };
    Error::external_for_request(operation, decision.classification(), source)
}

fn transport_failure(operation: OperationId, read: bool, error: reqwest::Error) -> Error {
    let code = if read {
        crate::http::transport_failure(&error).0
    } else {
        crate::wire::ErrorCode::Indeterminate
    };
    exchange_failure(operation, read, code, error.without_url())
}

/// Native network capability with explicit host credentials.
pub struct NativeMcpHttpProvider(reqwest::Client);

impl NativeMcpHttpProvider {
    /// Creates a provider with redirects and retries disabled.
    pub fn new(headers: reqwest::header::HeaderMap) -> Result<Self> {
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(Self(client))
    }
}

impl McpHttpProvider for NativeMcpHttpProvider {
    fn exchange<'a>(
        &'a self,
        operation: OperationId,
        request: McpHttpRequest,
    ) -> BoxProviderFuture<'a, Result<McpHttpResponse>> {
        Box::pin(async move {
            if request.timeout_ms == Some(0) || request.maximum_response_bytes == Some(0) {
                return Err(Error::Invalid(
                    "HTTP exchange requires positive configured allowances".into(),
                ));
            }
            let method = reqwest::Method::from_bytes(request.method.as_bytes())
                .map_err(|e| Error::Invalid(e.to_string()))?;
            let streaming = request.method == "GET";
            let mut builder = self.0.request(method, &request.endpoint).body(request.body);
            if !streaming && let Some(timeout) = request.timeout_ms {
                builder = builder.timeout(Duration::from_millis(timeout));
            }
            for (key, value) in request.headers {
                builder = builder.header(key, value);
            }
            let response = super::super::deadline::until(
                super::super::deadline::after(request.timeout_ms.map(Duration::from_millis))?,
                builder.send(),
            )
            .await
            .map_err(|error| {
                exchange_failure(
                    operation,
                    streaming,
                    crate::wire::ErrorCode::Unavailable,
                    error,
                )
            })?
            .map_err(|error| transport_failure(operation, streaming, error))?;
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .map(|(key, value)| {
                    value
                        .to_str()
                        .map(|value| (key.as_str().to_owned(), value.to_owned()))
                        .map_err(|e| Error::Invalid(e.to_string()))
                })
                .collect::<Result<_>>()?;
            let body = response.bytes_stream().map(move |chunk| {
                chunk.map_err(|error| transport_failure(operation, streaming, error))
            });
            Ok(McpHttpResponse {
                status,
                headers,
                body: if streaming {
                    Box::pin(body)
                } else {
                    Box::pin(bounded(body, request.maximum_response_bytes, operation))
                },
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>> {
        // HTTP request IDs do not constitute a server idempotency/receipt API.
        Box::pin(async { Ok(None) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{McpToolTransport, http::HttpMcpTransport};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn notification_request(
        listener: &tokio::net::TcpListener,
    ) -> Result<tokio::net::TcpStream> {
        use tokio::io::{AsyncBufReadExt as _, BufReader};
        let (socket, _) = listener.accept().await.map_err(Error::process_io)?;
        let mut socket = BufReader::new(socket);
        let mut line = String::new();
        socket
            .read_line(&mut line)
            .await
            .map_err(Error::process_io)?;
        assert!(line.starts_with("GET "));
        loop {
            line.clear();
            assert_ne!(
                socket
                    .read_line(&mut line)
                    .await
                    .map_err(Error::process_io)?,
                0
            );
            if line == "\r\n" {
                break;
            }
        }
        Ok(socket.into_inner())
    }

    #[tokio::test]
    async fn notification_transients_reconnect_and_invalidate_the_disconnected_gap() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(Error::process_io)?;
        let address = listener.local_addr().map_err(Error::process_io)?;
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let server = tokio::spawn(async move {
            for index in 0..4 {
                let mut socket = notification_request(&listener).await?;
                accepted.fetch_add(1, Ordering::SeqCst);
                let status = match index {
                    0 => "503 Service Unavailable",
                    3 => "405 Method Not Allowed",
                    _ => "200 OK",
                };
                let body = if index == 2 { "data: {" } else { "" };
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.map_err(Error::process_io)?;
                socket.shutdown().await.map_err(Error::process_io)?;
            }
            Ok::<_, Error>(())
        });
        let provider = Arc::new(NativeMcpHttpProvider::new(
            reqwest::header::HeaderMap::new(),
        )?);
        let mut transport = HttpMcpTransport::new(
            provider,
            format!("http://{address}/mcp"),
            Some("session".into()),
            8192,
            1000,
            None,
        )?;
        transport.watch_tools()?;
        let mut watch = Arc::try_unwrap(transport.notifications.take().expect("watch armed"))
            .ok()
            .expect("one watch owner");
        (&mut watch.task)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            connections.load(Ordering::SeqCst),
            4,
            "transient HTTP and EOF reconnect until the server declines GET"
        );
        assert_eq!(
            transport.tools_generation(),
            5,
            "both EOF and resubscription invalidate each disconnected interval"
        );
        server
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
        Ok(())
    }

    #[tokio::test]
    async fn notification_permanent_failures_are_retained_without_reconnecting() -> Result<()> {
        use crate::wire::ErrorCode;
        use std::sync::atomic::Ordering;
        for (status, media, body, code) in [
            (
                "401 Unauthorized",
                "text/event-stream",
                "",
                ErrorCode::Unauthorized,
            ),
            (
                "404 Not Found",
                "text/event-stream",
                "",
                ErrorCode::NotFound,
            ),
            (
                "200 OK",
                "text/event-stream",
                "data: not-json\n\n",
                ErrorCode::Invalid,
            ),
            ("200 OK", "application/json", "{}", ErrorCode::Invalid),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(Error::process_io)?;
            let address = listener.local_addr().map_err(Error::process_io)?;
            let server = tokio::spawn(async move {
                let mut socket = notification_request(&listener).await?;
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {media}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.map_err(Error::process_io)?;
                Ok::<_, Error>(())
            });
            let provider = Arc::new(ObservedProvider {
                inner: NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())?,
                exchanges: Default::default(),
            });
            let mut transport = HttpMcpTransport::new(
                provider.clone(),
                format!("http://{address}/mcp"),
                Some("session".into()),
                8192,
                1000,
                None,
            )?;
            transport.watch_tools()?;
            let mut watch = Arc::try_unwrap(transport.notifications.take().expect("watch armed"))
                .ok()
                .expect("one watch owner");
            // Wait for actual completion, then observe the retained cause
            // through ordinary catalog reads without another exchange.
            (&mut watch.task)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            let watch = Arc::new(watch);
            transport.notifications = Some(watch.clone());
            let (_, original) = watch.failure.get().expect("terminal cause retained");
            for _ in 0..2 {
                let error = transport
                    .list_tools(OperationId::new(), None)
                    .await
                    .expect_err("watch failure reaches catalog refresh");
                assert_eq!(error.code(), code);
                let Error::Command(error) = error else {
                    panic!("retained local cause expected");
                };
                assert!(std::ptr::eq(
                    error.original().expect("local cause"),
                    original.as_ref()
                ));
            }
            assert!(transport.tools_generation() > 0);
            assert_eq!(provider.exchanges.load(Ordering::SeqCst), 1);
            server
                .await
                .map_err(|error| Error::Storage(error.to_string()))??;
        }
        Ok(())
    }

    async fn failed_exchange(
        scheme: &'static str,
        method: &'static str,
        reply: &'static [u8],
    ) -> Result<Error> {
        use futures::TryStreamExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(Error::process_io)?;
        let address = listener.local_addr().map_err(Error::process_io)?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(Error::process_io)?;
            // Receive the request or ClientHello before replying, then drain
            // after write-half closure so the reply is not reset away.
            let mut input = [0; 4096];
            assert_ne!(socket.read(&mut input).await.map_err(Error::process_io)?, 0);
            socket.write_all(reply).await.map_err(Error::process_io)?;
            socket.shutdown().await.map_err(Error::process_io)?;
            tokio::io::copy(&mut socket, &mut tokio::io::sink())
                .await
                .map_err(Error::process_io)?;
            Ok::<_, Error>(())
        });
        let provider = NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())?;
        let operation = OperationId::new();
        let result = provider
            .exchange(
                operation,
                McpHttpRequest {
                    endpoint: format!("{scheme}://{address}/mcp?opaque=query-marker"),
                    method: method.into(),
                    headers: Default::default(),
                    body: Vec::new(),
                    timeout_ms: None,
                    maximum_response_bytes: Some(8192),
                },
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(response) => response
                .body
                .try_collect::<Vec<_>>()
                .await
                .expect_err("body was cut off"),
        };
        if method == "POST" {
            assert_eq!(error.operation_id(), Some(operation));
            assert!(error.leaves_effect_uncertain());
        } else {
            assert!(!error.leaves_effect_uncertain());
        }
        assert!(!error.to_string().contains("query-marker"));
        drop(provider);
        server
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
        Ok(error)
    }

    #[tokio::test]
    async fn native_read_failures_keep_their_causes_without_authorizing_write_replay() -> Result<()>
    {
        use crate::wire::ErrorCode;
        for (scheme, reply, code) in [
            ("http", b"not an HTTP status\r\n\r\n".as_slice(), ErrorCode::Invalid),
            ("https", b"not a TLS record\r\n\r\n".as_slice(), ErrorCode::Unauthorized),
            ("http", b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1024\r\nConnection: close\r\n\r\ndata: {".as_slice(), ErrorCode::Unavailable),
        ] {
            for method in ["GET", "POST"] {
                let error = failed_exchange(scheme, method, reply).await?;
                assert_eq!(error.code(), if method == "GET" { code } else { ErrorCode::Indeterminate });
                let mut cause: &(dyn std::error::Error + 'static) = &error;
                loop {
                    if let Some(cause) = cause.downcast_ref::<reqwest::Error>() {
                        assert!(cause.url().is_none(), "transport URL must not enter diagnostics");
                        break;
                    }
                    cause = cause.source().expect("original reqwest cause retained");
                }
            }
        }
        Ok(())
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>> {
        let mut input = Vec::new();
        let mut buffer = [0_u8; 512];
        loop {
            let count = socket
                .read(&mut buffer)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            if count == 0 {
                return Err(Error::Invalid("fixture request disconnected".into()));
            }
            input.extend_from_slice(&buffer[..count]);
            if let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let header = std::str::from_utf8(&input[..end])
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|n| n.trim().parse::<usize>().ok())
                    })
                    .ok_or_else(|| Error::Invalid("fixture length absent".into()))?;
                if input.len() >= end + 4 + length {
                    break;
                }
            }
        }
        Ok(input)
    }

    struct ObservedProvider {
        inner: NativeMcpHttpProvider,
        exchanges: std::sync::atomic::AtomicUsize,
    }
    impl McpHttpProvider for ObservedProvider {
        fn exchange<'a>(
            &'a self,
            operation: OperationId,
            request: McpHttpRequest,
        ) -> BoxProviderFuture<'a, Result<McpHttpResponse>> {
            self.exchanges
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.exchange(operation, request)
        }
        fn reconcile<'a>(
            &'a self,
            operation: OperationId,
        ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>> {
            self.inner.reconcile(operation)
        }
    }

    #[tokio::test]
    async fn real_http_catalog_reservations_charge_filtered_tools_and_stop_before_another_exchange()
    -> Result<()> {
        use crate::mcp::{McpDiscoveryLimits, tool_pages};
        use futures::{StreamExt as _, TryStreamExt as _};
        let first: rmcp::model::ListToolsResult = serde_json::from_value(json!({
            "tools":[{"name":"blocked","inputSchema":{"type":"object"}}, {"name":"also_blocked","inputSchema":{"type":"object"}}],
            "nextCursor":"next"
        })).map_err(|e| Error::Invalid(e.to_string()))?;
        let first_bytes = u32::try_from(serde_json::to_vec(&first).unwrap().len()).unwrap();
        for limits in [
            McpDiscoveryLimits::default(),
            McpDiscoveryLimits {
                pages: Some(1),
                ..Default::default()
            },
            McpDiscoveryLimits {
                tools: Some(1),
                ..Default::default()
            },
            McpDiscoveryLimits {
                bytes: Some(first_bytes),
                ..Default::default()
            },
        ] {
            let expected = if limits == McpDiscoveryLimits::default() {
                2
            } else {
                1
            };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(Error::process_io)?;
            let address = listener.local_addr().map_err(Error::process_io)?;
            let first = first.clone();
            let server = tokio::spawn(async move {
                for index in 0..expected {
                    let (mut socket, _) = listener.accept().await.map_err(Error::process_io)?;
                    let input = read_http_request(&mut socket).await?;
                    let header = input
                        .windows(4)
                        .position(|part| part == b"\r\n\r\n")
                        .expect("HTTP body");
                    let request: serde_json::Value = serde_json::from_slice(&input[header + 4..])
                        .map_err(|e| Error::Invalid(e.to_string()))?;
                    assert_eq!(request["method"], "tools/list");
                    if index == 1 {
                        assert_eq!(request["params"]["cursor"], "next");
                    }
                    let result = if index == 0 {
                        first.clone()
                    } else {
                        rmcp::model::ListToolsResult::default()
                    };
                    let body = serde_json::to_string(
                        &json!({"jsonrpc":"2.0", "id":request["id"], "result":result}),
                    )
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .map_err(Error::process_io)?;
                    socket.shutdown().await.map_err(Error::process_io)?;
                }
                Ok::<_, Error>(())
            });
            let provider = Arc::new(ObservedProvider {
                inner: NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())?,
                exchanges: std::sync::atomic::AtomicUsize::new(0),
            });
            let transport = HttpMcpTransport::new(
                provider.clone(),
                format!("http://{address}/mcp"),
                Some("session".into()),
                None,
                None,
                None,
            )?;
            let pages = tool_pages(
                |cursor| transport.list_tools(OperationId::new(), cursor),
                limits,
                |_| false,
            )?;
            futures::pin_mut!(pages);
            if limits.tools == Some(1) {
                assert!(matches!(pages.try_next().await, Err(Error::Invalid(_))));
            } else {
                assert!(pages.try_next().await?.unwrap().is_empty());
                if expected == 2 {
                    assert!(pages.try_next().await?.unwrap().is_empty());
                    assert!(pages.try_next().await?.is_none());
                } else {
                    assert!(matches!(pages.try_next().await, Err(Error::Invalid(_))));
                }
            }
            assert!(pages.next().await.is_none());
            server.await.map_err(|e| Error::Storage(e.to_string()))??;
            assert_eq!(
                provider.exchanges.load(std::sync::atomic::Ordering::SeqCst),
                expected
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn real_local_http_json_sse_and_lost_response_never_repost() -> Result<()> {
        for mode in [
            "json",
            "sse",
            "lost",
            "redirect",
            "json-unbounded",
            "sse-unbounded",
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            let address = listener
                .local_addr()
                .map_err(|e| Error::Storage(e.to_string()))?;
            let operation = OperationId::from_bytes([9; 16]);
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener
                    .accept()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let input = read_http_request(&mut socket).await?;
                let request =
                    String::from_utf8(input).map_err(|e| Error::Invalid(e.to_string()))?;
                assert!(request.contains("mcp-session-id: session"));
                assert!(request.contains("mcp-protocol-version: 2025-11-25"));
                assert!(request.contains("tools/call"));
                if mode == "lost" {
                    drop(socket);
                    assert!(
                        tokio::time::timeout(Duration::from_millis(100), listener.accept())
                            .await
                            .is_err()
                    );
                    return Ok(());
                }
                let text = if mode.ends_with("unbounded") {
                    let request: serde_json::Value =
                        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1)
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                    assert_eq!(
                        request["params"]["arguments"]["payload"]
                            .as_str()
                            .unwrap()
                            .len(),
                        1_048_577
                    );
                    "x".repeat(1_048_577)
                } else {
                    "local".into()
                };
                let result=json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"content":[{"type":"text","text":text}],"isError":false}}).to_string();
                let response = if mode == "redirect" {
                    format!(
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                } else {
                    let body = if mode.starts_with("sse") {
                        format!("data: {result}\r\n\r\n")
                    } else {
                        result
                    };
                    let media = if mode.starts_with("sse") {
                        "text/event-stream"
                    } else {
                        "application/json"
                    };
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {media}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                };
                socket
                    .write_all(response.as_bytes())
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                socket
                    .shutdown()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                // A second connection would prove an implicit retry/redirect.
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), listener.accept())
                        .await
                        .is_err()
                );
                Ok::<_, Error>(())
            });
            let provider = Arc::new(NativeMcpHttpProvider::new(
                reqwest::header::HeaderMap::new(),
            )?);
            let transport = HttpMcpTransport::new(
                provider,
                format!("http://{address}/mcp"),
                Some("session".into()),
                if mode.ends_with("unbounded") {
                    None
                } else {
                    Some(8192)
                },
                1000,
                None,
            )?;
            let arguments = if mode.ends_with("unbounded") {
                json!({"payload":"x".repeat(1_048_577)})
            } else {
                json!({})
            };
            let result = transport.call(operation, "echo", arguments, None).await;
            assert!(transport.reconcile(operation, None).await?.is_none());
            server.await.map_err(|e| Error::Storage(e.to_string()))??;
            if matches!(mode, "json" | "sse" | "json-unbounded" | "sse-unbounded") {
                let expected = if mode.ends_with("unbounded") {
                    "x".repeat(1_048_577)
                } else {
                    "local".into()
                };
                assert_eq!(result?.content, [rmcp::model::ContentBlock::text(expected)]);
            } else {
                let error = result.expect_err("the original POST reply was lost");
                assert_eq!(error.code(), crate::wire::ErrorCode::Indeterminate);
                assert_eq!(error.operation_id(), Some(operation));
                assert!(error.leaves_effect_uncertain());
            }
        }
        Ok(())
    }
}
