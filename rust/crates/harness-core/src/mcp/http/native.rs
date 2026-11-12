//! Optional native HTTP I/O, without request replay or a second receipt journal.

use super::{McpHttpProvider, McpHttpRequest, McpHttpResponse, body::bounded};
use crate::{Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::time::Duration;

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
            .map_err(|_| Error::Indeterminate(operation))?
            .map_err(|_| Error::Indeterminate(operation))?;
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
            let body = response
                .bytes_stream()
                .map(move |chunk| chunk.map_err(|_| Error::Indeterminate(operation)));
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

    #[tokio::test]
    async fn notification_eof_reconnects_and_invalidates_the_disconnected_gap() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncBufReadExt as _, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(Error::process_io)?;
        let address = listener.local_addr().map_err(Error::process_io)?;
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let server = tokio::spawn(async move {
            for index in 0..3 {
                let (socket, _) = listener.accept().await.map_err(Error::process_io)?;
                accepted.fetch_add(1, Ordering::SeqCst);
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
                let status = if index == 2 {
                    "405 Method Not Allowed"
                } else {
                    "200 OK"
                };
                socket.get_mut().write_all(format!("HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.map_err(Error::process_io)?;
                socket
                    .get_mut()
                    .shutdown()
                    .await
                    .map_err(Error::process_io)?;
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
        (&mut watch.0)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            connections.load(Ordering::SeqCst),
            3,
            "EOF must reconnect until the server declines GET"
        );
        assert_eq!(
            transport.tools_generation(),
            4,
            "both EOF and resubscription invalidate each disconnected interval"
        );
        server
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
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
                assert!(matches!(result, Err(Error::Indeterminate(_))));
            }
        }
        Ok(())
    }
}
