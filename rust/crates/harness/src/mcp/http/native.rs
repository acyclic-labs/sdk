//! Optional native HTTP I/O, without request replay or a second receipt journal.

use super::{McpHttpProvider, McpHttpRequest, McpHttpResponse};
use crate::{Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::time::Duration;

/// Native network capability with explicit host credentials. Construction does
/// no I/O; the normal Harness tool journal owns admission and result retention.
pub struct NativeMcpHttpProvider(reqwest::Client);

impl NativeMcpHttpProvider {
    /// Creates a provider with redirects and request retries disabled. A host
    /// may provide credential headers; they are absent from portable bindings.
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
            if request.timeout_ms == 0 || request.maximum_response_bytes == 0 {
                return Err(Error::Invalid("HTTP exchange needs finite bounds".into()));
            }
            let method = reqwest::Method::from_bytes(request.method.as_bytes())
                .map_err(|e| Error::Invalid(e.to_string()))?;
            let mut builder = self
                .0
                .request(method, &request.endpoint)
                .timeout(Duration::from_millis(u64::from(request.timeout_ms)))
                .body(request.body);
            for (key, value) in request.headers {
                builder = builder.header(key, value);
            }
            let response = builder
                .send()
                .await
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
            let mut received = 0_usize;
            let body = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|_| Error::Indeterminate(operation))?;
                received = received.saturating_add(chunk.len());
                if received as u64 > u64::from(request.maximum_response_bytes) {
                    return Err(Error::Indeterminate(operation));
                }
                Ok(chunk)
            });
            Ok(McpHttpResponse {
                status,
                headers,
                body: Box::pin(body),
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
    async fn real_local_http_json_sse_and_lost_response_never_repost() -> Result<()> {
        for mode in ["json", "sse", "lost", "redirect"] {
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
                    if input.len() > 8192 {
                        return Err(Error::Invalid("fixture request bound".into()));
                    }
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
                let result=json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"content":[{"type":"text","text":"local"}],"isError":false}}).to_string();
                let response = if mode == "redirect" {
                    format!(
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                } else {
                    let body = if mode == "sse" {
                        format!("data: {result}\r\n\r\n")
                    } else {
                        result
                    };
                    let media = if mode == "sse" {
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
                8192,
                1000,
            )?;
            let result = transport.call(operation, "echo", json!({})).await;
            assert!(transport.reconcile(operation).await?.is_none());
            server.await.map_err(|e| Error::Storage(e.to_string()))??;
            if matches!(mode, "json" | "sse") {
                assert_eq!(result?.content[0]["text"], "local");
            } else {
                assert!(matches!(result, Err(Error::Indeterminate(_))));
            }
        }
        Ok(())
    }
}
