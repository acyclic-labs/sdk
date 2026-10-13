use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Fixture {
    status: u16,
    sse: bool,
    split: usize,
    calls: AtomicUsize,
    requests: Mutex<Vec<McpHttpRequest>>,
}
fn message(operation: OperationId) -> String {
    json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"content":[{"type":"text","text":"hello 🦀"}],"isError":false}}).to_string()
}
impl McpHttpProvider for Fixture {
    fn exchange<'a>(
        &'a self,
        operation: OperationId,
        request: McpHttpRequest,
    ) -> BoxProviderFuture<'a, Result<McpHttpResponse>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.requests
                .lock()
                .map_err(|e| Error::Storage(e.to_string()))?
                .push(request);
            let bytes = if self.sse {
                format!(
                    "id: cursor\r\ndata:\r\n\r\ndata: {}\r\n\r\n",
                    message(operation)
                )
                .into_bytes()
            } else {
                message(operation).into_bytes()
            };
            let chunks = bytes
                .chunks(self.split)
                .map(Bytes::copy_from_slice)
                .map(Ok)
                .collect::<Vec<_>>();
            Ok(McpHttpResponse {
                status: self.status,
                headers: BTreeMap::from([(
                    "content-type".into(),
                    if self.sse {
                        "text/event-stream"
                    } else {
                        "application/json"
                    }
                    .into(),
                )]),
                body: Box::pin(futures::stream::iter(chunks)),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>> {
        Box::pin(async { Ok(None) })
    }
}
fn provider(status: u16, sse: bool, split: usize) -> Arc<Fixture> {
    Arc::new(Fixture {
        status,
        sse,
        split,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
    })
}

#[tokio::test]
async fn json_and_every_sse_chunk_width_preserve_session_identity() -> Result<()> {
    let operation = OperationId::from_bytes([7; 16]);
    for sse in [false, true] {
        for width in 1..=message(operation).len() + 50 {
            let provider = provider(200, sse, width);
            let transport = HttpMcpTransport::new(
                provider.clone(),
                "http://localhost/mcp".into(),
                Some("session".into()),
                4096,
                100,
                None,
            )?;
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
            let result = transport.call(operation, "echo", json!({}), None).await?;
            assert_eq!(result["content"][0]["text"], "hello 🦀");
            let requests = provider
                .requests
                .lock()
                .map_err(|e| Error::Storage(e.to_string()))?;
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].headers["mcp-session-id"], "session");
            assert_eq!(
                requests[0].headers["mcp-protocol-version"],
                PROTOCOL_VERSION
            );
            assert_eq!(
                requests[0].headers["accept"],
                "application/json, text/event-stream"
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn expired_session_and_receipt_absence_never_repost() -> Result<()> {
    let operation = OperationId::from_bytes([1; 16]);
    let provider = provider(404, false, 1);
    let transport = HttpMcpTransport::new(
        provider.clone(),
        "http://localhost/mcp".into(),
        Some("session".into()),
        4096,
        100,
        None,
    )?;
    assert!(matches!(
        transport.call(operation, "echo", json!({}), None).await,
        Err(Error::Indeterminate(_))
    ));
    assert!(transport.reconcile(operation, None).await?.is_none());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
async fn envelope_and_callback_negative_controls() -> Result<()> {
    let operation = OperationId::from_bytes([1; 16]);
    assert!(matches!(
        sse::response(
            Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                b"data: {\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sampling/createMessage\"}\n\n"
            ))])),
            operation,
            4096,
            None,
        )
        .await,
        Err(Error::Unsupported(_))
    ));
    assert!(!valid_session("inject\r\nheader"));
    Ok(())
}

#[tokio::test]
async fn initialization_pins_negotiated_session_and_rejects_foreign_version() -> Result<()> {
    let operation = OperationId::from_bytes([4; 16]);
    for version in [PROTOCOL_VERSION, "unknown"] {
        let provider = provider(200, false, 1);
        let transport = HttpMcpTransport::new(
            provider.clone(),
            "http://localhost/mcp".into(),
            None,
            4096,
            100,
            None,
        )?;
        let request = transport.initialization_request(operation, "fixture", "1")?;
        assert!(!request.headers.contains_key("mcp-session-id"));
        let input: Value =
            serde_json::from_slice(&request.body).map_err(|e| Error::Invalid(e.to_string()))?;
        assert_eq!(input["params"]["capabilities"], json!({}));
        let body=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{
            "protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}))
            .map_err(|e|Error::Invalid(e.to_string()))?;
        let response = McpHttpResponse {
            status: 200,
            headers: BTreeMap::from([
                (
                    "content-type".into(),
                    "Application/JSON; charset=utf-8".into(),
                ),
                ("mcp-session-id".into(), "negotiated".into()),
            ]),
            body: Box::pin(futures::stream::iter([Ok(Bytes::from(body))])),
        };
        let initialized = transport.accept_initialization(operation, response).await;
        if version == PROTOCOL_VERSION {
            let (transport, _, ready) = initialized?;
            assert_eq!(ready.headers["mcp-session-id"], "negotiated");
            let ready: Value =
                serde_json::from_slice(&ready.body).map_err(|e| Error::Invalid(e.to_string()))?;
            assert_eq!(ready["method"], "notifications/initialized");
            assert!(ready.get("id").is_none());
            assert!(
                transport
                    .initialization_request(operation, "fixture", "1")
                    .is_err()
            );
        } else {
            assert!(matches!(initialized, Err(Error::Unsupported(_))));
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
