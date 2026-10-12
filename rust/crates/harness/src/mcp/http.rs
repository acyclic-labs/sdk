//! Streamable HTTP protocol over a consumer-supplied native or browser provider.

use super::rpc::{decode_tool_result, notification, request, rpc_result};
use super::{McpInitializeResult, McpToolResult, McpToolTransport, McpToolsPage, PROTOCOL_VERSION};
use crate::{Error, OperationId, Result};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use bytes::Bytes;
use futures::{StreamExt as _, FutureExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use super::callbacks::{McpCallbackAdmission, McpCallbackPolicy, McpCallbackRequest};
use crate::runtime::ToolContext;

#[cfg(all(feature = "mcp-http", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "mcp-http", not(target_arch = "wasm32")))]
pub use native::NativeMcpHttpProvider;

/// Exact portable HTTP input. Credentials belong to the bound provider.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpHttpRequest {
    /// Pinned HTTP(S) endpoint.
    pub endpoint: String,
    /// HTTP method selected by the protocol owner.
    pub method: String,
    /// Protocol headers; keys use lowercase ASCII.
    pub headers: BTreeMap<String, String>,
    /// One UTF-8 JSON-RPC message, or empty for GET/DELETE.
    pub body: Vec<u8>,
    /// Finite whole-exchange provider deadline.
    pub timeout_ms: u32,
    /// Finite total response-body allowance.
    pub maximum_response_bytes: u32,
}

/// Streaming response.
pub struct McpHttpResponse {
    /// Observed HTTP status.
    pub status: u16,
    /// Response headers normalized to lowercase ASCII keys.
    pub headers: BTreeMap<String, String>,
    /// Provider-owned bounded response stream, cancelled when dropped.
    pub body: BoxProviderStream<'static, Result<Bytes>>,
}

/// Platform network I/O only; Rust owns protocol/session/result semantics.
pub trait McpHttpProvider: acyclic_stream::ProviderPlatform {
    /// Performs exactly one admitted exchange with finite I/O bounds.
    fn exchange<'a>(
        &'a self,
        operation: OperationId,
        request: McpHttpRequest,
    ) -> BoxProviderFuture<'a, Result<McpHttpResponse>>;
    /// Reads a provider-retained response to the exact operation without POST.
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>>;
}

/// Pinned HTTP endpoint, session, and finite exchange allowances.
#[derive(Clone)]
pub struct HttpMcpTransport {
    provider: Arc<dyn McpHttpProvider>,
    endpoint: String,
    session: Option<String>,
    maximum_bytes: u32,
    timeout_ms: u32,
    callbacks: Option<(McpCallbackPolicy, Arc<dyn McpCallbackAdmission>)>,
}

impl HttpMcpTransport {
    /// Constructs a bounded initialize request for staging through an ordinary admitted host operation.
    pub fn initialization_request(
        &self,
        operation: OperationId,
        name: &str,
        version: &str,
    ) -> Result<McpHttpRequest> {
        if self.session.is_some() {
            return Err(Error::Conflict(
                "MCP initialization cannot reuse a session".into(),
            ));
        }
        let body = request(
            operation,
            "initialize",
            json!({"protocolVersion":PROTOCOL_VERSION,
            "capabilities":self.callbacks.as_ref().map(|(policy, _)| policy.capabilities()).transpose()?.unwrap_or_else(|| json!({})),"clientInfo":{"name":name,"version":version}}),
        )?;
        self.request("POST", body)
    }

    /// Validates an observed initialization response, returning a new immutable binding and the exact initialized notification for separate admission.
    pub async fn accept_initialization(
        self,
        operation: OperationId,
        response: McpHttpResponse,
    ) -> Result<(Self, McpInitializeResult, McpHttpRequest)> {
        if self.session.is_some() {
            return Err(Error::Conflict("MCP session already bound".into()));
        }
        let session = response.headers.get("mcp-session-id").cloned();
        if session
            .as_ref()
            .is_some_and(|session| !valid_session(session))
        {
            return Err(Error::Invalid(
                "MCP initialization session header invalid".into(),
            ));
        }
        let initialization: McpInitializeResult =
            serde_json::from_value(self.response(operation, response).await?)
                .map_err(|e| Error::Invalid(e.to_string()))?;
        initialization.validate()?;
        let binding = Self { session, ..self };
        let notification = binding.request(
            "POST",
            br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_vec(),
        )?;
        Ok((binding, initialization, notification))
    }

    /// Discovers one explicitly admitted page.
    pub async fn list_tools(
        &self,
        operation: OperationId,
        cursor: Option<&str>,
    ) -> Result<McpToolsPage> {
        let params = cursor.map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}));
        serde_json::from_value(self.rpc(operation, "tools/list", params).await?)
            .map_err(|e| Error::Invalid(e.to_string()))
    }
    /// Binds a host-supplied session or sessionless endpoint.
    pub fn new(
        provider: Arc<dyn McpHttpProvider>,
        endpoint: String,
        session: Option<String>,
        maximum_bytes: u32,
        timeout_ms: u32,
    ) -> Result<Self> {
        let uri = endpoint
            .parse::<::http::Uri>()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if !uri.scheme_str().is_some_and(|scheme| {
            scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
        }) || uri.authority().is_none_or(|authority| {
            authority.host().is_empty()
                || authority.as_str().contains('@')
                || (authority.port().is_some() && authority.port_u16().is_none())
        }) || endpoint.contains('#')
            || maximum_bytes == 0
            || timeout_ms == 0
            || session.as_ref().is_some_and(|s| !valid_session(s))
        {
            return Err(Error::Invalid(
                "invalid MCP HTTP binding or allowance".into(),
            ));
        }
        Ok(Self {
            provider,
            endpoint,
            session,
            maximum_bytes,
            callbacks: None,
            timeout_ms,
        })
    }

    /// Binds explicit callback admission before negotiation.
    pub fn with_callbacks(mut self, policy: McpCallbackPolicy, admission: Arc<dyn McpCallbackAdmission>) -> Result<Self> {
        policy.validate()?;
        if self.session.is_some() { return Err(Error::Conflict("MCP callback policy requires fresh negotiation".into())); }
        self.callbacks = Some((policy, admission));
        Ok(self)
    }

    /// Executes under the enclosing ordinary task's authority.
    pub fn rpc_with_context<'a>(&'a self, context: ToolContext, method: &'a str, params: Value) -> BoxProviderFuture<'a, Result<Value>> {
        Box::pin(async move {
            let operation = context.operation_id();
            let request = self.request("POST", request(operation, method, params)?)?;
            let response = self.provider.exchange(operation, request).await?;
            self.response_with_context(operation, response, Some(context)).await
        })
    }

    /// Produces the exact next request, suitable for ordinary request staging and admission before the host invokes its network provider.
    pub fn request(&self, method: &str, body: Vec<u8>) -> Result<McpHttpRequest> {
        if !matches!(method, "POST" | "GET" | "DELETE")
            || body.len() as u64 > u64::from(self.maximum_bytes)
        {
            return Err(Error::Invalid("invalid MCP HTTP request".into()));
        }
        let mut headers = BTreeMap::from([
            (
                "accept".into(),
                "application/json, text/event-stream".into(),
            ),
            ("mcp-protocol-version".into(), PROTOCOL_VERSION.into()),
        ]);
        if method == "POST" {
            headers.insert("content-type".into(), "application/json".into());
        }
        if let Some(session) = &self.session {
            headers.insert("mcp-session-id".into(), session.clone());
        }
        Ok(McpHttpRequest {
            endpoint: self.endpoint.clone(),
            method: method.into(),
            headers,
            body,
            timeout_ms: self.timeout_ms,
            maximum_response_bytes: self.maximum_bytes,
        })
    }

    async fn response(
        &self,
        operation: OperationId,
        response: McpHttpResponse,
    ) -> Result<Value> {
        self.response_with_context(operation, response, None).await
    }

    async fn response_with_context(&self, operation: OperationId, mut response: McpHttpResponse, context: Option<ToolContext>) -> Result<Value> {
        if response.status == 404 && self.session.is_some() {
            // A future admission may explicitly initialize a new session.
            return Err(Error::Indeterminate(operation));
        }
        if response.status != 200 {
            return Err(Error::Indeterminate(operation));
        }
        let media = response.headers.get("content-type").map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        });
        let sse = match media.as_deref() {
            Some("application/json") => false,
            Some("text/event-stream") => true,
            _ => {
                return Err(Error::Invalid(
                    "MCP HTTP response content type unsupported".into(),
                ));
            }
        };
        if sse {
            return if let Some(context) = context {
                sse_response_scoped(response.body, operation, self.maximum_bytes, Some((self, context))).await
            } else {
                sse_response(response.body, operation, self.maximum_bytes).await
            };
        }
        let mut bytes = Vec::new();
        let mut received = 0_usize;
        while let Some(chunk) = response.body.next().await {
            let chunk = chunk?;
            received = received.saturating_add(chunk.len());
            if received as u64 > u64::from(self.maximum_bytes) {
                return Err(Error::Indeterminate(operation));
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = crate::contract::json_from_slice(&bytes).map_err(|error| {
            if error.is_eof() {
                Error::Indeterminate(operation)
            } else {
                Error::Invalid(error.to_string())
            }
        })?;
        rpc_result(value, operation, None)
    }

    /// Exchanges one already admitted JSON-RPC request.
    pub fn rpc<'a>(
        &'a self,
        operation: OperationId,
        method: &'a str,
        params: Value,
    ) -> BoxProviderFuture<'a, Result<Value>> {
        Box::pin(async move {
            let body = request(operation, method, params)?;
            let request = self.request("POST", body)?;
            let response = self.provider.exchange(operation, request).await?;
            self.response(operation, response).await
        })
    }
}

impl McpToolTransport for HttpMcpTransport {
    fn callback_policy(&self) -> Option<&McpCallbackPolicy> {
        self.callbacks.as_ref().map(|(policy, _)| policy)
    }
    fn call<'a>(
        &'a self,
        operation: OperationId,
        name: &'a str,
        arguments: Value,
    ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
        Box::pin(async move {
            decode_tool_result(
                self.rpc(
                    operation,
                    "tools/call",
                    json!({"name":name,"arguments":arguments}),
                )
                .await?,
            )
        })
    }
    fn call_with_context<'a>(&'a self, context: ToolContext, name: &'a str, arguments: Value) -> BoxProviderFuture<'a, Result<McpToolResult>> {
        Box::pin(async move {
            decode_tool_result(self.rpc_with_context(context, "tools/call", json!({"name":name,"arguments":arguments})).await?)
        })
    }
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
        Box::pin(async move {
            match self.provider.reconcile(operation).await? {
                Some(response) => {
                    decode_tool_result(self.response(operation, response).await?).map(Some)
                }
                None => Ok(None),
            }
        })
    }
}

fn valid_session(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

async fn sse_response(
    body: BoxProviderStream<'static, Result<Bytes>>,
    operation: OperationId,
    maximum_bytes: u32,
) -> Result<Value> {
    sse_response_scoped(body, operation, maximum_bytes, None).await
}

async fn sse_response_scoped(
    body: BoxProviderStream<'static, Result<Bytes>>,
    operation: OperationId,
    maximum_bytes: u32,
    callbacks: Option<(&HttpMcpTransport, ToolContext)>,
) -> Result<Value> {
    let mut requests = 0_u32;
    let mut pending = std::collections::VecDeque::new();
    let mut stream_open = true;
    // Bound input before the library parser can buffer it.
    let mut received = 0_usize;
    let bounded = body.map(move |chunk| -> Result<Bytes> {
        let chunk = chunk?;
        received = received
            .checked_add(chunk.len())
            .filter(|total| *total as u64 <= u64::from(maximum_bytes))
            .ok_or(Error::Indeterminate(operation))?;
        Ok(chunk)
    });
    let events = sse_stream::SseByteStream::new(bounded);
    futures::pin_mut!(events);
    loop {
        let value = if let Some(value) = pending.pop_front() {
            value
        } else {
            let Some(event) = events.next().await else { break; };
            let Some(value) = decode_sse_event(event)? else { continue; };
            value
        };
        if value.get("id") == Some(&json!(operation.to_string())) {
            return rpc_result(value, operation, None);
        }
        if value.get("method").is_some() && value.get("id").is_some() {
            let (transport, context) = callbacks.as_ref().ok_or_else(|| Error::Unsupported("MCP callback router is not installed".into()))?;
            let (policy, admission) = transport.callbacks.as_ref().ok_or_else(|| Error::Unsupported("MCP callback policy is not installed".into()))?;
            requests = requests.checked_add(1).filter(|count| *count <= policy.maximum_requests)
                .ok_or_else(|| Error::Invalid("MCP callback count exceeds admitted bound".into()))?;
            let callback = McpCallbackRequest { parent: operation, policy: policy.clone(), message: value };
            let callback_operation = callback.operation()?;
            callback.validate()?;
            let admitted = admission.admit(context.clone(), &callback).fuse();
            futures::pin_mut!(admitted);
            let reply = loop {
                if !stream_open { break admitted.await?; }
                futures::select! {
                    reply = admitted => break reply?,
                    event = events.next().fuse() => {
                        let Some(event) = event else {
                            stream_open = false;
                            if !pending.iter().any(|value: &Value| value.get("id") == Some(&json!(operation.to_string()))) {
                                return Err(Error::Indeterminate(operation));
                            }
                            continue;
                        };
                        let Some(value) = decode_sse_event(event)? else { continue; };
                        if value.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
                            && value.pointer("/params/requestId") == callback.message.get("id")
                        {
                            notification(&value, None)?;
                            return Err(Error::Indeterminate(operation));
                        }
                        if value.get("method").is_some() && value.get("id").is_none() {
                            notification(&value, None)?;
                        } else {
                            if pending.len() as u64 >= u64::from(policy.maximum_requests) {
                                return Err(Error::Invalid("MCP pending callback bound exceeded".into()));
                            }
                            pending.push_back(value);
                        }
                    }
                }
            };
            callback.validate_reply(&reply)?;
            let request = transport.request("POST", crate::contract::canonical_json_bytes(&reply)?)?;
            let acknowledgement = transport.provider.exchange(callback_operation, request).await.map_err(|_| Error::Indeterminate(operation))?;
            if acknowledgement.status != 202 { return Err(Error::Indeterminate(operation)); }
            drop(acknowledgement);
            continue;
        }
        notification(&value, None)?;
    }
    Err(Error::Indeterminate(operation))
}

fn decode_sse_event(event: std::result::Result<sse_stream::Sse, sse_stream::Error>) -> Result<Option<Value>> {
    let event = event.map_err(|error| match error {
        sse_stream::Error::Body(error) => error.downcast::<Error>()
            .map_or_else(|error| Error::Invalid(error.to_string()), |error| *error),
        error => Error::Invalid(error.to_string()),
    })?;
    event.data.filter(|data| !data.trim().is_empty())
        .map(|data| crate::contract::json_from_slice(data.as_bytes()).map_err(|error| Error::Invalid(error.to_string())))
        .transpose()
}

#[cfg(test)]
mod tests {
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
                )?;
                assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
                let result = transport.call(operation, "echo", json!({})).await?;
                assert_eq!(result.content[0]["text"], "hello 🦀");
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
        )?;
        assert!(matches!(
            transport.call(operation, "echo", json!({})).await,
            Err(Error::Indeterminate(_))
        ));
        assert!(transport.reconcile(operation).await?.is_none());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
    #[tokio::test]
    async fn envelope_and_callback_negative_controls() -> Result<()> {
        let operation = OperationId::from_bytes([1; 16]);
        let value: Value =
            serde_json::from_str(&message(operation)).map_err(|e| Error::Invalid(e.to_string()))?;
        for field in ["id", "jsonrpc", "result"] {
            let mut malformed = value.clone();
            malformed
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("fixture object absent".into()))?
                .remove(field);
            assert!(rpc_result(malformed, operation, None).is_err());
        }
        let mut both = value;
        both["error"] = json!({"code":-1,"message":"no"});
        assert!(rpc_result(both, operation, None).is_err());
        assert!(matches!(
            sse_response(Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                b"data: {\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sampling/createMessage\"}\n\n"))])),operation,4096).await,
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
                let ready: Value = serde_json::from_slice(&ready.body)
                    .map_err(|e| Error::Invalid(e.to_string()))?;
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
}
