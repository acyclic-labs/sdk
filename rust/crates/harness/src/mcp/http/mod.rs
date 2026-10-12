//! Streamable HTTP over a host-supplied bounded provider.

use super::{
    McpInitializeResult, McpToolResult, McpToolTransport, McpToolsPage, PROTOCOL_VERSION,
    callbacks::{McpCallbackAdmission, McpCallbackPolicy},
    rpc::{decode_tool_result, request, rpc_result},
};
use crate::{Error, OperationId, Result, runtime::ToolContext};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use bytes::Bytes;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[cfg(all(feature = "mcp-http", not(target_arch = "wasm32")))]
mod native;
mod sse;
#[cfg(all(feature = "mcp-http", not(target_arch = "wasm32")))]
pub use native::NativeMcpHttpProvider;

/// One portable HTTP request with finite I/O bounds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct McpHttpRequest {
    /// Pinned endpoint.
    pub endpoint: String,
    /// Protocol HTTP method.
    pub method: String,
    /// Lowercase protocol headers.
    pub headers: BTreeMap<String, String>,
    /// UTF-8 JSON-RPC message, or empty GET/DELETE body.
    pub body: Vec<u8>,
    /// Whole-exchange deadline.
    pub timeout_ms: u32,
    /// Total response body allowance.
    pub maximum_response_bytes: u32,
}

/// Bounded streaming HTTP response.
pub struct McpHttpResponse {
    /// HTTP status.
    pub status: u16,
    /// Lowercase response headers.
    pub headers: BTreeMap<String, String>,
    /// Bounded response stream, cancelled when dropped.
    pub body: BoxProviderStream<'static, Result<Bytes>>,
}

/// Platform I/O for admitted HTTP operations.
pub trait McpHttpProvider: acyclic_stream::ProviderPlatform {
    /// Performs one exchange with finite I/O bounds.
    fn exchange<'a>(
        &'a self,
        operation: OperationId,
        request: McpHttpRequest,
    ) -> BoxProviderFuture<'a, Result<McpHttpResponse>>;
    /// Reads the retained response without POST.
    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>>;
}

/// Immutable endpoint, session and exchange bounds.
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
    /// Binds a host-selected endpoint and optional session.
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
            || session.as_ref().is_some_and(|value| !valid_session(value))
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
            timeout_ms,
            callbacks: None,
        })
    }

    /// Binds callback admission before session negotiation.
    pub fn with_callbacks(
        mut self,
        policy: McpCallbackPolicy,
        admission: Arc<dyn McpCallbackAdmission>,
    ) -> Result<Self> {
        policy.validate()?;
        if self.session.is_some() {
            return Err(Error::Conflict(
                "MCP callback policy requires fresh negotiation".into(),
            ));
        }
        self.callbacks = Some((policy, admission));
        Ok(self)
    }

    /// Constructs initialization for separate host admission.
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
        self.request("POST", request(operation, "initialize", json!({
            "protocolVersion":PROTOCOL_VERSION,
            "capabilities":self.callbacks.as_ref().map(|(policy, _)| policy.capabilities()).transpose()?.unwrap_or_else(|| json!({})),
            "clientInfo":{"name":name,"version":version},
        }))?)
    }

    /// Accepts initialization and returns the separately admitted readiness request.
    pub async fn accept_initialization(
        self,
        operation: OperationId,
        response: McpHttpResponse,
    ) -> Result<(Self, McpInitializeResult, McpHttpRequest)> {
        if self.session.is_some() {
            return Err(Error::Conflict("MCP session already bound".into()));
        }
        let session = response.headers.get("mcp-session-id").cloned();
        if session.as_ref().is_some_and(|value| !valid_session(value)) {
            return Err(Error::Invalid(
                "MCP initialization session header invalid".into(),
            ));
        }
        let initialization: McpInitializeResult =
            serde_json::from_value(self.response(operation, response, None).await?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        initialization.validate()?;
        let binding = Self { session, ..self };
        let notification = binding.request(
            "POST",
            br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_vec(),
        )?;
        Ok((binding, initialization, notification))
    }

    /// Discovers one admitted tool page.
    pub async fn list_tools(
        &self,
        operation: OperationId,
        cursor: Option<&str>,
    ) -> Result<McpToolsPage> {
        let params = cursor.map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}));
        serde_json::from_value(self.rpc(operation, "tools/list", params, None).await?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Constructs the exact request for separate host admission.
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
        mut response: McpHttpResponse,
        context: Option<ToolContext>,
    ) -> Result<Value> {
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
        match media.as_deref() {
            Some("text/event-stream") => {
                return sse::response(
                    response.body,
                    operation,
                    self.maximum_bytes,
                    context.map(|context| (self, context)),
                )
                .await;
            }
            Some("application/json") => {}
            _ => {
                return Err(Error::Invalid(
                    "MCP HTTP response content type unsupported".into(),
                ));
            }
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.body.next().await {
            let chunk = chunk?;
            bytes
                .len()
                .checked_add(chunk.len())
                .filter(|total| *total as u64 <= u64::from(self.maximum_bytes))
                .ok_or(Error::Indeterminate(operation))?;
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

    /// Exchanges an admitted request with optional enclosing task authority.
    pub fn rpc<'a>(
        &'a self,
        operation: OperationId,
        method: &'a str,
        params: Value,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Value>> {
        Box::pin(async move {
            if context
                .as_ref()
                .is_some_and(|context| context.operation_id() != operation)
            {
                return Err(Error::Unauthorized(
                    "MCP task context operation mismatch".into(),
                ));
            }
            let request = self.request("POST", request(operation, method, params)?)?;
            let response = self.provider.exchange(operation, request).await?;
            self.response(operation, response, context).await
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
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<McpToolResult>> {
        Box::pin(async move {
            decode_tool_result(
                self.rpc(
                    operation,
                    "tools/call",
                    json!({"name":name,"arguments":arguments}),
                    context,
                )
                .await?,
            )
        })
    }

    fn reconcile<'a>(
        &'a self,
        operation: OperationId,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<McpToolResult>>> {
        Box::pin(async move {
            if context
                .as_ref()
                .is_some_and(|context| context.operation_id() != operation)
            {
                return Err(Error::Unauthorized(
                    "MCP task context operation mismatch".into(),
                ));
            }
            match self.provider.reconcile(operation).await? {
                Some(response) => {
                    decode_tool_result(self.response(operation, response, None).await?).map(Some)
                }
                None => Ok(None),
            }
        })
    }
}

fn valid_session(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

#[cfg(test)]
mod tests;
