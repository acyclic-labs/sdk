//! One response decoder for JSON and SSE, independent of platform HTTP I/O.
use super::{HttpMcpTransport, McpHttpResponse, sse};
use crate::{Error, OperationId, Result, mcp::rpc::rpc_result, runtime::ToolContext};
use futures::StreamExt as _;
use serde_json::Value;

pub(super) async fn decode(
    mut response: McpHttpResponse,
    operation: OperationId,
    maximum_bytes: Option<u32>,
    callbacks: Option<(&HttpMcpTransport, ToolContext)>,
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
            return sse::response(response.body, operation, maximum_bytes, callbacks).await;
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
            .filter(|total| maximum_bytes.is_none_or(|maximum| *total as u64 <= u64::from(maximum)))
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
