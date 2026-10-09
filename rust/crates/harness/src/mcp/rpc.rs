//! Shared JSON-RPC response boundary for HTTP and approved stdio.
use super::McpToolResult;
use crate::{Error, OperationId, Result};
use rmcp::model::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId};
use serde_json::{Map, Value};

pub(super) fn rpc_result(value: Value, operation: OperationId) -> Result<Value> {
    if value.get("method").is_some()
        || (value.get("result").is_some() == value.get("error").is_some())
    {
        return Err(Error::Invalid(
            "MCP response identity or envelope mismatch".into(),
        ));
    }
    let response: JsonRpcResponse<Value> =
        serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    if response.id != RequestId::String(operation.to_string().into()) {
        return Err(Error::Invalid("MCP response identity mismatch".into()));
    }
    Ok(response.result)
}

pub(super) fn request(operation: OperationId, method: &str, params: Value) -> Result<Vec<u8>> {
    let mut payload = Map::new();
    payload.insert("method".into(), Value::String(method.into()));
    payload.insert("params".into(), params);
    serde_json::to_vec(&JsonRpcRequest::new(
        RequestId::String(operation.to_string().into()),
        Value::Object(payload),
    ))
    .map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn notification(value: &Value) -> Result<()> {
    if value.get("method").and_then(Value::as_str).is_none()
        || value.get("result").is_some()
        || value.get("error").is_some()
    {
        return Err(Error::Invalid("invalid MCP notification".into()));
    }
    if value.get("id").is_some() {
        return Err(Error::Unsupported(
            "MCP server request needs a callback router".into(),
        ));
    }
    let _: JsonRpcNotification<Value> =
        serde_json::from_value(value.clone()).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

pub(super) fn decode_tool_result(value: Value) -> Result<McpToolResult> {
    validate_tool_result(&value)?;
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn validate_tool_result(value: &Value) -> Result<()> {
    let _: rmcp::model::CallToolResult =
        serde_json::from_value(value.clone()).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}
