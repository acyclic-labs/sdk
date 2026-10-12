//! Shared admitted JSON-RPC boundary.
use super::{McpInitializeResult, McpToolResult};
use crate::{Error, NativeWork, OperationId, Result, wire::ErrorCode};
use rmcp::model::{JsonRpcRequest, RequestId};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

pub(super) fn rpc_result(
    mut value: Value,
    operation: OperationId,
    work: NativeWork<'_>,
) -> Result<Value> {
    if field(&value, "method", work)?.is_some()
        || (field(&value, "result", work)?.is_some() == field(&value, "error", work)?.is_some())
        || field(&value, "jsonrpc", work)?.and_then(Value::as_str) != Some("2.0")
        || field(&value, "error", work)?.is_some()
    {
        return Err(protocol_error(
            work,
            ErrorCode::Invalid,
            "MCP response identity or envelope mismatch",
        ));
    }
    // Compare the actual admitted UUID spelling, without allocating a new ID.
    let mut spelling = [0; 36];
    let identity = uuid::Uuid::from_bytes(operation.into_bytes());
    let expected: &str = identity.hyphenated().encode_lower(&mut spelling);
    if field(&value, "id", work)?.and_then(Value::as_str) != Some(expected) {
        return Err(protocol_error(
            work,
            ErrorCode::Invalid,
            "MCP response identity mismatch",
        ));
    }
    checkpoint(work, 1, 0)?;
    value
        .as_object_mut()
        .and_then(|object| object.remove("result"))
        .ok_or_else(|| {
            protocol_error(
                work,
                ErrorCode::Invalid,
                "MCP response identity or envelope mismatch",
            )
        })
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

pub(super) fn notification(value: &Value, work: NativeWork<'_>) -> Result<()> {
    if field(value, "method", work)?
        .and_then(Value::as_str)
        .is_none()
        || field(value, "result", work)?.is_some()
        || field(value, "error", work)?.is_some()
    {
        return Err(protocol_error(
            work,
            ErrorCode::Invalid,
            "invalid MCP notification",
        ));
    }
    if field(value, "id", work)?.is_some() {
        return Err(protocol_error(
            work,
            ErrorCode::Unsupported,
            "MCP server request needs a callback router",
        ));
    }
    if field(value, "jsonrpc", work)?.and_then(Value::as_str) != Some("2.0") {
        return Err(protocol_error(
            work,
            ErrorCode::Invalid,
            "invalid MCP notification",
        ));
    }
    Ok(())
}

pub(super) fn initialization(value: &Value, work: NativeWork<'_>) -> Result<()> {
    check::<McpInitializeResult>(value, work)?;
    let info = field(value, "serverInfo", work)?.unwrap_or(&Value::Null);
    if field(value, "protocolVersion", work)?.and_then(Value::as_str)
        != Some(super::PROTOCOL_VERSION)
        || !field(value, "capabilities", work)?.is_some_and(Value::is_object)
        || field(info, "name", work)?.and_then(Value::as_str).is_none()
        || field(info, "version", work)?
            .and_then(Value::as_str)
            .is_none()
    {
        return Err(protocol_error(
            work,
            ErrorCode::Unsupported,
            "MCP initialization contract unsupported",
        ));
    }
    Ok(())
}

fn field<'a>(value: &'a Value, key: &str, work: NativeWork<'_>) -> Result<Option<&'a Value>> {
    checkpoint(work, 1, 0)?;
    Ok(value.get(key))
}

pub(super) fn decode_tool_result(value: Value) -> Result<McpToolResult> {
    check::<rmcp::model::CallToolResult>(&value, None)?;
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn check<T: DeserializeOwned>(value: &Value, work: NativeWork<'_>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let custody = if let Some(work) = work {
        let quota = quota::dto_quota(value, work, 0)?;
        Some(
            work.claim(quota, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .map_err(native_work_error)?,
        )
    } else {
        None
    };
    let decoded = T::deserialize(value).map(drop);
    match decoded {
        Ok(()) => {
            // Every typed temporary has physically dropped before its quota.
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            drop(custody);
            Ok(())
        }
        Err(source) => {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeJson {
                    code: crate::wire::ErrorCode::Invalid,
                    source,
                    custody,
                });
            }
            Err(Error::Invalid(source.to_string()))
        }
    }
}

pub(super) fn protocol_error(
    work: NativeWork<'_>,
    code: ErrorCode,
    message: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code,
            reason: message,
        };
    }
    let _ = work;
    match code {
        ErrorCode::Unsupported => Error::Unsupported(message.into()),
        _ => Error::Invalid(message.into()),
    }
}

pub(super) fn checkpoint(work: NativeWork<'_>, items: u64, bytes: u64) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: items,
            bytes_copied: bytes,
            ..Default::default()
        })
        .map_err(native_work_error)?;
    }
    let _ = (work, items, bytes);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork {
        cause,
        custody: None,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod quota;
#[cfg(test)]
mod tests;
