//! Shared admitted JSON-RPC boundary.
use super::{charge, error};
use crate::{Error, NativeWork, OperationId, Result, wire::ErrorCode};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

pub(super) fn rpc_result(
    mut value: Value,
    operation: OperationId,
    work: NativeWork<'_>,
) -> Result<Value> {
    charge(work, 6, 0, 0)?;
    let fields = value.as_object_mut().ok_or_else(|| {
        error(
            work,
            ErrorCode::Invalid,
            "MCP response identity or envelope mismatch",
        )
    })?;
    if fields.contains_key("method")
        || fields.contains_key("error")
        || fields.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
    {
        return Err(error(
            work,
            ErrorCode::Invalid,
            "MCP response identity or envelope mismatch",
        ));
    }
    let mut spelling = [0; 36];
    let identity = uuid::Uuid::from_bytes(operation.into_bytes());
    let expected: &str = identity.hyphenated().encode_lower(&mut spelling);
    if fields.get("id").and_then(Value::as_str) != Some(expected) {
        return Err(error(
            work,
            ErrorCode::Invalid,
            "MCP response identity mismatch",
        ));
    }
    fields.remove("result").ok_or_else(|| {
        error(
            work,
            ErrorCode::Invalid,
            "MCP response identity or envelope mismatch",
        )
    })
}

#[derive(Serialize)]
pub(super) struct Envelope<'a, P> {
    pub(super) id: OperationId,
    pub(super) jsonrpc: &'static str,
    pub(super) method: &'a str,
    pub(super) params: P,
}

pub(super) fn request(operation: OperationId, method: &str, params: Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&Envelope {
        id: operation,
        jsonrpc: "2.0",
        method,
        params,
    })
    .map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn notification(value: &Value, work: NativeWork<'_>) -> Result<()> {
    charge(work, 5, 0, 0)?;
    if value.get("method").and_then(Value::as_str).is_none()
        || value.get("result").is_some()
        || value.get("error").is_some()
    {
        return Err(error(work, ErrorCode::Invalid, "invalid MCP notification"));
    }
    if value.get("id").is_some() {
        return Err(error(
            work,
            ErrorCode::Unsupported,
            "MCP server request needs a callback router",
        ));
    }
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(error(work, ErrorCode::Invalid, "invalid MCP notification"));
    }
    Ok(())
}

pub(super) fn initialization(value: &Value, work: NativeWork<'_>) -> Result<()> {
    charge(work, 6, 0, 0)?;
    if value
        .get("instructions")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err(error(
            work,
            ErrorCode::Invalid,
            "invalid MCP server instructions",
        ));
    }
    let info = value.get("serverInfo").unwrap_or(&Value::Null);
    if value.get("protocolVersion").and_then(Value::as_str) != Some(super::PROTOCOL_VERSION)
        || !value.get("capabilities").is_some_and(Value::is_object)
        || info.get("name").and_then(Value::as_str).is_none()
        || info.get("version").and_then(Value::as_str).is_none()
    {
        return Err(error(
            work,
            ErrorCode::Unsupported,
            "MCP initialization contract unsupported",
        ));
    }
    Ok(())
}

pub(super) fn check<T: DeserializeOwned>(value: &Value, work: NativeWork<'_>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        use std::{borrow::Cow, mem::size_of};
        const SLOT: u64 = (size_of::<rmcp::model::ListToolsResult>()
            + size_of::<rmcp::model::Tool>()
            + size_of::<rmcp::model::Resource>()
            + size_of::<rmcp::model::ResourceTemplate>()
            + size_of::<rmcp::model::Prompt>()
            + size_of::<rmcp::model::PromptArgument>()
            + size_of::<rmcp::model::PromptMessage>()
            + size_of::<rmcp::model::ContentBlock>()
            + size_of::<rmcp::model::ResourceContents>()
            + size_of::<rmcp::model::Icon>()
            + size_of::<rmcp::model::CallToolRequestParams>()
            + size_of::<rmcp::model::ReadResourceRequestParams>()
            + size_of::<rmcp::model::GetPromptRequestParams>()
            + size_of::<rmcp::model::CallToolResult>()
            + size_of::<(String, Value)>()) as u64;
        let remaining = work.remaining()?;
        if remaining.peak_allocation_bytes < SLOT {
            return Err(error(
                Some(work),
                ErrorCode::Invalid,
                "MCP typed quota unavailable",
            ));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut held = None;
        let decoded = crate::json_from_value_bounded::<T, _>(
            Cow::Borrowed(value),
            crate::JsonBounds {
                maximum_bytes: remaining.peak_allocation_bytes,
                maximum_items: remaining.items_examined,
                maximum_depth: 256,
            },
            0,
            Some(ErrorCode::Invalid),
            crate::JsonValueSchema::None,
            &mut |event| {
                if let crate::JsonCodecCharge::Admission { value_shape, .. } = event {
                    let owned = crate::executor::json::json_typed_value_decode_owned_bytes(
                        value_shape,
                        SLOT,
                    )?
                    .checked_add(value_shape.input_bytes().checked_mul(16).ok_or_else(|| {
                        error(Some(work), ErrorCode::Invalid, "MCP typed quota overflow")
                    })?)
                    .and_then(|bytes| bytes.checked_add((16 * size_of::<(String, Value)>()) as u64))
                    .ok_or_else(|| {
                        error(Some(work), ErrorCode::Invalid, "MCP typed quota overflow")
                    })?;
                    held = Some(io.reserve_decode(owned)?);
                    Ok(())
                } else {
                    io.codec_charge(event, false)
                }
            },
        )
        .map(drop);
        return match decoded {
            Err(Error::NativeJson {
                code,
                source,
                custody: None,
            }) => {
                let (source, custody) = crate::filesystem::native_decoded((
                    source,
                    held.ok_or_else(|| {
                        error(
                            Some(work),
                            ErrorCode::Storage,
                            "MCP typed admission lacks ownership",
                        )
                    })?,
                ))?;
                Err(Error::NativeJson {
                    code,
                    source,
                    custody: Some(custody),
                })
            }
            result => result,
        };
    }
    let _ = work;
    T::deserialize(value)
        .map(drop)
        .map_err(|source| Error::Invalid(source.to_string()))
}

#[cfg(test)]
mod tests;
