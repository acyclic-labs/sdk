//! Shared JSON-RPC response boundary for HTTP and approved stdio.
use super::McpToolResult;
use crate::{Error, OperationId, Result};
use rmcp::model::{JsonRpcRequest, RequestId};
use serde::Deserialize;
use serde_json::{Map, Value};

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type NativeWork<'a> = Option<&'a acyclic_fs::NativeMaterializationWork>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type NativeWork<'a> = Option<&'a ()>;

pub(super) fn rpc_result(
    mut value: Value, operation: OperationId, work: NativeWork<'_>,
) -> Result<Value> {
    if field(&value, "method", work)?.is_some()
        || (field(&value, "result", work)?.is_some() == field(&value, "error", work)?.is_some())
        || field(&value, "jsonrpc", work)?.and_then(Value::as_str) != Some("2.0")
        || field(&value, "error", work)?.is_some()
    {
        return Err(protocol_error(work, false, "MCP response identity or envelope mismatch"));
    }
    // Compare the actual admitted UUID spelling, without allocating a new ID.
    let mut spelling = [0; 36];
    let identity = uuid::Uuid::from_bytes(operation.into_bytes());
    let expected: &str = identity.hyphenated().encode_lower(&mut spelling);
    if field(&value, "id", work)?.and_then(Value::as_str)
        != Some(expected)
    {
        return Err(protocol_error(work, false, "MCP response identity mismatch"));
    }
    checkpoint(work, 1, 0)?;
    value.as_object_mut().and_then(|object| object.remove("result"))
        .ok_or_else(|| protocol_error(work, false, "MCP response identity or envelope mismatch"))
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
    if field(value, "method", work)?.and_then(Value::as_str).is_none()
        || field(value, "result", work)?.is_some() || field(value, "error", work)?.is_some()
    {
        return Err(protocol_error(work, false, "invalid MCP notification"));
    }
    if field(value, "id", work)?.is_some() {
        return Err(protocol_error(work, true, "MCP server request needs a callback router"));
    }
    if field(value, "jsonrpc", work)?.and_then(Value::as_str) != Some("2.0") {
        return Err(protocol_error(work, false, "invalid MCP notification"));
    }
    Ok(())
}

pub(super) fn initialization(value: &Value, work: NativeWork<'_>) -> Result<()> {
    check_dto(Dto::Initialize, value, work)?;
    let info = field(value, "serverInfo", work)?.unwrap_or(&Value::Null);
    if field(value, "protocolVersion", work)?.and_then(Value::as_str) != Some(super::PROTOCOL_VERSION)
        || !field(value, "capabilities", work)?.is_some_and(Value::is_object)
        || field(info, "name", work)?.and_then(Value::as_str).is_none()
        || field(info, "version", work)?.and_then(Value::as_str).is_none()
    {
        return Err(protocol_error(work, true, "MCP initialization contract unsupported"));
    }
    Ok(())
}

fn field<'a>(value: &'a Value, key: &str, work: NativeWork<'_>) -> Result<Option<&'a Value>> {
    checkpoint(work, 1, 0)?;
    Ok(value.get(key))
}

pub(super) fn decode_tool_result(value: Value) -> Result<McpToolResult> {
    check_dto(Dto::CallToolResult, &value, None)?;
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

#[derive(Clone, Copy)]
pub(super) enum Dto {
    Initialize, PaginatedParams, CallToolParams, ReadResourceParams, GetPromptParams,
    Resources, ResourceTemplates, ReadResource, Prompts, GetPrompt, Tools, CallToolResult,
}

pub(super) fn check_dto(dto: Dto, value: &Value, work: NativeWork<'_>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let custody = if let Some(work) = work {
        let quota = dto_quota(value, work, 0)?;
        Some(work.retain_owned(quota).map_err(native_work_error)?)
    } else { None };
    macro_rules! check {
        ($ty:ty) => { <$ty>::deserialize(value).map(drop) };
    }
    let decoded = match dto {
        Dto::Initialize => check!(super::McpInitializeResult),
        Dto::PaginatedParams => check!(rmcp::model::PaginatedRequestParams),
        Dto::CallToolParams => check!(rmcp::model::CallToolRequestParams),
        Dto::ReadResourceParams => check!(rmcp::model::ReadResourceRequestParams),
        Dto::GetPromptParams => check!(rmcp::model::GetPromptRequestParams),
        Dto::Resources => check!(rmcp::model::ListResourcesResult),
        Dto::ResourceTemplates => check!(rmcp::model::ListResourceTemplatesResult),
        Dto::ReadResource => check!(rmcp::model::ReadResourceResult),
        Dto::Prompts => check!(rmcp::model::ListPromptsResult),
        Dto::GetPrompt => check!(rmcp::model::GetPromptResult),
        Dto::Tools => check!(super::McpToolsPage),
        Dto::CallToolResult => check!(rmcp::model::CallToolResult),
    };
    match decoded {
        Ok(()) => {
            // Every typed temporary has physically dropped before its quota.
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            drop(custody);
            Ok(())
        }
        Err(source) => {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() { return Err(Error::NativeDecode { source, custody }); }
            Err(Error::Invalid(source.to_string()))
        }
    }
}

pub(super) fn protocol_error(work: NativeWork<'_>, unsupported: bool, message: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: if unsupported { crate::wire::ErrorCode::Unsupported } else { crate::wire::ErrorCode::Invalid },
            reason: message,
        };
    }
    let _ = work;
    if unsupported { Error::Unsupported(message.into()) } else { Error::Invalid(message.into()) }
}

pub(super) fn checkpoint(work: NativeWork<'_>, items: u64, bytes: u64) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: items, bytes_copied: bytes, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
    }
    let _ = (work, items, bytes);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn dto_quota(value: &Value, work: &acyclic_fs::NativeMaterializationWork, depth: u32) -> Result<u64> {
    use std::mem::size_of;
    checkpoint(Some(work), 1, 0)?;
    if depth > 256 { return Err(protocol_error(Some(work), false, "MCP typed value exceeds structural depth")); }
    let typed_slot = size_of::<super::McpInitializeResult>()
        + size_of::<super::McpToolDefinition>() + size_of::<super::McpToolsPage>()
        + size_of::<rmcp::model::Resource>() + size_of::<rmcp::model::ResourceTemplate>()
        + size_of::<rmcp::model::Prompt>() + size_of::<rmcp::model::PromptArgument>()
        + size_of::<rmcp::model::PromptMessage>() + size_of::<rmcp::model::ContentBlock>()
        + size_of::<rmcp::model::ResourceContents>() + size_of::<rmcp::model::Icon>()
        + size_of::<rmcp::model::CallToolRequestParams>()
        + size_of::<rmcp::model::ReadResourceRequestParams>()
        + size_of::<rmcp::model::GetPromptRequestParams>()
        + size_of::<rmcp::model::CallToolResult>()
        + size_of::<(String, Value)>();
    let overflow = || protocol_error(Some(work), false, "MCP typed quota overflow");
    let mut quota = (typed_slot as u64).checked_mul(8)
        .and_then(|bytes| bytes.checked_add(16 * size_of::<(String, Value)>() as u64))
        .ok_or_else(overflow)?;
    match value {
        Value::String(string) => {
            // Inspect the already-owned UTF-8 length, without rescanning Unicode or reporting a copy that an ignored field may never perform.
            quota = quota.checked_add((string.len() as u64).checked_mul(16).ok_or_else(overflow)?).ok_or_else(overflow)?;
        }
        Value::Number(number) => {
            struct Length(u64);
            impl std::fmt::Write for Length {
                fn write_str(&mut self, value: &str) -> std::fmt::Result {
                    self.0 = self.0.checked_add(value.len() as u64).ok_or(std::fmt::Error)?;
                    Ok(())
                }
            }
            let mut length = Length(0);
            std::fmt::write(&mut length, format_args!("{number}")).map_err(|_| overflow())?;
            quota = quota.checked_add(length.0.checked_mul(16).ok_or_else(overflow)?).ok_or_else(overflow)?;
        }
        Value::Array(values) => for value in values {
            quota = quota.checked_add(dto_quota(value, work, depth + 1)?).ok_or_else(overflow)?;
        },
        Value::Object(values) => for (key, value) in values {
            checkpoint(Some(work), 1, 0)?;
            quota = quota.checked_add((key.len() as u64).checked_mul(16).ok_or_else(overflow)?)
                .and_then(|bytes| bytes.checked_add(8 * size_of::<(String, Value)>() as u64)).ok_or_else(overflow)?;
            quota = quota.checked_add(dto_quota(value, work, depth + 1)?).ok_or_else(overflow)?;
        },
        _ => {}
    }
    Ok(quota)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn response_moves_original_payload_and_matches_exact_uuid() {
        let operation = OperationId::from_bytes([7; 16]);
        let payload = String::from("original response");
        let pointer = payload.as_ptr();
        let mut value = json!({"jsonrpc":"2.0","id":operation.to_string()});
        value.as_object_mut().expect("envelope").insert("result".into(), Value::String(payload));
        let result = rpc_result(value, operation, None).expect("matching response");
        assert_eq!(result.as_str().expect("original string").as_ptr(), pointer);
        let foreign = OperationId::from_bytes([8; 16]);
        assert!(rpc_result(json!({"jsonrpc":"2.0","id":foreign.to_string(),"result":null}), operation, None).is_err());
        assert!(rpc_result(json!({"jsonrpc":"2.0","id":operation.to_string(),"result":null,"error":{}}), operation, None).is_err());
        assert!(rpc_result(json!({"jsonrpc":"1.0","id":operation.to_string(),"result":null}), operation, None).is_err());
    }

    #[test]
    fn notification_and_initialization_share_the_closed_policy() {
        notification(&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}), None).expect("hint only");
        assert!(matches!(notification(&json!({"jsonrpc":"2.0","method":"sampling/createMessage","id":"callback"}), None), Err(Error::Unsupported(_))));
        assert!(notification(&json!({"jsonrpc":"2.0","method":7}), None).is_err());
        initialization(&json!({
            "protocolVersion":super::super::PROTOCOL_VERSION,
            "capabilities":{},"serverInfo":{"name":"external","version":"1"},"instructions":"not authority"
        }), None).expect("supported negotiation");
        assert!(initialization(&json!({
            "protocolVersion":super::super::PROTOCOL_VERSION,"capabilities":[],
            "serverInfo":{"name":"external","version":"1"}
        }), None).is_err());
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_work(bytes: u64, items: u64) -> acyclic_fs::NativeMaterializationWork {
        acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            peak_allocation_bytes: bytes, items_examined: items,
            allocation_operations: 32, ..Default::default()
        }, &acyclic_fs::CancellationToken::new()).expect("finite original operation")
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn closed_dto_quota_denial_precedes_serde_and_does_not_report_allocator_calls() {
        let work = native_work(acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1, 100);
        let before = work.receipt();
        assert!(matches!(check_dto(Dto::GetPrompt, &json!({"messages":[]}), Some(&work)),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: None })));
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        let bounded = native_work(1 << 20, 1);
        assert!(check_dto(Dto::ReadResource, &json!({"contents":[{"uri":"x","text":"y"}]}), Some(&bounded)).is_err());
        assert_eq!(bounded.receipt().items_examined, 1);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn malformed_typed_cause_keeps_original_quota_until_error_drop() {
        let work = native_work(1 << 20, 1000);
        let original = work.retain_owned(0).expect("original ledger witness");
        let baseline = work.remaining().expect("remaining").peak_allocation_bytes;
        let before = work.receipt();
        let live_before = work.live_allocation_bytes();
        let failure = check_dto(Dto::GetPrompt, &json!({
            "messages":[{"role":"untrusted\u{1}role","content":{"type":"text","text":"external"}}]
        }), Some(&work)).expect_err("invalid role");
        match &failure {
            Error::NativeDecode { source, custody: Some(custody) } => {
                assert!(source.is_data());
                assert!(custody.bytes() > 0);
                assert!(custody.shares_ledger(&original));
            }
            other => panic!("actual typed cause was replaced: {other:?}"),
        }
        assert!(work.remaining().expect("retained error").peak_allocation_bytes < baseline);
        work.cancellation().cancel();
        drop(failure);
        assert_eq!(original.work().receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.live_allocation_bytes(), live_before);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn foreign_identity_and_callback_keep_static_original_classification() {
        let work = native_work(1 << 20, 100);
        let operation = OperationId::from_bytes([2; 16]);
        assert!(matches!(rpc_result(json!({
            "jsonrpc":"2.0","id":OperationId::from_bytes([3; 16]).to_string(),"result":{}
        }), operation, Some(&work)), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert!(matches!(notification(&json!({
            "jsonrpc":"2.0","method":"roots/list","id":"server"
        }), Some(&work)), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
    }
}
