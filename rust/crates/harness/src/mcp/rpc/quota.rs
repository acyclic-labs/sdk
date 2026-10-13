use super::*;

pub(super) fn dto_quota(
    value: &Value,
    work: &acyclic_fs::NativeMaterializationWork,
    depth: u32,
) -> Result<u64> {
    use std::mem::size_of;
    charge(Some(work), 1, 0, 0)?;
    if depth > 256 {
        return Err(error(
            Some(work),
            ErrorCode::Invalid,
            "MCP typed value exceeds structural depth",
        ));
    }
    let typed_slot = size_of::<super::super::McpToolDefinition>()
        + size_of::<rmcp::model::ListToolsResult>()
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
        + size_of::<(String, Value)>();
    let overflow = || error(Some(work), ErrorCode::Invalid, "MCP typed quota overflow");
    let mut quota = (typed_slot as u64)
        .checked_mul(8)
        .and_then(|bytes| bytes.checked_add(16 * size_of::<(String, Value)>() as u64))
        .ok_or_else(overflow)?;
    match value {
        Value::String(string) => {
            quota = quota
                .checked_add((string.len() as u64).checked_mul(16).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
        }
        Value::Number(number) => {
            let length = crate::contract::json_byte_length(number, u64::MAX, &mut None)
                .map_err(|_| overflow())?;
            quota = quota
                .checked_add(length.checked_mul(16).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
        }
        Value::Array(values) => {
            for value in values {
                quota = quota
                    .checked_add(dto_quota(value, work, depth + 1)?)
                    .ok_or_else(overflow)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                charge(Some(work), 1, 0, 0)?;
                quota = quota
                    .checked_add((key.len() as u64).checked_mul(16).ok_or_else(overflow)?)
                    .and_then(|bytes| bytes.checked_add(8 * size_of::<(String, Value)>() as u64))
                    .ok_or_else(overflow)?;
                quota = quota
                    .checked_add(dto_quota(value, work, depth + 1)?)
                    .ok_or_else(overflow)?;
            }
        }
        _ => {}
    }
    Ok(quota)
}
