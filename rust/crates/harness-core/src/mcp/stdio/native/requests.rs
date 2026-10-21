use super::*;
use rmcp::model::*;

pub(super) fn request(method: &str, params: Value) -> Result<ClientRequest> {
    if !params.is_object() {
        return Err(Error::Invalid("MCP parameters must be an object".into()));
    }
    let invalid = |error: serde_json::Error| Error::Invalid(error.to_string());
    Ok(match method {
        "tools/list" => ClientRequest::ListToolsRequest(ListToolsRequest::with_param(
            serde_json::from_value(params).map_err(invalid)?,
        )),
        "resources/list" => ClientRequest::ListResourcesRequest(ListResourcesRequest::with_param(
            serde_json::from_value(params).map_err(invalid)?,
        )),
        "resources/templates/list" => {
            ClientRequest::ListResourceTemplatesRequest(ListResourceTemplatesRequest::with_param(
                serde_json::from_value(params).map_err(invalid)?,
            ))
        }
        "resources/read" => ClientRequest::ReadResourceRequest(ReadResourceRequest::new(
            serde_json::from_value(params).map_err(invalid)?,
        )),
        "prompts/list" => ClientRequest::ListPromptsRequest(ListPromptsRequest::with_param(
            serde_json::from_value(params).map_err(invalid)?,
        )),
        "prompts/get" => ClientRequest::GetPromptRequest(GetPromptRequest::new(
            serde_json::from_value(params).map_err(invalid)?,
        )),
        "tools/call" => {
            crate::mcp::services::reject_tool_continuation(&params, None)?;
            ClientRequest::CallToolRequest(CallToolRequest::new(
                serde_json::from_value(params).map_err(invalid)?,
            ))
        }
        _ => return Err(Error::Unsupported("MCP service method not selected".into())),
    })
}

pub(super) fn result(method: &str, result: ServerResult) -> Result<Value> {
    let valid = matches!(
        (method, &result),
        ("tools/list", ServerResult::ListToolsResult(_))
            | ("resources/list", ServerResult::ListResourcesResult(_))
            | (
                "resources/templates/list",
                ServerResult::ListResourceTemplatesResult(_)
            )
            | ("resources/read", ServerResult::ReadResourceResult(_))
            | ("prompts/list", ServerResult::ListPromptsResult(_))
            | ("prompts/get", ServerResult::GetPromptResult(_))
            | ("tools/call", ServerResult::CallToolResult(_))
    );
    if !valid {
        return Err(Error::Invalid(
            "MCP service returned another result type".into(),
        ));
    }
    serde_json::to_value(result).map_err(|error| Error::Invalid(error.to_string()))
}
