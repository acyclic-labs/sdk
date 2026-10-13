//! Typed resource and prompt operations over the same admitted protocol boundary.
use super::{charge, error, rpc::check};
use crate::{Error, NativeWork, Result, wire::ErrorCode};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

/// Validates service input on the supplied work ledger.
pub fn validate_params(method: &str, params: &Value, work: NativeWork<'_>) -> Result<()> {
    charge(work, 1, 0, 0)?;
    if !params.is_object() {
        return Err(error(
            work,
            ErrorCode::Invalid,
            "MCP service parameters must be an object",
        ));
    }
    match method {
        "resources/list" | "resources/templates/list" | "prompts/list" | "tools/list" => {
            check::<rmcp::model::PaginatedRequestParams>(params, work)
        }
        "resources/read" => check::<rmcp::model::ReadResourceRequestParams>(params, work),
        "prompts/get" => check::<rmcp::model::GetPromptRequestParams>(params, work),
        "tools/call" => {
            for key in [
                "task",
                "requestState",
                "inputResponses",
                "request_state",
                "input_responses",
            ] {
                charge(work, 1, 0, 0)?;
                if params.get(key).is_some() {
                    return Err(error(
                        work,
                        ErrorCode::Unsupported,
                        "MCP stdio continuation needs its own authority owner",
                    ));
                }
            }
            check::<rmcp::model::CallToolRequestParams>(params, work)
        }
        _ => {
            return Err(error(
                work,
                ErrorCode::Unsupported,
                "MCP service method not selected",
            ));
        }
    }
}

/// Validates protocol results using the maintained protocol library.
pub fn validate_result(method: &str, value: Value, work: NativeWork<'_>) -> Result<Value> {
    match method {
        "resources/list" => check::<rmcp::model::ListResourcesResult>(&value, work),
        "resources/templates/list" => {
            check::<rmcp::model::ListResourceTemplatesResult>(&value, work)
        }
        "resources/read" => check::<rmcp::model::ReadResourceResult>(&value, work),
        "prompts/list" => check::<rmcp::model::ListPromptsResult>(&value, work),
        "prompts/get" => check::<rmcp::model::GetPromptResult>(&value, work),
        "tools/list" => {
            if let Some(tools) = value.get("tools").and_then(Value::as_array) {
                for tool in tools {
                    charge(work, 1, 0, 0)?;
                    if tool
                        .get("description")
                        .is_some_and(|description| !description.is_string())
                    {
                        return Err(error(
                            work,
                            ErrorCode::Invalid,
                            "invalid MCP tool description",
                        ));
                    }
                }
            }
            check::<rmcp::model::ListToolsResult>(&value, work)
        }
        "tools/call" => {
            charge(work, 2, 0, 0)?;
            if !value.get("content").is_some_and(Value::is_array)
                || value
                    .get("isError")
                    .is_some_and(|error| !error.is_boolean())
            {
                return Err(error(
                    work,
                    ErrorCode::Invalid,
                    "invalid MCP tool result envelope",
                ));
            }
            check::<rmcp::model::CallToolResult>(&value, work)
        }
        _ => {
            return Err(error(
                work,
                ErrorCode::Unsupported,
                "MCP method is not an admitted service",
            ));
        }
    }?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuations_are_rejected_before_dispatch() {
        assert!(
            validate_params("tools/call", &json!({"name":"exact","arguments":{}}), None).is_ok()
        );
        for key in [
            "task",
            "requestState",
            "inputResponses",
            "request_state",
            "input_responses",
        ] {
            let mut params = json!({"name":"exact","arguments":{}});
            params
                .as_object_mut()
                .expect("params")
                .insert(key.into(), Value::Null);
            assert!(matches!(
                validate_params("tools/call", &params, None),
                Err(Error::Unsupported(_))
            ));
        }
    }
}
