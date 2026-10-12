//! Typed resource and prompt operations over the same admitted protocol boundary.
use super::rpc::{check, checkpoint, protocol_error};
use crate::{Error, NativeWork, OperationId, Result, wire::ErrorCode};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

/// Validates service input on the supplied work ledger.
pub fn validate_params(method: &str, params: &Value, work: NativeWork<'_>) -> Result<()> {
    checkpoint(work, 1, 0)?;
    if !params.is_object() {
        return Err(protocol_error(
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
                checkpoint(work, 1, 0)?;
                if params.get(key).is_some() {
                    return Err(protocol_error(
                        work,
                        ErrorCode::Unsupported,
                        "MCP stdio continuation needs its own authority owner",
                    ));
                }
            }
            check::<rmcp::model::CallToolRequestParams>(params, work)
        }
        _ => {
            return Err(protocol_error(
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
        "tools/list" => check::<super::McpToolsPage>(&value, work),
        "tools/call" => check::<rmcp::model::CallToolResult>(&value, work),
        _ => {
            return Err(protocol_error(
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

    fn service_cases() -> [(&'static str, Value, Value); 5] {
        [
            (
                "resources/list",
                json!({"resources":[{"uri":"file:///exact","name":"external"}]}),
                json!({"resources":[{"uri":9,"name":"external"}]}),
            ),
            (
                "resources/templates/list",
                json!({"resourceTemplates":[{"uriTemplate":"file:///{name}","name":"template"}]}),
                json!({"resourceTemplates":[{"uriTemplate":[],"name":"template"}]}),
            ),
            (
                "resources/read",
                json!({"contents":[{"uri":"file:///exact","blob":"AAEC","mimeType":"application/octet-stream"}]}),
                json!({"contents":[{"uri":"file:///exact","blob":false}]}),
            ),
            (
                "prompts/list",
                json!({"prompts":[{"name":"external","arguments":[{"name":"input","required":true}]}]}),
                json!({"prompts":[{"name":"external","arguments":[{"required":true}]}]}),
            ),
            (
                "prompts/get",
                json!({"messages":[{"role":"user","content":{"type":"resource","resource":{"uri":"file:///exact","text":"external"}}}]}),
                json!({"messages":[{"role":"system","content":{"type":"text","text":"not authority"}}]}),
            ),
        ]
    }

    #[test]
    fn five_services_validate_maintained_dtos_without_projecting_authority() {
        for (method, valid, invalid) in service_cases() {
            assert_eq!(
                validate_result(method, valid.clone(), None).expect("closed valid DTO"),
                valid
            );
            assert!(validate_result(method, invalid, None).is_err(), "{method}");
        }
        assert!(validate_result("sampling/createMessage", json!({}), None).is_err());
        assert!(validate_params("resources/read", &json!({"uri":9}), None).is_err());
        assert!(
            validate_params(
                "prompts/get",
                &json!({"name":"exact","arguments":{"input":9}}),
                None
            )
            .is_err()
        );
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
