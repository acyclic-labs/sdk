//! Typed resource and prompt operations over the same admitted protocol boundary.
use crate::{Error, OperationId, Result};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use super::http::HttpMcpTransport;
use super::rpc::{Dto, NativeWork, check_dto, checkpoint, protocol_error};

/// Validates service input on the supplied work ledger.
pub fn validate_params(
    method: &str, params: &Value, work: NativeWork<'_>,
) -> Result<()> {
    checkpoint(work, 1, 0)?;
    if !params.is_object() {
        return Err(protocol_error(work, false, "MCP service parameters must be an object"));
    }
    let dto = match method {
        "resources/list" | "resources/templates/list" | "prompts/list" | "tools/list" => Dto::PaginatedParams,
        "resources/read" => Dto::ReadResourceParams,
        "prompts/get" => Dto::GetPromptParams,
        "tools/call" => {
            for key in ["task", "requestState", "inputResponses", "request_state", "input_responses"] {
                checkpoint(work, 1, 0)?;
                if params.get(key).is_some() {
                    return Err(protocol_error(work, true, "MCP stdio continuation needs its own authority owner"));
                }
            }
            Dto::CallToolParams
        },
        _ => return Err(protocol_error(work, true, "MCP service method not selected")),
    };
    check_dto(dto, params, work)
}

/// Validates protocol results using the maintained protocol library.
pub fn validate_result(
    method: &str, value: Value, work: NativeWork<'_>,
) -> Result<Value> {
    let dto = match method {
        "resources/list" => Dto::Resources,
        "resources/templates/list" => Dto::ResourceTemplates,
        "resources/read" => Dto::ReadResource,
        "prompts/list" => Dto::Prompts,
        "prompts/get" => Dto::GetPrompt,
        "tools/list" => Dto::Tools,
        "tools/call" => Dto::CallToolResult,
        _ => return Err(protocol_error(work, true, "MCP method is not an admitted service")),
    };
    check_dto(dto, &value, work)?;
    Ok(value)
}

impl HttpMcpTransport {
    /// One explicitly admitted resource discovery page; never auto-reloads.
    pub async fn list_resources(&self, operation: OperationId, cursor: Option<&str>) -> Result<rmcp::model::ListResourcesResult> {
        self.service(operation, "resources/list", cursor_params(cursor)).await
    }
    /// One explicitly admitted resource template page.
    pub async fn list_resource_templates(&self, operation: OperationId, cursor: Option<&str>) -> Result<rmcp::model::ListResourceTemplatesResult> {
        self.service(operation, "resources/templates/list", cursor_params(cursor)).await
    }
    /// Reads an exact URI under the host's existing network admission.
    pub async fn read_resource(&self, operation: OperationId, uri: &str) -> Result<rmcp::model::ReadResourceResult> {
        self.service(operation, "resources/read", json!({"uri":uri})).await
    }
    /// One explicitly admitted prompt discovery page.
    pub async fn list_prompts(&self, operation: OperationId, cursor: Option<&str>) -> Result<rmcp::model::ListPromptsResult> {
        self.service(operation, "prompts/list", cursor_params(cursor)).await
    }
    /// Gets one exact prompt. Returned messages remain external content.
    pub async fn get_prompt(&self, operation: OperationId, name: &str, arguments: std::collections::BTreeMap<String, String>) -> Result<rmcp::model::GetPromptResult> {
        self.service(operation, "prompts/get", json!({"name":name,"arguments":arguments})).await
    }
    async fn service<T: DeserializeOwned>(&self, operation: OperationId, method: &str, params: Value) -> Result<T> {
        validate_params(method, &params, None)?;
        serde_json::from_value(validate_result(method, self.rpc(operation, method, params).await?, None)?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }
}
fn cursor_params(cursor: Option<&str>) -> Value {
    cursor.map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service_cases() -> [(&'static str, Value, Value); 5] {
        [
            ("resources/list", json!({"resources":[{"uri":"file:///exact","name":"external"}]}), json!({"resources":[{"uri":9,"name":"external"}]})),
            ("resources/templates/list", json!({"resourceTemplates":[{"uriTemplate":"file:///{name}","name":"template"}]}), json!({"resourceTemplates":[{"uriTemplate":[],"name":"template"}]})),
            ("resources/read", json!({"contents":[{"uri":"file:///exact","blob":"AAEC","mimeType":"application/octet-stream"}]}), json!({"contents":[{"uri":"file:///exact","blob":false}]})),
            ("prompts/list", json!({"prompts":[{"name":"external","arguments":[{"name":"input","required":true}]}]}), json!({"prompts":[{"name":"external","arguments":[{"required":true}]}]})),
            ("prompts/get", json!({"messages":[{"role":"user","content":{"type":"resource","resource":{"uri":"file:///exact","text":"external"}}}]}), json!({"messages":[{"role":"system","content":{"type":"text","text":"not authority"}}]})),
        ]
    }

    #[test]
    fn five_services_validate_maintained_dtos_without_projecting_authority() {
        for (method, valid, invalid) in service_cases() {
            assert_eq!(validate_result(method, valid.clone(), None).expect("closed valid DTO"), valid);
            assert!(validate_result(method, invalid, None).is_err(), "{method}");
        }
        assert!(validate_result("sampling/createMessage", json!({}), None).is_err());
        assert!(validate_params("resources/read", &json!({"uri":9}), None).is_err());
        assert!(validate_params("prompts/get", &json!({"name":"exact","arguments":{"input":9}}), None).is_err());
        assert!(validate_params("tools/call", &json!({"name":"exact","arguments":{}}), None).is_ok());
        for key in ["task", "requestState", "inputResponses", "request_state", "input_responses"] {
            let mut params = json!({"name":"exact","arguments":{}});
            params.as_object_mut().expect("params").insert(key.into(), Value::Null);
            assert!(matches!(validate_params("tools/call", &params, None), Err(Error::Unsupported(_))));
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn finite_seven_method_validation_uses_original_ledger_and_retires_typed_temporaries() {
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            peak_allocation_bytes: 16 << 20, items_examined: 10_000,
            allocation_operations: 32, ..Default::default()
        }, &acyclic_fs::CancellationToken::new()).expect("original finite work");
        let baseline = work.live_allocation_bytes();
        let before = work.receipt();
        for (method, valid, invalid) in service_cases() {
            validate_result(method, valid, Some(&work)).expect("bounded closed DTO");
            assert_eq!(work.live_allocation_bytes(), baseline);
            let error = validate_result(method, invalid, Some(&work)).expect_err("malformed result");
            assert!(matches!(&error, Error::NativeDecode { custody: Some(_), .. }));
            assert!(work.live_allocation_bytes() > baseline);
            drop(error);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        validate_result("tools/list", json!({"tools":[]}), Some(&work)).expect("tool discovery");
        validate_result("tools/call", json!({
            "content":[{"type":"image","data":"AAEC","mimeType":"image/png"}],
            "structuredContent":null
        }), Some(&work)).expect("binary and present null tool result");
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert!(work.receipt().items_examined > before.items_examined);
        work.cancellation().cancel();
        assert!(matches!(validate_params("tools/list", &json!({}), Some(&work)),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    }
}
