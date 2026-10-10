//! Typed resource and prompt operations over the same admitted protocol boundary.
use crate::{Error, OperationId, Result};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use super::http::HttpMcpTransport;

fn check<T: DeserializeOwned>(value: &Value) -> Result<()> {
    T::deserialize(value)
        .map(|_| ())
        .map_err(|error| Error::Invalid(error.to_string()))
}

/// Validates service input before network dispatch. The allowlist cannot be used
/// to invoke arbitrary callbacks, continuations or remote effect methods.
pub fn validate_params(method: &str, params: &Value) -> Result<()> {
    if !params.is_object() { return Err(Error::Invalid("MCP service parameters must be an object".into())); }
    match method {
        "resources/list" | "resources/templates/list" | "prompts/list" | "tools/list" => check::<rmcp::model::PaginatedRequestParams>(params),
        "resources/read" => check::<rmcp::model::ReadResourceRequestParams>(params),
        "prompts/get" => check::<rmcp::model::GetPromptRequestParams>(params),
        _ => Err(Error::Unsupported("MCP service method not selected".into())),
    }
}

/// Validates protocol results using the maintained protocol library. Neither
/// resources nor prompts are instructions or grants; hosts explicitly project them.
pub fn validate_result(method: &str, value: Value) -> Result<Value> {
    match method {
        "resources/list" => check::<rmcp::model::ListResourcesResult>(&value)?,
        "resources/templates/list" => check::<rmcp::model::ListResourceTemplatesResult>(&value)?,
        "resources/read" => check::<rmcp::model::ReadResourceResult>(&value)?,
        "prompts/list" => check::<rmcp::model::ListPromptsResult>(&value)?,
        "prompts/get" => check::<rmcp::model::GetPromptResult>(&value)?,
        "tools/list" => check::<super::McpToolsPage>(&value)?,
        "tools/call" => super::rpc::validate_tool_result(&value)?,
        _ => return Err(Error::Unsupported("MCP method is not an admitted service".into())),
    }
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
    /// Validated service operation with opt-in callbacks under this exact task.
    /// The host admits the enclosing request; returned prompts/resources grant no authority.
    pub async fn service_with_context(&self, context: crate::runtime::ToolContext, method: &str, params: Value) -> Result<Value> {
        validate_params(method, &params)?;
        validate_result(method, self.rpc_with_context(context, method, params).await?)
    }
    async fn service<T: DeserializeOwned>(&self, operation: OperationId, method: &str, params: Value) -> Result<T> {
        validate_params(method, &params)?;
        serde_json::from_value(validate_result(method, self.rpc(operation, method, params).await?)?)
            .map_err(|error| Error::Invalid(error.to_string()))
    }
}
fn cursor_params(cursor: Option<&str>) -> Value {
    cursor.map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}))
}
