//! Stdio is a native-host client adapter, never a task owner or gateway.
use super::{handoff, native};
use crate::{Error, Result};
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
    },
    service::RequestContext,
};
use serde_json::{Value, json};

/// Coding tools and task commands share one native catalog.
pub fn native_tool_catalog() -> Result<Value> {
    Ok(json!({"tools":super::command_catalog::catalog()?}))
}

fn tools() -> Result<Vec<Tool>> {
    super::command_catalog::catalog()?
        .into_iter()
        .map(|definition| {
            let Value::Object(schema) = handoff::schema(definition.input_schema) else {
                return Err(Error::Invalid("tool schema is not an object".into()));
            };
            Ok(Tool::new(definition.name, definition.description, schema))
        })
        .collect()
}

struct Tools;

impl ServerHandler for Tools {
    fn get_info(&self) -> ServerConfig {
        let mut config = ServerConfig::default();
        config.capabilities = ServerCapabilities::builder().enable_tools().build();
        config
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, rmcp::ErrorData> {
        let mut result = ListToolsResult::default();
        result.tools =
            tools().map_err(|error| rmcp::ErrorData::internal_error(error.to_string(), None))?;
        Ok(result)
    }

    async fn call_tool(
        &self,
        call: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, rmcp::ErrorData> {
        // This reads the immutable result published by PreToolUse. Reopen
        // and duplicate delivery use exactly the same operation identity.
        let result = native::read_tool_result(
            &call.name,
            Value::Object(call.arguments.unwrap_or_default()),
        )
        .await
        .and_then(super::result::value);
        let result = match result {
            Ok(value) => CallToolResult::structured(value),
            Err(error) => CallToolResult::error(vec![ContentBlock::text(error.to_string())]),
        };
        Ok(result.into())
    }
}

/// Serves the native host's MCP connection using the platform/file client.
pub async fn serve_native_tools() -> Result<()> {
    Tools
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .waiting()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}
