use super::McpToolTransport;
use crate::{
    Error, NativeWork, Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;
use std::sync::Arc;

pub(super) struct RemoteTool {
    pub(super) server: String,
    pub(super) name: String,
    pub(super) output_schema: Option<Value>,
    pub(super) transport: Arc<dyn McpToolTransport>,
}
impl RemoteTool {
    pub(super) fn result(&self, mut result: rmcp::model::CallToolResult) -> Result<ToolResult> {
        if result.is_error != Some(true)
            && let Some(schema) = &self.output_schema
        {
            let structured = result
                .structured_content
                .as_ref()
                .ok_or_else(|| Error::Invalid("MCP structured result missing".into()))?;
            crate::executor::validate_json_schema_value(
                schema,
                structured,
                "MCP output",
                &mut None,
            )?;
        }
        let structured = result.structured_content.take();
        let mut value =
            serde_json::to_value(result).map_err(|error| Error::Invalid(error.to_string()))?;
        let fields = value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("MCP tool result must be an object".into()))?;
        fields.retain(|key, value| {
            matches!(
                key.as_str(),
                "content" | "structuredContent" | "isError" | "_meta"
            ) && (key != "_meta" || !value.is_null())
        });
        fields.entry("isError").or_insert(Value::Bool(false));
        if let Some(structured) = structured {
            fields.insert("structuredContent".into(), structured);
        }
        Ok(ToolResult { value })
    }
}
impl ToolExecutor for RemoteTool {
    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
    ) -> Result<()> {
        if context.is_some_and(|context| {
            context.operation_id() != invocation.operation_id
                || context.call_id() != invocation.call_id
        }) {
            return Err(Error::Unauthorized(
                "MCP tool context identity mismatch".into(),
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "tool executor has no native authorization producer",
            });
        }
        let _ = work;
        let scope = scope.or_else(|| context.map(|context| context.task().scope()));
        if !scope.is_some_and(|scope| {
            scope
                .grants()
                .iter()
                .any(|grant| grant.strip_prefix("mcp:call:") == Some(self.server.as_str()))
        }) {
            return Err(Error::Unauthorized("MCP server capability absent".into()));
        }
        Ok(())
    }
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            if context.is_some() {
                self.authorize(None, context.as_ref(), &invocation, None)?;
            }
            self.result(
                self.transport
                    .call(
                        invocation.operation_id,
                        &self.name,
                        invocation.arguments,
                        context,
                    )
                    .await?,
            )
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            if context.is_some() {
                self.authorize(None, context.as_ref(), &invocation, None)?;
            }
            self.transport
                .reconcile(invocation.operation_id, context)
                .await?
                .map(|result| self.result(result))
                .transpose()
        })
    }
}
