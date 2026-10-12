use super::McpToolTransport;
use crate::{
    Error, Result,
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
    fn authorize_context(&self, context: &ToolContext, invocation: &ToolInvocation) -> Result<()> {
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::Unauthorized(
                "MCP tool context identity mismatch".into(),
            ));
        }
        self.authorize(Some(context.task().scope()), invocation)
    }

    pub(super) fn result(&self, result: Value) -> Result<ToolResult> {
        let mut value = super::services::validate_result("tools/call", result, None)?;
        if value.get("isError").and_then(Value::as_bool) != Some(true)
            && let Some(schema) = &self.output_schema
        {
            let result = value
                .get("structuredContent")
                .ok_or_else(|| Error::Invalid("MCP structured result missing".into()))?;
            crate::contract::validate_json_schema_value(schema, result, "MCP output", &mut None)?;
        }
        if let Some(fields) = value.as_object_mut() {
            fields.retain(|key, value| {
                matches!(
                    key.as_str(),
                    "content" | "structuredContent" | "isError" | "_meta"
                ) && (key != "_meta" || !value.is_null())
            });
            fields.entry("isError").or_insert(Value::Bool(false));
        }
        Ok(ToolResult { value })
    }
}

impl ToolExecutor for RemoteTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
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
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.result(
                self.transport
                    .call(
                        invocation.operation_id,
                        &self.name,
                        invocation.arguments,
                        None,
                    )
                    .await?,
            )
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.transport
                .reconcile(invocation.operation_id, None)
                .await?
                .map(|result| self.result(result))
                .transpose()
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize_context(&context, &invocation)?;
            self.result(
                self.transport
                    .call(
                        invocation.operation_id,
                        &self.name,
                        invocation.arguments,
                        Some(context),
                    )
                    .await?,
            )
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize_context(&context, &invocation)?;
            self.transport
                .reconcile(invocation.operation_id, Some(context))
                .await?
                .map(|result| self.result(result))
                .transpose()
        })
    }
}
