use super::{McpToolResult, McpToolTransport, rpc};
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

    pub(super) fn result(&self, result: McpToolResult) -> Result<ToolResult> {
        if !result.is_error
            && let Some(schema) = &self.output_schema
        {
            let value = result
                .structured_content
                .as_ref()
                .ok_or_else(|| Error::Invalid("MCP structured result missing".into()))?;
            crate::contract::validate_json_schema_value(schema, value, "MCP output", &mut None)?;
        }
        let value =
            serde_json::to_value(result).map_err(|error| Error::Invalid(error.to_string()))?;
        rpc::check::<rmcp::model::CallToolResult>(&value, None)?;
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
