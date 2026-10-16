use super::mcp_script::{McpScript, Reply};
use acyclic_harness::{
    Capabilities, Error, OperationId, Result,
    conversation::Limits,
    mcp::McpToolTransport,
    runtime::{RuntimeScope, ToolContext},
    tool::{JsonToolProjection, ToolProjection, ToolRegistry},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;
use std::sync::Arc;
impl McpScript {
    pub fn bind(&self, limits: Limits) -> Result<(ToolRegistry, RuntimeScope)> {
        let mut registry = ToolRegistry::default();
        let transport: Arc<dyn McpToolTransport> = Arc::new(Transport(self.replies.clone()));
        let projection: Arc<dyn ToolProjection> = Arc::new(JsonToolProjection);
        self.catalog
            .install(&mut registry, None, &transport, &projection, 32, 262_144)?;
        Ok((
            registry,
            RuntimeScope::new(Capabilities::new(self.grants.clone()), limits)?,
        ))
    }
}

// Immutable matching has no shared cursor, lock, timing or network dependency.
struct Transport(Vec<Reply>);

impl McpToolTransport for Transport {
    fn call<'a>(
        &'a self,
        _: OperationId,
        name: &'a str,
        arguments: Value,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<rmcp::model::CallToolResult>> {
        Box::pin(async move {
            let reply = self
                .0
                .iter()
                .find(|reply| reply.name == name && reply.arguments == arguments)
                .ok_or_else(|| Error::Invalid("unscripted MCP call or arguments".into()))?;
            serde_json::from_value(reply.result.clone())
                .map_err(|error| Error::Invalid(error.to_string()))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: OperationId,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<rmcp::model::CallToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}
