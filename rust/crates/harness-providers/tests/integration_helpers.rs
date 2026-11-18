use std::sync::Arc;

pub(super) type TestResult<T = ()> =
    std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) fn echo_tool(name: &str) -> acyclic_harness_core::tool::Tool {
    use acyclic_harness_core::tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
    };
    struct Echo;
    impl ToolExecutor for Echo {
        fn execute<'a>(
            &'a self,
            invocation: ToolInvocation,
            _: Option<acyclic_harness_core::runtime::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness_core::Result<ToolResult>>
        {
            Box::pin(async move {
                Ok(ToolResult {
                    value: invocation.arguments,
                })
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<acyclic_harness_core::runtime::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness_core::Result<Option<ToolResult>>>
        {
            Box::pin(async { Ok(None) })
        }
    }
    let schema = serde_json::json!({"type":"object"});
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            projection_schema: acyclic_harness_core::tool::json_projection_schema(schema),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(JsonToolProjection),
    }
}
