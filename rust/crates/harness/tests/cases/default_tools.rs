//! Focused default-tool composition, dispatch, and file-selection scenarios.
use acyclic_harness::*;
use acyclic_harness::{
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry,
        ToolResult,
    },
    tools::parse_command,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

#[path = "../default_tools/catalog.rs"]
mod catalog;
#[path = "../default_tools/command.rs"]
mod command;
#[path = "../default_tools/registry.rs"]
mod registry;

struct Echo;
impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn example_tool() -> Tool {
    let schema = json!({"type":"object"});
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "example.echo".into(),
            revision: "1".into(),
            description: "Echo arguments".into(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            projection_schema: tool::json_projection_schema(schema),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(JsonToolProjection),
    }
}
