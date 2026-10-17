//! Focused default-tool composition, dispatch, and file-selection scenarios.
use acyclic_harness::*;
use acyclic_harness::{
    agent_loop::{AgentInput, AgentLoop, AgentOutput},
    conversation::Limits,
    filesystem::MemoryHarnessStorage,
    runtime::{Bindings, TaskDefinition},
    tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry,
        ToolResult,
    },
    tools::parse_command,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

#[path = "default_tools/command.rs"]
mod command;
#[path = "default_tools/ranges.rs"]
mod ranges;
#[path = "default_tools/registry.rs"]
mod registry;
#[path = "default_tools/catalog.rs"]
mod catalog;

struct DelegatingLoop;
impl AgentLoop for DelegatingLoop {
    fn run(
        &self,
        _: runtime::TaskContext,
        _: AgentInput,
    ) -> BoxFuture<'static, Result<AgentOutput>> {
        Box::pin(async { Ok(AgentOutput::text(String::new())) })
    }
}

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

async fn run_admitted_file_case<F, Fut>(
    storage: &MemoryHarnessStorage,
    limits: Limits,
    handler: F,
) -> Result<()>
where
    F: Fn(runtime::TaskContext) -> Fut + acyclic_stream::ProviderPlatform + 'static,
    Fut: std::future::Future<Output = Result<()>> + acyclic_stream::ProviderTask + 'static,
{
    let bundle = storage
        .builder()
        .agent_loop(Arc::new(DelegatingLoop))
        .limits(limits)
        .grant("task:spawn:file_case@1")
        .task(TaskDefinition::live(
            "file_case",
            "1",
            move |context, (): ()| handler(context),
        )?)?
        .build()?;
    let task = bundle.runtime().task::<(), ()>("file_case")?;
    assert_eq!(
        bundle.runtime().spawn(&task, ()).await?.result().await?,
        Outcome::Succeeded(())
    );
    Ok(())
}
