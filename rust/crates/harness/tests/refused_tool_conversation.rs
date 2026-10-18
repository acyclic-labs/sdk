//! Review P1: every settled exchange, refused before dispatch or failed after it, reaches the
//! canonical conversation through one publication path, and the next turn sees it exactly.
#![cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]

use acyclic_harness::{
    AgentId, Error, OperationId, Result,
    context::ModelTokenCount,
    filesystem::MemoryHarnessStorage,
    model::{
        Model, ModelContentPart, ModelEvent, ModelProvider, PreparedModelRequest, ToolCall,
        ToolResultContent,
    },
    runtime::ToolContext,
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Echoes its arguments, and fails definitively when asked to.
#[derive(Default)]
struct Echo(AtomicUsize);

impl ToolExecutor for Echo {
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            if invocation.arguments.get("fail").is_some() {
                return Err(Error::Conflict("echo declined".into()));
            }
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }
}

impl ToolProjection for Echo {
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        _: acyclic_harness::NativeWork<'_>,
    ) -> Result<(Value, acyclic_harness::NativeCustody)> {
        Ok((json!({"kind":"json", "value":result.value}), None))
    }
}

fn counter(request: &PreparedModelRequest) -> Result<ModelTokenCount> {
    Ok(ModelTokenCount::from_upper_bounds(
        request.request_digest(),
        128,
        vec![128; request.request().messages.len()],
    ))
}

fn call(id: &str, name: &str, arguments: Value) -> ScriptedEvent {
    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
        id.into(),
        name.into(),
        arguments,
    )))
}

/// The ordered exchange in the provider's last request: calls, then results with their text.
fn exchange(provider: &ScriptedModelProvider) -> Vec<String> {
    provider
        .requests()
        .last()
        .expect("model request")
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolCall(call) => Some(format!("call:{}", call.call_id)),
            ModelContentPart::ToolResult {
                call_id,
                content: ToolResultContent::Error { message, .. },
                ..
            } => Some(format!("error:{call_id}:{message}")),
            ModelContentPart::ToolResult { call_id, .. } => Some(format!("result:{call_id}")),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn refused_and_failed_calls_publish_their_exchange_for_the_next_turn() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let echo = Arc::new(Echo::default());
    let first = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                call("ghost-call", "ghost", json!({})),
                call("bad-call", "echo", json!(7)),
                call("fail-call", "echo", json!({"fail":true})),
                call("good-call", "echo", json!({"value":1})),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Text("first complete".into()),
        ])
        .with_token_counter(counter),
    );
    let build = |provider: Arc<dyn ModelProvider>| {
        storage
            .builder()
            .model(
                Model::new("fixture".into(), "refusals".into(), "1".into(), json!({}))?,
                provider,
            )
            .tool(Tool {
                hook_names: &[],
                definition: ToolDefinition {
                    name: "echo".into(),
                    revision: "1".into(),
                    description: "echo".into(),
                    input_schema: json!({"type":"object"}),
                    output_schema: json!({"type":"object"}),
                    projection_schema: acyclic_harness::tool::json_projection_schema(
                        json!({"type":"object"}),
                    ),
                },
                executor: echo.clone(),
                projection: echo.clone(),
            })
            .grant("model:generate")
            .grant("tool:call:echo")
            .build()
    };
    let first_bundle = build(first.clone())?;
    let operation = OperationId::new();
    let input = storage
        .stage(operation, "first.txt", b"first", "text/plain", "first.txt")
        .await?;
    let output = storage
        .run_conversation(&first_bundle, operation, input.clone(), Vec::new(), 2)
        .await?;
    assert_eq!(output.text, "first complete");
    assert_eq!(first.requests().len(), 2);
    let delivered = exchange(&first);
    assert_eq!(delivered.len(), 8, "{delivered:#?}");
    assert_eq!(
        echo.0.load(Ordering::SeqCst),
        2,
        "only admitted calls dispatch"
    );

    // Replay publishes nothing new and dispatches nothing.
    let replay = Arc::new(ScriptedModelProvider::new([]).with_token_counter(counter));
    assert_eq!(
        storage
            .run_conversation(&build(replay.clone())?, operation, input, Vec::new(), 2)
            .await?,
        output
    );
    assert!(replay.requests().is_empty());

    // The next turn projects the committed conversation: the same calls and the same results,
    // refused, failed and completed alike, in model order.
    let next = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("second complete".into())])
            .with_token_counter(counter),
    );
    assert_eq!(
        storage
            .run_prompt(&build(next.clone())?, "next", Vec::new())
            .await?
            .text,
        "second complete"
    );
    assert_eq!(exchange(&next), delivered);
    for (index, expected) in [
        "call:ghost-call",
        "call:bad-call",
        "call:fail-call",
        "call:good-call",
        "error:ghost-call:tool `ghost` is not available; call one of the declared tools",
        "error:bad-call:tool `echo` rejected its arguments: ",
        "error:fail-call:tool executor rejected the admitted call (conflict): ",
        "result:good-call",
    ]
    .into_iter()
    .enumerate()
    {
        assert!(
            delivered[index].starts_with(expected),
            "{index}: {}",
            delivered[index]
        );
    }
    assert_eq!(echo.0.load(Ordering::SeqCst), 2);
    Ok(())
}
