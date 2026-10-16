#![cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]

use acyclic_harness::{
    AgentId, OperationId, Result,
    context::ModelTokenCount,
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelContentPart, ModelEvent, ModelProvider, PreparedModelRequest, ToolCall},
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
    Ok(ModelTokenCount {
        request_digest: request.request_digest(),
        fixed_tokens: 128,
        message_tokens: vec![128; request.request().messages.len()],
    })
}

fn assert_call(provider: &ScriptedModelProvider, metadata: &Value) {
    let requests = provider.requests();
    let calls: Vec<_> = requests
        .last()
        .expect("model request")
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].call_id, "signed-call");
    assert_eq!(calls[0].arguments, json!({"value":1}));
    assert_eq!(&calls[0].metadata, metadata);
}

#[tokio::test]
async fn completed_turn_retains_provider_metadata_in_next_conversation_and_replay() -> Result<()> {
    for metadata in [
        json!({"gemini_thought_signature":"opaque-gemini"}),
        json!({"anthropic_thinking":[{"type":"thinking","thinking":"plan","signature":"opaque-anthropic"}]}),
        json!({"openai_reasoning":[{"type":"reasoning","encrypted_content":"opaque-openai","summary":[]}]}),
    ] {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
        let echo = Arc::new(Echo::default());
        let call = ToolCall::new("signed-call".into(), "echo".into(), json!({"value":1}))
            .with_metadata(metadata.clone());
        let first = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(call)),
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
                    Model::new("fixture".into(), "signed".into(), "1".into(), json!({}))?,
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
                })?
                .grant("model:generate")
                .grant("tool:call:echo")
                .build()
        };
        let first_bundle = build(first.clone())?;
        let operation = OperationId::new();
        let input = storage
            .stage(
                operation,
                "first.txt",
                b"first question",
                "text/plain",
                "first.txt",
            )
            .await?;
        let output = storage
            .run_conversation(&first_bundle, operation, input.clone(), Vec::new(), 2)
            .await?;
        assert_eq!(output.text, "first complete");
        assert_eq!(first.remaining(), 0);
        assert_call(&first, &metadata);

        // A fresh provider has no signature cache. The committed conversation
        // supplies both the invocation and its original provider continuation.
        let next = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("second complete".into())])
                .with_token_counter(counter),
        );
        let next_bundle = build(next.clone())?;
        assert_eq!(
            storage
                .run_conversation(&next_bundle, operation, input, Vec::new(), 2)
                .await?,
            output
        );
        assert_eq!(next.remaining(), 1, "replay never dispatches a new request");
        assert_eq!(
            storage
                .run_prompt(&next_bundle, "next question", Vec::new())
                .await?
                .text,
            "second complete"
        );
        assert_eq!(next.remaining(), 0);
        assert_call(&next, &metadata);
        assert_eq!(echo.0.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
