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
    Ok(ModelTokenCount::from_upper_bounds(
        request.request_digest(),
        128,
        vec![128; request.request().messages.len()],
    ))
}

fn assert_call(provider: &ScriptedModelProvider, metadata: &Value, final_reasoning: bool) {
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
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].call_id, "signed-call");
    assert_eq!(calls[0].arguments, json!({"value":1}));
    assert_eq!(&calls[0].metadata, metadata);
    assert_eq!(calls[1].call_id, "parallel-call");
    assert!(calls[1].metadata.is_null());
    let ordered: Vec<_> = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::Reasoning { block } => {
                Some(format!("reasoning:{}", block.origin.model))
            }
            ModelContentPart::ToolCall(call) => Some(format!("call:{}", call.call_id)),
            ModelContentPart::ToolResult { call_id, .. } => Some(format!("result:{call_id}")),
            _ => None,
        })
        .collect();
    let mut expected = vec![
        "reasoning:0",
        "call:signed-call",
        "reasoning:1",
        "call:parallel-call",
        "result:signed-call",
        "result:parallel-call",
    ];
    if final_reasoning {
        expected.push("reasoning:2");
    }
    assert_eq!(ordered, expected);
    for block in requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::Reasoning { block } => Some(block),
            _ => None,
        })
    {
        let expected = reasoning(block.origin.model.parse().unwrap());
        assert_eq!(
            block, &expected,
            "opaque call-free state and native bytes survive next-turn projection"
        );
    }
    let exchange: Vec<_> = requests
        .last()
        .expect("model request")
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolCall(call) => Some(("call", call.call_id.as_str())),
            ModelContentPart::ToolResult { call_id, .. } => Some(("result", call_id.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(
        exchange,
        [
            ("call", "signed-call"),
            ("call", "parallel-call"),
            ("result", "signed-call"),
            ("result", "parallel-call")
        ]
    );
}

fn reasoning(index: usize) -> acyclic_harness::model::ReasoningBlock {
    use acyclic_harness::model::{
        ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome,
    };
    ReasoningBlock {
        kind: ReasoningKind::Encrypted, text: None,
        origin: ReasoningOrigin { provider:"responses".into(), model:index.to_string(), credential_scope:Some([7;32]) },
        continuity:format!("{{ \"type\": \"reasoning\", \"id\": \"rs_{index}\", \"encrypted_content\": \"opaque-{index}\", \"summary\": [] }}").into_bytes(),
        outcome:ReasoningOutcome::PreservedOpaque,
    }
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
                    ScriptedEvent::Event(ModelEvent::ReasoningBlock {
                        block: reasoning(0),
                    }),
                    ScriptedEvent::Event(ModelEvent::ToolCall(call)),
                    ScriptedEvent::Event(ModelEvent::ReasoningBlock {
                        block: reasoning(1),
                    }),
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        "parallel-call".into(),
                        "echo".into(),
                        json!({"value":2}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]),
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::Content {
                        delta: "first complete".into(),
                    }),
                    ScriptedEvent::Event(ModelEvent::ReasoningBlock {
                        block: reasoning(2),
                    }),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]),
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
                })
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
        assert_call(&first, &metadata, false);

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
        assert_call(&next, &metadata, true);
        assert_eq!(echo.0.load(Ordering::SeqCst), 2);
    }
    Ok(())
}
