//! A settled turn's native parts cross the turn boundary byte for byte: the
//! staged output keeps them, and the next turn's request replays them through
//! the real renderer for the same origin and drops them for any other.
use super::*;
use acyclic_harness_core::{executor::TurnOutput, model::ModelContentPart};
use serde_json::Value;

/// One final step per family: reasoning, then the visible answer.
fn final_step(adapter: Adapter) -> &'static str {
    match adapter {
        Adapter::Anthropic => concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"run\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig=\"}}\n\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        ),
        Adapter::Responses => concat!(
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"encrypted_content\":\"enc=\",\"summary\":[]}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"run\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n",
        ),
        Adapter::Gemini => concat!(
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"plan\",\"thought\":true,\"thoughtSignature\":\"t=\"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"done\", \"thoughtSignature\":\"a\\u003d\"}]},\"finishReason\":\"STOP\"}],\"responseId\":\"run\",\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":3,\"totalTokenCount\":8}}\n\n",
        ),
        Adapter::Chat => concat!(
            "data: {\"id\":\"run\",\"choices\":[{\"delta\":{\"reasoning_content\":\"plan\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n",
            "data: [DONE]\n\n",
        ),
    }
}

/// The assistant turn the next request replays for the same origin.
fn golden(adapter: Adapter) -> Value {
    match adapter {
        Adapter::Anthropic => json!({"role":"assistant","content":[
            {"type":"thinking","thinking":"plan","signature":"sig="},
            {"type":"text","text":"done"},
        ]}),
        Adapter::Responses => json!([
            {"type":"reasoning","id":"rs_1","encrypted_content":"enc=","summary":[]},
            {"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]},
        ]),
        Adapter::Gemini => json!({"role":"model","parts":[
            {"text":"plan","thought":true,"thoughtSignature":"t="},
            {"text":"done","thoughtSignature":"a="},
        ]}),
        Adapter::Chat => json!({"role":"assistant","content":"done","reasoning_content":"plan"}),
    }
}

fn assistant(part: ModelContentPart) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(part),
    }
}

/// The settled turn's output exactly as the turn stages it.
fn settle(adapter: Adapter, provider: &ResolvedModel) -> TurnOutput {
    let events: Vec<ModelEvent> = futures::executor::block_on(
        stream::events_named(
            adapter,
            chunks(final_step(adapter), 7),
            4096,
            parser_dispatch(),
            None,
            Some(provider.reasoning_origin(provider.key.as_deref(), None)),
            None,
        )
        .try_collect(),
    )
    .unwrap();
    let mut text = String::new();
    let mut parts = Vec::new();
    for event in events {
        match event {
            ModelEvent::Content { delta } => text.push_str(&delta),
            ModelEvent::MessageCompleted { text: completed } => text = completed,
            ModelEvent::ReasoningBlock { block } => {
                parts.push(assistant(ModelContentPart::Reasoning { block }))
            }
            ModelEvent::TextPart { text, native } => {
                parts.push(assistant(ModelContentPart::Text { text, native }));
            }
            _ => {}
        }
    }
    let output = TurnOutput {
        text,
        attachments: Vec::new(),
        metadata: Value::Null,
        steps: 1,
        transcript: parts,
    };
    let staged: TurnOutput = serde_json::from_slice(&serde_json::to_vec(&output).unwrap()).unwrap();
    assert_eq!(staged, output, "{adapter:?} staged output changed");
    staged
}

/// The next turn's request: the settled reply, then the new user input.
fn next_turn(provider: &ResolvedModel, reply: &TurnOutput) -> ModelRequest {
    let (prepared, _) = request(provider).unwrap();
    let mut next = prepared.request().clone();
    next.messages.extend(reply.transcript.iter().cloned());
    next.messages.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("next".into()),
    });
    next
}

fn replayed(adapter: Adapter, body: &Value) -> Value {
    match adapter {
        Adapter::Responses => Value::Array(
            body["input"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["role"] != "system" && item["role"] != "user")
                .cloned()
                .collect(),
        ),
        Adapter::Gemini => body["contents"][1].clone(),
        Adapter::Anthropic => body["messages"][1].clone(),
        Adapter::Chat => body["messages"][2].clone(),
    }
}

#[test]
fn settled_native_parts_replay_exactly_in_the_next_turn() {
    let families = [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Gemini,
        Adapter::Chat,
    ];
    for adapter in families {
        let (provider, _) = provider(adapter, 200);
        let reply = settle(adapter, &provider);
        assert_eq!(reply.text, "done");
        let next = next_turn(&provider, &reply);
        let body = projection::body(&provider, &next).unwrap();
        assert_eq!(replayed(adapter, &body), golden(adapter), "{adapter:?}");
        // Each kept native part reaches the encoded wire as its original bytes.
        let encoded = serde_json::to_vec(
            &projection::body_named(
                &provider,
                &next,
                &crate::tool_names::Names::new(&next),
                &provider.reasoning_origin(provider.key.as_deref(), None),
                &mut crate::files::FileText::default(),
            )
            .unwrap(),
        )
        .unwrap();
        for message in &reply.transcript {
            let ModelContent::Part(part) = &message.content else {
                continue;
            };
            let bytes = match part {
                ModelContentPart::Reasoning { block } if adapter != Adapter::Chat => {
                    &block.continuity
                }
                ModelContentPart::Text {
                    native: Some(native),
                    ..
                } => &native.continuity,
                _ => continue,
            };
            let found = encoded
                .windows(bytes.len())
                .filter(|window| window == bytes)
                .count();
            assert_eq!(found, 1, "{adapter:?} changed or repeated a native part");
        }
        // Any other origin receives the canonical answer and no native bytes.
        for target in families.into_iter().filter(|target| *target != adapter) {
            let (foreign, _) = super::provider(target, 200);
            let body = projection::body(&foreign, &next).unwrap();
            let encoded = body.to_string();
            assert!(encoded.contains("done"), "{adapter:?} -> {target:?}");
            for marker in ["sig=", "enc=", "t=", "a=", "a\\u003d"] {
                assert!(
                    !encoded.contains(marker),
                    "{adapter:?} -> {target:?} leaked {marker}"
                );
            }
        }
    }
}
