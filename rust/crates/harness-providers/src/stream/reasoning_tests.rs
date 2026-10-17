use super::{parser_dispatch, provider, request};
use crate::{
    Adapter, Result, invalid,
    stream::{State, finish, parse},
};
use acyclic_harness::{conversation::Limits, model::*};
use proptest::prelude::*;
use serde_json::json;

fn scenario(
    adapter: Adapter,
    order: &[bool],
    text: &str,
) -> Result<(Vec<ModelEvent>, Vec<String>, crate::ResolvedModel)> {
    let (provider, _) = provider(adapter, 200);
    let mut state = State::new(65_536);
    state.set_origin(Some(
        provider.reasoning_origin(provider.key.as_deref(), None),
    ))?;
    let mut native = Vec::new();
    for (index, reasoning) in order.iter().enumerate() {
        let raw = if *reasoning {
            match adapter {
                Adapter::Responses => format!(
                    "{{ \"type\": \"reasoning\", \"id\": \"rs_{index}\", \"summary\": [{{\"type\":\"summary_text\",\"text\":{}}}], \"encrypted_content\": \"opaque_{index}\" }}",
                    json!(text)
                ),
                _ => format!(
                    "{{ \"type\": \"thinking\", \"thinking\": {}, \"signature\": \"opaque_{index}\" }}",
                    json!(text)
                ),
            }
        } else {
            match adapter {
                Adapter::Responses => json!({"type":"function_call", "call_id":format!("call_{index}"), "name":"echo", "arguments":"{}"}).to_string(),
                _ => json!({"type":"tool_use", "id":format!("call_{index}"), "name":"echo", "input":{}}).to_string(),
            }
        };
        let event = match adapter {
            Adapter::Responses => format!(
                "{{\"type\":\"response.output_item.done\",\"output_index\":{index},\"item\":{raw}}}"
            ),
            _ => format!(
                "{{\"type\":\"content_block_start\",\"index\":{index},\"content_block\":{raw}}}"
            ),
        };
        parse(&mut state, adapter, &event)?;
        if *reasoning {
            native.push(raw);
        }
        if adapter == Adapter::Anthropic {
            parse(
                &mut state,
                adapter,
                &json!({"type":"content_block_stop","index":index}).to_string(),
            )?;
        }
    }
    state.terminal = true;
    Ok((finish(state, adapter, parser_dispatch())?, native, provider))
}

fn projection(provider: &crate::ResolvedModel, events: Vec<ModelEvent>) -> Result<Vec<u8>> {
    let (request, _) = request(provider)?;
    let mut request = request.request().clone();
    request.tools.push(acyclic_harness::tool::ToolDefinition {
        name: "echo".into(),
        revision: "1".into(),
        description: "echo".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        projection_schema: json!({}),
    });
    let mut results = Vec::new();
    for event in events {
        let part = match event {
            ModelEvent::ReasoningBlock { block } => ModelContentPart::Reasoning { block },
            ModelEvent::ToolCall(call) => {
                results.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                        content: ToolResultContent::Json { value: json!({}) },
                    }),
                });
                ModelContentPart::ToolCall(call)
            }
            _ => continue,
        };
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(part),
        });
    }
    request.messages.extend(results);
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    let names = crate::tool_names::Names::new(prepared.request());
    serde_json::to_vec(&crate::projection::body_named(
        provider,
        prepared.request(),
        &names,
        &provider.reasoning_origin(provider.key.as_deref(), None),
        &mut crate::files::FileText::default(),
    )?)
    .map_err(|_| invalid("fixture request encoding failed"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn interleavings_round_trip_with_exact_native_bytes(
        order in prop::collection::vec(any::<bool>(), 1..9), chars in prop::collection::vec(any::<char>(), 0..20),
    ) {
        let text = format!("first\n\"{}\\last", chars.into_iter().collect::<String>());
        for adapter in [Adapter::Responses, Adapter::Anthropic] {
            let (events, raw, provider) = scenario(adapter, &order, &text).unwrap();
            let observed: Vec<bool> = events.iter().filter_map(|event| match event {
                ModelEvent::ReasoningBlock { .. } => Some(true), ModelEvent::ToolCall(_) => Some(false), _ => None,
            }).collect();
            prop_assert_eq!(&observed, &order);
            let encoded = serde_json::to_vec(&events).unwrap();
            let replay: Vec<ModelEvent> = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(&events, &replay);
            let body = projection(&provider, replay).unwrap();
            let mut cursor = 0;
            for raw in raw {
                let found = body[cursor..].windows(raw.len()).position(|part| part == raw.as_bytes());
                prop_assert!(found.is_some(), "native payload lost or reordered");
                cursor += found.unwrap() + raw.len();
            }
        }
    }
}

#[test]
fn first_call_attachment_negative_control_loses_order_and_call_free_reasoning() {
    for order in [vec![true], vec![true, false, true, false]] {
        let (events, _, _) = scenario(Adapter::Responses, &order, "plan").unwrap();
        let broken: Vec<_> = events
            .iter()
            .filter(|event| matches!(event, ModelEvent::ReasoningBlock { .. }))
            .map(|_| true)
            .chain(
                events
                    .iter()
                    .filter(|event| matches!(event, ModelEvent::ToolCall(_)))
                    .map(|_| false),
            )
            .collect();
        if order.contains(&false) {
            assert_ne!(broken, order);
        } else {
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, ModelEvent::ReasoningBlock { .. }))
            );
            assert_ne!(Vec::<bool>::new(), order);
        }
    }
}

#[test]
fn signed_multiline_fixture_keeps_the_original_native_block_after_durable_replay() {
    let fixture = include_str!("../fixtures/anthropic-multiline-thinking.sse");
    let (provider, _) = provider(Adapter::Anthropic, 200);
    let mut state = State::new(65_536);
    state
        .set_origin(Some(
            provider.reasoning_origin(provider.key.as_deref(), None),
        ))
        .unwrap();
    for frame in fixture.split("\n\n").filter(|frame| !frame.is_empty()) {
        parse(
            &mut state,
            Adapter::Anthropic,
            frame.strip_prefix("data: ").unwrap(),
        )
        .unwrap();
    }
    let events = finish(state, Adapter::Anthropic, parser_dispatch()).unwrap();
    let block = events
        .iter()
        .find_map(|event| match event {
            ModelEvent::ReasoningBlock { block } => Some(block),
            _ => None,
        })
        .expect("signed reasoning block");
    let native = block.continuity.clone();
    let seed =
        include_bytes!("../../fuzz/corpus/provider_stream/anthropic-multiline-reasoning.json");
    let seeded: ReasoningBlock = serde_json::from_slice(&seed[1..]).unwrap();
    assert_eq!(seeded.continuity, native);
    assert!(
        crate::projection::native_reasoning::validated(Adapter::Anthropic, &seeded.continuity)
            .is_some()
    );
    crate::fuzzing::assert_stream_invariants(seed);
    assert!(String::from_utf8_lossy(&native).contains(r#"first\n\"quoted\"\\path"#));
    let replay = serde_json::from_slice(&serde_json::to_vec(&events).unwrap()).unwrap();
    let body = projection(&provider, replay).unwrap();
    assert!(body.windows(native.len()).any(|bytes| bytes == native));
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let thinking = &body["messages"][1]["content"][0];
    assert_eq!(thinking["type"], "thinking");
    assert_eq!(thinking["thinking"], "first\n\"quoted\"\\path");
    assert_eq!(thinking["signature"], "signed\n\"opaque\"\\payload==");
    crate::fuzzing::assert_stream_invariants(include_bytes!(
        "../../fuzz/corpus/provider_stream/anthropic-multiline-thinking.sse"
    ));
}
