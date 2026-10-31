use super::*;
use proptest::prelude::*;
use serde_json::json;

// These are generated parser mutations, not captured provider transcripts.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn optional_nulls_preserve_omission_events_and_retained_state(text in ".{1,40}") {
        let cases = [
            (Adapter::Chat, vec![
                (json!({"id":"original","model":"original","choices":[]}),
                 json!({"id":"original","model":"original","choices":[]})),
                (json!({"choices":[{"delta":{}}]}),
                 json!({"id":null,"model":null,"choices":[{"delta":{"reasoning_content":null,"reasoning":null}}]})),
                (json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]}),
                 json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})),
            ]),
            (Adapter::Anthropic, vec![
                (json!({"type":"message_start","message":{"id":"original","model":"original"}}),
                 json!({"type":"message_start","message":{"id":"original","model":"original"}})),
                (json!({"type":"message_start","message":{}}),
                 json!({"type":"message_start","message":{"id":null,"model":null}})),
                (json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta"}}),
                 json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":null}})),
                (json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":text,"signature":"original"}}),
                 json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":text,"signature":"original"}})),
                (json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta"}}),
                 json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":null}})),
                (json!({"type":"message_stop"}), json!({"type":"message_stop"})),
            ]),
            (Adapter::Responses, vec![
                (json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}}),
                 json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}})),
                (json!({"type":"response.completed","response":{}}),
                 json!({"type":"response.completed","response":{"id":null,"model":null,"output":null}})),
            ]),
            (Adapter::Gemini, vec![
                (json!({"responseId":"original","candidates":[]}),
                 json!({"responseId":"original","candidates":[]})),
                (json!({"candidates":[{"content":{"parts":[{"text":text}]}}]}),
                 json!({"responseId":null,"promptFeedback":{"blockReason":null},"candidates":[{"content":{"parts":[{"text":text,"thought":null,"thoughtSignature":null,"functionCall":null}]}}]})),
                (json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"tool"}}]},"finishReason":"STOP"}]}),
                 json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"tool","args":null}}]},"finishReason":"STOP"}]})),
            ]),
        ];
        for (adapter, frames) in cases {
            let mut omitted = State::new(16384);
            let mut nullable = State::new(16384);
            for (left, right) in frames {
                let expected = parse(&mut omitted, adapter, &left.to_string()).unwrap();
                let actual = parse(&mut nullable, adapter, &right.to_string()).unwrap();
                prop_assert_eq!(actual, expected);
                prop_assert_eq!(&nullable.metadata, &omitted.metadata);
                prop_assert_eq!(&nullable.completed_message, &omitted.completed_message);
                prop_assert_eq!(nullable.calls.len(), omitted.calls.len());
                prop_assert_eq!(nullable.thinking.len(), omitted.thinking.len());
                prop_assert_eq!(nullable.budget.used(), omitted.budget.used());
                if adapter == Adapter::Anthropic && left["delta"]["type"] == "thinking_delta" {
                    prop_assert!(nullable.thinking.is_empty(), "absent reasoning deltas must not admit a block");
                }
            }
            let dispatch = ModelDispatch {
                operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
                step: 0,
                request_digest: [0; 32],
            };
            let expected = finish(omitted, adapter, dispatch).unwrap();
            let actual = finish(nullable, adapter, dispatch).unwrap();
            if adapter == Adapter::Anthropic {
                let original = json!({"type":"thinking","thinking":text,"signature":"original"}).to_string();
                let retained = actual.iter().find_map(|event| match event {
                    ModelEvent::ReasoningBlock { block } => Some(block.continuity.as_slice()),
                    _ => None,
                });
                prop_assert_eq!(retained, Some(original.as_bytes()));
            }
            prop_assert_eq!(actual, expected);
        }

        // Presence still validates: null handling must not swallow bad scalars.
        for (adapter, frame) in [
            (Adapter::Responses, json!({"type":"response.completed","response":{"output":true}})),
            (Adapter::Gemini, json!({"candidates":[{"content":{"parts":[{"functionCall":true}]}}]})),
            (Adapter::Gemini, json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"tool","args":true}}]}}]})),
            (Adapter::Anthropic, json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":true}})),
        ] {
            prop_assert!(parse(&mut State::new(16384), adapter, &frame.to_string()).is_err(), "present malformed optional field must refuse");
        }
    }
}
