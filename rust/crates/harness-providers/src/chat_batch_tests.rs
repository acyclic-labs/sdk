use super::*;
use acyclic_harness::model::{ModelContentPart, ToolCall, ToolResultContent};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn parallel_chat_batches_keep_every_result_adjacent_after_replay(calls in 1usize..8, turns in 1usize..4) {
        let (provider, _) = provider(Adapter::Chat, 200);
        let (initial, _) = request(&provider).unwrap();
        let mut input = initial.request().clone();
        input.tools.push(acyclic_harness::tool::ToolDefinition {
            name: "echo".into(), revision: "1".into(), description: "Echo".into(),
            input_schema: json!({"type":"object"}), output_schema: json!({}), projection_schema: json!({}),
        });
        for turn in 0..turns {
            for index in 0..calls {
                input.messages.push(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Parts(vec![
                        ModelContentPart::Text { text: format!("call {index}") },
                        ModelContentPart::ToolCall(ToolCall::new(format!("{turn}-{index}"), "echo".into(), json!({}))),
                    ]),
                });
            }
            for index in 0..calls {
                input.messages.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: format!("{turn}-{index}"), name: "echo".into(),
                        content: ToolResultContent::Json { value: json!({"ok":true}) },
                    }),
                });
            }
        }
        let prepared = PreparedModelRequest::prepare(input, Limits::default(), None).unwrap();
        let replayed = PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None).unwrap();
        for request in [prepared.request(), replayed.request()] {
            let body = projection::body(&provider, request).unwrap();
            let messages = body["messages"].as_array().unwrap();
            prop_assert_eq!(messages.len(), 2 + turns * (calls + 1));
            for turn in 0..turns {
                let start = 2 + turn * (calls + 1);
                let batch = &messages[start];
                prop_assert_eq!(&batch["role"], "assistant");
                prop_assert_eq!(batch["tool_calls"].as_array().unwrap().len(), calls);
                let expected: String = (0..calls).map(|index| format!("call {index}"))
                    .collect::<Vec<_>>().join("\n");
                prop_assert_eq!(batch["content"].as_str().unwrap(), expected);
                for index in 0..calls {
                    let id = format!("{turn}-{index}");
                    prop_assert_eq!(&batch["tool_calls"][index]["id"], &id);
                    let result = &messages[start + index + 1];
                    prop_assert_eq!(&result["role"], "tool");
                    prop_assert_eq!(&result["tool_call_id"], &id);
                }
            }
        }
    }
}
