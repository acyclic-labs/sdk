use super::*;
use acyclic_harness::model::{ModelContentPart, ToolCall};
use proptest::prelude::*;

fn labels(adapter: Adapter, body: &Value) -> Vec<String> {
    let items = match adapter {
        Adapter::Responses => body["input"].as_array(),
        Adapter::Anthropic => body["messages"]
            .as_array()
            .and_then(|messages| {
                messages
                    .iter()
                    .find(|message| message["role"] == "assistant")
            })
            .and_then(|message| message["content"].as_array()),
        Adapter::Gemini => body["contents"]
            .as_array()
            .and_then(|messages| messages.iter().find(|message| message["role"] == "model"))
            .and_then(|message| message["parts"].as_array()),
        Adapter::Chat => unreachable!(),
    }
    .map(Vec::as_slice)
    .unwrap_or_default();
    items
        .iter()
        .map(|item| {
            if adapter == Adapter::Responses && item["role"] == "assistant" {
                item["content"][0]["text"].as_str().unwrap().into()
            } else if let Some(text) = item["text"].as_str() {
                text.into()
            } else if adapter == Adapter::Gemini {
                item["functionCall"]["id"].as_str().unwrap().into()
            } else {
                item[if adapter == Adapter::Responses {
                    "call_id"
                } else {
                    "id"
                }]
                .as_str()
                .unwrap()
                .into()
            }
        })
        .collect()
}

fn project(adapter: Adapter, pattern: &[bool], text: &str) -> (Vec<String>, Vec<String>) {
    let (provider, _) = provider(adapter, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    let mut expected = Vec::new();
    let parts = pattern
        .iter()
        .enumerate()
        .map(|(i, call)| {
            let label = if *call {
                format!("call{i}")
            } else {
                format!("text{i}:{text}")
            };
            expected.push(label.clone());
            if *call {
                ModelContentPart::ToolCall(ToolCall::new(label, "echo".into(), json!({})))
            } else {
                ModelContentPart::Text { text: label }
            }
        })
        .collect();
    request.messages = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Parts(parts),
    }];
    let encoded = serde_json::to_vec(&request).unwrap();
    let request = serde_json::from_slice(&encoded).unwrap();
    let body = projection::body(&provider, &request).unwrap();
    (labels(adapter, &body), expected)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: every text/call interleaving remains ordered in each protocol
    // that represents ordered assistant parts, including after durable codec replay.
    #[test]
    fn assistant_parts_keep_their_order(pattern in prop::collection::vec(any::<bool>(), 0..32), text in ".{0,32}") {
        for adapter in [Adapter::Anthropic, Adapter::Responses, Adapter::Gemini] {
            let (actual, expected) = project(adapter, &pattern, &text);
            prop_assert_eq!(actual, expected);
        }
    }
}

#[test]
fn empty_parts_do_not_fabricate_ordered_content() {
    for adapter in [Adapter::Anthropic, Adapter::Responses, Adapter::Gemini] {
        let (actual, expected) = project(adapter, &[], "");
        assert_eq!(actual, expected);
        assert!(actual.is_empty());
    }
}

#[test]
fn text_then_calls_partition_is_detected_by_the_order_oracle() {
    let pattern = [false, true, false, true];
    for adapter in [Adapter::Anthropic, Adapter::Responses, Adapter::Gemini] {
        let (actual, expected) = project(adapter, &pattern, "fixture");
        assert_eq!(actual, expected);
        let broken: Vec<_> = expected
            .iter()
            .filter(|s| s.starts_with("text"))
            .chain(expected.iter().filter(|s| s.starts_with("call")))
            .cloned()
            .collect();
        assert_ne!(
            broken, expected,
            "negative control detects the former partition"
        );
    }
}
