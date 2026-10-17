use super::*;
use acyclic_harness::model::{
    ModelContentPart, ReasoningBlock, ReasoningKind, ReasoningOutcome, ToolCall, ToolResultContent,
};
use proptest::prelude::*;

fn whitespace_wire(early: bool) -> (Value, Vec<u8>) {
    let (provider, _) = provider(Adapter::Anthropic, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    let signed =
        br#"{ "type":"thinking", "thinking":"opaque\n", "signature":"native-signature" }"#.to_vec();
    for (index, text) in ["answer", "\r", " tail"].iter().enumerate() {
        if !(early && text.trim().is_empty()) {
            request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text((*text).into()),
            });
        }
        if index == 0 {
            request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::Reasoning {
                    block: ReasoningBlock {
                        kind: ReasoningKind::Readable,
                        text: Some("opaque\n".into()),
                        origin: provider.reasoning_origin(provider.key.as_deref(), None),
                        continuity: signed.clone(),
                        outcome: ReasoningOutcome::PreservedReadable,
                    },
                }),
            });
        }
    }
    let encoded = serde_json::to_vec(
        &projection::body_named(
            &provider,
            &request,
            &crate::tool_names::Names::new(&request),
            &provider.reasoning_origin(provider.key.as_deref(), None),
            &mut crate::files::FileText::default(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        encoded.windows(signed.len()).any(|bytes| bytes == signed),
        "native signature bytes changed"
    );
    (serde_json::from_slice(&encoded).unwrap(), signed)
}
fn check_whitespace(early: bool) {
    let (body, _) = whitespace_wire(early);
    let parts = body["messages"][1]["content"].as_array().unwrap();
    let text: String = parts
        .iter()
        .filter(|part| part["type"] == "text")
        .map(|part| part["text"].as_str().unwrap())
        .collect();
    assert_eq!(text, "answer\r tail", "lost whitespace before role merge");
    assert_eq!(
        parts
            .iter()
            .filter(|part| part["type"] == "thinking")
            .count(),
        1
    );
    assert!(crate::validate_input(Adapter::Anthropic, &body).is_empty());
}
#[test]
fn whitespace_crosses_role_fragments_without_crossing_signed_part_boundaries() {
    check_whitespace(false);
}
#[test]
#[should_panic(expected = "lost whitespace before role merge")]
fn negative_control_normalizing_each_message_before_merging_loses_whitespace() {
    check_whitespace(true);
}

fn batch_wire(order: &[usize]) -> Value {
    let (provider, _) = provider(Adapter::Gemini, 200);
    let (prepared, _) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Parts(
            (0..order.len())
                .map(|index| {
                    ModelContentPart::ToolCall(ToolCall::new(
                        format!("call_{index}"),
                        format!("echo_{}", index % 2),
                        json!({"ordinal":index}),
                    ))
                })
                .collect(),
        ),
    });
    request
        .messages
        .extend(order.iter().map(|index| ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: format!("call_{index}"),
                name: format!("echo_{}", index % 2),
                content: ToolResultContent::Json {
                    value: json!({"ordinal":index}),
                },
            }),
        }));
    projection::body(&provider, &request).unwrap()
}
fn assert_order(body: &Value) {
    let calls: Vec<_> = body["contents"][1]["parts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|part| &part["functionCall"]["id"])
        .collect();
    let results = body["contents"][2]["parts"].as_array().unwrap();
    let ids: Vec<_> = results
        .iter()
        .map(|part| &part["functionResponse"]["id"])
        .collect();
    assert_eq!(ids, calls, "results must follow ordered calls");
    for (ordinal, result) in results.iter().enumerate() {
        let output: Value = serde_json::from_str(
            result["functionResponse"]["response"]["output"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(output["ordinal"], ordinal);
    }
    assert!(crate::validate_input(Adapter::Gemini, body).is_empty());
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn complete_parallel_batches_accept_every_result_permutation(priorities in prop::collection::vec(any::<u64>(), 1..9)) {
        let mut order: Vec<_> = (0..priorities.len()).collect();
        order.sort_by_key(|index| (priorities[*index], *index));
        assert_order(&batch_wire(&order));
    }
}
#[test]
fn reversed_batch_regression_preserves_response_identity_and_payload() {
    assert_order(&batch_wire(&[1, 0]));
}
#[test]
#[should_panic(expected = "results must follow ordered calls")]
fn negative_control_retaining_completion_order_is_detected() {
    let mut body = batch_wire(&[1, 0]);
    body["contents"][2]["parts"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert_order(&body);
}
