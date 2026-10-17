//! The cargo-fuzz entry point uses the same streaming path as model dispatch.
use crate::{Adapter, stream};
use acyclic_harness::{
    OperationId,
    model::{ModelDispatch, ModelEvent},
};
use bytes::Bytes;
use futures::TryStreamExt as _;

fn parse(adapter: Adapter, input: &[u8], width: usize) -> Result<Vec<ModelEvent>, String> {
    parse_at(adapter, input, width, [1; 16])
}

/// Tool-call arguments from raw bytes, through the real Chat stream. A call is
/// emitted exactly when its arguments are a JSON object that the independent
/// recording reference admits, and then unchanged; anything else fails the
/// whole stream rather than failing later when the executor records the call.
pub fn assert_tool_arguments(input: &[u8]) {
    let Ok(text) = std::str::from_utf8(input) else {
        return;
    };
    let calls =
        serde_json::json!([{"index":0,"id":"call","function":{"name":"echo","arguments":text}}]);
    let sse = format!(
        "data: {}\n\ndata: [DONE]\n\n",
        serde_json::json!({"choices":[{"delta":{"tool_calls":calls}}]})
    );
    let body = Box::pin(futures::stream::iter([Ok(Bytes::from(sse))]));
    let dispatch = ModelDispatch {
        operation_id: OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: [0; 32],
    };
    let events: Result<Vec<_>, _> = futures::executor::block_on(
        stream::events(Adapter::Chat, body, 1 << 20, dispatch).try_collect(),
    );
    let emitted = events.ok().and_then(|events| {
        events.into_iter().find_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
    });
    // Shallow values only, so serde's own recursion limit is never the cause.
    let literal = crate::projection::arguments::literal(input)
        .ok()
        .filter(|value| depth(value) <= 64);
    match (emitted, literal) {
        (Some(call), literal) => {
            assert!(
                call.arguments.is_object() && portable(&call.arguments),
                "the stream emitted arguments the recording rule rejects"
            );
            if let Some(literal) = literal {
                assert_eq!(call.arguments, literal);
            }
        }
        (None, Some(literal)) => assert!(
            !(literal.is_object() && portable(&literal)),
            "the stream rejected portable object arguments"
        ),
        (None, None) => {}
    }
}

fn depth(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(values) => 1 + values.iter().map(depth).max().unwrap_or(0),
        serde_json::Value::Object(values) => 1 + values.values().map(depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Independent reference for the recording rule, deliberately not
/// `acyclic_harness::conversation`: no private serde marker key, and every
/// number round-trips through a JavaScript Number exactly.
pub(crate) fn portable(value: &serde_json::Value) -> bool {
    const SAFE: u64 = (1 << 53) - 1;
    match value {
        serde_json::Value::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(integer), _) => integer.unsigned_abs() <= SAFE,
            (None, Some(integer)) => integer <= SAFE,
            (None, None) => number.as_f64().is_some_and(|float| {
                float.is_finite()
                    && float.to_bits() != (-0.0f64).to_bits()
                    && (float.fract() != 0.0 || float.abs() <= SAFE as f64)
            }),
        },
        serde_json::Value::Array(values) => values.iter().all(portable),
        serde_json::Value::Object(values) => values.iter().all(|(key, value)| {
            key != "$serde_json::private::RawValue"
                && key != "$serde_json::private::Number"
                && portable(value)
        }),
        _ => true,
    }
}
/// Accepted alloy manifests preserve policy and deterministic selection through JSON.
pub fn assert_alloy_invariants(input: &[u8]) {
    if let Ok(spec) = serde_json::from_slice::<crate::AlloySpec>(input) {
        let encoded = serde_json::to_vec(&spec).unwrap();
        let replay: crate::AlloySpec = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(spec, replay);
        assert_eq!(spec.validate().is_ok(), replay.validate().is_ok());
        if spec.validate().is_ok() {
            for ordinal in [0, 1, u64::MAX] {
                for role in ["", "worker", "reviewer"] {
                    let turn = crate::AlloyTurn {
                        operation_id: OperationId::from_bytes([1; 16]),
                        ordinal,
                        role,
                    };
                    assert!(spec.index(turn) < spec.models.len());
                    assert_eq!(spec.index(turn), replay.index(turn));
                }
            }
        }
    }
}

fn parse_at(
    adapter: Adapter,
    input: &[u8],
    width: usize,
    operation: [u8; 16],
) -> Result<Vec<ModelEvent>, String> {
    let body = Box::pin(futures::stream::iter(
        input
            .chunks(width)
            .map(|part| Ok(Bytes::copy_from_slice(part))),
    ));
    let dispatch = ModelDispatch {
        operation_id: OperationId::from_bytes(operation),
        step: 0,
        request_digest: [0; 32],
    };
    futures::executor::block_on(stream::events(adapter, body, 4096, dispatch).try_collect())
        .map_err(|error| error.to_string())
}

/// Chunking cannot change acceptance, errors or the decoded events. Accepted
/// events must round-trip through the durable model codec without losing data.
pub fn assert_stream_invariants(input: &[u8]) {
    let Some((&kind, input)) = input.split_first() else {
        return;
    };
    if kind == b'F' {
        if let Ok(file) = serde_json::from_slice::<acyclic_harness::conversation::FileRef>(input) {
            let marker = crate::files::FileText::default()
                .project(
                    &file,
                    &acyclic_harness::model::FileProjectionPolicy::Reference,
                )
                .unwrap();
            let restored: acyclic_harness::conversation::FileRef = serde_json::from_str(
                marker
                    .strip_prefix("[file: ")
                    .unwrap()
                    .strip_suffix(']')
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(file, restored);
        }
        return;
    }
    if kind == b'T' {
        if let Ok(fragments) = serde_json::from_slice::<Vec<String>>(input) {
            crate::projection::text::assert_normalization(&fragments);
        }
        return;
    }
    if kind == b'V' {
        let Some((&kind, input)) = input.split_first() else {
            return;
        };
        if let Ok(body) = serde_json::from_slice::<serde_json::Value>(input) {
            let adapter = [
                Adapter::Anthropic,
                Adapter::Responses,
                Adapter::Chat,
                Adapter::Gemini,
            ][usize::from(kind) % 4];
            let before = crate::validate_input(adapter, &body);
            let encoded = serde_json::to_vec(&body).unwrap();
            let replay = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(before, crate::validate_input(adapter, &replay));
        }
        return;
    }
    if kind == b'L' {
        assert_alloy_invariants(input);
        return;
    }
    if kind == b'P' {
        if let Ok(value) = crate::projection::arguments::literal(input) {
            crate::projection::arguments::assert_normalization(&value);
        }
        return;
    }
    if kind == b'K' {
        use crate::counting::wire::{TokenCountField, TokenCounterSpec, response};
        for field in [
            TokenCountField::InputTokens,
            TokenCountField::TotalTokens,
            TokenCountField::Count,
        ] {
            let spec = TokenCounterSpec {
                path: "count".into(),
                adapter: Adapter::Responses,
                response_field: field,
                framing_tokens: 0,
            };
            if let Ok(count) = response(input, &spec) {
                let encoded = match field {
                    TokenCountField::InputTokens => serde_json::json!({"input_tokens":count}),
                    TokenCountField::TotalTokens => serde_json::json!({"totalTokens":count}),
                    TokenCountField::Count => serde_json::json!({"count":count}),
                };
                assert_eq!(
                    response(&serde_json::to_vec(&encoded).unwrap(), &spec).unwrap(),
                    count
                );
            }
        }
        return;
    }
    if kind == b'R' {
        if let Ok(block) = serde_json::from_slice::<acyclic_harness::model::ReasoningBlock>(input) {
            let encoded = serde_json::to_vec(&block).unwrap();
            let replay: acyclic_harness::model::ReasoningBlock =
                serde_json::from_slice(&encoded).unwrap();
            assert_eq!(block, replay);
            for adapter in [crate::Adapter::Responses, crate::Adapter::Anthropic] {
                if let Some(raw) =
                    crate::projection::native_reasoning::validated(adapter, &block.continuity)
                {
                    // Independent bounded literal decoder: native admission must
                    // reject invalid strings, including inside skipped fields.
                    let bounds = acyclic_harness::JsonBounds {
                        maximum_bytes: block.continuity.len() as u64,
                        maximum_items: block.continuity.len() as u64,
                        maximum_depth: 128,
                    };
                    assert!(
                        acyclic_harness::json_value_bounded(raw, bounds, None, &mut |_| Ok(()),)
                            .is_ok()
                    );
                    assert_eq!(serde_json::to_vec(raw).unwrap(), block.continuity);
                }
            }
            for family in ["anthropic", "responses", "gemini", "chat"] {
                let mut target = block.origin.clone();
                target.provider = family.into();
                assert_eq!(
                    crate::project_reasoning(&block, &target),
                    crate::project_reasoning(&replay, &target)
                );
            }
            let event = ModelEvent::ReasoningBlock { block: replay };
            let encoded = serde_json::to_vec(&event).unwrap();
            assert_eq!(
                serde_json::from_slice::<ModelEvent>(&encoded).unwrap(),
                event
            );
        }
        return;
    }
    if kind == b'C' {
        catalog_round_trip(input);
        return;
    }
    if kind == b'N' {
        if let Ok(request) = serde_json::from_slice::<acyclic_harness::model::ModelRequest>(input) {
            crate::tool_names::Names::assert_round_trip(&request);
            crate::call_ids::CallIds::assert_round_trip(&request);
            let encoded = serde_json::to_vec(&request).unwrap();
            let replay = serde_json::from_slice(&encoded).unwrap();
            crate::tool_names::Names::assert_round_trip(&replay);
            crate::call_ids::CallIds::assert_round_trip(&replay);
        }
        return;
    }
    if kind == b'A' {
        if let Ok(auth) = crate::codex::decode_auth(input) {
            let refresh = crate::codex::needs_refresh(&auth, time::OffsetDateTime::UNIX_EPOCH);
            let canonical = serde_json::to_vec(&auth).unwrap();
            let restored = crate::codex::decode_auth(&canonical).unwrap();
            assert_eq!(
                crate::codex::needs_refresh(&restored, time::OffsetDateTime::UNIX_EPOCH),
                refresh
            );
            assert_eq!(restored, auth);
        }
        return;
    }
    let adapter = [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Gemini,
    ][usize::from(kind) % 4];
    let whole = parse(adapter, input, input.len().max(1));
    assert_eq!(whole, parse(adapter, input, 1));
    if let Ok(events) = whole {
        if adapter == Adapter::Gemini {
            crate::projection::gemini::assert_visible_replay(&events);
        }
        // Identical provider bytes in a different turn must never reuse canonical
        // IDs. Bindings remain byte-for-byte equal, independently of chunking.
        let other = parse_at(adapter, input, input.len().max(1), [2; 16]).unwrap();
        let first: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ModelEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        let second: Vec<_> = other
            .iter()
            .filter_map(|event| match event {
                ModelEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        assert_eq!(first.len(), second.len());
        for (ordinal, (first, second)) in first.iter().zip(&second).enumerate() {
            let a = ModelDispatch {
                operation_id: OperationId::from_bytes([1; 16]),
                step: 0,
                request_digest: [0; 32],
            };
            let b = ModelDispatch {
                operation_id: OperationId::from_bytes([2; 16]),
                step: 0,
                request_digest: [0; 32],
            };
            assert_eq!(first.call_id, a.tool_call_id(ordinal as u64));
            assert_eq!(second.call_id, b.tool_call_id(ordinal as u64));
            assert_eq!(first.metadata, second.metadata);
        }
        let ids: std::collections::BTreeSet<_> =
            first.iter().map(|call| call.call_id.as_str()).collect();
        assert!(
            second
                .iter()
                .all(|call| !ids.contains(call.call_id.as_str()))
        );
        for event in events {
            let bytes = serde_json::to_vec(&event).unwrap();
            let restored: ModelEvent = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(restored, event);
        }
    }
}

fn catalog_round_trip(input: &[u8]) -> bool {
    let Ok(spec) = serde_json::from_slice::<crate::ProviderSpec>(input) else {
        return false;
    };
    let canonical = serde_json::to_vec(&spec).unwrap();
    let restored: crate::ProviderSpec = serde_json::from_slice(&canonical).unwrap();
    assert_eq!(serde_json::to_vec(&restored).unwrap(), canonical);
    assert_eq!(
        crate::registry::validate_spec("fixture", &spec).is_ok(),
        crate::registry::validate_spec("fixture", &restored).is_ok()
    );
    true
}

#[cfg(test)]
mod properties {
    use super::*;
    use crate::sse;
    use proptest::prelude::*;
    use serde_json::json;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn tool_arguments_are_admitted_by_the_recording_rule(
            entries in prop::collection::vec((0usize..4, prop::collection::vec(0usize..8, 1..3), any::<bool>()), 0..5),
            wrapped in any::<bool>(),
        ) {
            const KEYS: [&str; 4] = ["a", "$serde_json::private::RawValue", "$serde_json::private::Number", "$serde_json::private"];
            const NUMBERS: [&str; 8] = ["0", "-0.0", "-0.5", "9007199254740991", "-9007199254740991", "9007199254740992", "1e16", "18446744073709551615"];
            let fields: Vec<String> = entries.iter().map(|(key, numbers, array)| {
                let numbers: Vec<_> = numbers.iter().map(|number| NUMBERS[*number]).collect();
                let value = if *array { format!("[{}]", numbers.join(",")) } else { numbers[0].to_owned() };
                format!("{}:{value}", json!(KEYS[*key]))
            }).collect();
            let object = format!("{{{}}}", fields.join(","));
            let text = if wrapped { format!("[{object}]") } else { object };
            assert_tool_arguments(text.as_bytes());
        }
        #[test]
        fn arbitrary_bytes_have_chunk_invariant_stream_results(input in prop::collection::vec(any::<u8>(), 0..2048)) {
            assert_stream_invariants(&input);
        }

        #[test]
        fn catalog_codec_has_an_idempotent_canonical_form(endpoint in "[a-z]{0,32}", maximum in 1usize..65536) {
            for mut spec in crate::default_catalog().into_values() {
                spec.base_url = format!("https://example.test/{endpoint}");
                spec.max_event_bytes = maximum;
                prop_assert!(catalog_round_trip(&serde_json::to_vec(&spec).unwrap()));
            }
        }

        #[test]
        fn unicode_text_round_trips_through_every_adapter(text in ".{0,96}", width in 1usize..64) {
            let value = json!(text);
            let streams = [
                (Adapter::Anthropic, format!("data: {{\"type\":\"content_block_delta\",\"delta\":{{\"type\":\"text_delta\",\"text\":{value}}}}}\n\ndata: {{\"type\":\"message_stop\"}}\n\n")),
                (Adapter::Responses, format!("data: {{\"type\":\"response.output_text.delta\",\"delta\":{value}}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\"}}}}\n\n")),
                (Adapter::Chat, format!("data: {{\"choices\":[{{\"delta\":{{\"content\":{value}}}}}]}}\n\ndata: [DONE]\n\n")),
                (Adapter::Gemini, format!("data: {{\"candidates\":[{{\"content\":{{\"parts\":[{{\"text\":{value}}}]}},\"finishReason\":\"STOP\"}}]}}\n\n")),
            ];
            for (adapter, input) in streams {
                let events = parse(adapter, input.as_bytes(), width).unwrap();
                let decoded: String = events.iter().filter_map(|event| match event {
                    ModelEvent::Content { delta } => Some(delta.as_str()), _ => None,
                }).collect();
                prop_assert_eq!(&decoded, &text);
                prop_assert_eq!(events, parse(adapter, input.as_bytes(), input.len()).unwrap());
            }
        }

        #[test]
        fn sse_framing_round_trips_unicode_data(lines in prop::collection::vec("[^\\r\\n]{0,64}", 1..8), width in 1usize..64) {
            let input: String = lines.iter().map(|line| format!("data: {line}\n")).chain(["\n".into()]).collect();
            let chunks = input.as_bytes().chunks(width).map(|part| Ok(Bytes::copy_from_slice(part)));
            let frames = futures::executor::block_on(sse::decode(Box::pin(futures::stream::iter(chunks)), 4096).try_collect::<Vec<_>>()).unwrap();
            prop_assert_eq!(frames, vec![lines.join("\n")]);
        }
    }

    #[test]
    fn checked_in_shape_regressions_obey_the_parser_oracles() {
        for seed in [
            include_bytes!("../fuzz/corpus/provider_stream/native-summary-surrogate.json")
                .as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/responses-output-missing-text.json")
                .as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/responses-output-wrong-position.json")
                .as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/chat-numeric-call-content.json")
                .as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/chat-file-without-source.json")
                .as_slice(),
        ] {
            assert_stream_invariants(seed);
        }
    }

    #[test]
    fn checked_in_stream_error_corpus_obeys_the_properties() {
        for seed in [
            include_bytes!("../fuzz/corpus/provider_stream/anthropic-error.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/responses-error.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/chat-error.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/gemini-error.sse").as_slice(),
        ] {
            assert_stream_invariants(seed);
        }
    }

    #[test]
    fn checked_in_stream_corpus_obeys_the_properties() {
        assert!(catalog_round_trip(
            &include_bytes!("../fuzz/corpus/provider_stream/catalog.json")[1..]
        ));
        for input in [
            include_bytes!("../fuzz/corpus/provider_stream/anthropic.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/responses.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/chat.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/chat-reused-id.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/gemini.sse").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/gemini-signed-answer.sse").as_slice(),
        ] {
            assert_stream_invariants(input);
            let (&kind, body) = input.split_first().unwrap();
            let adapter = [
                Adapter::Anthropic,
                Adapter::Responses,
                Adapter::Chat,
                Adapter::Gemini,
            ][usize::from(kind) % 4];
            assert!(
                parse(adapter, body, 7).is_ok(),
                "seed {adapter:?} must exercise accepted events"
            );
        }
    }
}

/// Typed model/tool JSON must preserve literal maps, including reserved serde keys.
pub(crate) fn assert_model_json_round_trip(value: &serde_json::Value) {
    use acyclic_harness::model::{
        Model, ModelContentPart, ModelEvent, ToolCall, ToolResultContent,
    };
    fn round_trip<
        T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
    >(
        value: T,
    ) {
        let encoded = serde_json::to_value(&value).unwrap();
        let replay: T = serde_json::from_value(encoded).unwrap();
        assert_eq!(replay, value);
    }
    let call =
        ToolCall::new("call".into(), "echo".into(), value.clone()).with_metadata(value.clone());
    round_trip(ModelEvent::ToolCall(call.clone()));
    round_trip(ModelContentPart::ToolCall(call));
    round_trip(ModelEvent::Completed {
        metadata: value.clone(),
    });
    round_trip(ToolResultContent::Json {
        value: value.clone(),
    });
    round_trip(Model {
        provider: "fixture".into(),
        name: "model".into(),
        revision: "1".into(),
        options: value.clone(),
    });
    round_trip(acyclic_harness::tool::ToolInvocation {
        operation_id: acyclic_harness::OperationId::from_bytes([1; 16]),
        call_id: "call".into(),
        name: "echo".into(),
        arguments: value.clone(),
    });
    round_trip(acyclic_harness::tool::ToolResult {
        value: value.clone(),
    });
    round_trip(acyclic_harness::tool::ToolDefinition {
        name: "echo".into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: value.clone(),
        output_schema: value.clone(),
        projection_schema: value.clone(),
    });
}

/// OAR-RSN-02 from raw bytes. Validation agrees with an independent
/// input-derived reference, and the production sequence fold repairs exactly
/// the unanswered reasoning items without dropping or reordering any item.
pub fn assert_response_sequence(input: &[u8]) {
    use crate::projection::native_reasoning::replay_responses;
    let Ok(body) = crate::json(input) else { return };
    let errors = crate::validate_input(Adapter::Responses, &body);
    let replay = crate::json(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(body, replay);
    assert_eq!(errors, crate::validate_input(Adapter::Responses, &replay));
    let Some(items) = body["input"].as_array() else {
        return;
    };
    assert_eq!(
        errors.contains(&"OAR-RSN-02"),
        unanswered_reasoning(items) > 0
    );
    let output = replay_responses(items);
    assert_eq!(response_repair_violation(items, &output), None);
    assert_eq!(
        replay_responses(&output),
        output,
        "repair is not idempotent"
    );
}

/// The structured detector shared by the fuzz oracle and its negative controls.
pub(crate) fn response_repair_violation(
    items: &[serde_json::Value],
    output: &[serde_json::Value],
) -> Option<&'static str> {
    if unanswered_reasoning(output) > 0 {
        return Some("unanswered reasoning");
    }
    if output.len() != items.len() + unanswered_reasoning(items) {
        return Some("unrequired repair");
    }
    let mut rest = output.iter();
    (!items.iter().all(|item| rest.any(|next| next == item))).then_some("lost item")
}

/// Independent reference, deliberately not `ResponseItem`: each reasoning item
/// is immediately followed by a function call or an assistant message.
fn unanswered_reasoning(items: &[serde_json::Value]) -> usize {
    (0..items.len())
        .filter(|&index| {
            items[index]["type"] == "reasoning"
                && !items.get(index + 1).is_some_and(|next| {
                    next["type"] == "function_call"
                        || (next["type"].as_str().unwrap_or("message") == "message"
                            && next["role"] == "assistant")
                })
        })
        .count()
}

/// Role-alternating turns from raw bytes: an even first byte selects
/// Anthropic, an odd one Gemini, and the rest is the wire message array.
/// Validation agrees with an independent reference for turn boundaries and
/// Gemini call/response pairing, and the production sequence fold repairs both.
pub fn assert_turns(input: &[u8]) {
    let Some((&selector, input)) = input.split_first() else {
        return;
    };
    let adapter = [Adapter::Anthropic, Adapter::Gemini][usize::from(selector % 2)];
    let Ok(serde_json::Value::Array(messages)) = crate::json(input) else {
        return;
    };
    let errors = crate::validate_input(adapter, &turn_body(adapter, &messages));
    let opens = user_turn(messages.first());
    let closes = user_turn(messages.last());
    if adapter == Adapter::Anthropic {
        assert_eq!(errors.contains(&"ANT-SEQ-04"), !opens);
        assert_eq!(errors.contains(&"ANT-SEQ-06"), !closes);
        assert_eq!(
            errors.contains(&"ANT-TOOL-05"),
            messages.iter().any(interrupted_results)
        );
    } else {
        assert_eq!(errors.contains(&"GEM-SEQ-05"), !(opens && closes));
        assert_eq!(
            errors.contains(&"GEM-TOOL-05"),
            unanswered_exchange(&messages)
        );
    }
    let replay = crate::projection::wire::replay_turns;
    let Some(output) = replay(adapter, &messages) else {
        return;
    };
    assert_eq!(turn_repair_violation(adapter, &messages, &output), None);
    assert_eq!(
        replay(adapter, &output).as_ref(),
        Some(&output),
        "turn repair is not idempotent"
    );
}

/// The structured detector shared by the turn oracle and its negative controls.
pub(crate) fn turn_repair_violation(
    adapter: Adapter,
    input: &[serde_json::Value],
    output: &[serde_json::Value],
) -> Option<&'static str> {
    if !user_turn(output.first()) {
        return Some("open");
    }
    if !user_turn(output.last()) {
        return Some("close");
    }
    if output
        .windows(2)
        .any(|pair| pair[0]["role"] == pair[1]["role"])
    {
        return Some("alternation");
    }
    let field = if adapter == Adapter::Anthropic {
        "content"
    } else {
        "parts"
    };
    // Text runs are normalized; every other part survives exactly once.
    let kept = |messages: &[serde_json::Value]| {
        let mut parts: Vec<_> = messages
            .iter()
            .filter_map(|message| message[field].as_array())
            .flatten()
            .filter(|part| !part["text"].is_string())
            .map(serde_json::Value::to_string)
            .collect();
        parts.sort();
        parts
    };
    if kept(input) != kept(output) {
        return Some("lost part");
    }
    // A canonical text run has no blank fragment beside another text part.
    let blank = |part: &serde_json::Value| part["text"].as_str().map(|text| text.trim().is_empty());
    if output
        .iter()
        .filter_map(|message| message[field].as_array())
        .any(|parts| {
            parts
                .windows(2)
                .any(|pair| match (blank(&pair[0]), blank(&pair[1])) {
                    (Some(left), Some(right)) => left || right,
                    _ => false,
                })
        })
    {
        return Some("fragmented text");
    }
    if adapter == Adapter::Anthropic && output.iter().any(interrupted_results) {
        return Some("interrupted results");
    }
    if adapter == Adapter::Gemini && output.windows(2).any(|pair| unordered(&pair[0], &pair[1])) {
        return Some("unordered responses");
    }
    let errors = crate::validate_input(adapter, &turn_body(adapter, output));
    errors
        .iter()
        .any(|rule| matches!(*rule, "ANT-SEQ-04" | "ANT-SEQ-06" | "GEM-SEQ-05"))
        .then_some("validator boundary")
}

fn turn_body(adapter: Adapter, messages: &[serde_json::Value]) -> serde_json::Value {
    if adapter == Adapter::Anthropic {
        serde_json::json!({ "messages": messages })
    } else {
        serde_json::json!({ "contents": messages })
    }
}

fn user_turn(message: Option<&serde_json::Value>) -> bool {
    message.is_some_and(|message| message["role"] == "user")
}

/// Independent reference, deliberately not `protocol::sequence`: some
/// `tool_result` block comes after a block of any other kind.
fn interrupted_results(message: &serde_json::Value) -> bool {
    let Some(parts) = message["content"].as_array() else {
        return false;
    };
    let result = |part: &serde_json::Value| part["type"].as_str() == Some("tool_result");
    parts
        .iter()
        .position(|part| !result(part))
        .is_some_and(|first| parts[first..].iter().any(result))
}

type Key<'a> = (Option<&'a str>, Option<&'a str>);

/// Independent reference keys, deliberately not `protocol::sequence`.
/// A part carrying both members counts as a call, as on the wire.
fn keys<'a>(parts: &'a [serde_json::Value], member: &str) -> Vec<Key<'a>> {
    parts
        .iter()
        .filter(|part| member == "functionCall" || part.get("functionCall").is_none())
        .filter_map(|part| part.get(member))
        .map(|value| (value["name"].as_str(), value["id"].as_str()))
        .collect()
}

fn unanswered_exchange(contents: &[serde_json::Value]) -> bool {
    let mut pending = Vec::new();
    let mut unanswered = false;
    for content in contents {
        let Some(parts) = content["parts"].as_array() else {
            continue;
        };
        unanswered |= keys(parts, "functionResponse") != pending;
        pending = keys(parts, "functionCall");
    }
    unanswered || !pending.is_empty()
}

/// A model turn whose distinct calls each have exactly one response in the
/// next user turn must be answered first and in call order: its raw leading
/// parts are exactly those responses, with no text ahead of them.
fn unordered(model: &serde_json::Value, user: &serde_json::Value) -> bool {
    let (Some(calls), Some(responses)) = (model["parts"].as_array(), user["parts"].as_array())
    else {
        return false;
    };
    if model["role"] != "model" || user["role"] != "user" {
        return false;
    }
    let calls = keys(calls, "functionCall");
    // The raw leading parts, text included, must be the responses.
    let leading: Vec<_> = responses
        .iter()
        .take(calls.len())
        .map_while(|part| keys(std::slice::from_ref(part), "functionResponse").pop())
        .collect();
    let responses = keys(responses, "functionResponse");
    let distinct = calls
        .iter()
        .enumerate()
        .all(|(index, call)| !calls[..index].contains(call));
    let complete = calls.iter().all(|call| {
        responses
            .iter()
            .filter(|response| *response == call)
            .count()
            == 1
    });
    !calls.is_empty() && distinct && complete && leading != calls
}

#[cfg(test)]
mod tool_arguments {
    use super::{assert_tool_arguments, portable};
    use serde_json::json;

    #[test]
    fn checked_in_byte_corpus() {
        for bytes in [
            include_bytes!("../fuzz/corpus/tool_arguments/portable-boundaries").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/negative-zero").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/unsafe-integer").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/raw-value-marker").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/number-marker-nested").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/non-object").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/duplicate-keys").as_slice(),
            include_bytes!("../fuzz/corpus/tool_arguments/lone-surrogate").as_slice(),
        ] {
            assert_tool_arguments(bytes);
        }
    }

    #[test]
    fn negative_control_reference_rejects_what_the_old_stream_rule_admitted() {
        // The deleted stream rule checked only magnitude, so it admitted both.
        for value in [
            json!({"value":-0.0}),
            json!({"$serde_json::private::RawValue":"null"}),
            json!({"n":[{"$serde_json::private::Number":"1"}]}),
        ] {
            assert!(!portable(&value), "{value}");
            assert!(acyclic_harness::conversation::validate_portable_json(&value).is_err());
        }
        for value in [
            json!({"value":9_007_199_254_740_992_u64}),
            json!({"value":1e16}),
        ] {
            assert!(!portable(&value), "{value}");
        }
        assert!(portable(
            &json!({"max":9_007_199_254_740_991_u64, "half":-0.5, "$serde_json::private":1})
        ));
    }
}
