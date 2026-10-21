//! The cargo-fuzz entry point uses the same streaming path as model dispatch.
use crate::{Adapter, stream};
use acyclic_harness_core::{
    OperationId,
    model::{ModelDispatch, ModelEvent},
};
use bytes::Bytes;
use futures::TryStreamExt as _;

mod projections;
mod tool_arguments;
mod turn_text;
pub use projections::{assert_alloy_invariants, assert_image_invariants};
pub use tool_arguments::assert_tool_arguments;
use turn_text::text_matches;

fn parse(adapter: Adapter, input: &[u8], width: usize) -> Result<Vec<ModelEvent>, String> {
    parse_at(adapter, input, width, [1; 16])
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
    if kind == b'H' {
        if let Ok(header) = std::str::from_utf8(input) {
            let clock = || std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
            let wait = crate::retry::retry_after_at(header, clock);
            let digits = header.trim();
            if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) {
                assert!(wait.is_some());
            }
            if let Some(wait) = wait {
                // Normalize either accepted form to delta-seconds without losing its delay.
                assert_eq!(
                    crate::retry::retry_after_at(&wait.as_secs().to_string(), clock),
                    Some(wait)
                );
            }
        }
        return;
    }
    if kind == b'F' {
        if let Ok(file) =
            serde_json::from_slice::<acyclic_harness_core::conversation::FileRef>(input)
        {
            let marker = crate::files::FileText::default()
                .project(
                    &file,
                    &acyclic_harness_core::model::FileProjectionPolicy::Reference,
                )
                .unwrap();
            let restored: acyclic_harness_core::conversation::FileRef = serde_json::from_str(
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
        if let Ok(block) =
            serde_json::from_slice::<acyclic_harness_core::model::ReasoningBlock>(input)
        {
            let encoded = serde_json::to_vec(&block).unwrap();
            let replay: acyclic_harness_core::model::ReasoningBlock =
                serde_json::from_slice(&encoded).unwrap();
            assert_eq!(block, replay);
            for adapter in [crate::Adapter::Responses, crate::Adapter::Anthropic] {
                if let Some(raw) =
                    crate::projection::native_reasoning::validated(adapter, &block.continuity)
                {
                    // Independent bounded literal decoder: native admission must
                    // reject invalid strings, including inside skipped fields.
                    let bounds = acyclic_harness_core::JsonBounds {
                        maximum_bytes: block.continuity.len() as u64,
                        maximum_items: block.continuity.len() as u64,
                        maximum_depth: 128,
                    };
                    assert!(
                        acyclic_harness_core::json_value_bounded(
                            raw,
                            bounds,
                            None,
                            &mut |_| Ok(()),
                        )
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
        if let Ok(request) =
            serde_json::from_slice::<acyclic_harness_core::model::ModelRequest>(input)
        {
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
    fn checked_in_retry_after_corpus_obeys_the_properties() {
        for seed in [
            include_bytes!("../fuzz/corpus/provider_stream/retry-after-date").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/retry-after-delta").as_slice(),
            include_bytes!("../fuzz/corpus/provider_stream/retry-after-overflow").as_slice(),
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
    use acyclic_harness_core::model::{
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
    round_trip(acyclic_harness_core::tool::ToolInvocation {
        operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
        call_id: "call".into(),
        name: "echo".into(),
        arguments: value.clone(),
    });
    round_trip(acyclic_harness_core::tool::ToolResult {
        value: value.clone(),
    });
    round_trip(acyclic_harness_core::tool::ToolDefinition {
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
    if errors.is_empty() && canonical_turns(adapter, &messages) {
        assert_eq!(output, messages, "valid canonical turns were changed");
    }
    assert_eq!(
        replay(adapter, &output).as_ref(),
        Some(&output),
        "turn repair is not idempotent"
    );
}

/// Turns repair must leave unchanged: envelopes holding only the role and
/// parts that replay represents, non-empty, alternating, already satisfying
/// every repair rule, and with no blank Anthropic text block.
pub(crate) fn canonical_turns(adapter: Adapter, messages: &[serde_json::Value]) -> bool {
    let field = turn_field(adapter);
    messages.iter().all(|message| {
        message
            .as_object()
            .is_some_and(|envelope| envelope.len() == 2)
            && message["role"].is_string()
            && message[field].as_array().is_some_and(|parts| {
                !parts.is_empty()
                    && (adapter == Adapter::Gemini
                        || parts.iter().all(|part| {
                            part["text"]
                                .as_str()
                                .is_none_or(|text| !text.trim().is_empty())
                        }))
            })
    }) && turn_repair_violation(adapter, messages, messages).is_none()
}

fn turn_field(adapter: Adapter) -> &'static str {
    if adapter == Adapter::Anthropic {
        "content"
    } else {
        "parts"
    }
}

/// Every non-text block in order, split into the groups repair may move
/// relative to each other: results (whose Gemini order follows the calls) and
/// everything else.
fn blocks<'a>(
    adapter: Adapter,
    messages: &'a [serde_json::Value],
) -> (Vec<&'a serde_json::Value>, Vec<&'a serde_json::Value>) {
    messages
        .iter()
        .filter_map(|message| message[turn_field(adapter)].as_array())
        .flatten()
        .filter(|part| !part["text"].is_string())
        .partition(|part| {
            if adapter == Adapter::Anthropic {
                part["type"].as_str() == Some("tool_result")
            } else {
                part.get("functionCall").is_none() && part.get("functionResponse").is_some()
            }
        })
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
    let field = turn_field(adapter);
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
    // Within each group, blocks keep their order; Gemini results are ordered
    // by their calls instead, which `unordered` checks below.
    let ((results, others), (moved, rest)) = (blocks(adapter, input), blocks(adapter, output));
    if others != rest || (adapter == Adapter::Anthropic && results != moved) {
        return Some("reordered parts");
    }
    if !text_matches(adapter, input, output) {
        return Some("changed text");
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

/// Accepted provider catalogs preserve capacities/defaults through the production parser.
pub fn assert_catalog_invariants(input: &[u8]) {
    if let Ok(document) = crate::catalog_document::parse(input) {
        let encoded = serde_json::to_vec(&document).unwrap();
        let replay = crate::catalog_document::parse(&encoded).unwrap();
        assert_eq!(document.default_model, replay.default_model);
        assert_eq!(
            serde_json::to_value(document.providers).unwrap(),
            serde_json::to_value(replay.providers).unwrap()
        );
    }
}
