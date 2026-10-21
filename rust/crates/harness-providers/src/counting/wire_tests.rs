use super::*;
use proptest::prelude::*;
use serde_json::Value;
use serde_json::json;

fn body(value: Value, model: &str, adapter: Adapter) -> Result<Value> {
    let field = match adapter {
        Adapter::Responses => "input",
        Adapter::Gemini => "contents",
        _ => "messages",
    };
    let generation = crate::projection::wire::Body::fixture(value, field);
    serde_json::to_value(super::body(&generation, model, adapter))
        .map_err(|_| invalid("fixture encoding failed"))
}

fn spec(adapter: Adapter) -> TokenCounterSpec {
    TokenCounterSpec {
        path: "responses/input_tokens".into(),
        adapter,
        response_field: TokenCountField::InputTokens,
        framing_tokens: 0,
    }
}

fn assert_count_wire(actual: &Value, expected: &Value) {
    assert_eq!(actual, expected);
}

#[test]
fn response_count_request_keeps_every_input_field_without_generation_controls() {
    let original = json!({"model":"gpt-6-luna", "input":[{"role":"user", "content":"dense"}],
        "instructions":"system", "tools":[{"type":"function", "name":"echo"}],
        "tool_choice":"required", "parallel_tool_calls":false,
        "reasoning":{"effort":"high"}, "personality":"pragmatic", "truncation":"disabled",
        "text":{"format":{"type":"json_schema", "name":"result", "schema":{"type":"object"}}},
        "stream":true, "store":false, "max_output_tokens":128});
    let mut expected = original.clone();
    for field in ["stream", "store", "max_output_tokens"] {
        expected.as_object_mut().unwrap().remove(field);
    }
    assert_eq!(
        body(original, "gpt-6-luna", Adapter::Responses).unwrap(),
        expected
    );
}

#[test]
fn gemini_counts_the_full_generate_request_with_system_tools_and_schema() {
    let input = json!({"contents":[{"role":"user", "parts":[{"text":"dense"}]}],
        "systemInstruction":{"parts":[{"text":"system"}]},
        "tools":[{"functionDeclarations":[{"name":"echo"}]}],
        "generationConfig":{"responseJsonSchema":{"type":"object"}}});
    let counted = body(input.clone(), "gemini-2.5-pro", Adapter::Gemini).unwrap();
    let mut expected = input;
    expected["model"] = json!("models/gemini-2.5-pro");
    assert_eq!(counted, json!({"generateContentRequest":expected}));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn borrowed_count_serialization_matches_the_protocol_and_preserves_generation(text in ".{0,64}", output in any::<u32>()) {
        for adapter in [Adapter::Chat, Adapter::Anthropic, Adapter::Responses, Adapter::Gemini] {
            let field = match adapter { Adapter::Responses => "input", Adapter::Gemini => "contents", _ => "messages" };
            let mut original = json!({"model":"model", "stream":true, "max_output_tokens":output,
                "tools":[], "reasoning":{"effort":"high"}, "text":{"format":{"type":"text"}}});
            original[field] = json!([{"role":"user","content":text}]);
            let generation = crate::projection::wire::Body::fixture(original.clone(), field);
            let counted = serde_json::to_value(super::body(&generation, "model", adapter)).unwrap();
            // Independent wire expectations, rather than comparing two implementations.
            let mut expected = json!({"model":"model", "tools":[]});
            expected[field] = original[field].clone();
            if adapter == Adapter::Responses {
                expected["reasoning"] = original["reasoning"].clone();
                expected["text"] = original["text"].clone();
            }
            if adapter == Adapter::Gemini {
                expected = original.clone();
                expected["model"] = json!("models/model");
                expected = json!({"generateContentRequest":expected});
            }
            assert_count_wire(&counted, &expected);
            // A missing filter/envelope fails the same independent oracle.
            prop_assert!(std::panic::catch_unwind(|| assert_count_wire(&original, &expected)).is_err());
            prop_assert_eq!(serde_json::to_value(&generation).unwrap(), original);
        }
    }
    #[test]
    fn admitted_counts_include_the_configured_framing_bound(count in 0u64..=9_007_199_254_740_991, overhead in any::<u32>()) {
        let mut policy = spec(Adapter::Responses);policy.framing_tokens = overhead;
        let bytes = serde_json::to_vec(&json!({"input_tokens":count})).unwrap();
        let admitted = response(&bytes, &policy);
        let expected = count + u64::from(overhead);
        if expected > acyclic_harness_core::conversation::MAX_EXACT_JS_INTEGER {
            prop_assert!(admitted.is_err());
        } else {
            prop_assert_eq!(admitted.unwrap(), expected);
            prop_assert!(expected >= count);
        }
    }
    #[test]
    fn count_paths_never_forward_credentials_to_another_origin(path in ".{0,96}") {
        let mut policy = spec(Adapter::Responses); policy.path = path;
        if let Ok(resolved) = endpoint("https://provider.test/v1", "gpt-6-luna", &policy) {
            prop_assert_eq!(url::Url::parse(&resolved).unwrap().origin(), url::Url::parse("https://provider.test").unwrap().origin());
        }
    }
    #[test]
    fn accepted_count_wire_values_have_a_lossless_canonical_codec(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        let policy = spec(Adapter::Responses);
        if let Ok(count) = response(&bytes, &policy) {
            let encoded = serde_json::to_vec(&json!({"input_tokens":count})).unwrap();
            prop_assert_eq!(response(&encoded, &policy).unwrap(), count);
        }
    }
}

#[test]
fn malformed_counts_never_expose_response_values() {
    for value in [
        json!(-1),
        json!(1.5),
        json!("fixture-secret"),
        json!(null),
        json!(u64::MAX),
    ] {
        let result = response(
            &serde_json::to_vec(&json!({"input_tokens":value})).unwrap(),
            &spec(Adapter::Responses),
        );
        assert!(result.is_err());
        assert!(!result.unwrap_err().to_string().contains("fixture-secret"));
    }
}

#[test]
fn integer_fields_reject_private_marker_maps_instead_of_normalizing_them() {
    for marker in [
        "$serde_json::private::Number",
        "$serde_json::private::RawValue",
    ] {
        let bytes = serde_json::to_vec(&json!({"input_tokens":{marker:"0"}})).unwrap();
        assert!(response(&bytes, &spec(Adapter::Responses)).is_err());
    }
}
