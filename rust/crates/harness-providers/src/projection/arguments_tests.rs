use super::*;
use acyclic_harness::model::{ModelContentPart, ToolCall, ToolResultContent};
use proptest::prelude::*;

fn values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|value| json!(value)),
        prop::collection::vec(any::<char>(), 0..32)
            .prop_map(|chars| Value::String(chars.into_iter().collect()))
    ]
    .prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(".{0,32}", inner, 0..4)
                .prop_map(|values| Value::Object(values.into_iter().collect()))
        ]
    })
}

fn assert_projection(value: &Value) {
    crate::projection::arguments::assert_normalization(value);
    for adapter in [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Gemini,
    ] {
        let (provider, _) = provider(adapter, 200);
        let (prepared, _) = request(&provider).unwrap();
        let mut request = prepared.request().clone();
        request.messages.extend([
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                    "call_0".into(),
                    "echo".into(),
                    value.clone(),
                ))),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call_0".into(),
                    name: "echo".into(),
                    content: ToolResultContent::Json {
                        value: json!("done"),
                    },
                }),
            },
        ]);
        let names = crate::tool_names::Names::new(&request);
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let projected = crate::projection::body_named(
            &provider,
            &request,
            &names,
            &origin,
            &mut crate::files::FileText::default(),
        )
        .unwrap();
        let ModelContent::Part(ModelContentPart::ToolCall(source)) = &request.messages[2].content
        else {
            unreachable!()
        };
        let borrowed = projected.borrowed_arguments();
        assert_eq!(borrowed.len(), 1);
        assert_borrowed(&source.arguments, borrowed[0]);
        let wire = crate::projection::arguments::literal(&serde_json::to_vec(&projected).unwrap())
            .unwrap();
        let args = match adapter {
            Adapter::Anthropic => wire["messages"][1]["content"][0]["input"].clone(),
            Adapter::Gemini => wire["contents"][1]["parts"][0]["functionCall"]["args"].clone(),
            Adapter::Chat => crate::projection::arguments::literal(
                wire["messages"][2]["tool_calls"][0]["function"]["arguments"]
                    .as_str()
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap(),
            Adapter::Responses => crate::projection::arguments::literal(
                wire["input"][2]["arguments"].as_str().unwrap().as_bytes(),
            )
            .unwrap(),
        };
        crate::projection::arguments::assert_value(value, &args);
        if matches!(adapter, Adapter::Chat | Adapter::Responses) {
            assert!(
                crate::validate_input(adapter, &wire).is_empty(),
                "{adapter:?}: rejected projected arguments"
            );
        }
        assert_eq!(
            request.messages[2].content.parts()[0],
            ModelContentPart::ToolCall(ToolCall::new(
                "call_0".into(),
                "echo".into(),
                value.clone()
            ))
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn arbitrary_history_arguments_are_lossless_idempotent_objects_in_every_protocol(value in values()) { assert_projection(&value); }
}

#[test]
fn raw_marker_names_and_non_object_fixtures_are_preserved() {
    for value in [
        json!(null),
        json!([1]),
        json!({"_raw":[1]}),
        json!(["line\n", 0, false]),
        json!("{}"),
        json!({"_raw":null}),
        json!({"$serde_json::private::RawValue":"null","nested":[]}),
    ] {
        assert_projection(&value);
    }
}

#[test]
#[should_panic(expected = "normalized.is_object()")]
fn negative_control_unwrapped_arguments_fail_the_same_property() {
    let original = json!([1]);
    let wire = serde_json::to_vec(&original).unwrap();
    let normalized = serde_json::from_slice(&wire).unwrap();
    crate::projection::arguments::assert_value(&original, &normalized);
}

#[test]
#[should_panic]
fn negative_control_double_wrapping_changes_the_original_value() {
    let original = json!([1]);
    let normalized = json!({"_raw":{"_raw":original}});
    crate::projection::arguments::assert_value(&original, &normalized);
}

#[test]
fn negative_control_default_value_decode_misinterprets_literal_marker_keys() {
    let original = json!({"$serde_json::private::RawValue":"null","nested":[]});
    let bytes = serde_json::to_vec(&original).unwrap();
    assert!(serde_json::from_slice::<Value>(&bytes).is_err());
    assert!(serde_json::from_value::<Value>(original.clone()).is_err());
    assert_eq!(
        crate::projection::arguments::literal(&bytes).unwrap(),
        original
    );
}

#[test]
#[should_panic(expected = "exactly one _raw entry")]
fn negative_control_missing_null_is_not_an_empty_object() {
    crate::projection::arguments::assert_value(&Value::Null, &json!({}));
}

fn assert_borrowed(source: &Value, projected: &Value) {
    assert!(
        std::ptr::eq(source, projected),
        "wire calls must borrow canonical arguments"
    );
}

#[test]
#[should_panic(expected = "wire calls must borrow canonical arguments")]
fn negative_control_materialized_arguments_fail_borrow_identity() {
    let source = json!({"nested":[1,2,3]});
    let copied = source.clone();
    assert_borrowed(&source, &copied);
}

#[test]
fn object_argument_validation_checks_literals_without_marker_interpretation() {
    for raw in [
        r#"{"$serde_json::private::RawValue":"null","nested":[]}"#,
        r#" {"text":"quote\" newline\n slash\\"} "#,
    ] {
        assert!(crate::protocol::strict::object(raw));
    }
    for raw in [
        "null",
        "[]",
        r#"{"text":"\ud800"}"#,
        "{} trailing",
        r#"{"x":}"#,
    ] {
        assert!(!crate::protocol::strict::object(raw), "accepted {raw}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn generated_envelopes_preserve_every_admitted_argument_depth(depth in 0usize..=128) {
        let input = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        let value = crate::projection::arguments::literal(input.as_bytes()).unwrap();
        crate::projection::arguments::assert_normalization(&value);
    }
}

#[test]
fn checked_in_boundary_depth_runs_the_full_codec_fuzz_oracle() {
    let seed = include_bytes!("../../fuzz/corpus/provider_stream/arguments-depth-boundary.json");
    assert_eq!(seed.len(), 258);
    assert_eq!(seed.iter().filter(|byte| **byte == b'[').count(), 128);
    crate::projection::arguments::literal(&seed[1..]).unwrap();
    crate::fuzzing::assert_stream_invariants(seed);
    let too_deep = format!("{}0{}", "[".repeat(129), "]".repeat(129));
    assert!(crate::projection::arguments::literal(too_deep.as_bytes()).is_err());
}

#[test]
fn literal_model_payloads_reject_depth_beyond_the_existing_record_bound() {
    let arguments = (0..129).fold(json!(0), |inner, _| json!([inner]));
    let event = acyclic_harness::model::ModelEvent::ToolCall(ToolCall::new(
        "call".into(),
        "echo".into(),
        arguments,
    ));
    let value = serde_json::to_value(&event).unwrap();
    assert!(serde_json::from_value::<acyclic_harness::model::ModelEvent>(value).is_err());
}
