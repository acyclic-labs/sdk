use super::*;
use proptest::prelude::*;

fn values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        any::<f64>().prop_map(|value| serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)),
        ".{0,64}".prop_map(Value::String)
    ]
    .prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(".{0,16}", inner, 0..4)
                .prop_map(|entries| Value::Object(entries.into_iter().collect())),
        ]
    })
}

fn with_result(value: Value, schema: Value) -> ModelRequest {
    let mut request = request().unwrap();
    exchange(&mut request);
    request.tools[0].projection_schema = schema;
    let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) =
        &mut request.messages[2].content
    else {
        panic!("result")
    };
    *content = ToolResultContent::Json { value };
    request
}

fn content(request: &ModelRequest) -> &ToolResultContent {
    let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) =
        &request.messages[2].content
    else {
        panic!("result")
    };
    content
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Reusing the owned projection must preserve schema outcomes (including
    // exact errors), wire bytes, and the original value on success and failure.
    #[test]
    fn projection_reuse_matches_the_serialized_envelope(value in values(), schema in 0u8..6, wrapped in any::<bool>()) {
        let strict = json!({"type":"object", "properties":{"kind":{"const":"json"}}, "required":["kind"]});
        let schema = match schema {
            0 => json!(true), 1 => json!(false), 2 => strict.clone(),
            3 => json!({"properties":{"value":{"type":"string"}}}),
            4 => json!({"properties":{"value":{"required":["missing"]}}}),
            _ => json!({"not":{"properties":{"kind":{"const":"json"}}}}),
        };
        let value = if wrapped { json!({"sentinel":"retained", "payload":value}) } else { value };
        let mut request = with_result(value, schema.clone());
        let original = request.clone();
        let envelope = serde_json::to_value(content(&request)).unwrap();
        let ToolResultContent::Json { value } = content(&request) else { unreachable!() };
        let owned_pointer = value["sentinel"].as_str().filter(|s| !s.is_empty()).map(str::as_ptr);
        if let Some(pointer) = owned_pointer {
            prop_assert_ne!(pointer, envelope["value"]["sentinel"].as_str().unwrap().as_ptr());
        }
        let expected = crate::executor::validate_json_schema_value(&schema, &envelope, "model tool result", &mut None);
        let actual = request.validate(Limits::default(), None);
        prop_assert_eq!(actual.map_err(|e| e.to_string()), expected.map_err(|e| e.to_string()));
        prop_assert_eq!(&request, &original);
        let ToolResultContent::Json { value } = content(&request) else { unreachable!() };
        prop_assert_eq!(value["sentinel"].as_str().filter(|s| !s.is_empty()).map(str::as_ptr), owned_pointer);
        prop_assert_eq!(crate::contract::canonical_json_bytes(&request).unwrap(), crate::contract::canonical_json_bytes(&original).unwrap());
        request.tools[0].projection_schema = strict.clone();
        prop_assert!(request.validate(Limits::default(), None).is_ok());
        let mut wrong = envelope;
        wrong["kind"] = json!("parts");
        prop_assert!(crate::executor::validate_json_schema_value(&strict, &wrong, "model tool result", &mut None).is_err());
    }

    // A permissive schema needs no payload ownership. The real request
    // validator must allocate less than even one copy of the large string,
    // regardless of nesting; the former conversion violates this bound.
    #[test]
    fn request_validation_does_not_copy_large_json(extra in 0usize..8192, depth in 0usize..8) {
        with_result(Value::Null, json!(true)).validate(Limits::default(), None).unwrap();
        let length = 256 * 1024 + extra;
        let mut value = Value::String("x".repeat(length));
        for _ in 0..depth {
            value = Value::Object(serde_json::Map::from_iter([("nested".into(), Value::Array(vec![value]))]));
        }
        let mut request = with_result(value, json!(true));
        let mut result = None;
        let actual = acyclic_test_support::measure(|| { result = Some(request.validate(Limits::default(), None)); });
        prop_assert!(result.unwrap().is_ok());
        let mut copied = None;
        let old = acyclic_test_support::measure(|| { copied = Some(serde_json::to_value(content(&request)).unwrap()); });
        let no_payload_copy = |work: acyclic_test_support::AllocationInfo| work.bytes_total < length as u64;
        prop_assert!(no_payload_copy(actual), "request copied its JSON payload: {actual:?}");
        prop_assert!(!no_payload_copy(old), "former conversion escaped detection: {old:?}");
    }
}
