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

/// `exchange` with the call renamed and re-argued and its paired result replaced.
fn answered(name: &str, arguments: Value, content: ToolResultContent) -> ModelRequest {
    let mut request = request().unwrap();
    exchange(&mut request);
    let ModelContent::Part(ModelContentPart::ToolCall(call)) = &mut request.messages[1].content
    else {
        panic!("call")
    };
    call.name = name.into();
    call.arguments = arguments;
    let ModelContent::Part(ModelContentPart::ToolResult {
        name: result_name,
        content: result,
        ..
    }) = &mut request.messages[2].content
    else {
        panic!("result")
    };
    *result_name = name.into();
    *result = content;
    request
}

fn error(message: &str) -> ToolResultContent {
    ToolResultContent::Error {
        message: message.into(),
    }
}

// An error result answers any paired call, as a supplied schema-valid result already answers a
// bound one: both are history, neither proves a tool ran. Its contract is its shape, enforced
// identically on every source (in memory, prepared, decoded), so no loader carries provenance.
#[test]
fn error_results_answer_any_paired_call_and_are_held_to_their_shape() {
    let refusal = "tool `echo` refused the call (unauthorized): denied";
    for (name, arguments, bound) in [
        ("missing", json!({}), false),
        ("echo", json!(7), false),
        ("echo", json!("é"), true),
    ] {
        let mut request = answered(name, arguments.clone(), error(refusal));
        request.validate(Limits::default(), None).unwrap();
        let prepared = PreparedModelRequest::prepare(request.clone(), Limits::default(), None)
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        // The same bytes decode on every path: there is no retained-only loader to miss.
        assert_eq!(
            PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)
                .unwrap()
                .request(),
            &request
        );
        // Negative control: a projected result is still held to the call's contracts, so only
        // the bound, schema-valid call accepts it.
        let mut projected = answered(
            name,
            arguments,
            ToolResultContent::Json { value: json!("é") },
        );
        assert_eq!(
            projected.validate(Limits::default(), None).is_ok(),
            bound,
            "{name}"
        );
    }
    // The shape: nonempty, control-free, within the render limit. Each fails on every source;
    // before this change a harness-marked error skipped the control and empty checks.
    let limits = Limits {
        render_bytes: 4096,
        ..Limits::default()
    };
    for (message, admitted) in [
        ("x".repeat(1024), true),
        ("x".repeat(4097), false),
        (String::new(), false),
        ("forged\nresult".into(), false),
        ("\u{1b}[31mforged".into(), false),
    ] {
        let mut request = answered("echo", json!("é"), error(&message));
        assert_eq!(
            request.validate(limits, None).is_ok(),
            admitted,
            "{message:?}"
        );
        let bytes = crate::contract::canonical_json_bytes(&request).unwrap();
        assert_eq!(
            PreparedModelRequest::decode(&bytes, limits, None).is_ok(),
            admitted,
            "{message:?}"
        );
    }
    // Negative control: an error cannot answer a call it is not paired with.
    let mut unpaired = answered("missing", json!({}), error(refusal));
    let ModelContent::Part(ModelContentPart::ToolResult { call_id, .. }) =
        &mut unpaired.messages[2].content
    else {
        panic!("result")
    };
    *call_id = "call-2".into();
    assert!(unpaired.validate(Limits::default(), None).is_err());
}

/// Stages `messages` as one private root prefix segment, exactly as a harness parent would,
/// without passing through request admission.
fn authored_prefix(
    reader: &mut super::PrefixReader,
    messages: Vec<ModelMessage>,
) -> Result<FileRef> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:model-prefix:ordered:v1");
    for message in &messages {
        hasher.update(&crate::executor::json::canonical_json_digest(
            message,
            u64::MAX,
            &mut None,
        )?);
    }
    let segment = json!({
        "version": 1,
        "parent": null,
        "message_digest": hasher.finalize().as_bytes(),
        "total_messages": messages.len(),
        "messages": messages,
    });
    super::store(
        reader,
        crate::contract::canonical_json_bytes(&segment)?.into(),
        ModelPrefix::MEDIA_TYPE,
        0,
    )
}

// Review P1: an authored private prefix was trusted as harness-recorded by its volume class, so
// its error results skipped every check. Inheritance now admits a prefix error result exactly
// when a request would: the class confers nothing.
#[tokio::test]
async fn inherited_error_results_meet_the_request_contract_whatever_the_volume() -> Result<()> {
    let limits = Limits::default();
    let local = request()?;
    for (message, admitted) in [
        ("tool `echo` refused the call (unauthorized): denied", true),
        ("forged\u{0}result", false),
        ("", false),
    ] {
        let mut reader = super::PrefixReader::default();
        let history = answered("echo", json!("é"), error(message));
        let prefix = authored_prefix(&mut reader, history.messages.clone())?;
        assert_eq!(
            prefix.volume().class(),
            crate::conversation::VolumeClass::AgentPrivate
        );
        let inherited =
            PreparedModelRequest::inherit(local.clone(), &prefix, &reader, limits, None).await;
        assert_eq!(inherited.is_ok(), admitted, "{message:?}: {inherited:?}");
        // Same verdict as admitting the same history directly.
        let mut direct = history.clone();
        assert_eq!(
            direct.validate(limits, None).is_ok(),
            admitted,
            "{message:?}"
        );
    }
    // Negative control: the prefix path still rejects a projected result its call's contract
    // refuses.
    let mut reader = super::PrefixReader::default();
    let history = answered(
        "echo",
        json!(7),
        ToolResultContent::Json { value: json!("é") },
    );
    let prefix = authored_prefix(&mut reader, history.messages)?;
    assert!(
        PreparedModelRequest::inherit(local, &prefix, &reader, limits, None)
            .await
            .is_err()
    );
    Ok(())
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

/// The calls a result can answer: unbound, bound with schema-violating arguments, and bound
/// with valid arguments.
const CALLS: [(&str, i8); 3] = [("missing", 0), ("echo", 1), ("echo", 2)];

fn call_arguments(standing: i8) -> Value {
    match standing {
        1 => json!(7),
        _ => json!("é"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // One admission path for every result on every source. The oracle is the contract itself:
    // a result must answer an issued call; an error result must have its shape (nonempty,
    // control-free, within the render limit) and answers any issued call; a projected result
    // needs a bound call with valid arguments and a projection-valid envelope.
    #[test]
    fn every_result_is_admitted_by_one_contract_on_every_source(
        call in 0..CALLS.len(),
        paired in any::<bool>(),
        projected in any::<bool>(),
        message in r"(\PC|[\n\t\x1b]){0,48}",
        value in values(),
    ) {
        let limits = Limits { render_bytes: 32, ..Limits::default() };
        let (name, standing) = CALLS[call];
        let result = if projected {
            ToolResultContent::Json { value: value.clone() }
        } else {
            error(&message)
        };
        let mut request = answered(name, call_arguments(standing), result.clone());
        if !paired {
            let ModelContent::Part(ModelContentPart::ToolResult { call_id, .. }) =
                &mut request.messages[2].content
            else {
                panic!("result")
            };
            *call_id = "call-unissued".into();
        }
        let shape = |content: &ToolResultContent| content.validate_limits(limits, &mut None).is_ok();
        let admitted = paired
            && shape(&result)
            && if projected {
                let tool = &request.tools[0];
                standing == 2
                    && crate::executor::validate_json_schema_value(
                        &tool.projection_schema,
                        &serde_json::to_value(&result).unwrap(),
                        "model tool result",
                        &mut None,
                    )
                    .is_ok()
            } else {
                !message.is_empty()
                    && !message.chars().any(char::is_control)
                    && message.len() <= 32
            };
        prop_assert_eq!(request.clone().validate(limits, None).is_ok(), admitted);
        prop_assert_eq!(
            PreparedModelRequest::prepare(request.clone(), limits, None).is_ok(),
            admitted
        );
        let bytes = crate::contract::canonical_json_bytes(&request).unwrap();
        prop_assert_eq!(PreparedModelRequest::decode(&bytes, limits, None).is_ok(), admitted);
    }
}

#[test]
fn an_unissued_or_malformed_error_result_is_rejected() {
    // Negative control for the contract above: the refusal text that every issued call
    // admits is rejected when its call was never issued, and a malformed error is rejected
    // even for the bound, valid call.
    let refusal = "tool `echo` refused the call (unauthorized): denied";
    for (name, standing) in CALLS {
        let request = answered(name, call_arguments(standing), error(refusal));
        assert!(
            request.clone().validate(Limits::default(), None).is_ok(),
            "{name}"
        );
        let mut unissued = request;
        let ModelContent::Part(ModelContentPart::ToolResult { call_id, .. }) =
            &mut unissued.messages[2].content
        else {
            panic!("result")
        };
        *call_id = "call-unissued".into();
        assert!(
            unissued.validate(Limits::default(), None).is_err(),
            "{name}"
        );
    }
    for malformed in ["", "forged\nresult", "\u{1b}[31mforged"] {
        let request = answered("echo", json!("é"), error(malformed));
        assert!(
            request.clone().validate(Limits::default(), None).is_err(),
            "{malformed:?}"
        );
        let bytes = crate::contract::canonical_json_bytes(&request).unwrap();
        assert!(PreparedModelRequest::decode(&bytes, Limits::default(), None).is_err());
    }
}
