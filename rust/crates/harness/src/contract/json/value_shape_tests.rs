use super::*;
use super::value_shape::ValueInspector;
use std::collections::BTreeMap;

const RAW: &str = "$serde_json::private::RawValue";
const LIMITS: JsonBounds = JsonBounds {
    maximum_bytes: 16 * 1024 * 1024,
    maximum_items: 2 * 1024 * 1024,
    maximum_depth: 256,
};

// Oracle construction belongs exclusively to controls: production never makes
// an unchecked Value or reparses a Raw string to discover its resource shape.
fn raw_document(payload: &str) -> String {
    format!("{{\"{RAW}\":{}}}", serde_json::to_string(payload).unwrap())
}

fn raw_layers(mut payload: String, layers: usize) -> (String, u64, u64) {
    let mut bytes = 0;
    let mut items = 0;
    for _ in 0..layers {
        bytes += payload.len() as u64;
        items += lexical_items(payload.as_bytes());
        payload = raw_document(&payload);
    }
    (payload, bytes, items)
}

fn lexical_items(bytes: &[u8]) -> u64 {
    let mut scan = Scan::default();
    scan.push_inline(bytes, LIMITS, &mut |_| Ok(())).unwrap_or_else(|_| panic!("oracle fixture lexical bounds"));
    scan.end_inline().unwrap_or_else(|_| panic!("oracle fixture lexical framing"));
    scan.items
}

fn inspect<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], schema: Option<JsonValueSchema>, bounds: JsonBounds, charge: &mut F,
) -> std::result::Result<JsonValueShape, AdmissionFailure> {
    let mut inspector = ValueInspector::new(schema, bounds);
    for &byte in bytes { inspector.push(byte, charge)?; }
    inspector.finish(bytes.len() as u64, lexical_items(bytes))
}

fn shape(bytes: &[u8], schema: JsonValueSchema) -> JsonValueShape {
    inspect(bytes, Some(schema), LIMITS, &mut |_| Ok(()))
        .unwrap_or_else(|_| panic!("valid fixture must have an admitted shape"))
}

// These are concrete observable capacities, not allocator/RSS observations.
// BTreeMap nodes are opaque; its logical pairs are the documented codec model.
fn concrete_owned(value: &Value) -> u64 {
    match value {
        Value::String(text) => text.capacity() as u64,
        Value::Array(values) => (values.capacity() * std::mem::size_of::<Value>()) as u64
            + values.iter().map(concrete_owned).sum::<u64>(),
        Value::Object(values) => (values.len() * std::mem::size_of::<(String, Value)>()) as u64
            + values.iter().map(|(key, value)| key.capacity() as u64 + concrete_owned(value)).sum::<u64>(),
        _ => 0,
    }
}

fn semantic_items(value: &Value) -> u64 {
    1 + match value {
        Value::Array(values) => values.iter().map(semantic_items).sum::<u64>(),
        Value::Object(values) => values.iter().map(|(_, value)| 1 + semantic_items(value)).sum::<u64>(),
        _ => 0,
    }
}

#[test]
fn first_private_key_matches_pinned_serde_including_unicode_escapes() {
    let payload = r#"{"z":[0.1,-0.0],"a":18446744073709551615,"z":[2]}"#;
    let ordinary = raw_document(payload);
    let escaped = ordinary.replacen("$serde_json::private::RawValue", r#"\u0024serde_json::private::Raw\u0056alue"#, 1);
    let expected: Value = serde_json::from_str(payload).unwrap();
    for document in [&ordinary, &escaped] {
        let actual: Value = serde_json::from_str(document).unwrap();
        assert_eq!(actual, expected);
        let actual_shape = shape(document.as_bytes(), JsonValueSchema::Root);
        assert_eq!(actual_shape.raw_bytes, payload.len() as u64);
        assert_eq!(actual_shape.raw_frames, 1);
        assert_eq!(actual_shape.virtual_items, lexical_items(payload.as_bytes()));
        assert!(!actual_shape.unresolved_raw);
        assert!(actual_shape.lexical_items + actual_shape.virtual_items >= semantic_items(&actual));
        assert!(json_value_decode_owned_bytes(actual_shape).unwrap() >= concrete_owned(&actual));
    }
    let later = format!("{{\"ordinary\":0,\"{RAW}\":{}}}", serde_json::to_string(payload).unwrap());
    let actual: Value = serde_json::from_str(&later).unwrap();
    assert_eq!(actual[RAW], payload);
    let later_shape = shape(later.as_bytes(), JsonValueSchema::Root);
    assert_eq!(later_shape.raw_bytes, 0);
    assert_eq!(later_shape.raw_frames, 0);
    assert_eq!(later_shape.virtual_items, 0);
}

#[test]
fn raw_payload_bytes_and_frame_counts_are_conserved_across_layers() {
    let leaf = serde_json::to_string(&"\\\"\n😀".repeat(8192)).unwrap();
    for layers in 1..=5 {
        let (document, raw_bytes, virtual_items) = raw_layers(leaf.clone(), layers);
        let mut inspected = 0;
        let actual_shape = inspect(document.as_bytes(), Some(JsonValueSchema::Root), LIMITS, &mut |event| {
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        }).unwrap_or_else(|_| panic!("large recursive leaf is within declared limits"));
        let actual: Value = serde_json::from_str(&document).unwrap();
        assert_eq!(actual_shape.raw_bytes, raw_bytes);
        assert_eq!(actual_shape.raw_frames, layers as u64);
        assert_eq!(actual_shape.virtual_items, virtual_items);
        assert_eq!(inspected, raw_bytes);
        assert_eq!(actual_shape.input_bytes, document.len() as u64);
        assert_eq!(actual_shape.lexical_items, lexical_items(document.as_bytes()));
        // RawBox payloads, geometric parser scratch, and boxed-string shrink
        // overlap must not disappear when the final tree is only one String.
        let bound = json_value_decode_owned_bytes(actual_shape).unwrap();
        assert!(bound >= concrete_owned(&actual) + raw_bytes);
    }
}

#[test]
fn ordinary_typed_string_map_keeps_private_key_literal_and_skips_virtual_bounds() {
    let hidden = format!("{}0{}", "[".repeat(1024), "]".repeat(1024));
    let document = raw_document(&hidden);
    let limits = JsonBounds { maximum_items: 3, maximum_depth: 1, ..LIMITS };
    for schema in [None, Some(JsonValueSchema::None)] {
        let mut inspected = 0;
        let admitted = inspect(document.as_bytes(), schema, limits, &mut |event| {
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        });
        assert!(admitted.is_ok(), "unknown or audited no-Value must not semantically parse a StringMap");
        assert_eq!(inspected, 0);
        let admitted = admitted.unwrap_or_else(|_| panic!("ordinary typed scope remains admissible"));
        assert_eq!(admitted.unresolved_raw, schema.is_none(),
            "unknown typed scopes use conservative fallback, audited None needs no Raw quota");
        assert_eq!(admitted.virtual_items, 0);
        assert_eq!(admitted.raw_bytes, 0);
    }
    let decoded: BTreeMap<String, String> = json_from_slice_bounded_trusted(
        document.as_bytes(), limits, 32 * 1024, &mut |_| Ok(()),
    ).unwrap();
    assert_eq!(decoded[RAW], hidden);
}

#[test]
fn interaction_selection_is_gated_by_real_tag_not_field_order_or_name_alone() {
    let raw = raw_document("[0,1,2]");
    for document in [
        format!("{{\"type\":\"question\",\"prompt\":\"p\",\"response_schema\":{raw}}}"),
        format!("{{\"response_schema\":{raw},\"prompt\":\"p\",\"type\":\"question\"}}"),
        format!(r#"{{"response_\u0073chema":{raw},"prompt":"p","t\u0079pe":"ques\u0074ion"}}"#),
        format!("{{\"type\":\"form\",\"title\":\"f\",\"schema\":{raw}}}"),
        format!(r#"{{"sche\u006da":{raw},"title":"f","type":"f\u006frm"}}"#),
    ] {
        let actual: crate::interaction::Interaction = serde_json::from_str(&document).unwrap();
        assert!(matches!(actual, crate::interaction::Interaction::Question { .. } | crate::interaction::Interaction::Form { .. }));
        let selected = shape(document.as_bytes(), JsonValueSchema::Interaction);
        assert_eq!(selected.raw_bytes, 7);
        assert_eq!(selected.virtual_items, 4);
        assert_eq!(selected.raw_frames, 1);
    }
    let deep = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    let ignored = format!("{{\"type\":\"choice\",\"prompt\":\"p\",\"options\":[{{\"id\":\"a\",\"label\":\"a\",\"schema\":{deep}}}],\"min_selections\":1,\"max_selections\":1}}");
    let actual: crate::interaction::Interaction = serde_json::from_str(&ignored).unwrap();
    assert!(matches!(actual, crate::interaction::Interaction::Choice { .. }));
    let ignored_shape = shape(ignored.as_bytes(), JsonValueSchema::Interaction);
    assert_eq!(ignored_shape.virtual_items, 0);
    assert_eq!(ignored_shape.raw_frames, 0);
    // A potential root schema preceding an unrelated discriminator may need
    // inspection work, but cannot become Value authority or a bounds rejection.
    let unrelated = format!("{{\"schema\":{deep},\"type\":\"choice\"}}");
    assert!(inspect(unrelated.as_bytes(), Some(JsonValueSchema::Interaction), JsonBounds {
        maximum_items: 16, maximum_depth: 3, ..LIMITS
    }, &mut |_| Ok(())).is_ok());
}

#[test]
fn control_response_selects_only_root_result_not_ordinary_strings_or_unknowns() {
    let raw = raw_document("[true,false]");
    let selected = format!("{{\"requestId\":\"r\",\"result\":{raw}}}");
    let selected_shape = shape(selected.as_bytes(), JsonValueSchema::ControlResponse);
    assert_eq!(selected_shape.raw_bytes, 12);
    assert_eq!(selected_shape.virtual_items, 3);
    for document in [
        format!("{{\"requestId\":{},\"error\":{{\"message\":{}}}}}", serde_json::to_string(&raw).unwrap(), serde_json::to_string(&raw).unwrap()),
        format!("{{\"unknown\":{raw},\"error\":{{\"result\":{raw}}}}}"),
    ] {
        let ordinary = shape(document.as_bytes(), JsonValueSchema::ControlResponse);
        assert_eq!(ordinary.raw_bytes, 0);
        assert_eq!(ordinary.virtual_items, 0);
        assert_eq!(ordinary.raw_frames, 0);
    }
}

#[test]
fn every_closed_descriptor_follows_its_literal_producer_value_paths() {
    let raw = raw_document("[0]");
    let part = format!(r#"{{"arguments":{raw},"kind":"tool_\u0063all"}}"#);
    let result = format!(r#"{{"content":{{"value":{raw},"kind":"j\u0073on"}},"kind":"tool_result"}}"#);
    let context = format!(r#"{{"messages":[{{"content":{part}}}]}}"#);
    let event = format!(r#"{{"resolution":{{"outcome":{raw}}},"kind":"interaction_re\u0073olved"}}"#);
    let fixtures = [
        (JsonValueSchema::EventPayload, event.clone(), 1),
        (JsonValueSchema::Snapshot, format!(r#"{{"events":[{{"payload":{event}}}],"projection":{{"interactions":{{"00000000-0000-0000-0000-000000000001":[null,{{"outcome":{raw}}}]}}}}}}"#), 2),
        (JsonValueSchema::Context, context.clone(), 1),
        (JsonValueSchema::ResponseProjection, format!(r#"{{"context":{context}}}"#), 1),
        (JsonValueSchema::ModelEvent, format!(r#"{{"metadata":{raw},"kind":"completed"}}"#), 1),
        (JsonValueSchema::ModelEvent, part.clone(), 1),
        (JsonValueSchema::ModelRequest, format!(r#"{{"model":{{"options":{raw}}},"messages":[{{"content":{part}}}],"tools":[{{"input_schema":{raw},"output_schema":{raw},"projection_schema":{raw}}}]}}"#), 5),
        (JsonValueSchema::ModelContent, format!("[{part},{result}]"), 2),
        (JsonValueSchema::ModelContent, part.clone(), 1),
        (JsonValueSchema::ModelContentPart, part, 1),
        (JsonValueSchema::ModelContentPart, result, 1),
        (JsonValueSchema::WorkflowRecord, format!(r#"{{"input":{raw},"prior":{{"state":{raw}}},"next":{{"state":{raw}}},"transition":{{"state":{raw},"status":{{"value":{raw},"kind":"completed"}}}}}}"#), 5),
        (JsonValueSchema::WorkflowAdmission, format!(r#"{{"initial":{{"state":{raw}}}}}"#), 1),
        (JsonValueSchema::MachineCheckpoint, format!(r#"{{"state":{raw}}}"#), 1),
    ];
    for (schema, document, selected) in fixtures {
        // This table exercises literal reachability, not arbitrary typed
        // Deserialize heap proofs or validity of omitted DTO required fields.
        let actual = shape(document.as_bytes(), schema);
        assert_eq!(actual.raw_frames, selected, "{schema:?}: {document}");
        assert_eq!(actual.raw_bytes, selected * 3, "{schema:?}: {document}");
        assert_eq!(actual.virtual_items, selected * 2, "{schema:?}: {document}");
        let unknown = format!(r#"{{"unknown":{raw}}}"#);
        let ignored = shape(unknown.as_bytes(), schema);
        assert_eq!(ignored.raw_frames, 0, "{schema:?} must ignore unknown fields");
        assert_eq!(ignored.virtual_items, 0, "{schema:?} must ignore unknown fields");
    }
}

#[test]
fn nonvalue_model_tags_and_plain_model_text_never_acquire_value_authority() {
    let hidden = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    for (schema, document) in [
        (JsonValueSchema::ModelEvent, format!(r#"{{"arguments":{hidden},"kind":"text_delta"}}"#)),
        (JsonValueSchema::ModelContentPart, format!(r#"{{"content":{{"value":{hidden},"kind":"text"}},"kind":"tool_result"}}"#)),
        (JsonValueSchema::ModelContent, serde_json::to_string(&hidden).unwrap()),
        (JsonValueSchema::WorkflowRecord, format!(r#"{{"transition":{{"status":{{"value":{hidden},"kind":"running"}}}}}}"#)),
    ] {
        let admitted = inspect(document.as_bytes(), Some(schema), JsonBounds {
            maximum_items: 64, maximum_depth: 5, ..LIMITS
        }, &mut |_| Ok(()));
        let actual = admitted.unwrap_or_else(|_| panic!("unselected potential Raw must not reject {schema:?}"));
        assert_eq!(actual.raw_frames, 0);
        assert_eq!(actual.virtual_items, 0);
    }
}

#[test]
fn consuming_inspection_uses_real_sorted_map_order_not_original_lexical_order() {
    let payload = "[0,1,2]";
    let reordered = format!(r#"{{"z":0,"{RAW}":{}}}"#, serde_json::to_string(payload).unwrap());
    let invocation = format!(r#"{{"operation_id":"00000000-0000-0000-0000-000000000001","call_id":"c","name":"n","arguments":{reordered}}}"#);
    let result = format!(r#"{{"kind":"json","value":{reordered}}}"#);
    for (document, schema) in [
        (&invocation, JsonValueSchema::ProjectedToolInvocation),
        (&result, JsonValueSchema::ToolResultContent),
    ] {
        let resident: Value = serde_json::from_str(document).unwrap();
        let lexical = shape(document.as_bytes(), schema);
        assert_eq!(lexical.raw_frames, 0, "ordinary first lexical key preserves the original map");
        let mut inspected = 0;
        let owned = json_owned_value_shape_bounded(&resident, LIMITS, schema, &mut |event| {
            assert!(!matches!(event, JsonCodecCharge::Read { .. }), "resident inspection is not a source read");
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        }).unwrap();
        assert_eq!(owned.raw_frames, 1, "BTreeMap iteration exposes the private key first");
        assert_eq!(owned.raw_bytes, payload.len() as u64);
        assert_eq!(owned.virtual_items, 4);
        assert!(inspected >= owned.raw_bytes);
        assert!(json_owned_value_conversion_additional_bytes(owned).unwrap() >= owned.raw_bytes);
        assert!(json_owned_value_shape_bounded(&resident, JsonBounds {
            maximum_items: lexical.lexical_items, ..LIMITS
        }, schema, &mut |_| Ok(())).is_err(), "virtual growth is admitted before consuming serde");
    }
    // The pinned consuming visitor parses the hidden source before noticing
    // the remaining ordinary key. Do not replace its genuine eventual error.
    let invocation: Value = serde_json::from_str(&invocation).unwrap();
    assert!(serde_json::from_value::<crate::tool::ToolInvocation>(invocation).is_err());
    let result: Value = serde_json::from_str(&result).unwrap();
    assert!(serde_json::from_value::<crate::model::ToolResultContent>(result).is_err());
}

#[test]
fn consuming_shape_excludes_moved_strings_but_conserves_raw_overlap_and_first_cause() {
    let resident = serde_json::json!({"kind":"json","value":"resident".repeat(8192)});
    let ordinary = json_owned_value_shape_bounded(&resident, LIMITS, JsonValueSchema::ToolResultContent, &mut |_| Ok(())).unwrap();
    let additional = json_owned_value_conversion_additional_bytes(ordinary).unwrap();
    let full = json_value_decode_owned_bytes(ordinary).unwrap();
    assert_eq!(full - additional, 4 * ordinary.input_bytes.max(8));
    assert!(additional < concrete_owned(&resident), "normal resident strings move under the old guard");
    let decoded: crate::model::ToolResultContent = serde_json::from_value(resident).unwrap();
    assert!(matches!(decoded, crate::model::ToolResultContent::Json { .. }));
    let slice = shape(br#"{"kind":"json","value":"resident"}"#, JsonValueSchema::ToolResultContent);
    assert!(json_owned_value_conversion_additional_bytes(slice).is_err(), "slice witness cannot claim consuming origin");

    let (raw, raw_bytes, _) = raw_layers(serde_json::to_string(&"leaf".repeat(4096)).unwrap(), 3);
    // Original lexical Raw is consumed by from_slice, so build a resident
    // Raw wrapper without deserializing away its private representation.
    let resident = serde_json::json!({"kind":"json","value":{(RAW): raw}});
    let mut callbacks = 0;
    let raw_shape = json_owned_value_shape_bounded(&resident, LIMITS, JsonValueSchema::ToolResultContent, &mut |_| {
        callbacks += 1;
        Ok(())
    }).unwrap();
    assert!(raw_shape.raw_bytes >= raw_bytes);
    assert!(json_owned_value_conversion_additional_bytes(raw_shape).unwrap() >= raw_shape.raw_bytes);
    assert!(json_owned_value_shape_bounded(&resident, JsonBounds {
        maximum_depth: 2, ..LIMITS
    }, JsonValueSchema::ToolResultContent, &mut |_| Ok(())).is_err());
    for denied in 0..callbacks {
        let cause = uuid::Uuid::new_v4();
        let mut reached = 0;
        let failure = json_owned_value_shape_bounded(&resident, LIMITS, JsonValueSchema::ToolResultContent, &mut |_| {
            let current = reached;
            reached += 1;
            if current == denied { return Err(Error::Indeterminate(cause)); }
            Ok(())
        }).unwrap_err();
        assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
        assert_eq!(reached, denied + 1);
    }
    let old_owned = concrete_owned(&resident);
    let parsed: crate::model::ToolResultContent = serde_json::from_value(resident).unwrap();
    let crate::model::ToolResultContent::Json { value } = parsed else { panic!("actual closed JSON result"); };
    assert_eq!(value, Value::String("leaf".repeat(4096)));
    assert!(old_owned + json_owned_value_conversion_additional_bytes(raw_shape).unwrap()
        >= concrete_owned(&value) + raw_shape.raw_bytes);
}

// Model only the genuine scalar/string sizing writers. Delimiters and null's
// fixed literal are arithmetic, not fictitious inspected encoded buffers.
fn tree_sizing_inspections(value: &Value) -> u64 {
    match value {
        Value::Null => 0,
        Value::Bool(_) | Value::Number(_) | Value::String(_) => serde_json::to_vec(value).unwrap().len() as u64,
        Value::Array(values) => values.iter().map(tree_sizing_inspections).sum(),
        Value::Object(values) => values.iter().map(|(key, value)| {
            serde_json::to_vec(key).unwrap().len() as u64 + tree_sizing_inspections(value)
        }).sum(),
    }
}

#[test]
fn direct_tree_work_never_fabricates_encoded_bytes_or_source_reads() {
    let value = serde_json::json!({
        "escape\"\\\n": ["\0\t😀\"\\", u64::MAX, i64::MIN, -0.0, 1.5e300, true, null],
        "nested": {"key": "value"}
    });
    let encoded = serde_json::to_vec(&value).unwrap().len() as u64;
    for maximum_bytes in [encoded, LIMITS.maximum_bytes] {
        let mut inspected = 0;
        let mut items = 0;
        let actual = json_value_bounded_trusted(&value, JsonBounds { maximum_bytes, ..LIMITS }, &mut |event| {
            match event {
                JsonCodecCharge::Work { bytes, items: nodes } => {
                    assert_eq!(bytes, 0, "tree production emits no encoded wire buffer");
                    items += nodes;
                }
                JsonCodecCharge::Read { .. } => panic!("tree production has no input source read"),
                JsonCodecCharge::Inspect { items } => inspected += items,
                _ => {}
            }
            Ok(())
        }).unwrap();
        assert_eq!(actual, value);
        assert_eq!(items, semantic_items(&value));
        assert_eq!(inspected, tree_sizing_inspections(&value),
            "real sizing work is identical at exact and oversized byte bounds");
        assert!(inspected > 0);
    }
    assert!(json_value_bounded_trusted(&value, JsonBounds {
        maximum_bytes: encoded - 1, ..LIMITS
    }, &mut |_| Ok(())).is_err(), "logical encoded-shape limit still applies");
}

#[test]
fn typed_borrowed_quota_rejects_slice_and_consuming_origin() {
    let slice = shape(b"null", JsonValueSchema::Root);
    assert!(json_typed_value_decode_owned_bytes(slice, 0).is_err());
    let owned = json_owned_value_shape_bounded(
        &Value::Null, LIMITS, JsonValueSchema::Root, &mut |_| Ok(()),
    ).unwrap();
    assert!(json_typed_value_decode_owned_bytes(owned, 0).is_err());
}

#[test]
fn preparation_request_has_exactly_four_value_paths_not_command_fileref_payload() {
    let raw = raw_document("[0]");
    // Literal reachability control for the real request's audited fields. This
    // partial envelope is not claimed to satisfy its omitted DTO identities.
    let document = format!(r#"{{"payload":{raw},"child":{{"input":{raw},"input_schema":{raw},"output_schema":{raw}}},"command":{{"payload":{raw}}}}}"#);
    let selected = shape(document.as_bytes(), JsonValueSchema::ResourcePreparationRequest);
    assert_eq!(selected.raw_frames, 4);
    assert_eq!(selected.raw_bytes, 12);
    assert_eq!(selected.virtual_items, 8);
    let original: Value = serde_json::from_str(&document).unwrap();
    assert_eq!(original["payload"], serde_json::json!([0]));
    assert_eq!(original["child"]["input"], serde_json::json!([0]));
    assert_eq!(original["child"]["input_schema"], serde_json::json!([0]));
    assert_eq!(original["child"]["output_schema"], serde_json::json!([0]));
    let hidden = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    let ordinary = format!(r#"{{"command":{{"payload":{hidden}}},"unknown":{hidden}}}"#);
    let ignored = inspect(ordinary.as_bytes(), Some(JsonValueSchema::ResourcePreparationRequest),
        JsonBounds { maximum_items: 32, maximum_depth: 4, ..LIMITS }, &mut |_| Ok(()))
        .unwrap_or_else(|_| panic!("FileRef and unknown fields acquire no Value parser authority"));
    assert_eq!(ignored.raw_frames, 0);
    assert_eq!(ignored.virtual_items, 0);
}

#[test]
fn scheduler_schema_gates_include_real_orchestrated_serde_and_nested_late_reduce() {
    use crate::scheduler::{EntrypointRef, SchedulerEvent};
    let operation = uuid::Uuid::from_u128(1);
    let event = SchedulerEvent::Orchestrated {
        operation_id: operation, expected_revision: 0,
        outcome: crate::Outcome::Indeterminate { operation_id: operation },
        cancel: Vec::new(), reduction_digest: None,
        reducer: Some(EntrypointRef {
            name: "reducer".into(), version: "1".into(), digest: [7; 32],
            result_schema: serde_json::json!({(RAW): "[0]"}),
        }),
    };
    let original = serde_json::to_string(&event).unwrap();
    let expected: SchedulerEvent = serde_json::from_str(&original).unwrap();
    let reducer = match &event { SchedulerEvent::Orchestrated { reducer: Some(reducer), .. } => reducer, _ => unreachable!() };
    let outcome = serde_json::to_string(&crate::Outcome::<crate::conversation::FileRef>::Indeterminate { operation_id: operation }).unwrap();
    let late = format!(r#"{{"reducer":{},"operation_id":"{operation}","expected_revision":0,"outcome":{outcome},"cancel":[],"reduction_digest":null,"kind":"orche\u0073trated"}}"#, serde_json::to_string(reducer).unwrap());
    for document in [original, late] {
        let admitted = shape(document.as_bytes(), JsonValueSchema::SchedulerEvent);
        assert_eq!(admitted.raw_frames, 1);
        assert_eq!(admitted.raw_bytes, 3);
        assert_eq!(serde_json::from_str::<SchedulerEvent>(&document).unwrap(), expected);
    }
    let raw = raw_document("[0]");
    let declared = format!(r#"{{"spec":{{"entrypoint":{{"result_schema":{raw}}},"orchestration":{{"reducer":{{"result_schema":{raw}}},"kind":"re\u0064uce"}}}},"kind":"declared"}}"#);
    let admitted = shape(declared.as_bytes(), JsonValueSchema::SchedulerEvent);
    assert_eq!(admitted.raw_frames, 2);
    assert_eq!(admitted.virtual_items, 4);
    let deep = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    for document in [
        format!(r#"{{"spec":{{"entrypoint":{{"result_schema":{raw}}},"orchestration":{{"reducer":{{"result_schema":{deep}}},"kind":"leaf"}}}},"kind":"declared"}}"#),
        format!(r#"{{"spec":{{"entrypoint":{{"result_schema":{deep}}},"orchestration":{{"reducer":{{"result_schema":{deep}}},"kind":"reduce"}}}},"kind":"waiting_for_capacity"}}"#),
        format!(r#"{{"outcome":{{"Succeeded":{deep}}},"kind":"completed"}}"#),
    ] {
        let admitted = inspect(document.as_bytes(), Some(JsonValueSchema::SchedulerEvent),
            JsonBounds { maximum_items: 64, maximum_depth: 16, ..LIMITS }, &mut |_| Ok(()))
            .unwrap_or_else(|_| panic!("only final root/nested tags confer Value authority"));
        assert!(admitted.raw_frames <= 1);
        assert!(admitted.virtual_items <= 2);
    }
}

#[test]
fn public_interaction_response_schema_matches_original_serde_before_or_after_tag() {
    let raw = raw_document("[0]");
    for (tag, escaped_tag) in [("question", r#"ques\u0074ion"#), ("form", r#"f\u006frm"#)] {
        let before = format!(r#"{{"type":"{tag}","value":{raw}}}"#);
        let after = format!(r#"{{"val\u0075e":{raw},"t\u0079pe":"{escaped_tag}"}}"#);
        let expected: crate::interaction::InteractionResponse = serde_json::from_str(&before).unwrap();
        for document in [before, after] {
            assert_eq!(serde_json::from_str::<crate::interaction::InteractionResponse>(&document).unwrap(), expected);
            let admitted = shape(document.as_bytes(), JsonValueSchema::InteractionResponse);
            assert_eq!(admitted.raw_frames, 1);
            assert_eq!(admitted.raw_bytes, 3);
            assert_eq!(admitted.virtual_items, 2);
        }
    }
}

#[test]
fn recursive_wire_and_owned_profiles_follow_every_literal_container_gate() {
    let raw = raw_document("[0]");
    let admission = format!(r#"{{"input":{raw},"input_schema":{raw},"output_schema":{raw}}}"#);
    let request = format!(r#"{{"payload":{raw},"child":{admission}}}"#);
    let fork = format!(r#"{{"request":{request},"preparation":{admission}}}"#);
    let plan = format!(r#"{{"p\u006can":{fork},"kind":"f\u006frk"}}"#);
    let receipt = format!(r#"{{"provider_proof":{{"statement":{raw}}}}}"#);
    let fixtures = [
        (JsonValueSchema::TaskAdmissionRecord, admission, 3),
        (JsonValueSchema::RecursiveForkToolPlan, fork, 7),
        (JsonValueSchema::RecursivePlan, plan.clone(), 7),
        (JsonValueSchema::RecursiveInitialPlan, format!(r#"{{"plan":{plan},"command":{{"payload":{raw}}}}}"#), 7),
        (JsonValueSchema::RecursiveState, format!(r#"{{"a\u0072guments":{raw},"phase":"r\u0065ady"}}"#), 1),
        (JsonValueSchema::RecursiveState, format!(r#"{{"arguments":{raw},"phase":"source_pinned"}}"#), 1),
        (JsonValueSchema::RecursiveState, format!(r#"{{"arguments":{raw},"plan":{plan},"receipt":{receipt},"phase":"wai\u0074ing"}}"#), 9),
        (JsonValueSchema::RecursiveState, format!(r#"{{"value":{raw},"phase":"compl\u0065te"}}"#), 1),
        (JsonValueSchema::RecursiveReceipt, format!(r#"{{"value":{{"Succ\u0065eded":{raw}}}}}"#), 1),
        (JsonValueSchema::ProjectMergeReceipt, receipt, 1),
    ];
    for (schema, document, selected) in fixtures {
        // Private recursive DTO owners supply producer E2E. These controls
        // exercise actual SDK wire/owned selection, not omitted DTO validity.
        let wire = shape(document.as_bytes(), schema);
        assert_eq!(wire.raw_frames, selected, "{schema:?}");
        assert_eq!(wire.raw_bytes, 3 * selected, "{schema:?}");
        assert_eq!(wire.virtual_items, 2 * selected, "{schema:?}");
        let later_key = format!(r#"{{"z":0,"{RAW}":"[0]"}}"#);
        let reordered = document.replace(&raw, &later_key);
        assert_eq!(shape(reordered.as_bytes(), schema).raw_frames, 0);
        let resident: Value = serde_json::from_str(&reordered).unwrap();
        let owned = json_owned_value_shape_bounded(&resident, LIMITS, schema, &mut |event| {
            assert!(!matches!(event, JsonCodecCharge::Read { .. }));
            Ok(())
        }).unwrap();
        assert_eq!(owned.raw_frames, selected, "resident sorted map order: {schema:?}");
        assert_eq!(owned.raw_bytes, 3 * selected, "{schema:?}");
        assert!(json_owned_value_conversion_additional_bytes(owned).unwrap() >= owned.raw_bytes);
    }
    let hidden = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    for (schema, document) in [
        (JsonValueSchema::InteractionResponse, format!(r#"{{"value":{hidden},"type":"choice"}}"#)),
        (JsonValueSchema::TaskAdmissionRecord, format!(r#"{{"command":{{"payload":{hidden}}},"unknown":{hidden}}}"#)),
        (JsonValueSchema::RecursivePlan, format!(r#"{{"plan":{{"request":{{"payload":{hidden}}}}},"kind":"merge"}}"#)),
        (JsonValueSchema::RecursiveState, format!(r#"{{"plan":{{"kind":"fork","plan":{{"request":{{"payload":{hidden}}}}}}},"receipt":{{"provider_proof":{{"statement":{hidden}}}}},"phase":"ready"}}"#)),
        (JsonValueSchema::RecursiveState, format!(r#"{{"arguments":{hidden},"phase":"complete"}}"#)),
        (JsonValueSchema::RecursiveState, format!(r#"{{"command":{{"payload":{hidden}}},"phase":"source_pinned"}}"#)),
        (JsonValueSchema::RecursiveReceipt, format!(r#"{{"value":{{"Failed":{{"message":{hidden}}}}}}}"#)),
        (JsonValueSchema::RecursiveReceipt, format!(r#"{{"value":{{"status":"succeeded","value":{hidden}}}}}"#)),
        (JsonValueSchema::ProjectMergeReceipt, format!(r#"{{"outcome":{{"Succeeded":{hidden}}},"command":{{"payload":{hidden}}}}}"#)),
    ] {
        let admitted = inspect(document.as_bytes(), Some(schema), JsonBounds {
            maximum_items: 128, maximum_depth: 16, ..LIMITS
        }, &mut |_| Ok(())).unwrap_or_else(|_| panic!("unselected gated/profile fields remain ordinary: {schema:?}"));
        assert_eq!(admitted.raw_frames, 0, "{schema:?}");
        assert_eq!(admitted.virtual_items, 0, "{schema:?}");
    }
}

fn source_authorization_document(native_snapshot: &str) -> String {
    // Real public DTO metadata, including a genuine serialized Scope. This
    // fixture exercises Deserialize, not authorization/provider verification.
    let owner = crate::core::Authority {
        kind: crate::core::AggregateKind::Task, id: "source-owner".into(),
    };
    let issuer = crate::core::AuthorityIssuer::new("source-issuer", [7; 32], owner.clone());
    let provider = crate::resources::ProviderRef::new("acyclic", "filesystem", "2").unwrap();
    let project = crate::conversation::VolumeRef::new(
        provider.clone(), "project", crate::conversation::VolumeClass::Project,
        crate::conversation::VolumeOwner::Project("source-owner".into()),
    ).unwrap();
    let generation = crate::resources::GenerationRef::new(provider, [8; 32], None).unwrap();
    let id = "00000000-0000-0000-0000-000000000001";
    let metadata = serde_json::json!({
        "owner": owner, "caller_task": id, "caller_admission_digest": vec![0u8; 32],
        "caller_fence": {"reservation_id": "lease", "placement": "worker"},
        "operation_id": id, "source_task": id, "project": project,
        "generation": generation, "native_snapshot": null, "graph_root": id,
        "maximum_work": 4096, "maximum_bytes": 65536,
        "policy_scope": issuer.root("source-read", crate::Capabilities::new(["read"]))
    });
    serde_json::to_string(&metadata).unwrap()
        .replace("\"native_snapshot\":null", &format!("\"native_snapshot\":{native_snapshot}"))
}

#[test]
fn real_source_authorization_serde_selects_private_raw_and_exact_item_admission() {
    let payload = r#"{"z":[0,1],"a":"snapshot"}"#;
    let document = source_authorization_document(&raw_document(payload));
    let expected: crate::source_authorization::ProjectSourceAuthorization =
        serde_json::from_str(&document).unwrap();
    assert_eq!(expected.native_snapshot, Some(serde_json::from_str(payload).unwrap()));
    let selected = shape(document.as_bytes(), JsonValueSchema::ProjectSourceAuthorization);
    assert_eq!(selected.raw_frames, 1);
    assert_eq!(selected.raw_bytes, payload.len() as u64);
    assert_eq!(selected.virtual_items, lexical_items(payload.as_bytes()));
    let exact = selected.lexical_items + selected.virtual_items;
    let mut inspected = 0;
    let mut admitted = false;
    let actual = json_from_slice_seed_bounded_trusted(
        std::marker::PhantomData::<crate::source_authorization::ProjectSourceAuthorization>,
        document.as_bytes(), JsonBounds { maximum_items: exact, ..LIMITS },
        65536, JsonValueSchema::ProjectSourceAuthorization, &mut |event| {
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        },
    ).unwrap();
    assert_eq!(actual, expected);
    assert!(admitted);
    assert_eq!(inspected, selected.raw_bytes);
    admitted = false;
    assert!(json_from_slice_seed_bounded_trusted(
        std::marker::PhantomData::<crate::source_authorization::ProjectSourceAuthorization>,
        document.as_bytes(), JsonBounds { maximum_items: exact - 1, ..LIMITS },
        65536, JsonValueSchema::ProjectSourceAuthorization, &mut |event| {
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        },
    ).is_err());
    assert!(!admitted);
    let absent = source_authorization_document("null");
    assert!(serde_json::from_str::<crate::source_authorization::ProjectSourceAuthorization>(&absent)
        .unwrap().native_snapshot.is_none());
    assert_eq!(shape(absent.as_bytes(), JsonValueSchema::ProjectSourceAuthorization).raw_frames, 0);
}

#[test]
fn source_authorization_composites_preserve_wire_and_owned_map_order() {
    let raw = raw_document("[0]");
    let authorization = format!(r#"{{"native_\u0073napshot":{raw}}}"#);
    let request = format!(r#"{{"authorized_\u0073ource":{authorization}}}"#);
    let report = format!(r#"{{"request":{request}}}"#);
    let seed = format!(r#"{{"authorized_source":{authorization}}}"#);
    let intent = format!(r#"{{"request":{request}}}"#);
    let fork_event = format!(r#"{{"seed":{seed},"kind":"fork_pu\u0062lished"}}"#);
    let merge_event = format!(r#"{{"receipt":{{"provider_proof":{{"statement":{raw}}}}},"kind":"project_merge_published"}}"#);
    let snapshot = format!(r#"{{"events":[{{"payload":{fork_event}}},{{"payload":{merge_event}}}],"projection":{{"forks":[[{{"kind":"task","id":"parent"}},{seed}]],"published_merges":[["operation",[0]]]}}}}"#);
    let merge_plan = format!(r#"{{"authorized_source":{authorization}}}"#);
    let recursive_merge = format!(r#"{{"p\u006can":{merge_plan},"kind":"m\u0065rge"}}"#);
    for (schema, document, selected) in [
        (JsonValueSchema::ProjectSourceAuthorization, authorization.clone(), 1),
        (JsonValueSchema::ForkRequest, request.clone(), 1),
        (JsonValueSchema::ForkReport, report.clone(), 1),
        (JsonValueSchema::ForkSeed, seed, 1),
        (JsonValueSchema::RecursiveForkSourceIntent, intent.clone(), 1),
        (JsonValueSchema::RecursiveSourcePin, format!(r#"{{"intent":{intent}}}"#), 1),
        (JsonValueSchema::ForkPreparationCommandResults, format!(r#"{{"commands":[{{"value":{{"Succ\u0065eded":{report}}}}}]}}"#), 1),
        (JsonValueSchema::ResourcePreparationRequest, format!(r#"{{"read_authorization":{authorization}}}"#), 1),
        (JsonValueSchema::RecursiveState, format!(r#"{{"intent":{intent},"phase":"source_pinned"}}"#), 1),
        (JsonValueSchema::RecursiveState, format!(r#"{{"report":{report},"phase":"waiting"}}"#), 1),
        (JsonValueSchema::EventPayload, fork_event, 1),
        (JsonValueSchema::EventPayload, merge_event, 1),
        (JsonValueSchema::Snapshot, snapshot.clone(), 3),
        (JsonValueSchema::ConfiguredRecursiveSourceCheckpoint, format!(r#"{{"snapshot":{snapshot},"request":{request}}}"#), 4),
        (JsonValueSchema::RecursiveMergeToolPlan, merge_plan, 1),
        (JsonValueSchema::RecursivePlan, recursive_merge.clone(), 1),
        (JsonValueSchema::RecursiveInitialPlan, format!(r#"{{"plan":{recursive_merge}}}"#), 1),
        (JsonValueSchema::RecursiveState, format!(r#"{{"plan":{recursive_merge},"phase":"waiting"}}"#), 1),
    ] {
        // Literal graph controls for the joined source carriers/private DTOs:
        // omitted required fields make these explicitly NOT producer E2E.
        let wire = shape(document.as_bytes(), schema);
        assert_eq!(wire.raw_frames, selected, "{schema:?}");
        assert_eq!(wire.raw_bytes, selected * 3, "{schema:?}");
        assert_eq!(wire.virtual_items, selected * 2, "{schema:?}");
        let reordered = document.replace(&raw, &format!(r#"{{"z":0,"{RAW}":"[0]"}}"#));
        assert_eq!(shape(reordered.as_bytes(), schema).raw_frames, 0, "{schema:?}");
        let resident: Value = serde_json::from_str(&reordered).unwrap();
        let mut inspected = 0;
        let owned = json_owned_value_shape_bounded(&resident, LIMITS, schema, &mut |event| {
            assert!(!matches!(event, JsonCodecCharge::Read { .. }));
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        }).unwrap();
        assert_eq!(owned.raw_frames, selected, "{schema:?}");
        assert_eq!(owned.raw_bytes, selected * 3, "{schema:?}");
        assert!(inspected >= owned.raw_bytes, "{schema:?}");
        assert!(json_owned_value_conversion_additional_bytes(owned).unwrap() >= owned.raw_bytes);
    }
}

#[test]
fn authorization_names_dormant_variants_and_failed_outcomes_remain_ordinary() {
    let hidden = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    let authorization = format!(r#"{{"native_snapshot":{hidden}}}"#);
    let request = format!(r#"{{"authorized_source":{authorization}}}"#);
    for (schema, document) in [
        (JsonValueSchema::ProjectSourceAuthorization, format!(r#"{{"project":{{"native_snapshot":{hidden}}},"owner":{{"native_snapshot":{hidden}}}}}"#)),
        (JsonValueSchema::ForkRequest, format!(r#"{{"native_snapshot":{hidden},"unknown":{{"authorized_source":{authorization}}}}}"#)),
        (JsonValueSchema::ForkReport, format!(r#"{{"authorized_source":{authorization}}}"#)),
        (JsonValueSchema::ForkSeed, format!(r#"{{"request":{request}}}"#)),
        (JsonValueSchema::RecursiveForkSourceIntent, format!(r#"{{"authorized_source":{authorization}}}"#)),
        (JsonValueSchema::RecursiveSourcePin, format!(r#"{{"request":{request}}}"#)),
        (JsonValueSchema::ResourcePreparationRequest, format!(r#"{{"native_snapshot":{hidden},"command":{{"read_authorization":{authorization}}}}}"#)),
        (JsonValueSchema::RecursiveState, format!(r#"{{"intent":{{"request":{request}}},"report":{{"request":{request}}},"phase":"ready"}}"#)),
        (JsonValueSchema::EventPayload, format!(r#"{{"seed":{{"authorized_source":{authorization}}},"receipt":{{"provider_proof":{{"statement":{hidden}}}}},"kind":"interaction_resolved"}}"#)),
        (JsonValueSchema::Snapshot, format!(r#"{{"projection":{{"forks":[[{{"authorized_source":{authorization}}},null]],"published_merges":[["operation",{hidden}]]}}}}"#)),
        (JsonValueSchema::ForkPreparationCommandResults, format!(r#"{{"commands":[{{"value":{{"Failed":{{"request":{request},"message":{}}}}}}}]}}"#, serde_json::to_string(&hidden).unwrap())),
        (JsonValueSchema::ForkPreparationCommandResults, format!(r#"{{"commands":[{{"value":{{"succeeded":{{"request":{request}}}}}}}]}}"#)),
        (JsonValueSchema::RecursiveMergeToolPlan, format!(r#"{{"source_project":{{"native_snapshot":{hidden}}},"request":{request}}}"#)),
        (JsonValueSchema::RecursivePlan, format!(r#"{{"plan":{{"authorized_source":{authorization}}},"kind":"fork"}}"#)),
        (JsonValueSchema::RecursivePlan, format!(r#"{{"plan":{{"request":{request}}},"kind":"merge"}}"#)),
        (JsonValueSchema::RecursivePlan, format!(r#"{{"kind":"fork","plan":{{"authorized_source":{authorization}}},"kind":"merge"}}"#)),
        (JsonValueSchema::RecursivePlan, format!(r#"{{"kind":"merge","plan":{{"authorized_source":{authorization}}},"kind":"merge"}}"#)),
        (JsonValueSchema::RecursiveState, format!(r#"{{"plan":{{"plan":{{"authorized_source":{authorization}}},"kind":"merge"}},"phase":"ready"}}"#)),
    ] {
        let admitted = inspect(document.as_bytes(), Some(schema), JsonBounds {
            maximum_items: 256, maximum_depth: 16, ..LIMITS
        }, &mut |_| Ok(())).unwrap_or_else(|_| panic!("ordinary or dormant source path: {schema:?}"));
        assert_eq!(admitted.raw_frames, 0, "{schema:?}");
        assert_eq!(admitted.virtual_items, 0, "{schema:?}");
    }
}

#[test]
fn portable_selected_seed_matches_pinned_hidden_value_and_exact_structural_cap() {
    let document = format!("[{}]", raw_document("[0,1]"));
    let expected: Vec<Value> = serde_json::from_str(&document).unwrap();
    let selected = shape(document.as_bytes(), JsonValueSchema::Root);
    let exact = selected.lexical_items + selected.virtual_items;
    let mut read = 0;
    let mut inspected = 0;
    let mut admitted = None;
    let actual = json_from_slice_seed_bounded_trusted(
        std::marker::PhantomData::<Vec<Value>>, document.as_bytes(),
        JsonBounds { maximum_items: exact, ..LIMITS }, 16384, JsonValueSchema::Root, &mut |event| {
            match event {
                JsonCodecCharge::Read { bytes } => read += bytes,
                JsonCodecCharge::Inspect { items } => inspected += items,
                JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    admitted = Some(maximum_owned_bytes.max(json_value_decode_owned_bytes(value_shape)?));
                }
                _ => {}
            }
            Ok(())
        },
    ).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(read, document.len() as u64);
    assert_eq!(inspected, selected.raw_bytes);
    assert!(admitted.unwrap() >= actual.iter().map(concrete_owned).sum::<u64>()
        + (actual.capacity() * std::mem::size_of::<Value>()) as u64);
    let mut admission_seen = false;
    assert!(json_from_slice_seed_bounded_trusted(
        std::marker::PhantomData::<Vec<Value>>, document.as_bytes(),
        JsonBounds { maximum_items: exact - 1, ..LIMITS }, 16384, JsonValueSchema::Root, &mut |event| {
            admission_seen |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        },
    ).is_err());
    assert!(!admission_seen, "selected virtual items deny before the portable seed visitor");
}

#[test]
fn stock_command_profiles_select_only_the_actual_closed_value_fields() {
    let raw = raw_document("[0]");
    let part = format!(r#"{{"arguments":{raw},"kind":"tool_\u0063all","call_id":"c","name":"n"}}"#);
    let fixtures = [
        (JsonValueSchema::ToolTaskCommand, format!(r#"{{"arguments":{raw},"name":"n","revision":"1"}}"#), 1),
        (JsonValueSchema::TaskAdmitCommand, format!(r#"{{"input":{raw},"name":"n","version":"1"}}"#), 1),
        (JsonValueSchema::ModelTaskCommand, format!(r#"{{"input":{part},"selected_context":{{"messages":[{{"content":{part}}}]}},"max_steps":1}}"#), 2),
        (JsonValueSchema::NativeProcessRequest, format!(r#"{{"mcp_stdio":{{"params":{raw}}}}}"#), 1),
    ];
    for (schema, document, selected) in fixtures {
        let wire = shape(document.as_bytes(), schema);
        assert_eq!(wire.raw_frames, selected, "{schema:?}");
        assert_eq!(wire.raw_bytes, 3 * selected, "{schema:?}");
        let later = format!(r#"{{"z":0,"{RAW}":"[0]"}}"#);
        let reordered = document.replace(&raw, &later);
        assert_eq!(shape(reordered.as_bytes(), schema).raw_frames, 0);
        let resident: Value = serde_json::from_str(&reordered).unwrap();
        let owned = json_owned_value_shape_bounded(&resident, LIMITS, schema, &mut |_| Ok(())).unwrap();
        assert_eq!(owned.raw_frames, selected, "actual resident map order: {schema:?}");
        assert_eq!(owned.virtual_items, selected * 2);
    }
    let hidden = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
    for (schema, document) in [
        (JsonValueSchema::ToolTaskCommand, format!(r#"{{"input":{hidden},"schema":{hidden}}}"#)),
        (JsonValueSchema::TaskAdmitCommand, format!(r#"{{"arguments":{hidden},"input_schema":{hidden}}}"#)),
        (JsonValueSchema::ModelTaskCommand, format!(r#"{{"input":{{"kind":"file","file":{hidden}}},"selected_context":{{"selection":{hidden}}}}}"#)),
        (JsonValueSchema::NativeProcessRequest, format!(r#"{{"mcp_stdio":null,"params":{hidden},"view":{{"params":{hidden}}}}}"#)),
    ] {
        let ignored = inspect(document.as_bytes(), Some(schema), JsonBounds {
            maximum_items: 64, maximum_depth: 8, ..LIMITS
        }, &mut |_| Ok(())).unwrap_or_else(|_| panic!("dormant/ordinary owner fields confer no Value authority"));
        assert_eq!(ignored.raw_frames, 0, "{schema:?}");
        assert_eq!(ignored.virtual_items, 0, "{schema:?}");
    }
    // NativeProcessRequest's complete view/MCP identities and private producer
    // execution are owned elsewhere; the partial profile table is not E2E.
}

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #[test]
    fn concrete_value_capacity_and_raw_work_model_is_conservative(
        strings in proptest::collection::vec(proptest::prelude::any::<String>(), 0..24),
        integers in proptest::collection::vec(proptest::prelude::any::<u64>(), 0..24),
        signed in proptest::prelude::any::<i64>(),
        float in -1.0e100f64..1.0e100f64,
        layers in 0usize..5,
    ) {
        let leaf = serde_json::to_string(&(strings, integers, signed, float)).unwrap();
        let (document, raw_bytes, virtual_items) = raw_layers(leaf.clone(), layers);
        let mut inspected = 0;
        let actual_shape = inspect(document.as_bytes(), Some(JsonValueSchema::Root), LIMITS, &mut |event| {
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        }).unwrap_or_else(|_| panic!("bounded generated fixture"));
        let actual: Value = serde_json::from_str(&document).unwrap();
        let expected: Value = serde_json::from_str(&leaf).unwrap();
        proptest::prop_assert_eq!(&actual, &expected);
        proptest::prop_assert_eq!(actual_shape.raw_bytes, raw_bytes);
        proptest::prop_assert_eq!(actual_shape.virtual_items, virtual_items);
        proptest::prop_assert_eq!(actual_shape.raw_frames, layers as u64);
        proptest::prop_assert_eq!(inspected, raw_bytes);
        proptest::prop_assert!(actual_shape.lexical_items + actual_shape.virtual_items >= semantic_items(&actual));
        proptest::prop_assert!(json_value_decode_owned_bytes(actual_shape).unwrap() >= concrete_owned(&actual) + raw_bytes);
        let mut actual = actual;
        let mut expected = expected;
        actual.sort_all_objects();
        expected.sort_all_objects();
        proptest::prop_assert_eq!(serde_json::to_vec(&actual).unwrap(), serde_json::to_vec(&expected).unwrap());
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use uuid::Uuid;

    fn decode<F: FnMut(JsonCodecCharge) -> Result<()>>(
        bytes: &[u8], bounds: JsonBounds, charge: &mut F,
    ) -> Result<Value> {
        json_from_slice_bounded_native_trusted(bytes, bounds, 0, crate::wire::ErrorCode::Storage, JsonValueSchema::Root, charge)
    }

    #[test]
    fn native_real_authorization_hidden_nodes_deny_before_original_dto_admission() {
        let payload = format!("[{}0]", "0,".repeat(999_999));
        let document = source_authorization_document(&raw_document(&payload));
        let mut admitted = false;
        let mut inspected = 0;
        let failure = json_from_slice_bounded_native_trusted::<
            crate::source_authorization::ProjectSourceAuthorization, _,
        >(
            document.as_bytes(), JsonBounds { maximum_items: 256, ..LIMITS },
            65536, crate::wire::ErrorCode::Storage, JsonValueSchema::ProjectSourceAuthorization,
            &mut |event| {
                admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
                Ok(())
            },
        ).unwrap_err();
        assert!(!admitted);
        assert!(inspected <= payload.len() as u64);
        assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
    }

    #[test]
    fn native_fork_authorization_preserves_aggregate_depth_and_first_owned_cause() {
        // Literal joined ForkRequest reachability, not an omitted-field
        // producer DTO claim. The public authorization DTO is tested above.
        let document = format!(r#"{{"authorized_source":{{"native_snapshot":{}}}}}"#, raw_document("[0]"));
        let ordinary = format!(r#"{{"authorized_source":{{"native_snapshot":{{"ordinary":"[0]"}}}}}}"#);
        assert!(inspect(ordinary.as_bytes(), Some(JsonValueSchema::ForkRequest),
            JsonBounds { maximum_depth: 3, ..LIMITS }, &mut |_| Ok(())).is_ok());
        let selected = shape(document.as_bytes(), JsonValueSchema::ForkRequest);
        for bounds in [
            JsonBounds { maximum_depth: 3, ..LIMITS },
            JsonBounds { maximum_items: selected.lexical_items + selected.virtual_items - 1, ..LIMITS },
        ] {
            let mut admitted = false;
            let failure = json_from_slice_bounded_native_trusted::<Value, _>(
                document.as_bytes(), bounds, 0, crate::wire::ErrorCode::Storage,
                JsonValueSchema::ForkRequest, &mut |event| {
                    admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                    Ok(())
                },
            ).unwrap_err();
            assert!(!admitted, "cumulative ancestry/hidden nodes precede original parser");
            assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
        }
        let expected: Value = serde_json::from_str(&document).unwrap();
        let actual = json_from_slice_bounded_native_trusted::<Value, _>(
            document.as_bytes(), JsonBounds { maximum_depth: 4, ..LIMITS }, 0,
            crate::wire::ErrorCode::Storage, JsonValueSchema::ForkRequest, &mut |_| Ok(()),
        ).unwrap();
        assert_eq!(actual, expected);
        for deny_inspection in [true, false] {
            let cause = Uuid::new_v4();
            let mut denied = false;
            let mut admitted = false;
            let failure = json_from_slice_bounded_native_trusted::<Value, _>(
                document.as_bytes(), LIMITS, 0, crate::wire::ErrorCode::Storage,
                JsonValueSchema::ForkRequest, &mut |event| {
                    if matches!(event, JsonCodecCharge::Release { .. }) { return Ok(()); }
                    assert!(!denied, "no checkpoint after the first owned cause");
                    admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                    if (deny_inspection && matches!(event, JsonCodecCharge::Inspect { .. }))
                        || (!deny_inspection && matches!(event, JsonCodecCharge::Admission { .. })) {
                        denied = true;
                        return Err(Error::Indeterminate(cause));
                    }
                    Ok(())
                },
            ).unwrap_err();
            assert!(denied);
            assert_eq!(admitted, !deny_inspection);
            assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
        }
    }

    #[test]
    fn hidden_million_item_payload_is_denied_before_original_value_growth() {
        let payload = format!("[{}0]", "0,".repeat(999_999));
        let document = raw_document(&payload);
        assert_eq!(lexical_items(document.as_bytes()), 3);
        let mut admitted = false;
        let mut inspected = 0;
        let failure = decode(document.as_bytes(), JsonBounds { maximum_items: 64, ..LIMITS }, &mut |event| {
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            if let JsonCodecCharge::Inspect { items } = event { inspected += items; }
            Ok(())
        }).unwrap_err();
        assert!(!admitted, "hidden semantic items must be denied before Value Deserialize");
        assert!(inspected <= payload.len() as u64);
        assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
    }

    #[test]
    fn hidden_semantic_depth_and_raw_parser_frames_are_admitted_before_growth() {
        let deep = raw_document(&format!("{}0{}", "[".repeat(1024), "]".repeat(1024)));
        let (frames, _, _) = raw_layers("0".into(), 6);
        for document in [deep, frames] {
            let mut admitted = false;
            let failure = decode(document.as_bytes(), JsonBounds { maximum_depth: 3, ..LIMITS }, &mut |event| {
                admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                Ok(())
            }).unwrap_err();
            assert!(!admitted);
            assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
        }
        // Both serde parsers have local depth one; the parent raw-map visitor
        // remains live while the nested array visits its scalar, global depth
        // two. The finite bound deliberately covers this cumulative ancestry.
        let document = raw_document("[0]");
        let original: Value = serde_json::from_slice(document.as_bytes()).unwrap();
        assert_eq!(original, serde_json::json!([0]));
        let mut admitted = false;
        let failure = decode(document.as_bytes(), JsonBounds { maximum_depth: 1, ..LIMITS }, &mut |event| {
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        }).unwrap_err();
        assert!(!admitted, "local parser counter resets must not bypass the global boundary");
        assert!(matches!(failure, Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. }));
        let decoded = decode(document.as_bytes(), JsonBounds { maximum_depth: 2, ..LIMITS }, &mut |_| Ok(())).unwrap();
        assert_eq!(decoded, original, "at the cumulative boundary original Value semantics are unchanged");
    }

    #[test]
    fn native_single_read_and_truthful_virtual_work_feed_real_admission_shape() {
        let (document, raw_bytes, _) = raw_layers(r#"["owned",18446744073709551615,-0.0]"#.into(), 3);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite native ownership ledger"));
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let mut read = 0;
        let mut inspected = 0;
        let mut admitted = None;
        let actual = decode(document.as_bytes(), LIMITS, &mut |event| {
            match event {
                JsonCodecCharge::Read { bytes } => read += bytes,
                JsonCodecCharge::Inspect { items } => inspected += items,
                JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let concrete_quota = json_value_decode_owned_bytes(value_shape)?.max(maximum_owned_bytes);
                    original.resize_retained(concrete_quota).unwrap();
                    admitted = Some((maximum_owned_bytes, value_shape));
                }
                _ => {}
            }
            Ok(())
        }).unwrap();
        let (maximum_owned_bytes, admitted_shape) = admitted.unwrap();
        assert_eq!(read, document.len() as u64, "original scan emits Read exactly once");
        assert_eq!(inspected, raw_bytes);
        assert_eq!(admitted_shape.raw_bytes, raw_bytes);
        assert_eq!(admitted_shape.input_bytes, read);
        assert!(json_value_decode_owned_bytes(admitted_shape).unwrap().max(maximum_owned_bytes)
            >= concrete_owned(&actual) + raw_bytes);
        assert!(original.bytes() >= concrete_owned(&actual) + raw_bytes);
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, original.bytes());
        assert_eq!(actual, serde_json::from_str::<Value>(&document).unwrap());
        drop(actual);
        drop(original);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn cancellation_at_every_real_callback_keeps_first_owned_cause() {
        let (document, _, _) = raw_layers("[0,1]".into(), 2);
        let mut callbacks = 0;
        decode(document.as_bytes(), LIMITS, &mut |event| {
            if !matches!(event, JsonCodecCharge::Release { .. }) { callbacks += 1; }
            Ok(())
        }).unwrap();
        for denied in 0..callbacks {
            let cause = Uuid::new_v4();
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut reached = 0;
            let failure = decode(document.as_bytes(), LIMITS, &mut |event| {
                if matches!(event, JsonCodecCharge::Release { .. }) { return Ok(()); }
                let current = reached;
                reached += 1;
                if current >= denied { cancellation.cancel(); }
                cancellation.check().map_err(|_| Error::Indeterminate(cause))?;
                Ok(())
            }).unwrap_err();
            assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
            assert_eq!(reached, denied + 1, "no later checkpoint replaces a primary failure");
        }
    }

    #[test]
    fn owned_quota_denial_precedes_real_large_leaf_parser() {
        let (document, _, _) = raw_layers(serde_json::to_string(&"large".repeat(32768)).unwrap(), 3);
        let cause = Uuid::new_v4();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: 4096,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite quota for actual ledger denial"));
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let mut admission_seen = false;
        let failure = decode(document.as_bytes(), LIMITS, &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } = event {
                admission_seen = true;
                let concrete_quota = json_value_decode_owned_bytes(value_shape).unwrap().max(maximum_owned_bytes);
                original.resize_retained(concrete_quota).map_err(|_| Error::Indeterminate(cause))?;
            }
            Ok(())
        }).unwrap_err();
        assert!(admission_seen);
        assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before,
            "a denied real ledger growth leaves original custody unchanged");
    }

    #[test]
    fn real_native_source_holds_original_logical_custody_until_error_drop() {
        // This exercises the codec/error transfer boundary with the production
        // materializer ledger, not a mock allocator or a producer E2E claim.
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            source_bytes_read: LIMITS.maximum_bytes,
            items_examined: 4 * LIMITS.maximum_items,
            allocation_operations: 1,
            peak_allocation_bytes: 64 * 1024 * 1024,
            ..acyclic_fs::WorkBudget::default()
        };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .unwrap_or_else(|_| panic!("finite real materializer construction"));
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let operations = work.receipt().allocation_operations;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let document = raw_document("[0,]");
        let failure = decode(document.as_bytes(), LIMITS, &mut |event| {
            let delta = match event {
                JsonCodecCharge::Read { bytes } => acyclic_fs::WorkCounters {
                    source_bytes_read: bytes, ..acyclic_fs::WorkCounters::default()
                },
                JsonCodecCharge::Inspect { items } | JsonCodecCharge::Work { items, .. } => acyclic_fs::WorkCounters {
                    items_examined: items, ..acyclic_fs::WorkCounters::default()
                },
                JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let concrete_quota = json_value_decode_owned_bytes(value_shape).unwrap().max(maximum_owned_bytes);
                    original.resize_retained(concrete_quota).unwrap();
                    return Ok(());
                }
                _ => return Ok(()),
            };
            work.charge(delta).unwrap();
            Ok(())
        }).unwrap_err();
        let retained = original.bytes();
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, retained);
        let failure = match failure {
            Error::NativeDecode { code, source, custody: None } => Error::NativeDecode {
                code, source, custody: Some(original),
            },
            other => panic!("actual serde failure expected: {other}"),
        };
        let attached = std::error::Error::source(&failure).unwrap()
            .downcast_ref::<serde_json::Error>().unwrap();
        let field = match &failure {
            Error::NativeDecode { source, .. } => source,
            _ => unreachable!(),
        };
        assert!(std::ptr::eq(attached, field));
        assert_eq!(work.receipt().source_bytes_read, document.len() as u64);
        assert_eq!(work.receipt().allocation_operations, operations,
            "logical admission does not invent opaque serde allocation counts");
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, retained,
            "the original guard moves without a live-byte gap or second claim");
        drop(failure);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn malformed_virtual_json_keeps_actual_pinned_serde_source() {
        for payload in ["[0,]", "{", r#""\q""#, "0 1"] {
            let document = raw_document(payload);
            let expected = serde_json::from_str::<Value>(&document).unwrap_err();
            let error = decode(document.as_bytes(), LIMITS, &mut |_| Ok(())).unwrap_err();
            let source = std::error::Error::source(&error).expect("real serde source remains attached");
            let source = source.downcast_ref::<serde_json::Error>().expect("not a formatted replacement");
            assert_eq!(source.classify(), expected.classify());
            assert_eq!(source.line(), expected.line());
            assert_eq!(source.column(), expected.column());
            assert_eq!(source.to_string(), expected.to_string());
            assert!(matches!(error, Error::NativeDecode { code: crate::wire::ErrorCode::Storage, .. }));
        }
    }

    fn consuming_request_source(payload: Value) -> Value {
        let request = crate::resource_preparation::ResourcePreparationRequest {
            provider: crate::resources::ProviderRef::new("codec", "control", "1").unwrap(),
            child: crate::runtime::TaskAdmissionRecord {
                operation_id: Uuid::from_bytes([3; 16]),
                task: crate::registry::ComponentIdentity {
                    name: "control".into(), version: "1".into(), digest: [1; 32],
                },
                machine: crate::workflow::MachineIdentity {
                    name: "control".into(), version: "1".into(), digest: [2; 32],
                },
                input: Value::String("child input".into()),
                input_schema: Value::Null, output_schema: Value::Null,
                parent: None, resource_proofs: Vec::new(),
                grants: crate::Capabilities::new(["read"]),
                limits: crate::conversation::Limits::default(),
                run_limits: crate::runtime::TaskRunLimits::default(),
                policy: None, extensions: None, execution: None,
            },
            resources: Vec::new(), creation_grant: "create".into(),
            maximum_resources: 1, maximum_work: 4096, maximum_bytes: 65536,
            payload, command: None, source_checkpoint: None,
            input_files: Vec::new(), read_authorization: None,
        };
        // A real closed public Deserialize fixture, not provider validation or
        // authentication: the empty resources intentionally are not validated.
        serde_json::to_value(request).unwrap()
    }

    #[test]
    fn consuming_public_request_moves_strings_under_original_and_preclaimed_quota() {
        let source = consuming_request_source(Value::String("payload".repeat(1024)));
        let expected: crate::resource_preparation::ResourcePreparationRequest =
            serde_json::from_value(source.clone()).unwrap();
        let Value::String(input) = &source["child"]["input"] else { unreachable!() };
        let input_identity = (input.as_ptr(), input.capacity());
        let Value::String(payload) = &source["payload"] else { unreachable!() };
        let payload_identity = (payload.as_ptr(), payload.capacity());
        let token = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 16 * 1024 * 1024,
            ..acyclic_fs::WorkBudget::default()
        }, &token).unwrap();
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let operations = work.receipt().allocation_operations;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(concrete_owned(&source)).unwrap();
        let mut future = work.retain_owned(0).unwrap();
        future.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        // This concrete fixture has no nonempty resource/proof/file Vec. Its
        // two owned Value strings move; remaining typed fields are fixed slots.
        let slots = std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>() as u64;
        let mut admitted = false;
        let actual: crate::resource_preparation::ResourcePreparationRequest =
            json_from_value_owned_bounded_native_trusted(
                source, LIMITS, slots, crate::wire::ErrorCode::Storage,
                JsonValueSchema::ResourcePreparationRequest, &mut |event| {
                    match event {
                        JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                            assert!(!admitted);
                            assert_eq!(maximum_owned_bytes, slots);
                            assert!(json_typed_value_decode_owned_bytes(value_shape, slots).is_err());
                            let additional = json_owned_value_conversion_additional_bytes(value_shape)?;
                            future.resize_retained(slots + additional).unwrap();
                            admitted = true;
                        }
                        JsonCodecCharge::Read { .. } | JsonCodecCharge::Copy { .. }
                        | JsonCodecCharge::AllocationOperation { .. } => panic!("resident consuming serde has no SDK copy/allocation operation"),
                        _ => {}
                    }
                    Ok(())
                },
            ).unwrap();
        assert!(admitted);
        assert!(actual == expected);
        let Value::String(input) = &actual.child.input else { unreachable!() };
        let Value::String(payload) = &actual.payload else { unreachable!() };
        assert_eq!((input.as_ptr(), input.capacity()), input_identity);
        assert_eq!((payload.as_ptr(), payload.capacity()), payload_identity);
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, original.bytes() + future.bytes());
        assert_eq!(work.receipt().allocation_operations, operations);
        assert_eq!(work.receipt().bytes_copied, 0);
        drop(actual);
        drop(future);
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, original.bytes());
        drop(original);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn consuming_real_serde_map_order_raw_and_none_string_map_match_original() {
        for first in [true, false] {
            let mut map = serde_json::Map::new();
            if !first { map.insert("!ordinary".into(), Value::Null); }
            map.insert(RAW.into(), Value::String("[0,1]".into()));
            let source = Value::Object(map);
            let expected = serde_json::from_value::<Value>(source.clone());
            let selected = json_owned_value_shape_bounded(&source, LIMITS, JsonValueSchema::Root, &mut |_| Ok(())).unwrap();
            assert_eq!(selected.raw_frames, u64::from(first));
            let actual = json_from_value_owned_bounded_native_trusted::<Value, _>(
                source, LIMITS, 0, crate::wire::ErrorCode::Storage,
                JsonValueSchema::Root, &mut |_| Ok(()),
            ).unwrap();
            assert_eq!(actual, expected.unwrap());
        }
        let hidden = format!("{}0{}", "[".repeat(1024), "]".repeat(1024));
        let source = serde_json::json!({(RAW): hidden});
        let expected: BTreeMap<String, String> = serde_json::from_value(source.clone()).unwrap();
        let actual: BTreeMap<String, String> = json_from_value_owned_bounded_native_trusted(
            source, JsonBounds { maximum_depth: 1, maximum_items: 3, ..LIMITS }, 0,
            crate::wire::ErrorCode::Storage, JsonValueSchema::None, &mut |event| {
                if let JsonCodecCharge::Admission { value_shape, .. } = event {
                    assert_eq!(value_shape.raw_frames, 0);
                    assert_eq!(value_shape.virtual_items, 0);
                }
                Ok(())
            },
        ).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn consuming_hidden_root_and_public_request_deny_before_serde_and_guard_real_error() {
        for schema in [JsonValueSchema::Root, JsonValueSchema::ResourcePreparationRequest] {
            let raw = serde_json::json!({(RAW): format!("[{}0]", "0,".repeat(1024))});
            let source = if schema == JsonValueSchema::Root { raw } else { consuming_request_source(raw) };
            let mut admitted = false;
            let failure = json_from_value_owned_bounded_native_trusted::<Value, _>(
                source, JsonBounds { maximum_items: 128, ..LIMITS }, 0,
                crate::wire::ErrorCode::Storage, schema, &mut |event| {
                    admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                    Ok(())
                },
            ).unwrap_err();
            assert!(!admitted);
            assert!(matches!(failure, Error::NativeBoundary { .. }));
        }
        let source = consuming_request_source(serde_json::json!({(RAW): "[0,]"}));
        let expected = serde_json::from_value::<crate::resource_preparation::ResourcePreparationRequest>(source.clone()).err().unwrap();
        let token = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
            ..acyclic_fs::WorkBudget::default()
        }, &token).unwrap();
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let operations = work.receipt().allocation_operations;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(concrete_owned(&source) + sdk_json_diagnostic_bytes()).unwrap();
        let error = json_from_value_owned_bounded_native_trusted::<crate::resource_preparation::ResourcePreparationRequest, _>(
            source, LIMITS, 0, crate::wire::ErrorCode::Storage,
            JsonValueSchema::ResourcePreparationRequest, &mut |event| {
                if let JsonCodecCharge::Admission { value_shape, .. } = event {
                    original.resize_retained(original.bytes() + json_owned_value_conversion_additional_bytes(value_shape)?).unwrap();
                }
                Ok(())
            },
        ).err().unwrap();
        let error = match error {
            Error::NativeDecode { code, source, custody: None } => Error::NativeDecode { code, source, custody: Some(original) },
            _ => panic!("original consuming serde source expected"),
        };
        let actual = std::error::Error::source(&error).unwrap().downcast_ref::<serde_json::Error>().unwrap();
        assert_eq!(actual.classify(), expected.classify());
        assert_eq!(actual.line(), expected.line());
        assert_eq!(actual.column(), expected.column());
        assert_eq!(actual.to_string(), expected.to_string());
        let Error::NativeDecode { source, custody: Some(guard), code } = &error else { unreachable!() };
        assert_eq!(*code, crate::wire::ErrorCode::Storage);
        assert!(std::ptr::eq(actual, source));
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, guard.bytes());
        assert_eq!(work.receipt().allocation_operations, operations);
        token.cancel();
        drop(error);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn consuming_every_callback_denial_preserves_owned_cause_without_running_converter() {
        std::thread_local! {
            static CONVERSIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        }
        struct CountedRequest(crate::resource_preparation::ResourcePreparationRequest);
        impl<'de> serde::Deserialize<'de> for CountedRequest {
            fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
                use serde::Deserialize as _;
                CONVERSIONS.with(|count| count.set(count.get() + 1));
                crate::resource_preparation::ResourcePreparationRequest::deserialize(decoder).map(Self)
            }
        }
        let fixture = || consuming_request_source(serde_json::json!({(RAW): "[0,1]"}));
        CONVERSIONS.with(|count| count.set(0));
        let mut trace = Vec::new();
        let converted = json_from_value_owned_bounded_native_trusted::<CountedRequest, _>(
            fixture(), LIMITS, 0, crate::wire::ErrorCode::Storage,
            JsonValueSchema::ResourcePreparationRequest, &mut |event| {
                trace.push(std::mem::discriminant(&event));
                Ok(())
            },
        ).unwrap();
        assert_eq!(converted.0.payload, serde_json::json!([0,1]));
        assert_eq!(CONVERSIONS.with(|count| count.get()), 1);
        drop(converted);
        for denied in 0..trace.len() {
            CONVERSIONS.with(|count| count.set(0));
            let token = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
                ..acyclic_fs::WorkBudget::default()
            }, &token).unwrap();
            let before = work.remaining().unwrap().peak_allocation_bytes;
            let operations = work.receipt().allocation_operations;
            let source = fixture();
            let mut input = work.retain_owned(0).unwrap();
            input.resize_retained(concrete_owned(&source)).unwrap();
            let mut future = work.retain_owned(0).unwrap();
            future.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
            let cause = String::from("original owned inspector/admission rejection");
            let identity = (cause.as_ptr(), cause.capacity());
            let mut cause = Some(Error::Invalid(cause));
            let mut prefix = Vec::new();
            let failure = json_from_value_owned_bounded_native_trusted::<CountedRequest, _>(
                source, LIMITS, 0, crate::wire::ErrorCode::Storage,
                JsonValueSchema::ResourcePreparationRequest, &mut |event| {
                    prefix.push(std::mem::discriminant(&event));
                    if prefix.len() == denied + 1 {
                        token.cancel();
                        return Err(cause.take().expect("the first owned cause moves once"));
                    }
                    token.check().map_err(|_| Error::Indeterminate(Uuid::nil()))?;
                    if let JsonCodecCharge::Admission { value_shape, .. } = event {
                        future.resize_retained(json_owned_value_conversion_additional_bytes(value_shape)?).unwrap();
                    }
                    Ok(())
                },
            ).err().unwrap();
            let Error::Invalid(text) = &failure else { panic!("first callback cause must remain original"); };
            assert_eq!((text.as_ptr(), text.capacity()), identity);
            assert_eq!(prefix, trace[..=denied]);
            assert_eq!(CONVERSIONS.with(|count| count.get()), 0);
            assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, input.bytes() + future.bytes());
            assert_eq!(work.receipt().allocation_operations, operations);
            // The consumed source has been physically dropped on return; the
            // caller's original AL stays live throughout that drop and cleanup.
            drop(failure);
            drop(future);
            drop(input);
            assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
        }
    }

    #[test]
    fn consuming_derive_string_capacity_is_moved_not_borrowed_clone() {
        #[derive(serde::Deserialize)]
        struct Text { text: String }
        let mut text = String::with_capacity(4096);
        text.push_str("owned");
        let identity = (text.as_ptr(), text.capacity());
        let source = serde_json::json!({"text": null});
        let mut source = source;
        source["text"] = Value::String(text);
        let borrowed: Text = json_from_value_bounded_native_trusted(
            &source, LIMITS, std::mem::size_of::<Text>() as u64,
            crate::wire::ErrorCode::Storage, JsonValueSchema::None, &mut |_| Ok(()),
        ).unwrap();
        assert_ne!(borrowed.text.as_ptr(), identity.0);
        let actual: Text = json_from_value_owned_bounded_native_trusted(
            source, LIMITS, std::mem::size_of::<Text>() as u64,
            crate::wire::ErrorCode::Storage, JsonValueSchema::None, &mut |_| Ok(()),
        ).unwrap();
        assert_eq!((actual.text.as_ptr(), actual.text.capacity()), identity);
        assert_eq!(actual.text, borrowed.text);
    }

    #[test]
    fn consuming_additional_quota_rejects_slice_and_borrowed_sealed_origins_statically() {
        let slice = shape(b"null", JsonValueSchema::Root);
        assert!(matches!(json_owned_value_conversion_additional_bytes(slice),
            Err(Error::NativeBoundary { .. })));
        let mut borrowed_shape = None;
        let _: Value = json_from_value_bounded_native_trusted(
            &Value::Null, LIMITS, 0, crate::wire::ErrorCode::Storage,
            JsonValueSchema::Root, &mut |event| {
                if let JsonCodecCharge::Admission { value_shape, .. } = event {
                    borrowed_shape = Some(value_shape);
                }
                Ok(())
            },
        ).unwrap();
        assert!(matches!(json_owned_value_conversion_additional_bytes(borrowed_shape.unwrap()),
            Err(Error::NativeBoundary { .. })));
    }

    #[test]
    fn borrowed_closed_derive_conversion_holds_real_original_and_new_guards() {
        let source = serde_json::json!({"kind":"json","value":{"text":"owned".repeat(1024),"array":[0,1,u64::MAX]}});
        let expected: crate::model::ToolResultContent = serde_json::from_value(source.clone()).unwrap();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite real conversion ledger"));
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let baseline_operations = work.receipt().allocation_operations;
        let mut source_guard = work.retain_owned(0).unwrap();
        source_guard.resize_retained(concrete_owned(&source)).unwrap();
        let audited_owned = std::mem::size_of::<crate::model::ToolResultContent>() as u64;
        let mut new_guard = work.retain_owned(0).unwrap();
        new_guard.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let mut inspected = 0;
        let mut admission = None;
        let actual: crate::model::ToolResultContent = json_from_value_bounded_native_trusted(
            &source, LIMITS, audited_owned, crate::wire::ErrorCode::Storage,
            JsonValueSchema::ToolResultContent, &mut |event| {
                assert!(!matches!(event, JsonCodecCharge::Read { .. }), "borrowing a resident tree performs no source read");
                match event {
                    JsonCodecCharge::Inspect { items } => inspected += items,
                    JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                        assert!(json_owned_value_conversion_additional_bytes(value_shape).is_err(),
                            "a borrowed witness cannot authorize moved-string quota");
                        let full = json_value_decode_owned_bytes(value_shape)?;
                        let nodes = value_shape.lexical_items + value_shape.virtual_items;
                        let slot = std::mem::size_of::<crate::model::ToolResultContent>() as u64;
                        assert_eq!(json_typed_value_decode_owned_bytes(value_shape, 0)?, full,
                            "shared strings/Raw/diagnostics enter the typed factor exactly once");
                        assert_eq!(json_typed_value_decode_owned_bytes(value_shape, slot)?, full + 8 * nodes * slot);
                        assert!(json_typed_value_decode_owned_bytes(value_shape, u64::MAX).is_err());
                        new_guard.resize_retained(maximum_owned_bytes + json_value_decode_owned_bytes(value_shape)?).unwrap();
                        admission = Some(value_shape);
                    }
                    _ => {}
                }
                Ok(())
            },
        ).unwrap();
        assert_eq!(actual, expected);
        assert!(inspected > 0);
        assert!(admission.is_some());
        let crate::model::ToolResultContent::Json { value } = &actual else { panic!("actual closed JSON result"); };
        assert!(new_guard.bytes() >= concrete_owned(value));
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, source_guard.bytes() + new_guard.bytes());
        assert_eq!(work.receipt().allocation_operations - baseline_operations, 0,
            "future quota and foreign serde storage do not invent allocation attempts");
        drop(actual);
        drop(new_guard);
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, source_guard.bytes());
        drop(source);
        drop(source_guard);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn borrowed_api_does_not_claim_custom_owned_string_visitor_equivalence() {
        #[derive(Debug)]
        struct OwnedStringOnly(String);
        impl<'de> serde::Deserialize<'de> for OwnedStringOnly {
            fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
                struct Visitor;
                impl<'de> serde::de::Visitor<'de> for Visitor {
                    type Value = OwnedStringOnly;
                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        formatter.write_str("an owned string callback")
                    }
                    fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<Self::Value, E> {
                        Ok(OwnedStringOnly(value))
                    }
                }
                decoder.deserialize_string(Visitor)
            }
        }
        let source = Value::String("literal".into());
        assert_eq!(serde_json::from_value::<OwnedStringOnly>(source.clone()).unwrap().0, "literal");
        let error = json_from_value_bounded_native_trusted::<OwnedStringOnly, _>(
            &source, LIMITS, 1024, crate::wire::ErrorCode::Storage, JsonValueSchema::None, &mut |_| Ok(()),
        ).unwrap_err();
        assert!(matches!(error, Error::NativeDecode { code: crate::wire::ErrorCode::Storage, .. }),
            "this custom visitor is explicitly outside the borrowed audited-derive contract");
    }

    #[test]
    fn native_direct_tree_keeps_inline_bounds_and_genuine_custom_serde_source() {
        struct CustomFailure;
        impl serde::Serialize for CustomFailure {
            fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("actual trusted serializer cause"))
            }
        }
        let expected = serde_json::to_value(CustomFailure).unwrap_err();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 4096,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite actual serializer diagnostic ledger"));
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let retained = original.bytes();
        let error = json_value_bounded_native_trusted(
            &CustomFailure, LIMITS, crate::wire::ErrorCode::Storage, &mut |_| Ok(()),
        ).unwrap_err();
        let actual = std::error::Error::source(&error).unwrap().downcast_ref::<serde_json::Error>().unwrap();
        assert_eq!(actual.classify(), expected.classify());
        assert_eq!(actual.to_string(), expected.to_string());
        assert!(matches!(error, Error::NativeEncode { code: crate::wire::ErrorCode::Storage, custody: None, .. }));
        let error = match error {
            Error::NativeEncode { code, source, custody: None } => Error::NativeEncode {
                code, source, custody: Some(original),
            },
            other => panic!("actual serializer source required: {other}"),
        };
        let source = std::error::Error::source(&error).unwrap().downcast_ref::<serde_json::Error>().unwrap();
        let field = match &error {
            Error::NativeEncode { source, .. } => source,
            _ => unreachable!(),
        };
        assert!(std::ptr::eq(source, field));
        assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, retained);
        drop(error);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
        let error = json_value_bounded_native_trusted(
            &"owned", JsonBounds { maximum_items: 0, ..LIMITS },
            crate::wire::ErrorCode::Storage, &mut |_| Ok(()),
        ).unwrap_err();
        assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }),
            "SDK bounds do not become formatted serde marker messages");
    }

    #[test]
    fn native_direct_tree_real_quota_denial_keeps_original_cause_and_cleanup() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 4096,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite direct-encoder ledger"));
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let baseline_operations = work.receipt().allocation_operations;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let diagnostic = original.bytes();
        let cause = Uuid::new_v4();
        let error = json_value_bounded_native_trusted(
            &"large".repeat(4096), LIMITS, crate::wire::ErrorCode::Storage, &mut |event| {
                match event {
                    JsonCodecCharge::Retain { bytes } => {
                        original.resize_retained(original.bytes() + bytes).map_err(|_| Error::Indeterminate(cause))?;
                    }
                    JsonCodecCharge::Release { bytes } => original.release(bytes).unwrap(),
                    _ => {}
                }
                Ok(())
            },
        ).unwrap_err();
        assert!(matches!(error, Error::Indeterminate(id) if id == cause));
        assert_eq!(original.bytes(), diagnostic);
        assert_eq!(work.receipt().allocation_operations - baseline_operations, 0);
        drop(original);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn native_direct_tree_only_emits_actual_sizing_and_node_work() {
        let value = serde_json::json!({"escaped":"\0\n😀\"\\","nodes":[u64::MAX,i64::MIN,-0.0,null]});
        let bytes = serde_json::to_vec(&value).unwrap().len() as u64;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 65536,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite real tree custody ledger"));
        let before = work.remaining().unwrap().peak_allocation_bytes;
        let baseline_operations = work.receipt().allocation_operations;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let diagnostic = original.bytes();
        let mut inspected = 0;
        let mut nodes = 0;
        let actual = json_value_bounded_native_trusted(
            &value, JsonBounds { maximum_bytes: bytes, ..LIMITS }, crate::wire::ErrorCode::Storage, &mut |event| {
                match event {
                    JsonCodecCharge::Work { bytes, items } => {
                        assert_eq!(bytes, 0);
                        nodes += items;
                    }
                    JsonCodecCharge::Inspect { items } => inspected += items,
                    JsonCodecCharge::Read { .. } => panic!("a direct tree is not a source read"),
                    JsonCodecCharge::Retain { bytes } => original.resize_retained(original.bytes() + bytes).unwrap(),
                    JsonCodecCharge::Release { bytes } => original.release(bytes).unwrap(),
                    _ => {}
                }
                Ok(())
            },
        ).unwrap();
        assert_eq!(actual, value);
        assert_eq!(nodes, semantic_items(&value));
        assert_eq!(inspected, tree_sizing_inspections(&value));
        assert!(inspected > 0);
        assert_eq!(original.bytes(), diagnostic + owned_logical_bytes(&actual));
        assert_eq!(work.receipt().allocation_operations - baseline_operations, 0,
            "future quota and serde's opaque allocations are not observed operations");
        drop(actual);
        drop(original);
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
    }

    #[test]
    fn native_sizer_counts_real_duplicate_bytes_without_owned_tree_or_wire_charges() {
        struct Duplicates;
        impl serde::Serialize for Duplicates {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("same", "\0\n😀\"\\")?;
                map.serialize_entry("same", &(u64::MAX, i64::MIN, 0.1, -0.0, Option::<u8>::None))?;
                map.end()
            }
        }
        let encoded = serde_json::to_vec(&Duplicates).unwrap();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 65536,
            ..acyclic_fs::WorkBudget::default()
        }, &cancellation).unwrap_or_else(|_| panic!("finite sizer diagnostic custody"));
        let baseline_operations = work.receipt().allocation_operations;
        let mut original = work.retain_owned(0).unwrap();
        original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
        let held = work.remaining().unwrap().peak_allocation_bytes;
        let mut trace = Vec::new();
        validate_json_byte_bound_native_trusted(&Duplicates, JsonBounds {
            maximum_bytes: encoded.len() as u64, ..LIMITS
        }, crate::wire::ErrorCode::Storage, &mut |event| {
            trace.push(match event {
                JsonCodecCharge::Inspect { items } => (1, items),
                JsonCodecCharge::Work { bytes: 0, items } => (0, items),
                _ => panic!("sizing never emits Read, wire Work bytes, Copy, Retain or allocation operations"),
            });
            Ok(())
        }).unwrap();
        assert_eq!(trace.iter().filter(|(kind, _)| *kind == 1).map(|(_, count)| count).sum::<u64>(), encoded.len() as u64);
        assert_eq!(trace.iter().filter(|(kind, _)| *kind == 0).map(|(_, count)| count).sum::<u64>(), lexical_items(&encoded));
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, held);
        assert_eq!(work.receipt().allocation_operations - baseline_operations, 0);
        let bound = validate_json_byte_bound_native_trusted(&Duplicates, JsonBounds {
            maximum_bytes: encoded.len() as u64 - 1, ..LIMITS
        }, crate::wire::ErrorCode::Storage, &mut |_| Ok(())).unwrap_err();
        assert!(matches!(bound, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }));
        for denied in 0..trace.len() {
            let token = acyclic_fs::CancellationToken::new();
            let cause = Uuid::new_v4();
            let mut prefix = Vec::new();
            let failure = validate_json_byte_bound_native_trusted(&Duplicates, LIMITS,
                crate::wire::ErrorCode::Storage, &mut |event| {
                    prefix.push(match event {
                        JsonCodecCharge::Inspect { items } => (1, items),
                        JsonCodecCharge::Work { bytes: 0, items } => (0, items),
                        _ => panic!("sizing creates no owned output"),
                    });
                    if prefix.len() == denied + 1 { token.cancel(); }
                    token.check().map_err(|_| Error::Indeterminate(cause))
                }).unwrap_err();
            assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
            assert_eq!(prefix, trace[..=denied]);
        }
        let resident = serde_json::json!({(RAW): "[[[[0]]]]"});
        let mut source_guard = work.retain_owned(0).unwrap();
        source_guard.resize_retained(concrete_owned(&resident)).unwrap();
        let source_shape = json_owned_value_shape_bounded(&resident, LIMITS, JsonValueSchema::None, &mut |_| Ok(())).unwrap();
        assert_eq!(source_shape.raw_frames, 0);
        validate_json_byte_bound_native_trusted(&resident, LIMITS, crate::wire::ErrorCode::Storage, &mut |event| {
            assert!(matches!(event, JsonCodecCharge::Inspect { .. } | JsonCodecCharge::Work { bytes: 0, .. }));
            Ok(())
        }).unwrap();
        drop(resident);
        drop(source_guard);
        struct SerializerFailure;
        impl serde::Serialize for SerializerFailure {
            fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("real sizing serializer failure"))
            }
        }
        let expected = serde_json::to_vec(&SerializerFailure).unwrap_err();
        let error = validate_json_byte_bound_native_trusted(
            &SerializerFailure, LIMITS, crate::wire::ErrorCode::Storage, &mut |_| Ok(()),
        ).unwrap_err();
        let error = match error {
            Error::NativeEncode { code, source, custody: None } => Error::NativeEncode {
                code, source, custody: Some(original),
            },
            other => panic!("actual sizing serde source required: {other}"),
        };
        let source = std::error::Error::source(&error).unwrap().downcast_ref::<serde_json::Error>().unwrap();
        assert_eq!(source.classify(), expected.classify());
        assert_eq!(source.to_string(), expected.to_string());
        assert_eq!(work.remaining().unwrap().peak_allocation_bytes, held);
        drop(error);
    }

    #[test]
    fn native_seed_member_bound_preserves_prefix_drop_custody_and_owned_sidecar() {
        use serde::de::{DeserializeSeed, Error as _, SeqAccess, Visitor};
        use std::cell::Cell;
        const MARKER: &str = "bounded member seed interrupted";
        struct Member<'a> {
            text: String, drops: &'a Cell<usize>,
            held: &'a Cell<u64>, work: &'a acyclic_fs::NativeMaterializationWork,
        }
        impl Drop for Member<'_> {
            fn drop(&mut self) {
                assert!(!self.text.is_empty());
                assert_eq!(self.work.remaining().unwrap().peak_allocation_bytes, self.held.get(),
                    "partial owned members drop while their original quota is still held");
                self.drops.set(self.drops.get() + 1);
            }
        }
        struct MemberSeed<'a, 'b> {
            count: &'a Cell<usize>, maximum: usize, drops: &'a Cell<usize>, held: &'a Cell<u64>,
            work: &'a acyclic_fs::NativeMaterializationWork,
            primary: &'b mut Option<Error>, original: &'b mut Option<Error>,
        }
        impl<'de, 'a> DeserializeSeed<'de> for MemberSeed<'a, '_> {
            type Value = Member<'a>;
            fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> std::result::Result<Self::Value, D::Error> {
                if self.count.get() >= self.maximum {
                    if self.primary.is_none() { *self.primary = self.original.take(); }
                    return Err(D::Error::custom(MARKER));
                }
                let text = <String as serde::Deserialize>::deserialize(decoder)?;
                self.count.set(self.count.get() + 1);
                Ok(Member { text, drops: self.drops, held: self.held, work: self.work })
            }
        }
        struct Seed<'a, 'b> {
            called: &'a Cell<bool>, count: &'a Cell<usize>, drops: &'a Cell<usize>, held: &'a Cell<u64>,
            work: &'a acyclic_fs::NativeMaterializationWork,
            primary: &'b mut Option<Error>, original: &'b mut Option<Error>,
        }
        impl<'de, 'a> DeserializeSeed<'de> for Seed<'a, '_> {
            type Value = Vec<Member<'a>>;
            fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> std::result::Result<Self::Value, D::Error> {
                self.called.set(true);
                struct Sequence<'a, 'b>(Seed<'a, 'b>);
                impl<'de, 'a> Visitor<'de> for Sequence<'a, '_> {
                    type Value = Vec<Member<'a>>;
                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("at most two strings") }
                    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error> {
                        let seed = self.0;
                        let mut values = Vec::new();
                        // One genuine positive test-seed Vec reservation; serde
                        // String allocations remain foreign and unobserved.
                        seed.work.charge(acyclic_fs::WorkCounters {
                            allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
                        }).unwrap();
                        values.try_reserve_exact(2).map_err(|_| A::Error::custom(MARKER))?;
                        while let Some(member) = sequence.next_element_seed(MemberSeed {
                            count: seed.count, maximum: 2, drops: seed.drops, held: seed.held, work: seed.work,
                            primary: &mut *seed.primary, original: &mut *seed.original,
                        })? {
                            assert!(values.len() < 2, "capacity is admitted before member growth");
                            values.push(member);
                        }
                        Ok(values)
                    }
                }
                decoder.deserialize_seq(Sequence(self))
            }
        }
        for (portable, schedule) in [(false, 0), (false, 1), (false, 2), (true, 0), (true, 1), (true, 2)] {
            let deny_admission = schedule == 1;
            let exact_maximum = schedule == 2;
            let token = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                allocation_operations: 2, peak_allocation_bytes: 65536,
                ..acyclic_fs::WorkBudget::default()
            }, &token).unwrap_or_else(|_| panic!("finite real seed ledger"));
            let before = work.remaining().unwrap().peak_allocation_bytes;
            let baseline_operations = work.receipt().allocation_operations;
            let mut guard = work.retain_owned(0).unwrap();
            guard.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
            let held = Cell::new(work.remaining().unwrap().peak_allocation_bytes);
            let called = Cell::new(false);
            let count = Cell::new(0);
            let drops = Cell::new(0);
            let cause = Uuid::new_v4();
            let mut primary = None;
            let mut original = Some(Error::Indeterminate(cause));
            let bytes = if exact_maximum { br#"["one","two"]"#.as_slice() }
                else { br#"["one","two","three"]"#.as_slice() };
            let seed = Seed { called: &called, count: &count, drops: &drops, held: &held, work: &work,
                primary: &mut primary, original: &mut original };
            let mut charge = |event| {
                if let JsonCodecCharge::Admission { maximum_owned_bytes, .. } = event {
                    if deny_admission { return Err(Error::Indeterminate(cause)); }
                    guard.resize_retained(maximum_owned_bytes).unwrap();
                    held.set(work.remaining().unwrap().peak_allocation_bytes);
                }
                Ok(())
            };
            let result = if portable {
                json_from_slice_seed_bounded_trusted(seed, bytes, LIMITS, 16384, JsonValueSchema::None, &mut charge)
            } else {
                json_from_slice_seed_bounded_native_trusted(seed, bytes, LIMITS, 16384,
                    crate::wire::ErrorCode::Storage, JsonValueSchema::None, &mut charge)
            };
            if deny_admission {
                assert!(matches!(result, Err(Error::Indeterminate(id)) if id == cause));
                assert!(!called.get());
                assert_eq!(count.get(), 0);
                assert_eq!(drops.get(), 0);
                assert_eq!(work.receipt().allocation_operations - baseline_operations, 0);
            } else if exact_maximum {
                let members = result.unwrap();
                assert!(called.get());
                assert_eq!(count.get(), 2);
                assert_eq!(drops.get(), 0);
                assert_eq!(work.receipt().allocation_operations - baseline_operations, 1);
                drop(members);
                assert_eq!(drops.get(), 2);
                assert!(primary.is_none());
            } else {
                assert!(if portable { matches!(&result, Err(Error::Invalid(_))) }
                    else { matches!(&result, Err(Error::NativeDecode { .. })) }, "serde's static marker is only the bridge");
                assert!(called.get());
                assert_eq!(count.get(), 2, "third member is denied before its String visitor or Vec growth");
                assert_eq!(drops.get(), 2, "the physically partial vector has already dropped");
                assert_eq!(work.receipt().allocation_operations - baseline_operations, 1);
                drop(result); // foreign marker source drops under the original guard
                assert!(matches!(primary.take(), Some(Error::Indeterminate(id)) if id == cause));
            }
            assert_eq!(work.remaining().unwrap().peak_allocation_bytes, held.get());
            drop(guard);
            assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
        }
    }

    #[test]
    fn native_seed_syntax_schema_and_trailing_errors_keep_actual_guarded_source() {
        for bytes in [
            br#"["owned",0]"#.as_slice(), br#"["owned",]"#.as_slice(), br#"["owned"] 0"#.as_slice(),
        ] {
            let expected = serde_json::from_slice::<Vec<String>>(bytes).unwrap_err();
            let token = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 65536,
                ..acyclic_fs::WorkBudget::default()
            }, &token).unwrap_or_else(|_| panic!("finite real seeded source ledger"));
            let before = work.remaining().unwrap().peak_allocation_bytes;
            let mut original = work.retain_owned(0).unwrap();
            original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
            let error = json_from_slice_seed_bounded_native_trusted(
                std::marker::PhantomData::<Vec<String>>, bytes, LIMITS, 16384,
                crate::wire::ErrorCode::Storage, JsonValueSchema::None, &mut |event| {
                    if let JsonCodecCharge::Admission { maximum_owned_bytes, .. } = event {
                        original.resize_retained(maximum_owned_bytes).unwrap();
                    }
                    Ok(())
                },
            ).unwrap_err();
            let retained = original.bytes();
            let error = match error {
                Error::NativeDecode { code, source, custody: None } => Error::NativeDecode {
                    code, source, custody: Some(original),
                },
                other => panic!("actual seeded serde source required: {other}"),
            };
            let source = std::error::Error::source(&error).unwrap().downcast_ref::<serde_json::Error>().unwrap();
            assert_eq!(source.classify(), expected.classify());
            assert_eq!(source.line(), expected.line());
            assert_eq!(source.column(), expected.column());
            assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, retained);
            drop(error);
            assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
        }
    }

    #[test]
    fn actual_stock_command_serde_values_match_their_finite_profiles() {
        let raw = raw_document("[0]");
        let tool = format!(r#"{{"arguments":{raw},"name":"n","revision":"1"}}"#);
        let decoded: crate::filesystem::ToolTaskCommand = serde_json::from_str(&tool).unwrap();
        assert_eq!(decoded.arguments, serde_json::json!([0]));
        assert_eq!(shape(tool.as_bytes(), JsonValueSchema::ToolTaskCommand).raw_frames, 1);
        let task = format!(r#"{{"input":{raw},"name":"n","version":"1"}}"#);
        let decoded: crate::filesystem::TaskAdmitCommand = serde_json::from_str(&task).unwrap();
        assert_eq!(decoded.input, serde_json::json!([0]));
        assert_eq!(shape(task.as_bytes(), JsonValueSchema::TaskAdmitCommand).raw_frames, 1);
        let model = format!(r#"{{"input":{{"arguments":{raw},"kind":"tool_call","call_id":"c","name":"n"}},"max_steps":1}}"#);
        let decoded: crate::filesystem::ModelTaskCommand = serde_json::from_str(&model).unwrap();
        let crate::model::ModelContent::Part(crate::model::ModelContentPart::ToolCall { arguments, .. }) = decoded.input
            else { panic!("actual stock model tool-call input"); };
        assert_eq!(arguments, serde_json::json!([0]));
        assert_eq!(shape(model.as_bytes(), JsonValueSchema::ModelTaskCommand).raw_frames, 1);
    }

    fn native_bytes<T: serde::Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
        canonical: bool, value: &T, bounds: JsonBounds, charge: &mut F,
    ) -> Result<Vec<u8>> {
        if canonical {
            canonical_json_bytes_bounded_native_trusted(value, bounds, crate::wire::ErrorCode::Storage, charge)
        } else {
            json_bytes_bounded_native_trusted(value, bounds, crate::wire::ErrorCode::Storage, charge)
        }
    }

    #[test]
    fn native_byte_companions_preserve_real_identity_and_every_callback_prefix() {
        struct Duplicates;
        impl serde::Serialize for Duplicates {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("z", &1u64)?;
                map.serialize_entry("a", &u64::MAX)?;
                map.serialize_entry("z", &2u64)?;
                map.end()
            }
        }
        #[derive(serde::Serialize)]
        struct Record<'a> { z: (u64, i64, f64, f64, bool, Option<u8>), a: &'a str, map: Duplicates }
        let value = Record { z: (u64::MAX, i64::MIN, 0.1, -0.0, true, None), a: "\0\n😀\"\\", map: Duplicates };
        let standard = serde_json::to_vec(&value).unwrap();
        let mut sorted = serde_json::to_value(&value).unwrap();
        sorted.sort_all_objects();
        let canonical_oracle = serde_json::to_vec(&sorted).unwrap();
        fn event_key(event: JsonCodecCharge) -> (u8, u64, u64) {
            match event {
                JsonCodecCharge::Work { bytes, items } => (0, bytes, items),
                JsonCodecCharge::Retain { bytes } => (1, bytes, 0),
                JsonCodecCharge::Release { bytes } => (2, bytes, 0),
                JsonCodecCharge::Admission { maximum_owned_bytes, .. } => (3, maximum_owned_bytes, 0),
                JsonCodecCharge::CopyAdmission { bytes } => (4, bytes, 0),
                JsonCodecCharge::AllocationOperation { bytes } => (5, bytes, 0),
                JsonCodecCharge::Copy { bytes } => (6, bytes, 0),
                JsonCodecCharge::Read { bytes } => (7, bytes, 0),
                JsonCodecCharge::Inspect { items } => (8, items, 0),
            }
        }
        for canonical in [true, false] {
            let expected = if canonical { &canonical_oracle } else { &standard };
            let portable = if canonical {
                canonical_json_bytes_bounded(&value, LIMITS, &mut |_| Ok(())).unwrap()
            } else { json_bytes_bounded_trusted(&value, LIMITS, &mut |_| Ok(())).unwrap() };
            assert_eq!(&portable, expected);
            let mut trace = Vec::new();
            let actual = native_bytes(canonical, &value, LIMITS, &mut |event| {
                if !matches!(event, JsonCodecCharge::Release { .. }) { trace.push(event_key(event)); }
                Ok(())
            }).unwrap();
            assert_eq!(&actual, expected);
            for denied in 0..trace.len() {
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                    allocation_operations: 256, peak_allocation_bytes: 1024 * 1024,
                    bytes_copied: 1024 * 1024, bytes_encoded: 1024 * 1024, items_examined: 65536,
                    ..acyclic_fs::WorkBudget::default()
                }, &cancellation).unwrap_or_else(|_| panic!("finite actual byte-encoder ledger"));
                let baseline_operations = work.receipt().allocation_operations;
                let mut original = work.retain_owned(0).unwrap();
                original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
                let diagnostic = original.bytes();
                let cause = Uuid::new_v4();
                let mut prefix = Vec::new();
                let failure = native_bytes(canonical, &value, LIMITS, &mut |event| {
                    if let JsonCodecCharge::Release { bytes } = event {
                        original.release(bytes).unwrap();
                        return Ok(()); // physical cleanup remains valid after cancellation
                    }
                    prefix.push(event_key(event));
                    if prefix.len() == denied + 1 { cancellation.cancel(); }
                    cancellation.check().map_err(|_| Error::Indeterminate(cause))?;
                    match event {
                        JsonCodecCharge::Retain { bytes } => original.resize_retained(original.bytes() + bytes).unwrap(),
                        JsonCodecCharge::AllocationOperation { .. } => work.charge(acyclic_fs::WorkCounters {
                            allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
                        }).unwrap(),
                        JsonCodecCharge::Copy { bytes } => work.charge(acyclic_fs::WorkCounters {
                            bytes_copied: bytes, ..acyclic_fs::WorkCounters::default()
                        }).unwrap(),
                        JsonCodecCharge::Work { bytes, items } => work.charge(acyclic_fs::WorkCounters {
                            bytes_encoded: bytes, items_examined: items, ..acyclic_fs::WorkCounters::default()
                        }).unwrap(),
                        _ => {}
                    }
                    Ok(())
                }).unwrap_err();
                assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
                assert_eq!(prefix, trace[..=denied], "the genuine copy/reserve prefix stops at the original cause");
                assert_eq!(original.bytes(), diagnostic, "all failed codec trees/vectors physically drop before cleanup releases");
                assert_eq!(work.receipt().allocation_operations - baseline_operations,
                    prefix[..denied].iter().filter(|(kind, _, _)| *kind == 5).count() as u64,
                    "only accepted genuine SDK reserve checkpoints increment the observed baseline");
                drop(original);
            }
            for bounds in [
                JsonBounds { maximum_bytes: 0, ..LIMITS },
                JsonBounds { maximum_items: 0, ..LIMITS },
                JsonBounds { maximum_depth: 0, ..LIMITS },
            ] {
                let error = native_bytes(canonical, &value, bounds, &mut |_| Ok(())).unwrap_err();
                assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }));
            }
        }
    }

    #[test]
    fn native_byte_errors_move_real_serde_source_and_changed_pass_is_inline() {
        struct Failure;
        impl serde::Serialize for Failure {
            fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("actual byte serializer failure"))
            }
        }
        let expected = serde_json::to_vec(&Failure).unwrap_err();
        for canonical in [true, false] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 65536,
                ..acyclic_fs::WorkBudget::default()
            }, &cancellation).unwrap_or_else(|_| panic!("finite byte source custody"));
            let before = work.remaining().unwrap().peak_allocation_bytes;
            let mut original = work.retain_owned(0).unwrap();
            original.resize_retained(sdk_json_diagnostic_bytes()).unwrap();
            let retained = original.bytes();
            let error = native_bytes(canonical, &Failure, LIMITS, &mut |_| Ok(())).unwrap_err();
            let error = match error {
                Error::NativeEncode { code, source, custody: None } => Error::NativeEncode {
                    code, source, custody: Some(original),
                },
                other => panic!("actual serde byte source required: {other}"),
            };
            let source = std::error::Error::source(&error).unwrap().downcast_ref::<serde_json::Error>().unwrap();
            assert_eq!(source.classify(), expected.classify());
            assert_eq!(source.to_string(), expected.to_string());
            assert_eq!(before - work.remaining().unwrap().peak_allocation_bytes, retained);
            drop(error);
            assert_eq!(work.remaining().unwrap().peak_allocation_bytes, before);
        }
        for canonical in [true, false] {
            // Genuine deterministic capacity-overflow carrier, prepared before
            // codec entry. This is not an invented codec allocation operation.
            let allocation = Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err();
            let mut original = Some(Error::Allocation(allocation));
            let mut callbacks = 0;
            let failure = native_bytes(canonical, &(), LIMITS, &mut |_| {
                callbacks += 1;
                Err(original.take().expect("the owned callback cause moves once"))
            }).unwrap_err();
            assert!(matches!(failure, Error::Allocation(_)), "typed allocation cause is never replaced by a serde marker");
            assert_eq!(callbacks, 1);
        }
        struct Changing(std::cell::Cell<usize>);
        impl serde::Serialize for Changing {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                let pass = self.0.get();
                self.0.set(pass + 1);
                serializer.serialize_str(if pass == 0 { "short" } else { "longer second-pass output" })
            }
        }
        let error = json_bytes_bounded_native_trusted(&Changing(std::cell::Cell::new(0)), LIMITS,
            crate::wire::ErrorCode::Storage, &mut |_| Ok(())).unwrap_err();
        assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }),
            "changed/output-cap denial has no boxed std-IO marker source");
    }

    #[test]
    fn native_identity_preserves_legacy_deep_value_bytes_without_weakening_finite_entry_points() {
        // This already resident migration-style Value is NOT untrusted wire.
        // Its private-looking map key must stay an ordinary literal identity.
        let mut value = serde_json::json!({
            "z": [u64::MAX, i64::MIN, -0.0, 0.1],
            "$serde_json::private::RawValue": "[0,1]",
            "a": "\0\n😀\"\\"
        });
        for _ in 0..300 { value = Value::Array(vec![value]); }
        let expected = canonical_json_bytes_trusted(&value).unwrap();
        assert_eq!(expected, serde_json::to_vec(&value).unwrap());
        let mut trace = Vec::new();
        let mut retained = 0u64;
        let bytes = canonical_json_bytes_identity_native_trusted(&value,
            crate::wire::ErrorCode::Storage, &mut |event| {
                match event {
                    JsonCodecCharge::Retain { bytes } => retained += bytes,
                    JsonCodecCharge::Release { bytes } => retained -= bytes,
                    JsonCodecCharge::Admission { .. } | JsonCodecCharge::Read { .. } =>
                        panic!("resident identity is not a second parser or raw-key reinterpretation"),
                    _ => {}
                }
                trace.push(event);
                Ok(())
            }).unwrap();
        assert_eq!(bytes, expected);
        assert_eq!(retained, bytes.capacity() as u64, "only genuine output capacity is transferred");
        drop(bytes);
        let finite_error = canonical_json_bytes_bounded_native_trusted(&value, JsonBounds::IDENTITY,
            crate::wire::ErrorCode::Storage, &mut |_| Ok(())).unwrap_err();
        assert!(matches!(finite_error, Error::NativeBoundary { .. }),
            "the explicit identity companion does not remove the finite codec's depth ceiling");
        let cancellable = trace.iter().filter(|event| !matches!(event, JsonCodecCharge::Release { .. })).count();
        for denied in [0, cancellable / 2, cancellable - 1] {
            let cause = Uuid::new_v4();
            let mut callbacks = 0;
            let mut retained = 0u64;
            let failure = canonical_json_bytes_identity_native_trusted(&value,
                crate::wire::ErrorCode::Storage, &mut |event| {
                    if let JsonCodecCharge::Release { bytes } = event {
                        retained -= bytes;
                        return Ok(()); // unconditional after physical drop
                    }
                    let current = callbacks;
                    callbacks += 1;
                    if current == denied { return Err(Error::Indeterminate(cause)); }
                    if let JsonCodecCharge::Retain { bytes } = event { retained += bytes; }
                    Ok(())
                }).unwrap_err();
            assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
            assert_eq!(retained, 0, "failure drops actual temporary tree/output before cleanup");
        }
    }

    fn prepaid_journal_fixture() -> Value {
        // Actual public DTO serialization, not a physical journal producer E2E:
        // these literal identities confer no root or preparation authority.
        let journal = acyclic_fs::MaterializationJournal {
            version: 1, revision: 0,
            plan: acyclic_fs::MaterializationPlan {
                operation_id: acyclic_fs::OperationId::new(),
                intent: acyclic_fs::MaterializationIntent::NativePreparation {
                    selection: Vec::new(), original: b"immutable original request".to_vec(),
                },
                edits: vec![acyclic_fs::MaterializationEdit::Remove {
                    path: "immutable/escaped-\n\"\\😀".into(),
                }],
            },
            root_identity: None, preparation_identity: None, preparation_progress: None,
            work: acyclic_fs::WorkCounters::default(), preimages: Vec::new(),
            phase: acyclic_fs::MaterializationPhase::Prepared, applied: 0, restored: 0,
        };
        let mut value = serde_json::to_value(journal).unwrap();
        value.sort_all_objects();
        value
    }

    fn update_prepaid_scalars(value: &mut Value, scalar: u64) {
        *value.get_mut("revision").unwrap() = Value::from(scalar);
        for slot in value.get_mut("work").unwrap().as_object_mut().unwrap().values_mut() {
            assert!(slot.is_u64(), "actual WorkCounters fields are u64 slots");
            *slot = Value::from(scalar);
        }
    }

    fn prepaid_trace_event(event: JsonCodecCharge) -> (bool, u64) {
        match event {
            JsonCodecCharge::Work { bytes, items: 0 } => (false, bytes),
            JsonCodecCharge::Copy { bytes } => (true, bytes),
            _ => panic!("prepaid final serde emits only actual Work and Copy"),
        }
    }

    fn prepaid_depth(value: &Value) -> usize {
        match value {
            Value::Array(values) => 1 + values.iter().map(prepaid_depth).max().unwrap_or(0),
            Value::Object(values) => 1 + values.values().map(prepaid_depth).max().unwrap_or(0),
            _ => 0,
        }
    }

    #[test]
    fn prepaid_public_journal_and_literal_float_vectors_preserve_paid_buffers() {
        // No accepted completion-reservation API exists in this dependency.
        // This is a final-writer/AL control, not post-effect native authority.
        for mut value in [
            prepaid_journal_fixture(),
            serde_json::json!({
                "revision": 0, "work": {"bytes_encoded": 0, "bytes_copied": 0},
                "immutable": ["\0\n😀\"\\", u64::MAX, i64::MIN, -0.0, 0.1,
                    f64::MIN_POSITIVE, f64::MAX, f64::from_bits(1)],
                "$serde_json::private::RawValue": "[0,1]",
            }),
        ] {
            value.sort_all_objects();
            update_prepaid_scalars(&mut value, u64::MAX);
            assert!(prepaid_depth(&value) <= 256, "semantic depth admitted before scalar updates");
            let maximum = serde_json::to_vec(&value).unwrap().len() as u64;
            let mut output = Vec::with_capacity(maximum as usize);
            let capacity = output.capacity();
            let pointer = output.as_ptr();
            let text = if value.get("plan").is_some() {
                value["plan"]["edits"][0]["Remove"]["path"].as_str().unwrap()
            } else { value["immutable"][0].as_str().unwrap() };
            let text_pointer = text.as_ptr();
            let token = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
                bytes_encoded: 1024 * 1024, bytes_copied: 1024 * 1024,
                ..acyclic_fs::WorkBudget::default()
            }, &token).unwrap();
            let baseline = work.remaining().unwrap().peak_allocation_bytes;
            let original = work.retain_owned(concrete_owned(&value) + capacity as u64
                + sdk_json_diagnostic_bytes()).unwrap();
            let paid = original.bytes();
            assert_eq!(work.remaining().unwrap().allocation_operations, 0);
            for scalar in [0, u64::MAX] {
                output.clear();
                update_prepaid_scalars(&mut value, scalar);
                let standard = serde_json::to_vec(&value).unwrap();
                assert_eq!(canonical_json_bytes_trusted(&value).unwrap(), standard);
                let before = work.receipt();
                let mut trace = Vec::new();
                canonical_json_value_into_prepaid_native_trusted(&value, &mut output, maximum,
                    crate::wire::ErrorCode::Storage, &mut |event| {
                        let (copy, bytes) = prepaid_trace_event(event);
                        trace.push((copy, bytes));
                        work.charge(acyclic_fs::WorkCounters {
                            bytes_copied: if copy { bytes } else { 0 },
                            bytes_encoded: if copy { 0 } else { bytes },
                            ..acyclic_fs::WorkCounters::default()
                        }).unwrap();
                        Ok(())
                    }).unwrap();
                assert_eq!(output, standard);
                assert_eq!(trace[0], (false, 0), "entry checkpoint precedes serde");
                assert_eq!(trace.iter().filter(|(copy, _)| *copy).map(|(_, bytes)| bytes).sum::<u64>(),
                    output.len() as u64);
                assert_eq!(work.receipt().bytes_copied - before.bytes_copied, output.len() as u64);
                assert_eq!(work.receipt().bytes_encoded - before.bytes_encoded, output.len() as u64);
                assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
                assert_eq!(output.as_ptr(), pointer);
                assert_eq!(output.capacity(), capacity);
                let text = if value.get("plan").is_some() {
                    value["plan"]["edits"][0]["Remove"]["path"].as_str().unwrap()
                } else { value["immutable"][0].as_str().unwrap() };
                assert_eq!(text.as_ptr(), text_pointer);
                assert_eq!(original.bytes(), paid);
            }
            drop(output);
            drop(value);
            drop(original);
            assert_eq!(work.remaining().unwrap().peak_allocation_bytes, baseline);
        }
    }

    #[test]
    fn prepaid_every_denial_keeps_original_cause_and_paid_physical_prefix() {
        let value = prepaid_journal_fixture();
        let expected = serde_json::to_vec(&value).unwrap();
        let maximum = expected.len() as u64;
        let mut output = Vec::with_capacity(expected.len());
        let mut trace = Vec::new();
        canonical_json_value_into_prepaid_native_trusted(&value, &mut output, maximum,
            crate::wire::ErrorCode::Storage, &mut |event| {
                trace.push(prepaid_trace_event(event)); Ok(())
            }).unwrap();
        for denied in 0..trace.len() {
            let token = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
                allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
                bytes_encoded: maximum, bytes_copied: maximum,
                ..acyclic_fs::WorkBudget::default()
            }, &token).unwrap();
            let value = value.clone(); // setup, not a helper allocation
            let mut output = Vec::with_capacity(expected.len());
            let pointer = output.as_ptr();
            let capacity = output.capacity();
            let baseline = work.live_allocation_bytes();
            let original = work.retain_owned(concrete_owned(&value) + capacity as u64
                + sdk_json_diagnostic_bytes()).unwrap();
            let paid = original.bytes();
            let cause = Uuid::new_v4();
            let mut reached = Vec::new();
            let failure = canonical_json_value_into_prepaid_native_trusted(&value, &mut output,
                maximum, crate::wire::ErrorCode::Storage, &mut |event| {
                    reached.push(prepaid_trace_event(event));
                    if reached.len() == denied + 1 { token.cancel(); }
                    token.check().map_err(|_| Error::Indeterminate(cause))?;
                    let (copy, bytes) = prepaid_trace_event(event);
                    work.charge(acyclic_fs::WorkCounters {
                        bytes_copied: if copy { bytes } else { 0 },
                        bytes_encoded: if copy { 0 } else { bytes },
                        ..acyclic_fs::WorkCounters::default()
                    }).unwrap();
                    Ok(())
                }).unwrap_err();
            assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
            assert_eq!(reached, trace[..=denied]);
            let copied = trace[..denied].iter().filter(|(copy, _)| *copy)
                .map(|(_, bytes)| *bytes).sum::<u64>() as usize;
            assert_eq!(output, expected[..copied], "only accepted Copy chunks physically extend");
            assert_eq!(work.receipt().bytes_copied, copied as u64);
            assert_eq!(work.receipt().bytes_encoded,
                trace[..denied].iter().filter(|(copy, _)| !*copy)
                    .map(|(_, bytes)| *bytes).sum::<u64>());
            assert_eq!(output.as_ptr(), pointer);
            assert_eq!(output.capacity(), capacity);
            assert_eq!(original.bytes(), paid, "no false release of partial caller output");
            output.clear(); // capacity is still live and paid
            assert_eq!(original.bytes(), paid);
            assert_eq!(work.live_allocation_bytes(), baseline + paid);
            drop(output);
            drop(value);
            drop(original);
            assert_eq!(work.live_allocation_bytes(), baseline,
                "original AL cleanup follows physical drops even after cancellation");
            assert_eq!(work.receipt().allocation_operations, 1);
        }
    }

    #[test]
    fn prepaid_static_preconditions_and_zero_copy_quota_do_not_fallback() {
        let value = serde_json::json!({"immutable": "\0\n😀\"\\", "revision": u64::MAX});
        for (mut output, maximum) in [(vec![7], 1), (Vec::new(), 1)] {
            let pointer = output.as_ptr();
            let capacity = output.capacity();
            let before = output.clone();
            let error = canonical_json_value_into_prepaid_native_trusted(&value, &mut output,
                maximum, crate::wire::ErrorCode::Storage, &mut |_| panic!("static precondition before callbacks"))
                .unwrap_err();
            assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }));
            assert_eq!(output, before);
            assert_eq!(output.as_ptr(), pointer);
            assert_eq!(output.capacity(), capacity);
        }
        let mut no_output = Vec::new();
        let mut checkpoint = Vec::new();
        let error = canonical_json_value_into_prepaid_native_trusted(&value, &mut no_output,
            0, crate::wire::ErrorCode::Storage, &mut |event| {
                checkpoint.push(prepaid_trace_event(event)); Ok(())
            }).unwrap_err();
        assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }));
        assert_eq!(checkpoint, [(false, 0)], "static byte bound rejects the first serde chunk without Copy");
        assert!(no_output.is_empty());
        assert_eq!(no_output.capacity(), 0, "no heap-engine fallback");
        let expected = serde_json::to_vec(&value).unwrap();
        let mut output = Vec::with_capacity(expected.len());
        let token = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 1024 * 1024,
            bytes_encoded: expected.len() as u64, bytes_copied: 0,
            ..acyclic_fs::WorkBudget::default()
        }, &token).unwrap();
        let original = work.retain_owned(concrete_owned(&value) + output.capacity() as u64).unwrap();
        let paid = original.bytes();
        let mut denied = None;
        let cause = Uuid::new_v4();
        let failure = canonical_json_value_into_prepaid_native_trusted(&value, &mut output,
            expected.len() as u64, crate::wire::ErrorCode::Storage, &mut |event| {
                let (copy, bytes) = prepaid_trace_event(event);
                work.charge(acyclic_fs::WorkCounters {
                    bytes_copied: if copy { bytes } else { 0 },
                    bytes_encoded: if copy { 0 } else { bytes },
                    ..acyclic_fs::WorkCounters::default()
                }).map_err(|error| { denied = Some(error); Error::Indeterminate(cause) })
            }).unwrap_err();
        assert!(matches!(failure, Error::Indeterminate(id) if id == cause));
        assert!(matches!(denied, Some(acyclic_fs::NativeTreeMaterializationError::Work(_))));
        assert!(output.is_empty(), "zero copy allowance denies the first real extend");
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(work.receipt().allocation_operations, 1);
        assert_eq!(original.bytes(), paid);
        drop(output);
        drop(value);
        drop(original);
    }
}
