use super::*;
use super::value_shape::ValueInspector;
use std::collections::BTreeMap;

const RAW: &str = "$serde_json::private::RawValue";
const LIMITS: JsonBounds = JsonBounds {
    maximum_bytes: 16 * 1024 * 1024,
    maximum_items: 2 * 1024 * 1024,
    maximum_depth: 256,
};

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
    let unrelated = format!("{{\"schema\":{deep},\"type\":\"choice\"}}");
    assert!(inspect(unrelated.as_bytes(), Some(JsonValueSchema::Interaction), JsonBounds {
        maximum_items: 16, maximum_depth: 3, ..LIMITS
    }, &mut |_| Ok(())).is_ok());
}

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #[test]
    fn real_outcome_value_branch_matches_original_decode_and_shape_model(
        strings in proptest::collection::vec(proptest::prelude::any::<String>(), 0..12),
        integers in proptest::collection::vec(proptest::prelude::any::<u64>(), 0..12),
        float in -1.0e100f64..1.0e100f64,
        layers in 0usize..5,
        succeeds in proptest::prelude::any::<bool>(),
    ) {
        let leaf = serde_json::to_string(&(strings, integers, float)).unwrap();
        let (payload, raw_bytes, virtual_items) = raw_layers(leaf, layers);
        let document = if succeeds {
            format!(r#"{{"Succeeded":{payload}}}"#)
        } else {
            serde_json::to_string(&crate::Outcome::<Value>::Failed { message: payload }).unwrap()
        };
        let expected: crate::Outcome<Value> = serde_json::from_str(&document).unwrap();
        let mut issued = None;
        let actual = json_from_slice_seed_bounded_trusted(
            std::marker::PhantomData::<crate::Outcome<Value>>, document.as_bytes(), LIMITS, 0,
            JsonValueSchema::TaskOutcome, &mut |event| {
                if let JsonCodecCharge::Admission { value_shape, .. } = event { issued = Some(value_shape); }
                Ok(())
            },
        ).unwrap();
        let issued = issued.unwrap();
        proptest::prop_assert_eq!(issued.raw_frames, if succeeds { layers as u64 } else { 0 });
        proptest::prop_assert_eq!(issued.raw_bytes, if succeeds { raw_bytes } else { 0 });
        proptest::prop_assert_eq!(issued.virtual_items, if succeeds { virtual_items } else { 0 });
        proptest::prop_assert_eq!(&actual, &expected);
        if let crate::Outcome::Succeeded(value) = &actual {
            proptest::prop_assert!(json_value_decode_owned_bytes(issued).unwrap() >= concrete_owned(value) + raw_bytes);
        }
        proptest::prop_assert_eq!(crate::contract::canonical_json_bytes(&actual).unwrap(),
            crate::contract::canonical_json_bytes(&expected).unwrap());
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;

    fn decode<F: FnMut(JsonCodecCharge) -> Result<()>>(
        bytes: &[u8], bounds: JsonBounds, charge: &mut F,
    ) -> Result<Value> {
        json_from_slice_bounded_native_trusted(bytes, bounds, 0, crate::wire::ErrorCode::Storage, JsonValueSchema::Root, charge)
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
        let error = json_value_bounded(
            &CustomFailure, LIMITS, Some(crate::wire::ErrorCode::Storage), &mut |_| Ok(()),
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
        let error = json_value_bounded(
            &"owned", JsonBounds { maximum_items: 0, ..LIMITS },
            Some(crate::wire::ErrorCode::Storage), &mut |_| Ok(()),
        ).unwrap_err();
        assert!(matches!(error, Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. }),
            "SDK bounds do not become formatted serde marker messages");
    }

}
