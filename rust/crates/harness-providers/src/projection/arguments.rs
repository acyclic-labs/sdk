//! N-07 / ANT-TOOL-03 / OAR-TOOL-02 / GEM-TOOL-03.
//! History may contain non-object arguments; preserve their value in an object.
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value;

/// Borrows the original value until the final wire serialization. No intermediate
/// argument tree is allocated, and already-portable objects remain unchanged.
pub(super) struct Arguments<'a>(pub &'a Value);

impl Serialize for Arguments<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_object() {
            self.0.serialize(serializer)
        } else {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("_raw", self.0)?;
            map.end()
        }
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn assert_normalization(value: &Value) {
    crate::fuzzing::assert_model_json_round_trip(value);
    // At most three generated levels: Gemini part, functionCall and _raw.
    assert_normalization_with_depth(value, 3);
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
fn assert_normalization_with_depth(value: &Value, envelope_depth: u32) {
    let output = |bytes: &[u8]| literal_at_depth(bytes, 128 + envelope_depth).unwrap();
    let bytes = serde_json::to_vec(&Arguments(value)).unwrap();
    let normalized = output(&bytes);
    assert_value(value, &normalized);
    assert_eq!(serde_json::to_vec(&Arguments(&normalized)).unwrap(), bytes);
    for adapter in [
        crate::Adapter::Anthropic,
        crate::Adapter::Responses,
        crate::Adapter::Chat,
        crate::Adapter::Gemini,
    ] {
        let call = super::call::Call {
            adapter,
            id: "call_0".into(),
            name: "echo".into(),
            arguments: value,
            signature: None,
        };
        let encoded = serde_json::to_vec(&call).unwrap();
        let wire = output(&encoded);
        let arguments = match adapter {
            crate::Adapter::Anthropic => wire["input"].clone(),
            crate::Adapter::Gemini => wire["functionCall"]["args"].clone(),
            crate::Adapter::Responses | crate::Adapter::Chat => {
                let raw = if adapter == crate::Adapter::Responses {
                    &wire["arguments"]
                } else {
                    &wire["function"]["arguments"]
                };
                let raw = raw.as_str().unwrap();
                // Codec fuzzing includes standalone values deeper than an
                // admitted ModelRequest. Validate those with the derived output
                // bound; actual request validator admission is checked separately.
                output(raw.as_bytes())
            }
        };
        assert_value(value, &arguments);
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn assert_value(value: &Value, normalized: &Value) {
    assert!(normalized.is_object());
    if value.is_object() {
        assert_eq!(normalized, value);
    } else {
        let object = normalized.as_object().unwrap();
        assert_eq!(
            object.len(),
            1,
            "non-object arguments need exactly one _raw entry"
        );
        assert_eq!(
            object.get("_raw"),
            Some(value),
            "missing or changed _raw argument"
        );
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn literal(bytes: &[u8]) -> acyclic_harness::Result<Value> {
    literal_at_depth(bytes, 128)
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
fn literal_at_depth(bytes: &[u8], maximum_depth: u32) -> acyclic_harness::Result<Value> {
    let raw: &serde_json::value::RawValue =
        serde_json::from_slice(bytes).map_err(|_| crate::invalid("invalid JSON fixture"))?;
    acyclic_harness::json_value_bounded(
        raw,
        acyclic_harness::JsonBounds {
            maximum_bytes: bytes.len() as u64,
            maximum_items: bytes.len() as u64,
            maximum_depth,
        },
        None,
        &mut |_| Ok(()),
    )
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
fn wire_wrapping_collision_leaves_distinct_canonical_arguments_unchanged() {
    // Wire envelopes have no inverse: replay always reads the canonical transcript.
    let array = serde_json::json!([1]);
    let object = serde_json::json!({"_raw":[1]});
    let encode = |value: &Value| serde_json::to_vec(&Arguments(value)).unwrap();
    assert_eq!(encode(&array), encode(&object));
    assert_ne!(array, object);
    assert_normalization(&array);
    assert_normalization(&object);
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
#[should_panic]
fn negative_control_input_depth_cannot_decode_the_normalized_boundary() {
    let value = (0..128).fold(serde_json::json!(0), |inner, _| Value::Array(vec![inner]));
    assert_normalization_with_depth(&value, 0);
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
#[should_panic]
fn negative_control_one_extra_level_cannot_decode_the_call_envelope() {
    let value = (0..128).fold(serde_json::json!(0), |inner, _| Value::Array(vec![inner]));
    assert_normalization_with_depth(&value, 1);
}
