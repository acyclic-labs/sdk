use acyclic_harness::executor::json::{
    JsonBounds, JsonValueSchema, canonical_json_bytes_bounded, json_bytes_bounded,
    json_from_slice_bounded, json_value_bounded,
};
use serde_json::Value;

pub fn check(input: &[u8]) {
    if input.len() > 16_384 {
        return;
    }
    let bounds = JsonBounds {
        maximum_bytes: 131_072,
        maximum_items: 1_048_576,
        maximum_depth: 256,
    };
    // Exercise refusals too; the independent serde oracle below only
    // compares documents within its own default recursion allowance.
    let decoded = json_from_slice_bounded(
        std::marker::PhantomData::<Value>,
        input,
        bounds,
        None,
        Some(JsonValueSchema::Root),
        &mut |_| Ok(()),
    );
    let Ok(mut expected) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    expected.sort_all_objects();
    let raw = serde_json::from_slice::<Box<serde_json::value::RawValue>>(input).unwrap();
    let compact = json_bytes_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&compact).unwrap(), expected);
    let bytes = canonical_json_bytes_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&expected).unwrap());
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), expected);
    assert_eq!(
        json_value_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap(),
        expected
    );
    assert_eq!(decoded.unwrap(), expected);
}
