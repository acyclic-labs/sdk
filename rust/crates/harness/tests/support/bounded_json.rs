use acyclic_harness::executor::json::{
    JsonBounds, canonical_json_bytes_bounded, json_bytes_bounded, json_value_bounded,
};
use serde_json::Value;

pub fn check(input: &[u8]) {
    if input.len() > 16_384 {
        return;
    }
    let Ok(mut expected) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    expected.sort_all_objects();
    let raw = serde_json::from_slice::<Box<serde_json::value::RawValue>>(input).unwrap();
    let bounds = JsonBounds {
        maximum_bytes: 131_072,
        maximum_items: 1_048_576,
        maximum_depth: 256,
    };
    let compact = json_bytes_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&compact).unwrap(), expected);
    let bytes = canonical_json_bytes_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&expected).unwrap());
    let mut rescanned = 0;
    let resident = canonical_json_bytes_bounded(&expected, bounds, None, &mut |event| {
        if let acyclic_harness::executor::json::JsonCodecCharge::Inspect { items } = event {
            rescanned += items;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(resident, bytes);
    assert_eq!(rescanned, 0, "canonical array index rescanned its document");
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), expected);
    assert_eq!(
        json_value_bounded(&raw, bounds, None, &mut |_| Ok(())).unwrap(),
        expected
    );
}
