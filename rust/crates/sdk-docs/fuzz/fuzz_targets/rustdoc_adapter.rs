#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../src/public_api/decode.rs"]
mod decode;
#[path = "../../src/relative_spans.rs"]
mod relative_spans;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(decoded) = fuzz_support::FuzzInput::parse(fuzz_input, decode::from_slice) {
        let paths = relative_spans::sources(decoded.index.values());
        for item in decoded.index.values().filter(|item| item.crate_id == 0) {
            if let Some(span) = item
                .span
                .as_ref()
                .filter(|span| !span.filename.is_absolute())
            {
                assert!(paths.contains(span.filename.as_path()));
            }
        }
        let encoded = serde_json::to_vec(&decoded).unwrap();
        let replay = fuzz_support::round_trip_by(
            &serde_json::to_value(&decoded).unwrap(),
            decode::from_slice(&encoded),
            serde_json::to_value,
        );
        assert_eq!(relative_spans::sources(replay.index.values()), paths);
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            serde_json::to_value(&replay).unwrap(),
        );
    }
    if let Some(value) = fuzz_support::json_input!(fuzz_input, serde_json::Value) {
        let owned = fuzz_support::input_result(&value, |value| {
            serde_json::from_value::<rustdoc_types::Crate>(value.clone())
        })
        .and_then(serde_json::to_value)
        .map_err(|_| ());
        let canonical = serde_json::to_vec(&*value).unwrap();
        let _ = fuzz_support::same_result(
            &owned,
            decode::from_slice(&canonical)
                .and_then(serde_json::to_value)
                .map_err(|_| ()),
        );
    }
});
