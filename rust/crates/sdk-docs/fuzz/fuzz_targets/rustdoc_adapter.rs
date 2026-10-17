#![no_main]

#[path = "../../src/public_api/decode.rs"]
mod decode;
#[path = "../../src/relative_spans.rs"]
mod relative_spans;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(decoded) = decode::from_slice(bytes) {
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
        let replay = decode::from_slice(&encoded).unwrap();
        assert_eq!(relative_spans::sources(replay.index.values()), paths);
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            serde_json::to_value(&replay).unwrap(),
        );
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let owned = serde_json::from_value::<rustdoc_types::Crate>(value.clone());
        let canonical = serde_json::to_vec(&value).unwrap();
        let decoded = decode::from_slice(&canonical);
        assert_eq!(owned.is_ok(), decoded.is_ok());
        if let (Ok(owned), Ok(decoded)) = (owned, decoded) {
            assert_eq!(
                serde_json::to_value(&owned).unwrap(),
                serde_json::to_value(&decoded).unwrap(),
            );
        }
    }
});
