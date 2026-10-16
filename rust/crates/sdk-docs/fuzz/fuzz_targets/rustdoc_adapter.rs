#![no_main]

#[path = "../../src/public_api/decode.rs"]
mod decode;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
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
