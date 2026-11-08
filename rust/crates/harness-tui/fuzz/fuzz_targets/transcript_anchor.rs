#![no_main]
use acyclic_harness_tui::TranscriptAnchor;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(anchor) = serde_json::from_slice::<TranscriptAnchor>(data) {
        let bytes = serde_json::to_vec(&anchor).unwrap();
        assert_eq!(
            serde_json::from_slice::<TranscriptAnchor>(&bytes).unwrap(),
            anchor
        );
        let mut value = serde_json::to_value(anchor).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<TranscriptAnchor>(value).is_err());
        let mut value = serde_json::to_value(anchor).unwrap();
        value["position"]["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<TranscriptAnchor>(value).is_err());
    }
});
