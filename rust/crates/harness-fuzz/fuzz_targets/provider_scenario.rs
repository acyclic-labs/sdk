#![no_main]
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness-providers/tests/integration/provider_case.rs"]
mod provider_case;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(case) = serde_json::from_slice::<provider_case::ProviderCase>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    assert_eq!(
        serde_json::from_slice::<provider_case::ProviderCase>(&encoded).unwrap(),
        case
    );
    if case.validate().is_ok() {
        assert!(!case.responses.is_empty());
        assert_eq!(case.responses.len(), case.expect.events.len());
        assert!(case.chunk_bytes.get() > 0);
    }
});
