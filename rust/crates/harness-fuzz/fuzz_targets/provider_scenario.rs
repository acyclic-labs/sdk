#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
#[path = "../../harness/tests/e2e/provider_case.rs"]
mod provider_case;
fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(case) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<provider_case::ProviderCase>,
    ) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let _ = fuzz_support::round_trip(
        &(case),
        serde_json::from_slice::<provider_case::ProviderCase>(&encoded),
    );
    if case.validate().is_ok() {
        assert!(!case.responses.is_empty());
        assert_eq!(case.responses.len(), case.expect.events.len());
        assert!(case.chunk_bytes.get() > 0);
    }
});
