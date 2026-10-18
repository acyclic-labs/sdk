#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../harness/tests/e2e/expectations.rs"]
mod expectations;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(case) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<expectations::Expectations>,
    ) else {
        return;
    };
    let _ = fuzz_support::round_trip(
        &case,
        serde_json::from_slice(&serde_json::to_vec(&case).unwrap()),
    );
    if let Some(events) = &case.model_events {
        let mut frames: Vec<_> = events
            .iter()
            .map(|event| serde_json::json!({"kind":"model","event":event}))
            .collect();
        assert!(case.matches_model_events(&frames));
        frames.push(serde_json::json!({"kind":"model","event":{"kind":"content","delta":""}}));
        assert!(!case.matches_model_events(&frames));
    }
});
