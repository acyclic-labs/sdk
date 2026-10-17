#![no_main]

#[path = "../../harness/tests/e2e/expectations.rs"]
mod expectations;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(case) = serde_json::from_slice::<expectations::Expectations>(bytes) else {
        return;
    };
    let decoded: expectations::Expectations =
        serde_json::from_slice(&serde_json::to_vec(&case).unwrap()).unwrap();
    assert_eq!(case, decoded);
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
