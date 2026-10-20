#![no_main]
#[path = "../../harness/tests/e2e/file_bytes.rs"]
mod file_bytes;

#[path = "../../harness/tests/e2e/expectations.rs"]
mod expectations;
#[path = "../../harness/tests/e2e/matchers.rs"]
mod matchers;

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
    // Treat script values as literal text, rather than executable $json or
    // $contains matchers. Exercise the full declared request list in one shape.
    let parts: Vec<_> = case.requests.as_deref().unwrap_or_default().iter()
        .map(|request| serde_json::json!({"kind":"text","text":serde_json::to_string(request).unwrap()}))
        .collect();
    let expected = serde_json::json!({"messages":[{"role":"user","content":parts}]});
    assert!(matchers::model_requests(&[expected.clone()], &[expected.clone()]).is_ok());
    let mut actual = expected.clone();
    let position = usize::from(bytes.first().copied().unwrap_or(0)) % (parts.len() + 1);
    actual["messages"][0]["content"]
        .as_array_mut()
        .unwrap()
        .insert(position, serde_json::json!({"kind":"text","text":""}));
    if !parts.is_empty() {
        assert!(matchers::ordered(&[expected.clone()], &[actual.clone()], "legacy").is_ok());
    }
    assert!(matchers::model_requests(&[expected], &[actual]).is_err());
});
