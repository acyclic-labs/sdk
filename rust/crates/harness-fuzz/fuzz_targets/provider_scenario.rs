#![no_main]
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness-providers/tests/integration/provider_case.rs"]
mod provider_case;
#[path = "../../harness/tests/support/scenario_matchers.rs"]
mod scenario_matchers;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(arguments) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let expected = serde_json::json!({"arguments":{"$json":arguments}});
        let actual = serde_json::json!({"arguments":serde_json::to_string(&arguments).unwrap()});
        assert!(scenario_matchers::subset(&expected, &actual));
        // Ordered-subsequence matching must not accept an extra argument.
        let expected = serde_json::json!({"arguments":{"$json":[arguments]}});
        let actual = serde_json::json!({"arguments":serde_json::to_string(&[arguments, serde_json::Value::Null]).unwrap()});
        assert!(!scenario_matchers::subset(&expected, &actual));
        assert!(!scenario_matchers::subset(
            &expected,
            &serde_json::json!({"arguments":"["})
        ));
    }
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
