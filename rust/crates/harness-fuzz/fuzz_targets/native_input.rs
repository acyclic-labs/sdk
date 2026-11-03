#![no_main]

// The installed binary and this target share admission before workspace lookup.
#[path = "../../../../plugin/src/input.rs"]
mod input;

fn check(bytes: &[u8]) {
    let admitted = input::read_json(bytes);
    if bytes.len() > 65_536 {
        assert!(
            admitted.is_err(),
            "native input exceeds its admitted byte bound"
        );
    } else {
        match (admitted, serde_json::from_slice::<serde_json::Value>(bytes)) {
            (Ok(actual), Ok(original)) => assert_eq!(actual, original),
            (Err(_), Err(_)) => {}
            _ => panic!("native input admission changed its original JSON"),
        }
    }
    if let Ok(text) = std::str::from_utf8(bytes)
        && let Ok(task) = input::parse_task(text)
    {
        assert_eq!(task.as_str(), text, "selected task identity changed");
    }
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    check(bytes);
    static BOUNDARIES: std::sync::Once = std::sync::Once::new();
    BOUNDARIES.call_once(|| {
        // Valid JSON on both sides of the bound distinguishes admission from
        // incidental parse refusal. The original spaces-only example did not.
        for length in [65_535, 65_536, 65_537] {
            let mut encoded = b"null".to_vec();
            encoded.resize(length, b' ');
            check(&encoded);
        }
        check(b"not-json");
    });
    let agent = acyclic_harness::agent::AgentPath::root(
        &acyclic_harness::agent::AgentId::from_bytes([13; 16]),
    );
    assert_eq!(input::parse_task(agent.as_str()).unwrap(), agent);
    let encoded = serde_json::to_vec(&serde_json::json!({
        "input": String::from_utf8_lossy(bytes),
    }))
    .unwrap();
    if encoded.len() <= 65_536 {
        input::read_json(encoded.as_slice()).expect("an in-bound JSON input must be admitted");
    }
});
