#![no_main]

// The same decoder is called by the stdin owner before conversation dispatch.
#[path = "../../harness/src/bin/graphcoder/headless/input.rs"]
mod input;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    if let Ok(request) = input::parse(text) {
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        let fields = value.as_object().expect("a request must be an object");
        let keys: &[&str] = match request {
            input::Request::Prompt { prompt, id } => {
                assert_eq!(fields["prompt"].as_str(), Some(prompt.as_str()));
                assert_eq!(fields.get("id").and_then(|id| id.as_str()), id.as_deref());
                &["prompt", "id"]
            }
            input::Request::Approval {
                approve,
                binding,
                approved,
            } => {
                assert_eq!(fields["approve"].as_str(), Some(approve.as_str()));
                let original: acyclic_harness::interaction::ApprovalBinding =
                    serde_json::from_value(fields["binding"].clone()).unwrap();
                assert_eq!(binding, original);
                assert_eq!(fields["approved"].as_bool(), Some(approved));
                &["approve", "binding", "approved"]
            }
            input::Request::Cancel { cancel } => {
                assert_eq!(fields["cancel"].as_bool(), Some(cancel));
                &["cancel"]
            }
        };
        assert!(
            fields.keys().all(|key| keys.contains(&key.as_str())),
            "a request must select exactly one action without unknown fields"
        );
    }
    // Guard against a decoder that refuses everything, preserving arbitrary
    // prompt contents through the actual input decoder and the JSON writer.
    let encoded = serde_json::json!({"prompt": text, "id": "oracle"}).to_string();
    match input::parse(&encoded).expect("a prompt must be admitted") {
        input::Request::Prompt { prompt, id } => {
            assert_eq!(prompt, text);
            assert_eq!(id.as_deref(), Some("oracle"));
        }
        _ => panic!("a prompt changed its selected action"),
    }
    // The real binding serializer supplies its wire representation; there is
    // no parallel approval schema in this target.
    let binding = acyclic_harness::interaction::ApprovalBinding {
        operation_id: acyclic_harness::OperationId::from_bytes([19; 16]),
        action_digest: *blake3::hash(bytes).as_bytes(),
    };
    for approved in [false, true] {
        let encoded = serde_json::json!({
            "approve": "oracle", "binding": binding, "approved": approved,
        })
        .to_string();
        match input::parse(&encoded).expect("an approval must be decoded") {
            input::Request::Approval {
                approve,
                binding: decoded,
                approved: decision,
            } => {
                assert_eq!(approve.as_str(), "oracle");
                assert_eq!(decoded, binding);
                assert_eq!(decision, approved);
            }
            _ => panic!("an approval changed its selected action"),
        }
    }
    for cancel in [false, true] {
        let encoded = serde_json::json!({"cancel": cancel}).to_string();
        match input::parse(&encoded).expect("a cancel must be decoded") {
            input::Request::Cancel { cancel: decoded } => assert_eq!(decoded, cancel),
            _ => panic!("a cancel changed its selected action"),
        }
    }
});
