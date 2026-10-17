use super::*;
use proptest::prelude::*;
use serde_json::json;

fn code(error: Error) -> u8 {
    match error {
        Error::Unauthorized(_) => 1,
        Error::ModelUnavailable { .. } => 2,
        Error::Invalid(_) => 3,
        other => panic!("unexpected class: {other}"),
    }
}

#[test]
fn recognized_stream_errors_match_http_classes_across_providers() {
    for (adapter, event, status) in [
        (
            Adapter::Chat,
            json!({"error":{"code":429,"message":"secret"}}),
            429,
        ),
        (
            Adapter::Responses,
            json!({"type":"error","code":"rate_limit_exceeded","message":"secret"}),
            429,
        ),
        (
            Adapter::Anthropic,
            json!({"type":"error","error":{"type":"authentication_error"}}),
            401,
        ),
        (
            Adapter::Anthropic,
            json!({"type":"error","error":{"type":"permission_error"}}),
            403,
        ),
        (
            Adapter::Anthropic,
            json!({"type":"error","error":{"type":"rate_limit_error"}}),
            429,
        ),
        (
            Adapter::Anthropic,
            json!({"type":"error","error":{"type":"overloaded_error"}}),
            529,
        ),
        (
            Adapter::Anthropic,
            json!({"type":"error","error":{"type":"api_error"}}),
            500,
        ),
        (
            Adapter::Responses,
            json!({"type":"response.failed","response":{"error":{"code":"server_error"}}}),
            500,
        ),
        (
            Adapter::Responses,
            json!({"type":"error","error":{"code":"rate_limit_exceeded"}}),
            429,
        ),
        (
            Adapter::Chat,
            json!({"error":{"code":"invalid_api_key"}}),
            401,
        ),
        (Adapter::Chat, json!({"error":{"type":"server_error"}}), 500),
        (Adapter::Gemini, json!({"error":{"code":403}}), 403),
        (
            Adapter::Gemini,
            json!({"error":{"status":"RESOURCE_EXHAUSTED"}}),
            429,
        ),
        (
            Adapter::Gemini,
            json!({"error":{"status":"UNAVAILABLE"}}),
            503,
        ),
        (
            Adapter::Gemini,
            json!({"error":{"status":"DEADLINE_EXCEEDED"}}),
            504,
        ),
    ] {
        let encoded = event.to_string();
        let mut state = crate::stream::State::new(4096);
        let error = crate::stream::parse(&mut state, adapter, &encoded).unwrap_err();
        assert!(error.to_string().contains(&status.to_string()));
        assert_eq!(code(error), code(crate::http_error(status)));
    }
    for adapter in [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Gemini,
    ] {
        assert_eq!(
            code(classify(
                adapter,
                &json!({"error":{"type":"unknown","code":"secret"}})
            )),
            3
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: untrusted messages and unknown discriminants cannot change a
    // recognized error class or appear in its sanitized diagnostic.
    #[test]
    fn messages_are_irrelevant_to_classification_and_chunking(message in ".{0,128}") {
        for (tag, adapter, event) in [
            (0u8, Adapter::Anthropic, json!({"type":"error","error":{"type":"overloaded_error","message":message}})),
            (1u8, Adapter::Responses, json!({"type":"response.failed","response":{"error":{"code":"server_error","message":message}}})),
            (2u8, Adapter::Chat, json!({"error":{"type":"rate_limit_error","message":message}})),
            (3u8, Adapter::Gemini, json!({"error":{"code":429,"message":message}})),
        ] {
            let expected = if adapter == Adapter::Anthropic {529} else if adapter == Adapter::Responses {500} else {429};
            prop_assert_eq!(classify(adapter, &event).to_string(), crate::http_error(expected).to_string());
            let mut seed = vec![tag];
            seed.extend_from_slice(format!("data: {event}\n\n").as_bytes());
            crate::fuzzing::assert_stream_invariants(&seed);
        }
    }
}
