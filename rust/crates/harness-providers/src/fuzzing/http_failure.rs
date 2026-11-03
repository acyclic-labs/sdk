//! Finite bodies drive the same refusal decoder as HTTP dispatch.
use acyclic_harness_core::{Error, wire::ErrorCode};
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn decode(status: u16, bytes: &[u8], width: usize, failure: bool) -> (ErrorCode, String) {
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&polls);
    let body = futures::stream::unfold(
        (Bytes::copy_from_slice(bytes), false),
        move |(mut remaining, failed)| {
            observed.fetch_add(1, Ordering::Relaxed);
            std::future::ready(if !remaining.is_empty() {
                let chunk = remaining.split_to(width.min(remaining.len()));
                Some((Ok(chunk), (remaining, failed)))
            } else if failure && !failed {
                Some((
                    Err(Error::Unsupported("HTTP body producer stopped".into())),
                    (remaining, true),
                ))
            } else {
                None
            })
        },
    );
    let response = crate::HttpResponse {
        status,
        body: Box::pin(body),
        request_id: None,
        retry_after: None,
    };
    let error =
        futures::executor::block_on(crate::http_failure(response, bytes.len().max(1), None));
    let polls = polls.load(Ordering::Relaxed);
    assert!(polls <= bytes.len().div_ceil(width) + 1);
    if !matches!(status, 400 | 408 | 413 | 429 | 500..=599) {
        assert_eq!(polls, 0, "status-only refusal read its discarded body");
    } else if failure && (matches!(status, 400 | 413) || bytes.len() < crate::EXCERPT_BYTES) {
        assert_eq!(error.code(), ErrorCode::Unsupported);
    }
    if let Error::ModelUnavailable { excerpt, .. } | Error::Invalid(excerpt) = &error {
        assert!(excerpt.len() <= crate::EXCERPT_BYTES);
        assert!(excerpt.chars().all(|character| !character.is_control()));
    }
    let diagnostic = match &error {
        Error::Invalid(message) if matches!(status, 400 | 413) && !failure => {
            Some((message, format!("model HTTP status {status}: ")))
        }
        Error::ModelUnavailable { excerpt, .. } => Some((excerpt, String::new())),
        _ => None,
    };
    if let Some((actual, prefix)) = diagnostic
        && let Ok(text) = std::str::from_utf8(&bytes[..bytes.len().min(crate::EXCERPT_BYTES)])
    {
        // An independent raw-input expectation catches loss shared by every
        // chunk width. Invalid UTF-8 still has the bounds/chunking checks above.
        let expected: String = prefix
            .chars()
            .chain(text.chars())
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .scan(0usize, |length, character| {
                *length += character.len_utf8();
                (*length <= crate::EXCERPT_BYTES).then_some(character)
            })
            .collect();
        assert_eq!(
            actual,
            expected.trim(),
            "HTTP refusal lost original diagnostic"
        );
    }
    (error.code(), error.to_string())
}

/// Status is a little-endian u16, followed by the unchanged response body.
/// Chunking and JSON string escapes preserve classification; body errors cannot
/// turn an incomplete context refusal into a successful classification.
pub fn assert_http_failure_invariants(input: &[u8]) {
    let Some((status, bytes)) = input.split_first_chunk::<2>() else {
        return;
    };
    let status = u16::from_le_bytes(*status);
    for failure in [false, true] {
        let whole = decode(status, bytes, bytes.len().max(1), failure);
        for width in [1, 17, 127] {
            assert_eq!(whole, decode(status, bytes, width, failure));
        }
    }
    if matches!(status, 400 | 413)
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes)
    {
        let canonical = serde_json::to_vec(&value).expect("JSON value encodes");
        let escaped = escaped_strings(&canonical);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&escaped).unwrap(),
            value,
            "escape mutation changed JSON meaning"
        );
        assert_eq!(
            decode(status, &canonical, canonical.len().max(1), false).0,
            decode(status, &escaped, escaped.len().max(1), false).0,
            "JSON spelling may change the original excerpt, never its class"
        );
    }
}

// Mutate canonical JSON, preserving existing escapes and all non-ASCII bytes.
fn escaped_strings(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = Vec::new();
    let mut quoted = false;
    let mut input = bytes.iter().copied();
    while let Some(byte) = input.next() {
        if quoted && byte == b'\\' {
            output.push(byte);
            let escape = input.next().expect("canonical escape has its second byte");
            output.push(escape);
            if escape == b'u' {
                for _ in 0..4 {
                    output.push(
                        input
                            .next()
                            .expect("canonical Unicode escape has four digits"),
                    );
                }
            }
        } else if byte == b'"' {
            quoted = !quoted;
            output.push(byte);
        } else if quoted && byte.is_ascii_alphabetic() {
            output.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 15)],
            ]);
        } else {
            output.push(byte);
        }
    }
    output
}

#[cfg(test)]
#[test]
fn recorded_http_refusals_preserve_class_under_equivalent_escapes_and_chunking() {
    for input in [
        include_bytes!("../../fuzz/corpus/http_failure/llamacpp-context").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/llamacpp-auth").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/proxy-rate").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/gemini-signature").as_slice(),
    ] {
        assert_http_failure_invariants(input);
    }
    let signature = include_bytes!("../../fuzz/corpus/http_failure/gemini-signature");
    assert_eq!(decode(400, &signature[2..], 1, false).0, ErrorCode::Invalid);

    // This is the same captured context refusal, with only its JSON spelling
    // changed. Native parsing must fund scratch, retire it, and retain refusal
    // of an exhausted or cancelled original invocation.
    let captured = include_bytes!("../../fuzz/corpus/http_failure/llamacpp-context");
    for status in [401u16, 403, 404, 409, 422] {
        let mut input = captured.to_vec();
        input[..2].copy_from_slice(&status.to_le_bytes());
        assert_http_failure_invariants(&input);
    }
    let value: serde_json::Value = serde_json::from_slice(&captured[2..]).unwrap();
    let mut control = value.clone();
    control["ignored"] = serde_json::Value::String("\u{000b}\\u000b".into());
    let mut control_input = captured[..2].to_vec();
    control_input.extend_from_slice(&serde_json::to_vec(&control).unwrap());
    assert_http_failure_invariants(&control_input);
    let canonical = serde_json::to_vec(&value).unwrap();
    let escaped = escaped_strings(&canonical);
    // Move the captured refusal through the supported envelopes; a selected
    // non-object error still takes precedence over the root's discriminants.
    for (mutation, expected) in [
        (
            serde_json::json!({"response": {"error": value["error"]}}),
            true,
        ),
        (
            serde_json::json!({"error": null, "response": {"error": value["error"]}}),
            true,
        ),
        (
            serde_json::json!({"error": false, "response": {"error": value["error"]}}),
            false,
        ),
        (
            serde_json::json!({"error": false, "code": value["error"]["code"]}),
            false,
        ),
    ] {
        let bytes = serde_json::to_vec(&mutation).unwrap();
        for spelling in [&bytes, &escaped_strings(&bytes)] {
            assert_eq!(
                crate::stream::context_overflow_bytes(spelling, None).unwrap(),
                expected
            );
        }
    }
    for bytes in [&canonical, &escaped] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(100_000, 4_000_000),
            &cancellation,
        )
        .unwrap();
        let baseline = work.live_allocation_bytes();
        assert!(crate::stream::context_overflow_bytes(bytes, Some(&work)).unwrap());
        assert_eq!(work.live_allocation_bytes(), baseline);
        cancellation.cancel();
        assert!(crate::stream::context_overflow_bytes(bytes, Some(&work)).is_err());
        assert_eq!(work.live_allocation_bytes(), baseline);

        let denied = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(
                100_000,
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ),
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap();
        let baseline = denied.live_allocation_bytes();
        assert!(crate::stream::context_overflow_bytes(bytes, Some(&denied)).is_err());
        assert_eq!(denied.live_allocation_bytes(), baseline);
    }
}
