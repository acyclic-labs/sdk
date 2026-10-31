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
        retry_after: None,
    };
    let error =
        futures::executor::block_on(crate::http_failure(response, bytes.len().max(1), None));
    let polls = polls.load(Ordering::Relaxed);
    assert!(polls <= bytes.len().div_ceil(width) + 1);
    if matches!(status, 401 | 403) {
        assert_eq!(polls, 0, "authentication refusal read its body");
    } else if failure && (matches!(status, 400 | 413) || bytes.len() < crate::EXCERPT_BYTES) {
        assert_eq!(error.code(), ErrorCode::Unsupported);
    }
    if let Error::ModelUnavailable { excerpt, .. } = &error {
        assert!(excerpt.len() <= crate::EXCERPT_BYTES);
        assert!(excerpt.chars().all(|character| !character.is_control()));
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
            decode(status, &canonical, canonical.len().max(1), false),
            decode(status, &escaped, escaped.len().max(1), false)
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
            output.push(input.next().expect("canonical escape has its second byte"));
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
    ] {
        assert_http_failure_invariants(input);
    }

    // This is the same captured context refusal, with only its JSON spelling
    // changed. Native parsing must fund scratch, retire it, and retain refusal
    // of an exhausted or cancelled original invocation.
    let captured = include_bytes!("../../fuzz/corpus/http_failure/llamacpp-context");
    let value: serde_json::Value = serde_json::from_slice(&captured[2..]).unwrap();
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
