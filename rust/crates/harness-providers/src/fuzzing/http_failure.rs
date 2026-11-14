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
    if !matches!(status, 400 | 413) {
        assert_eq!(polls, 0, "status-only refusal read its discarded body");
    } else if failure {
        assert_eq!(error.code(), ErrorCode::Unsupported);
    }
    if let Error::ModelUnavailable { excerpt, .. }
    | Error::Invalid(excerpt)
    | Error::ModelDiagnostic {
        message: excerpt, ..
    } = &error
    {
        assert!(excerpt.len() <= crate::EXCERPT_BYTES);
        assert!(excerpt.chars().all(|character| !character.is_control()));
    }
    match &error {
        Error::ModelUnavailable { excerpt, .. } => assert!(excerpt.is_empty()),
        Error::Invalid(message) | Error::ModelDiagnostic { message, .. }
            if matches!(status, 400 | 413) && !failure =>
        {
            assert_eq!(message, &format!("model HTTP status {status}"));
        }
        _ => {}
    }
    (error.code(), error.to_string())
}

/// Status is a little-endian u16, followed by the unchanged response body.
/// Chunking and JSON string escapes preserve classification; body errors cannot
/// turn an incomplete context refusal into a successful classification.
/// Returns whether valid context-status JSON reached the spelling comparison.
///
/// # Errors
/// Returns original codec errors or malformed canonical-escape errors.
///
/// # Panics
/// If chunking, spelling or body failure changes the original refusal class.
pub fn assert_http_failure_invariants(input: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
    let Some((status, bytes)) = input.split_first_chunk::<2>() else {
        return Ok(false);
    };
    let status = u16::from_le_bytes(*status);
    for failure in [false, true] {
        let whole = decode(status, bytes, bytes.len().max(1), failure);
        for width in [1, 17, 127] {
            assert_eq!(whole, decode(status, bytes, width, failure));
        }
    }
    if matches!(status, 400 | 413)
        && let Ok(value) = crate::json(bytes)
    {
        let canonical = serde_json::to_vec(&value)?;
        let escaped = escaped_strings(&canonical)?;
        assert_eq!(
            crate::json(&escaped)?,
            value,
            "escape mutation changed JSON meaning"
        );
        assert_eq!(
            decode(status, &canonical, canonical.len().max(1), false).0,
            decode(status, &escaped, escaped.len().max(1), false).0,
            "JSON spelling cannot change the status or context class"
        );
        return Ok(true);
    }
    assert!(
        !matches!(status, 400 | 413) || crate::json(bytes).is_err(),
        "production-accepted refusal skipped fuzz spelling comparison"
    );
    Ok(false)
}

// Mutate canonical JSON, preserving existing escapes and all non-ASCII bytes.
fn escaped_strings(bytes: &[u8]) -> super::ProofResult<Vec<u8>> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = Vec::new();
    let mut quoted = false;
    let mut input = bytes.iter().copied();
    while let Some(byte) = input.next() {
        if quoted && byte == b'\\' {
            output.push(byte);
            let escape = input
                .next()
                .ok_or_else(|| crate::invalid("canonical JSON escape is truncated"))?;
            output.push(escape);
            if escape == b'u' {
                for _ in 0..4 {
                    output.push(
                        input.next().ok_or_else(|| {
                            crate::invalid("canonical Unicode escape is truncated")
                        })?,
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
                HEX.get(usize::from(byte >> 4))
                    .copied()
                    .ok_or_else(|| crate::invalid("canonical escape has no high hex digit"))?,
                HEX.get(usize::from(byte & 15))
                    .copied()
                    .ok_or_else(|| crate::invalid("canonical escape has no low hex digit"))?,
            ]);
        } else {
            output.push(byte);
        }
    }
    Ok(output)
}

#[cfg(test)]
#[test]
fn recorded_http_refusals_preserve_class_under_equivalent_escapes_and_chunking() {
    for input in [
        include_bytes!("../../fuzz/corpus/http_failure/llamacpp-context").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/llamacpp-auth").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/proxy-rate").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/gemini-signature").as_slice(),
        include_bytes!("../../fuzz/corpus/http_failure/gemini-signature-literal-extension")
            .as_slice(),
    ] {
        assert_eq!(
            assert_http_failure_invariants(input)
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            Ok(())
        );
    }
    let signature = include_bytes!("../../fuzz/corpus/http_failure/gemini-signature");
    assert_eq!(decode(400, &signature[2..], 1, false).0, ErrorCode::Invalid);
    for marker in [
        "$serde_json::private::RawValue",
        "$serde_json::private::Number",
    ] {
        // Explicit ignored-field mutations of the original refusal body.
        let extension = serde_json::json!({(marker): "null", "nested": []});
        let mutated = std::str::from_utf8(&signature[2..]).unwrap().replacen(
            '{',
            &format!("{{\"extra\":{extension},"),
            1,
        );
        let mut input = signature[..2].to_vec();
        input.extend_from_slice(mutated.as_bytes());
        assert_eq!(
            assert_http_failure_invariants(&input).map_err(|error| format!("{error:?}")),
            Ok(true),
            "accepted literal-key refusal skipped spelling comparison"
        );
    }
    // These are explicit byte/status mutations of the original captured body,
    // not additional provider responses. Exercise valid scalars cut at each
    // boundary, malformed scalars, and a real replacement before a cut scalar.
    for marker in [
        b"\xc2\xa2".as_slice(),
        b"\xe2\x82\xac".as_slice(),
        b"\xf0\x9f\x92\xa9".as_slice(),
        b"\xff".as_slice(),
        b"\xc0\x80".as_slice(),
        b"\xed\xa0\x80".as_slice(),
        b"\xef\xbf\xbd\xe2\x82".as_slice(),
    ] {
        for offset in [1, 17, 251, 252, 253, 254, 255] {
            let mut input = signature.to_vec();
            input[..2].copy_from_slice(&429u16.to_le_bytes());
            input[2 + offset..2 + offset + marker.len()].copy_from_slice(marker);
            assert_eq!(
                assert_http_failure_invariants(&input)
                    .map(|_| ())
                    .map_err(|error| format!("{error:?}")),
                Ok(())
            );
        }
    }

    // Reclassify the captured authentication body as a non-context refusal.
    // The status survives chunking without retaining the captured response text.
    for status in [400u16, 413] {
        let mut input = include_bytes!("../../fuzz/corpus/http_failure/llamacpp-auth").to_vec();
        input[..2].copy_from_slice(&status.to_le_bytes());
        assert_eq!(
            assert_http_failure_invariants(&input)
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            Ok(())
        );
    }

    // This is the same captured context refusal, with only its JSON spelling
    // changed. Native parsing must fund scratch, retire it, and retain refusal
    // of an exhausted or cancelled original invocation.
    let captured = include_bytes!("../../fuzz/corpus/http_failure/llamacpp-context");
    for status in [401u16, 403, 404, 409, 422] {
        let mut input = captured.to_vec();
        input[..2].copy_from_slice(&status.to_le_bytes());
        assert_eq!(
            assert_http_failure_invariants(&input)
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            Ok(())
        );
    }
    let value = crate::json(&captured[2..]).unwrap();
    let mut control = value.clone();
    control["ignored"] = serde_json::Value::String("\u{000b}\\u000b".into());
    let mut control_input = captured[..2].to_vec();
    control_input.extend_from_slice(&serde_json::to_vec(&control).unwrap());
    assert_eq!(
        assert_http_failure_invariants(&control_input)
            .map(|_| ())
            .map_err(|error| format!("{error:?}")),
        Ok(())
    );
    let canonical = serde_json::to_vec(&value).unwrap();
    let escaped = escaped_strings(&canonical).unwrap();
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
        for spelling in [&bytes, &escaped_strings(&bytes).unwrap()] {
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
