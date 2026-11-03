//! One differential decoder oracle shared by properties and byte fuzzing.
use super::*;

#[path = "page_codec_reference.rs"]
mod reference;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Failure {
    Decode(CanonicalDecodeError),
    Denied,
}

impl From<CanonicalDecodeError> for Failure {
    fn from(error: CanonicalDecodeError) -> Self {
        Self::Decode(error)
    }
}

fn event(step: ReadAllocationStep) -> (u8, u64, u64) {
    match step {
        ReadAllocationStep::Allocate { bytes, operations } => (0, bytes, operations),
        ReadAllocationStep::ReserveFailed(_) => (1, 0, 0),
        ReadAllocationStep::Capacity {
            admitted_bytes,
            actual_bytes,
        } => (2, admitted_bytes, actual_bytes),
        ReadAllocationStep::CheckCopy(bytes) => (9, bytes, 0),
        ReadAllocationStep::BeforeCopy(bytes) => (3, bytes, 0),
        ReadAllocationStep::Copied(bytes) => (4, bytes, 0),
        ReadAllocationStep::BeforeHash(bytes) => (5, bytes, 0),
        ReadAllocationStep::Hashed(bytes) => (6, bytes, 0),
        ReadAllocationStep::BeforeItem => (7, 0, 0),
        ReadAllocationStep::Item => (8, 0, 0),
    }
}

pub(super) fn observed<const ORDER: bool, const LEGACY: bool>(
    bytes: &[u8],
    limits: DecodeLimits,
    deny_at: usize,
) -> (Result<FileTablePage, Failure>, Vec<(u8, u64, u64)>) {
    let mut events = Vec::new();
    let mut observe = |step| {
        events.push(event(step));
        if events.len() > deny_at {
            Err(Failure::Denied)
        } else {
            Ok(())
        }
    };
    let result = if LEGACY {
        reference::decode(bytes, limits, &mut observe)
    } else if ORDER {
        decode_file_table_page(bytes, limits, &mut observe)
    } else {
        decode_page::<false, Failure>(bytes, limits, &mut observe)
    };
    (result, events)
}

fn compare_observed<const ORDER: bool>(
    bytes: &[u8],
    limits: DecodeLimits,
    denied: usize,
    complete: &Result<FileTablePage, Failure>,
    tape: &[(u8, u64, u64)],
) -> Result<(), String> {
    let (actual, events) = observed::<ORDER, false>(bytes, limits, denied);
    let expected = if denied < tape.len() {
        Err(Failure::Denied)
    } else {
        complete.clone()
    };
    if actual != expected {
        return Err(format!(
            "decoder result differs: {actual:?} != {expected:?}"
        ));
    }
    let expected_events = &tape[..denied.saturating_add(1).min(tape.len())];
    if events != expected_events {
        return Err(format!(
            "decoder admission trace differs: {events:?} != {expected_events:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn compare<const ORDER: bool>(bytes: &[u8], limits: DecodeLimits) -> Result<(), String> {
    let (complete, tape) = observed::<false, true>(bytes, limits, usize::MAX);
    for denied in 0..=tape.len() {
        compare_observed::<ORDER>(bytes, limits, denied, &complete, &tape)?;
    }
    Ok(())
}

/// Compare success, failure and one input-selected refusal with the independent parser.
/// Fuzzing selects one prefix rather than performing quadratic work per input.
pub(crate) fn check<const ORDER: bool>(bytes: &[u8], limits: DecodeLimits, prefix: usize) {
    let _ = check_record(bytes);
    let (complete, tape) = observed::<false, true>(bytes, limits, usize::MAX);
    compare_observed::<ORDER>(bytes, limits, usize::MAX, &complete, &tape).unwrap();
    let denied = prefix % (tape.len() + 1);
    compare_observed::<ORDER>(bytes, limits, denied, &complete, &tape).unwrap();

    // Shape decoding checks framing, not record semantics or ordering. Exercise
    // arbitrary bytes, but require agreement only for fully validated pages.
    let shape = file_table_page_decode_shape(bytes, limits);
    let Ok(page) = complete else {
        return;
    };
    let (kind, items) = match &page {
        FileTablePage::Leaf(records) => {
            for record in records {
                let mut encoded = encode_file_record(
                    *record,
                    |_| Ok::<_, crate::FsError>(()),
                    |_| CanonicalDecodeError::AllocationFailed.into(),
                )
                .unwrap();
                assert_eq!(
                    check_record(&encoded),
                    Ok(*record),
                    "standalone record differs from the fully validated page"
                );
                let index = prefix % encoded.len();
                encoded[index] ^= 1;
                let _ = check_record(&encoded);
                encoded[index] ^= 1;
                assert!(
                    check_record(&encoded[..index]).is_err(),
                    "standalone record admitted a truncated frame"
                );
                encoded.push(0);
                assert!(
                    check_record(&encoded).is_err(),
                    "standalone record admitted trailing bytes"
                );
            }
            (DecodedPageKind::Leaf, records.len())
        }
        FileTablePage::Internal(children) => (DecodedPageKind::Internal, children.len()),
    };
    assert_eq!(
        shape.unwrap(),
        DecodedPageShape {
            kind,
            items,
            nested_bytes: 0,
        },
        "shape differs from the fully validated page"
    );
    assert!(
        file_table_page_decode_shape(&bytes[..prefix % bytes.len()], limits).is_err(),
        "shape admitted a truncated frame"
    );
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert!(
        file_table_page_decode_shape(&trailing, limits).is_err(),
        "shape admitted trailing bytes"
    );
}

fn check_record(bytes: &[u8]) -> Result<FileRecord, CanonicalDecodeError> {
    let result = decode_file_record(bytes);
    assert_eq!(
        result,
        reference::standalone_record(bytes),
        "standalone record result differs from the independent parser"
    );
    result
}
