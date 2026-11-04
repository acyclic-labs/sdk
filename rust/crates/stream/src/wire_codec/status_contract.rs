//! Contract oracle for the actual tonic trailer parser and Stream decoder.
use crate::{StreamError, wire_codec};
use tonic::{
    Code, Status,
    codegen::http::{HeaderMap, HeaderValue},
};

// A declarative contract, independent of both shipping conversion functions.
// Message-sensitive classes include their fallback, so a symmetric codec
// mutation cannot pass solely by agreeing with its own encoder.
const CONTRACT: &[(Code, &str, StreamError)] = &[
    (
        Code::DataLoss,
        "invalid_response",
        StreamError::InvalidResponse,
    ),
    (Code::DataLoss, "", StreamError::InvalidResponse),
    (Code::DeadlineExceeded, "", StreamError::DeadlineElapsed),
    (
        Code::InvalidArgument,
        "invalid_path",
        StreamError::InvalidPath,
    ),
    (
        Code::InvalidArgument,
        "limit_exceeded",
        StreamError::LimitExceeded,
    ),
    (Code::InvalidArgument, "", StreamError::InvalidArgument),
    (Code::NotFound, "", StreamError::NotFound),
    (Code::AlreadyExists, "", StreamError::AlreadyExists),
    (Code::OutOfRange, "", StreamError::OutOfRange),
    (Code::PermissionDenied, "", StreamError::AccessDenied),
    (Code::Unauthenticated, "", StreamError::AccessDenied),
    (Code::ResourceExhausted, "", StreamError::Capacity),
    (
        Code::FailedPrecondition,
        "hierarchy_changed",
        StreamError::HierarchyChanged,
    ),
    (
        Code::FailedPrecondition,
        "idempotency_mismatch",
        StreamError::IdempotencyMismatch,
    ),
    (
        Code::FailedPrecondition,
        "prefix_not_retained",
        StreamError::PrefixNotRetained,
    ),
    (
        Code::FailedPrecondition,
        "deadline_elapsed",
        StreamError::DeadlineElapsed,
    ),
    (
        Code::FailedPrecondition,
        "revision_foreign",
        StreamError::RevisionForeign,
    ),
    (Code::Unimplemented, "", StreamError::Unsupported),
    (Code::Ok, "", StreamError::Unavailable),
    (Code::Cancelled, "", StreamError::Unavailable),
    (Code::Unknown, "", StreamError::Unavailable),
    (Code::FailedPrecondition, "", StreamError::Unavailable),
    (Code::Aborted, "", StreamError::Unavailable),
    (Code::Internal, "", StreamError::Unavailable),
    (Code::Unavailable, "", StreamError::Unavailable),
];

fn expected(wire: &Status) -> &StreamError {
    CONTRACT
        .iter()
        .find(|(code, message, _)| *code == wire.code() && *message == wire.message())
        .or_else(|| {
            CONTRACT
                .iter()
                .find(|(code, message, _)| *code == wire.code() && message.is_empty())
        })
        .map(|(_, _, error)| error)
        .expect("every tonic code has a contract fallback")
}

fn check_decoded(wire: &Status, actual: &StreamError) -> Result<(), &'static str> {
    let expected = expected(wire);
    if actual.code() != expected.code() {
        return Err("status contract classification");
    }
    if actual.uncertain() != expected.uncertain() {
        return Err("status mutation uncertainty");
    }
    if *expected == StreamError::Unavailable {
        let encoded = wire_codec::error_status(actual);
        if encoded.code() != wire.code()
            || encoded.message() != wire.message()
            || encoded.details() != wire.details()
            || !encoded
                .metadata()
                .as_ref()
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("acyclic-"))
                .eq(wire
                    .metadata()
                    .as_ref()
                    .iter()
                    .filter(|(name, _)| name.as_str().starts_with("acyclic-")))
        {
            return Err("original refusal diagnostics");
        }
    }
    Ok(())
}

fn check_with(input: &[u8], decode: impl Fn(&Status) -> StreamError) -> Result<(), &'static str> {
    for (code, message, error) in CONTRACT {
        let wire = Status::new(*code, *message);
        check_decoded(&wire, &decode(&wire))?;
        if decode(&wire_codec::error_status(error)) != *error {
            return Err("canonical status round trip");
        }
    }
    // Exercise the real HTTP/2 trailer parser, including malformed percent
    // escapes and non-UTF8 diagnostics. Invalid HTTP field bytes are refused
    // by the same HTTP HeaderValue boundary used by tonic.
    let Ok(message) = HeaderValue::from_bytes(input) else {
        return Ok(());
    };
    for code in 0..=16 {
        let original = Status::with_details(Code::from_i32(code), "", input.to_vec().into());
        let mut headers = HeaderMap::new();
        original
            .add_header(&mut headers)
            .map_err(|_| "tonic writer refused status")?;
        headers.insert("grpc-message", message.clone());
        headers.append("acyclic-unserved-reason", message.clone());
        let wire = Status::from_header_map(&headers).ok_or("tonic lost original status")?;
        if wire.details() != input {
            return Err("tonic changed original status details");
        }
        let actual = decode(&wire);
        check_decoded(&wire, &actual)?;
        let encoded = wire_codec::error_status(&actual);
        if decode(&encoded) != actual {
            return Err("canonical status round trip");
        }
    }
    Ok(())
}

/// Arbitrary `grpc-message` field bytes through the shipping trailer decoder.
/// This establishes SDK codec semantics, not a captured remote service response.
pub fn check(input: &[u8]) -> Result<(), &'static str> {
    check_with(input, wire_codec::status)
}

/// Capture corpus fields from the original SDK status writer and tonic encoder.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, &'static str> {
    crate::StreamError::ALL
        .iter()
        .map(|error| {
            let mut trailers = HeaderMap::new();
            wire_codec::error_status(error)
                .add_header(&mut trailers)
                .map_err(|_| "tonic writer refused canonical status")?;
            Ok((
                error.code().to_owned(),
                trailers
                    .get("grpc-message")
                    .map(|value| value.as_bytes().to_vec())
                    .unwrap_or_default(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest::proptest! {
        #[test]
        fn status_contract_and_planted_decoder_faults(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256)) {
            proptest::prop_assert_eq!(check(&input), Ok(()));
            for (_, bytes) in seeds().expect("original status writer") {
                proptest::prop_assert_eq!(check(&bytes), Ok(()));
            }
            // Replace one decoder branch while keeping the same oracle. These
            // are planted decoder faults, never a production execution mode.
            for code in [Code::Unimplemented, Code::DeadlineExceeded, Code::InvalidArgument] {
                let broken = |wire: &Status| if wire.code() == code {
                    StreamError::Unavailable
                } else { wire_codec::status(wire) };
                proptest::prop_assert!(check_with(&input, broken).is_err());
            }
            let stripped = |wire: &Status| match wire_codec::status(wire) {
                StreamError::Refused(_) => StreamError::Unavailable,
                other => other,
            };
            proptest::prop_assert_eq!(check_with(&input, stripped), Err("original refusal diagnostics"));
            // The removed round-trip-only property accepts this symmetric
            // path/limit swap. The independent contract must reject it.
            let swap = |error| match error {
                StreamError::InvalidPath => StreamError::LimitExceeded,
                StreamError::LimitExceeded => StreamError::InvalidPath,
                other => other,
            };
            let symmetric = |wire: &Status| swap(wire_codec::status(wire));
            for error in StreamError::ALL {
                let encoded = wire_codec::error_status(&swap(error.clone()));
                proptest::prop_assert_eq!(symmetric(&encoded), error);
            }
            proptest::prop_assert!(check_with(&input, symmetric).is_err());
        }
    }
}
