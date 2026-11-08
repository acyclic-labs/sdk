use crate::StreamError;
use acyclic_auth::refusal::{Canonical, Refusal};
use tonic::{Code, Status, metadata::MetadataMap};

const UNAVAILABLE: Canonical<'static> = Canonical {
    code: Code::Unavailable as i32,
    message: "stream unavailable",
    details: b"",
    owned: &[],
};

/// Decodes a member's status. Unavailable keeps the member's original status
/// (code, message, details and product metadata) as its refusal.
///
/// Adapters use this shared conversion so status classification and recovery
/// semantics agree with the native and browser clients. Refusal diagnostics
/// never grant retry permission; recovery follows [`StreamError::uncertain`].
#[must_use]
pub fn status(error: &Status) -> StreamError {
    match category(error) {
        StreamError::Unavailable => Refusal::capture(
            error.code() as i32,
            error.message(),
            error.details(),
            error.metadata().as_ref(),
            UNAVAILABLE,
            None,
        )
        .map_or(StreamError::Unavailable, StreamError::Refused),
        category => category,
    }
}

/// Admits a platform callback's owned envelope through the native mapping.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn status_owned(parts: acyclic_auth::refusal::Parts) -> StreamError {
    match category_parts(Code::from_i32(parts.code), &parts.message) {
        StreamError::Unavailable => Refusal::capture_owned(parts, UNAVAILABLE, None)
            .map_or(StreamError::Unavailable, StreamError::Refused),
        category => category,
    }
}

/// The category alone, without retaining diagnostics.
pub(crate) fn category(error: &Status) -> StreamError {
    category_parts(error.code(), error.message())
}

fn category_parts(code: Code, message: &str) -> StreamError {
    match (code, message) {
        (Code::DataLoss, _) => StreamError::InvalidResponse,
        (Code::DeadlineExceeded, _) => StreamError::DeadlineElapsed,
        (Code::InvalidArgument, "invalid_path") => StreamError::InvalidPath,
        (Code::InvalidArgument, "limit_exceeded") => StreamError::LimitExceeded,
        (Code::InvalidArgument, _) => StreamError::InvalidArgument,
        (Code::NotFound, _) => StreamError::NotFound,
        (Code::AlreadyExists, _) => StreamError::AlreadyExists,
        (Code::OutOfRange, _) => StreamError::OutOfRange,
        (Code::PermissionDenied | Code::Unauthenticated, _) => StreamError::AccessDenied,
        (Code::ResourceExhausted, _) => StreamError::Capacity,
        (Code::FailedPrecondition, "hierarchy_changed") => StreamError::HierarchyChanged,
        (Code::FailedPrecondition, "idempotency_mismatch") => StreamError::IdempotencyMismatch,
        (Code::FailedPrecondition, "prefix_not_retained") => StreamError::PrefixNotRetained,
        (Code::FailedPrecondition, "deadline_elapsed") => StreamError::DeadlineElapsed,
        (Code::FailedPrecondition, "revision_foreign") => StreamError::RevisionForeign,
        // Whatever the server's message, an unimplemented operation is a refusal.
        (Code::Unimplemented, _) => StreamError::Unsupported,
        _ => StreamError::Unavailable,
    }
}

/// Encodes a Stream failure, reproducing a retained member status verbatim.
/// A failure without retained diagnostics uses its canonical status.
#[must_use]
pub fn error_status(error: &StreamError) -> Status {
    match error {
        StreamError::InvalidResponse => Status::data_loss("invalid_response"),
        StreamError::InvalidPath => Status::invalid_argument("invalid_path"),
        StreamError::InvalidArgument => Status::invalid_argument("invalid_argument"),
        StreamError::LimitExceeded => Status::invalid_argument("limit_exceeded"),
        StreamError::NotFound => Status::not_found(error.to_string()),
        StreamError::AlreadyExists => Status::already_exists(error.to_string()),
        StreamError::OutOfRange => Status::out_of_range(error.to_string()),
        StreamError::HierarchyChanged => Status::failed_precondition("hierarchy_changed"),
        StreamError::AccessDenied => Status::permission_denied(error.to_string()),
        StreamError::Capacity => Status::resource_exhausted(error.to_string()),
        StreamError::IdempotencyMismatch => Status::failed_precondition("idempotency_mismatch"),
        StreamError::PrefixNotRetained => Status::failed_precondition("prefix_not_retained"),
        StreamError::Unavailable => Status::unavailable(UNAVAILABLE.message),
        StreamError::Refused(refusal) => {
            let parts = Refusal::encode(Some(refusal), UNAVAILABLE);
            Status::with_details_and_metadata(
                Code::from_i32(parts.code),
                parts.message,
                parts.details,
                MetadataMap::from_headers(parts.metadata),
            )
        }
        StreamError::DeadlineElapsed => Status::failed_precondition("deadline_elapsed"),
        StreamError::Unsupported => Status::unimplemented("unsupported_capability"),
        StreamError::RevisionForeign => Status::failed_precondition("revision_foreign"),
    }
}
