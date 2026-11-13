use crate::StreamError;
use acyclic_auth::refusal::{Canonical, Refusal};
use tonic::{Code, Status, metadata::MetadataMap};

/// Decodes a member's category and retains everything beyond its canonical status.
#[must_use]
pub fn status(error: &Status) -> StreamError {
    let category = category(error);
    Refusal::capture(
        error.code() as i32,
        error.message(),
        error.details(),
        error.metadata().as_ref(),
        canonical(&category),
        None,
    )
    .map_or(category, StreamError::Refused)
}

/// Admits a platform callback's owned envelope through the same category mapping.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn status_owned(parts: acyclic_auth::refusal::Parts) -> StreamError {
    let category = StreamError::from_grpc_status(parts.code, &parts.message);
    Refusal::capture_owned(parts, canonical(&category), None).map_or(category, StreamError::Refused)
}

/// The category alone, without retaining diagnostics.
pub(crate) fn category(error: &Status) -> StreamError {
    StreamError::from_grpc_status(error.code() as i32, error.message())
}

fn canonical(error: &StreamError) -> Canonical<'static> {
    let (code, message) = match error {
        StreamError::Caused(failure) => return canonical(failure.classification()),
        StreamError::InvalidResponse => (Code::DataLoss, "invalid_response"),
        StreamError::InvalidPath => (Code::InvalidArgument, "invalid_path"),
        StreamError::InvalidArgument => (Code::InvalidArgument, "invalid_argument"),
        StreamError::LimitExceeded => (Code::InvalidArgument, "limit_exceeded"),
        StreamError::NotFound => (Code::NotFound, "stream not found"),
        StreamError::AlreadyExists => (Code::AlreadyExists, "stream already exists"),
        StreamError::OutOfRange => (Code::OutOfRange, "stream sequence out of range"),
        StreamError::HierarchyChanged => (Code::FailedPrecondition, "hierarchy_changed"),
        StreamError::AccessDenied => (Code::PermissionDenied, "stream access denied"),
        StreamError::Capacity => (Code::ResourceExhausted, "stream capacity exhausted"),
        StreamError::IdempotencyMismatch => (Code::FailedPrecondition, "idempotency_mismatch"),
        StreamError::PrefixNotRetained => (Code::FailedPrecondition, "prefix_not_retained"),
        StreamError::Unavailable | StreamError::Refused(_) => {
            (Code::Unavailable, "stream unavailable")
        }
        StreamError::DeadlineElapsed => (Code::FailedPrecondition, "deadline_elapsed"),
        StreamError::Unsupported => (Code::Unimplemented, "unsupported_capability"),
        StreamError::RevisionForeign => (Code::FailedPrecondition, "revision_foreign"),
    };
    Canonical {
        code: code as i32,
        message,
        details: b"",
        owned: &[],
        cause_is_embedded: false,
    }
}

/// The authoritative Stream error to gRPC status mapping. Servers must use
/// this conversion rather than reproduce the mapping themselves.
///
/// Reproduces a retained member refusal's code, message, details and product
/// metadata. A failure without retained diagnostics uses its canonical status.
impl From<&StreamError> for Status {
    fn from(error: &StreamError) -> Self {
        error_status(error)
    }
}

/// The owned form of the authoritative [`From<&StreamError>`] conversion.
impl From<StreamError> for Status {
    fn from(error: StreamError) -> Self {
        Self::from(&error)
    }
}

/// Encodes a Stream failure, reproducing its retained member diagnostics.
#[must_use]
fn error_status(error: &StreamError) -> Status {
    if let StreamError::Caused(failure) = error {
        return error_status(failure.classification());
    }
    let refusal = match error {
        StreamError::Refused(refusal) => Some(refusal),
        _ => None,
    };
    let parts = match Refusal::encode(refusal, canonical(error)) {
        Ok(parts) => parts,
        Err(error) => {
            let mut status = Status::internal("refusal attribution encoding failed");
            status.set_source(std::sync::Arc::new(error));
            return status;
        }
    };
    Status::with_details_and_metadata(
        Code::from_i32(parts.code),
        parts.message,
        parts.details,
        MetadataMap::from_headers(parts.metadata),
    )
}
