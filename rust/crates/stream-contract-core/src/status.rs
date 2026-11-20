//! The authoritative Stream error and gRPC status mapping, owned with
//! [`StreamError`] so the conversions are coherent in one crate.
use crate::{StreamCategory, StreamError};
use acyclic_auth::refusal::{Canonical, Refusal};
use tonic::{Code, Status, metadata::MetadataMap};

/// Decodes a member's category and retains everything beyond its canonical status.
#[must_use]
pub fn status(error: &Status) -> StreamError {
    let category = category(error);
    let result = Refusal::capture(
        error.code() as i32,
        error.message(),
        error.details(),
        error.metadata().as_ref(),
        canonical(category.category()),
        None,
    )
    .map_or_else(
        || category.clone(),
        |refusal| category.clone().with_refusal(refusal),
    );
    if std::error::Error::source(error).is_some() {
        result.with_source(error.clone())
    } else {
        result
    }
}

/// Admits a platform callback's owned envelope through the same category mapping.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub fn status_owned(parts: acyclic_auth::refusal::Parts) -> StreamError {
    let category = StreamError::from_grpc_status(parts.code, &parts.message);
    Refusal::capture_owned(parts, canonical(category.category()), None).map_or_else(
        || category.clone(),
        |refusal| category.clone().with_refusal(refusal),
    )
}

/// The category alone, without retaining diagnostics.
pub fn category(error: &Status) -> StreamError {
    StreamError::from_grpc_status(error.code() as i32, error.message())
}

/// The canonical status code and message for a category.
pub fn canonical(category: StreamCategory) -> Canonical<'static> {
    let (code, message) = match category {
        StreamCategory::InvalidResponse => (Code::DataLoss, "invalid_response"),
        StreamCategory::InvalidPath => (Code::InvalidArgument, "invalid_path"),
        StreamCategory::InvalidArgument => (Code::InvalidArgument, "invalid_argument"),
        StreamCategory::LimitExceeded => (Code::InvalidArgument, "limit_exceeded"),
        StreamCategory::NotFound => (Code::NotFound, "stream not found"),
        StreamCategory::AlreadyExists => (Code::AlreadyExists, "stream already exists"),
        StreamCategory::OutOfRange => (Code::OutOfRange, "stream sequence out of range"),
        StreamCategory::HierarchyChanged => (Code::FailedPrecondition, "hierarchy_changed"),
        StreamCategory::AccessDenied => (Code::PermissionDenied, "stream access denied"),
        StreamCategory::Capacity => (Code::ResourceExhausted, "stream capacity exhausted"),
        StreamCategory::IdempotencyMismatch => (Code::FailedPrecondition, "idempotency_mismatch"),
        StreamCategory::PrefixNotRetained => (Code::FailedPrecondition, "prefix_not_retained"),
        StreamCategory::Unavailable => (Code::Unavailable, "stream unavailable"),
        StreamCategory::DeadlineElapsed => (Code::FailedPrecondition, "deadline_elapsed"),
        StreamCategory::Unsupported => (Code::Unimplemented, "unsupported_capability"),
        StreamCategory::RevisionForeign => (Code::FailedPrecondition, "revision_foreign"),
        StreamCategory::Failed => (Code::FailedPrecondition, "failed"),
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
    let refusal = error.member_refusal().map(|refusal| &**refusal);
    let parts = match Refusal::encode(refusal, canonical(error.category())) {
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
