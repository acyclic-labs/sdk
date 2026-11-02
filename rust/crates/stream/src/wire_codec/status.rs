use crate::StreamError;
use tonic::{Code, Status};

/// Projects the original gRPC status through the canonical Stream failure contract.
pub fn error_from_status(error: &Status) -> StreamError {
    match (error.code(), error.message()) {
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
        _ => StreamError::Unavailable {
            reason: acyclic_auth::refusal::RefusalReason::from_header(
                error
                    .metadata()
                    .get(acyclic_auth::refusal::REASON_HEADER)
                    .map(|value| value.as_bytes()),
            ),
        },
    }
}

/// Encodes a Stream failure with the canonical gRPC code and semantic message.
/// Serving adapters retain the same original refusal through this encoder.
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
        StreamError::Unavailable { reason } => {
            let mut status = Status::unavailable("unavailable");
            if let Some(value) = reason.as_bytes() {
                match tonic::metadata::MetadataValue::try_from(value) {
                    Ok(value) => {
                        status
                            .metadata_mut()
                            .insert(acyclic_auth::refusal::REASON_HEADER, value);
                    }
                    // A diagnostic cannot make an undecided operation definite.
                    // These bytes cannot cross this header's encoding boundary;
                    // retain the original cause for the serving caller.
                    Err(_) => {
                        status.set_source(std::sync::Arc::new(error.clone()));
                    }
                }
            }
            status
        }
        StreamError::DeadlineElapsed => Status::failed_precondition("deadline_elapsed"),
        StreamError::Unsupported => Status::unimplemented("unsupported_capability"),
        StreamError::RevisionForeign => Status::failed_precondition("revision_foreign"),
    }
}
