use crate::StreamError;
use tonic::{Code, Status};

pub(crate) fn status(error: &Status) -> StreamError {
    match (error.code(), error.message()) {
        (Code::DataLoss, _) => StreamError::InvalidResponse,
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

pub(crate) fn error_status(error: &StreamError) -> Status {
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
        StreamError::Unavailable => Status::unavailable(error.to_string()),
        StreamError::DeadlineElapsed => Status::failed_precondition("deadline_elapsed"),
        StreamError::Unsupported => Status::unimplemented("unsupported_capability"),
        StreamError::RevisionForeign => Status::failed_precondition("revision_foreign"),
    }
}
