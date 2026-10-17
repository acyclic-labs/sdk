use crate::StreamError;
use tonic::{Code, Status};

pub(crate) fn status(error: &Status) -> StreamError {
    match (error.code(), error.message()) {
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
        // Whatever the server's message, an unimplemented operation is a refusal.
        (Code::Unimplemented, _) => StreamError::Unsupported,
        _ => StreamError::Unavailable,
    }
}
