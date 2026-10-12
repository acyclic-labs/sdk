use super::*;

pub(super) fn retryable(error: &Status) -> bool {
    if matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded) {
        return true;
    }
    if error.code() == Code::Unknown && std::error::Error::source(error).is_none() {
        return true;
    }
    if !matches!(error.code(), Code::Unknown | Code::Cancelled) {
        return false;
    }
    let mut cause = std::error::Error::source(error);
    while let Some(current) = cause {
        // Hyper cancels queued dispatch when its connection driver disappears. This is
        // transport uncertainty, unlike a peer's application-level Cancelled status.
        if error.code() == Code::Cancelled
            && current
                .downcast_ref::<hyper::Error>()
                .is_some_and(hyper::Error::is_canceled)
        {
            return true;
        }
        if error.code() == Code::Cancelled
            && current.downcast_ref::<h2::Error>().is_some_and(|error| {
                error.is_remote() && error.reason() == Some(h2::Reason::CANCEL)
            })
        {
            return true;
        }
        let io = current.downcast_ref::<std::io::Error>().or_else(|| {
            current
                .downcast_ref::<h2::Error>()
                .and_then(h2::Error::get_io)
        });
        if error.code() == Code::Unknown
            && io.is_some_and(|error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::TimedOut
                )
            })
        {
            return true;
        }
        cause = current.source();
    }
    false
}

pub(super) fn bounded_ca_certificate(value: &[u8]) -> Result<&[u8], ConnectError> {
    if value.is_empty() || value.len() > MAX_CA_CERTIFICATE_BYTES {
        Err(ConnectError::InvalidCaCertificate)
    } else {
        Ok(value)
    }
}

pub(super) fn bind_observation(
    requested: &IdempotencyKey,
    observation: Option<wire::IdempotencyObservation>,
) -> Result<Option<IdempotencyObservation>, StreamError> {
    let observation = observation.map(observation_from_wire).transpose()?;
    if observation
        .as_ref()
        .is_some_and(|observation| &observation.idempotency_key != requested)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(observation)
}

pub(super) fn error_status(error: &StreamError) -> Status {
    match error {
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
    }
}

pub(super) fn status(error: &tonic::Status) -> StreamError {
    match error.code() {
        Code::InvalidArgument if error.message() == "invalid_path" => StreamError::InvalidPath,
        Code::InvalidArgument if error.message() == "limit_exceeded" => StreamError::LimitExceeded,
        Code::InvalidArgument => StreamError::InvalidArgument,
        Code::NotFound => StreamError::NotFound,
        Code::AlreadyExists => StreamError::AlreadyExists,
        Code::OutOfRange => StreamError::OutOfRange,
        Code::FailedPrecondition if error.message() == "hierarchy_changed" => {
            StreamError::HierarchyChanged
        }
        Code::PermissionDenied | Code::Unauthenticated => StreamError::AccessDenied,
        Code::ResourceExhausted => StreamError::Capacity,
        Code::FailedPrecondition if error.message() == "idempotency_mismatch" => {
            StreamError::IdempotencyMismatch
        }
        Code::FailedPrecondition if error.message() == "prefix_not_retained" => {
            StreamError::PrefixNotRetained
        }
        Code::FailedPrecondition if error.message() == "deadline_elapsed" => {
            StreamError::DeadlineElapsed
        }
        Code::Unimplemented if error.message() == "unsupported_capability" => {
            StreamError::Unsupported
        }
        _ => StreamError::Unavailable,
    }
}
