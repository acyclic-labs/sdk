use super::*;

pub(super) fn ok<T>(reply: tonic::Response<T>) -> T {
    obs::record!("rpc.code" = tonic::Code::Ok as u64);
    reply.into_inner()
}

pub(super) fn error_from_status(
    status: &tonic::Status,
    controls: Option<&Arc<super::NativeTransportErrors>>,
) -> Error {
    let _detail_custody = if let Some(controls) = controls {
        let Some(maximum) = status.details().len().checked_mul(4) else {
            return response::invalid();
        };
        match acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, maximum) {
            Ok(allocation) => Some(allocation),
            Err(error) => return controls.fail(error),
        }
    } else {
        None
    };
    obs::record!("rpc.code" = status.code() as u64);
    if let Ok(detail) = wire::ErrorDetail::decode(status.details())
        && let Ok(code) = wire::ErrorCode::try_from(detail.code)
        && code != wire::ErrorCode::Unspecified
    {
        return code.into();
    }
    match status.code() {
        tonic::Code::InvalidArgument => wire::ErrorCode::InvalidArgument,
        tonic::Code::NotFound => wire::ErrorCode::NotFound,
        tonic::Code::AlreadyExists => wire::ErrorCode::AlreadyExists,
        tonic::Code::FailedPrecondition => wire::ErrorCode::PreconditionFailed,
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            wire::ErrorCode::AccessDenied
        }
        tonic::Code::ResourceExhausted => wire::ErrorCode::QuotaExceeded,
        tonic::Code::Unimplemented => wire::ErrorCode::Unsupported,
        _ => wire::ErrorCode::Unavailable,
    }
    .into()
}

pub(super) fn bounded<T: Message>(query: &T) -> Result<(), Error> {
    if query.encoded_len() > MESSAGE_BYTES {
        Err(wire::ErrorCode::QuotaExceeded.into())
    } else {
        Ok(())
    }
}

pub(super) fn protocol() -> Error {
    wire::ErrorCode::Unavailable.into()
}
