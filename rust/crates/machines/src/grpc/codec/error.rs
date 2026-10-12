use super::*;

pub(in super::super) fn mutation_error(
    key: IdempotencyKey,
    value: &tonic::Status,
) -> ProviderError {
    obs::failed_rpc(value);
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::Indeterminate(key),
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    }
}
pub(in super::super) fn watch_error(key: IdempotencyKey, value: &tonic::Status) -> ProviderError {
    obs::failed_rpc(value);
    ProviderError::Indeterminate(key)
}
pub(in super::super) fn operation_watch_error(operation: OperationId) -> ProviderError {
    ProviderError::OperationIndeterminate(operation)
}
pub(in super::super) fn recovery_error(
    key: IdempotencyKey,
    value: &tonic::Status,
) -> ProviderError {
    obs::failed_rpc(value);
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::Indeterminate(key),
        _ => ProviderError::Rejected(value.message().into()),
    }
}
pub(in super::super) fn read_error(value: &tonic::Status) -> ProviderError {
    obs::failed_rpc(value);
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => ProviderError::Unavailable,
        tonic::Code::NotFound => ProviderError::NotFound(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    }
}
