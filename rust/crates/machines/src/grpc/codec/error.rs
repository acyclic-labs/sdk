use super::*;

pub(in super::super) fn mutation_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::Indeterminate(key),
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        tonic::Code::FailedPrecondition => Precondition::from_wire(value.details()).map_or_else(
            || ProviderError::Rejected(value.message().into()),
            ProviderError::FailedPrecondition,
        ),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}
pub(in super::super) fn watch_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    ProviderError::Indeterminate(key).with_cause(value)
}
pub(in super::super) fn recovery_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::Indeterminate(key),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}
pub(in super::super) fn read_error(value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => ProviderError::Unavailable,
        tonic::Code::NotFound => ProviderError::NotFound(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}

/// Guest command and file admission share transport semantics while retaining
/// the original uncertain effect identity supplied by their caller.
pub(in super::super) fn guest_error(
    uncertain: ProviderError,
    value: tonic::Status,
) -> ProviderError {
    let classification = match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => uncertain,
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}
