use super::*;

// These statuses cannot attest whether the original effect was dispatched.
fn uncertain_effect(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
            | tonic::Code::Internal
    )
}

pub(in super::super) fn mutation_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    if value.code() != tonic::Code::FailedPrecondition {
        return guest_error(ProviderError::Indeterminate(key), value);
    }
    Precondition::from_wire(value.details())
        .map_or_else(
            || ProviderError::Rejected(value.message().into()),
            ProviderError::FailedPrecondition,
        )
        .with_cause(value)
}
pub(in super::super) fn watch_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    ProviderError::Indeterminate(key).with_cause(value)
}
pub(in super::super) fn recovery_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        code if uncertain_effect(code) => ProviderError::Indeterminate(key),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}
pub(in super::super) fn read_error(value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        code if code != tonic::Code::Internal && uncertain_effect(code) => {
            ProviderError::Unavailable
        }
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
        code if uncertain_effect(code) => uncertain,
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}
