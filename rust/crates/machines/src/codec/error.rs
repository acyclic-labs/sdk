use super::*;

#[cfg(any(target_arch = "wasm32", test))]
pub fn request_credential_status(mut message: String, detail: Option<String>) -> tonic::Status {
    if let Some(detail) = detail.filter(|detail| !detail.is_empty()) {
        message.push_str(": ");
        message.push_str(&detail);
    }
    let cause = acyclic_auth::credential::RequestCredentialError::from_browser(message.clone());
    let mut status = tonic::Status::unavailable(message);
    status.set_source(std::sync::Arc::new(cause));
    status
}

fn credential_before_dispatch(status: &tonic::Status) -> bool {
    std::error::Error::source(status)
        .is_some_and(|cause| cause.is::<acyclic_auth::credential::RequestCredentialError>())
}

// These statuses cannot attest whether the original effect was dispatched.
fn uncertain_effect(status: &tonic::Status) -> bool {
    std::error::Error::source(status)
        .is_some_and(|cause| cause.is::<acyclic_auth::credential::ResponseCredentialError>())
        || matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::Internal
        )
}

pub fn mutation_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    if credential_before_dispatch(&value) {
        return read_error(value);
    }
    if value.code() != tonic::Code::FailedPrecondition || uncertain_effect(&value) {
        return guest_error(ProviderError::Indeterminate(key), value);
    }
    Precondition::from_wire(value.details())
        .map_or_else(
            || ProviderError::Rejected(value.message().into()),
            ProviderError::FailedPrecondition,
        )
        .with_cause(value)
}
pub fn watch_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    ProviderError::Indeterminate(key).with_cause(value)
}
pub fn recovery_error(key: IdempotencyKey, value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        _ if uncertain_effect(&value) => ProviderError::Indeterminate(key),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}
pub fn read_error(value: tonic::Status) -> ProviderError {
    let classification = match value.code() {
        code if code != tonic::Code::Internal && uncertain_effect(&value) => {
            ProviderError::Unavailable
        }
        tonic::Code::NotFound => ProviderError::NotFound(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}

/// Guest command and file admission share transport semantics while retaining
/// the original uncertain effect identity supplied by their caller.
pub fn guest_error(uncertain: ProviderError, value: tonic::Status) -> ProviderError {
    if credential_before_dispatch(&value) {
        return read_error(value);
    }
    let classification = match value.code() {
        _ if uncertain_effect(&value) => uncertain,
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    };
    classification.with_cause(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn local_response_credential_refusal_keeps_recovery_identity() {
        let key = IdempotencyKey::from_digest([1; 32]);
        let request = request_credential_status(
            "credential source unavailable".into(),
            Some("credential callback failed".into()),
        );
        let expected = "credential source unavailable: credential callback failed";
        assert_eq!(request.message(), expected);
        assert_eq!(
            std::error::Error::source(&request)
                .map(ToString::to_string)
                .as_deref(),
            Some(expected)
        );
        assert_eq!(
            mutation_error(key, request.clone()).classification(),
            &ProviderError::Unavailable
        );
        assert_eq!(
            guest_error(
                ProviderError::CommandIndeterminate(MachineId::from_digest([2; 32])),
                request
            )
            .classification(),
            &ProviderError::Unavailable
        );
        let mut response = Status::permission_denied("credential expired");
        response.set_source(std::sync::Arc::new(
            acyclic_auth::credential::ResponseCredentialError::from_browser(),
        ));
        assert_eq!(
            mutation_error(key, response).classification(),
            &ProviderError::Indeterminate(key)
        );
        assert_eq!(
            mutation_error(key, Status::permission_denied("server denied")).classification(),
            &ProviderError::Rejected("server denied".into())
        );
    }
}
