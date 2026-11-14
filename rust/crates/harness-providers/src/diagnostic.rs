use acyclic_harness_core::{Error, wire::ErrorCode};

/// Mark semantic failures only after an HTTP refusal or response protocol error.
/// Storage, cancellation and native accounting failures keep their original class.
pub(crate) fn rejection(error: Error) -> Error {
    let (code, message) = match error {
        Error::Invalid(message) => (ErrorCode::Invalid, message),
        Error::Unauthorized(message) => (ErrorCode::Unauthorized, message),
        error => return error,
    };
    Error::ModelDiagnostic {
        code,
        message,
        custody: None,
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use acyclic_harness_core::ModelTransportFailure::*;

    #[test]
    fn transport_causes_never_render_or_allocate_on_either_path() {
        struct Cause;
        impl std::fmt::Display for Cause {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("untrusted transport text must never be rendered");
            }
        }
        for native in [false, true] {
            for failure in [Connect, Timeout, Interrupted, Tls, Status(503)] {
                let allocated = acyclic_test_support::measure(|| {
                    let error = crate::transport_failure(native, failure, Cause);
                    assert!(matches!(error,
                        Error::ModelUnavailable { failure: actual, excerpt }
                        if actual == failure && excerpt.is_empty()
                    ));
                });
                assert_eq!(allocated.count_total, 0);
            }
        }
    }
}
