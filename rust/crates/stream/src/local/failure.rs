pub use crate::LocalStreamFailure;

pub(crate) fn local_failure(
    operation: &'static str,
    error: &LocalStreamError,
    work: Option<&crate::native::Work>,
) -> LocalStreamFailure {
    if let Some(failure) = work.and_then(|work| work.failure(operation)) {
        return failure;
    }
    match error {
        LocalStreamError::Io(error) => LocalStreamFailure::io(operation, error),
        _ => LocalStreamFailure::new(operation, obs::ErrorKind::kind(error)),
    }
}
