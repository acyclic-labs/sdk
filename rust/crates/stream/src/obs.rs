//! Span helpers for the native providers and transports; see `docs/observability.md`.

use tracing::Span;

/// A failure with a stable, path-free kind to record as `error.kind`.
pub(crate) trait ErrorKind {
    fn kind(&self) -> &'static str;
}

impl ErrorKind for crate::StreamError {
    fn kind(&self) -> &'static str {
        self.code()
    }
}

/// Records `outcome` and, on failure, `error.kind` on the current span.
pub(crate) fn finish<T, E: ErrorKind>(result: Result<T, E>) -> Result<T, E> {
    match &result {
        Ok(_) => record("outcome", "ok"),
        Err(error) => failed(error.kind()),
    }
    result
}

/// Records a failure of `kind` on the current span.
pub(crate) fn failed(kind: &'static str) {
    Span::current()
        .record("outcome", "err")
        .record("error.kind", kind);
}

/// Records one field on the current span; a no-op without a subscriber.
pub(crate) fn record(field: &'static str, value: impl tracing::Value) {
    Span::current().record(field, value);
}
