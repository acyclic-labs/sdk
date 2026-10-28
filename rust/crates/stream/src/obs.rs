//! Stream result fields on SDK-owned operation spans.
pub(crate) use acyclic_native_runtime::obs::OperationSpan;
use tracing::Span;

/// A failure with a stable, path-free kind to record as `error.kind`.
#[cfg(any(feature = "http", feature = "local"))]
pub(crate) trait ErrorKind {
    fn kind(&self) -> &'static str;
}

#[cfg(any(feature = "http", feature = "local"))]
impl ErrorKind for crate::StreamError {
    fn kind(&self) -> &'static str {
        self.code()
    }
}

/// Records `outcome` and, on failure, `error.kind` on the owned operation span.
#[cfg(any(feature = "http", feature = "local"))]
pub(crate) fn finish<T, E: ErrorKind>(span: &Span, result: Result<T, E>) -> Result<T, E> {
    match &result {
        Ok(_) => {
            span.record("outcome", "ok");
        }
        Err(error) => failed(span, error.kind()),
    }
    result
}

/// Records a failure of `kind` on the owned operation span.
pub(crate) fn failed(span: &Span, kind: &'static str) {
    span.record("outcome", "err").record("error.kind", kind);
}
