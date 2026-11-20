//! Stream result fields on SDK-owned operation spans.
pub use acyclic_native_runtime::obs::OperationSpan;
use tracing::Span;

/// A failure with a stable, path-free kind to record as `error.kind`.
pub trait ErrorKind {
    fn kind(&self) -> &'static str;
}

impl ErrorKind for crate::StreamError {
    fn kind(&self) -> &'static str {
        self.code()
    }
}

/// Records `outcome` and, on failure, `error.kind` on the owned operation span.
pub fn finish<T, E: ErrorKind>(span: &Span, result: Result<T, E>) -> Result<T, E> {
    acyclic_observability::finish(span, result, E::kind)
}

/// Records a failure of `kind` on the owned operation span.
pub fn failed(span: &Span, kind: &'static str) {
    acyclic_observability::record_outcome(span, Some(kind));
}
