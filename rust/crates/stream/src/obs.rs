//! Span helpers for the native providers and transports; see `docs/observability.md`.

use tracing::Span;

pub(crate) use acyclic_native_runtime::obs::{OperationSpan, Scoped};

/// Records `outcome` and, on failure, `error.kind` on the owned operation span.
#[cfg(any(feature = "http", feature = "local"))]
pub(crate) fn finish<T>(span: &Span, result: Result<T, crate::StreamError>) -> Result<T, crate::StreamError> {
    match &result {
        Ok(_) => { span.record("outcome", "ok"); }
        Err(error) => failed(span, error.code()),
    }
    result
}

/// Records a failure of `kind` on the owned operation span.
pub(crate) fn failed(span: &Span, kind: &'static str) {
    span.record("outcome", "err").record("error.kind", kind);
}

/// Records cancellation if the caller drops an unfinished operation.
#[cfg(any(feature = "http", feature = "local"))]
pub(crate) struct Cancellation(pub(crate) Option<OperationSpan>);
#[cfg(any(feature = "http", feature = "local"))]
impl Drop for Cancellation {
    fn drop(&mut self) {
        if let Some(span) = &self.0 { failed(span, "cancelled"); }
    }
}
