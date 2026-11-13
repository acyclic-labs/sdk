//! Stream result fields on SDK-owned operation spans.
pub(crate) use acyclic_native_runtime::obs::OperationSpan;
use tracing::Span;

/// Keeps indexed reads on the same operation lifecycle and creating dispatcher.
pub(crate) async fn indexed<T>(
    operation: OperationSpan,
    future: impl std::future::Future<Output = Result<T, crate::StreamError>>,
    bytes: impl FnOnce(&T) -> Option<u64>,
) -> Result<T, crate::StreamError> {
    let work = async {
        let result = future.await;
        if !operation.is_disabled()
            && let Ok(value) = &result
            && let Some(bytes) = bytes(value)
        {
            operation.record("bytes", bytes);
        }
        result
    };
    operation
        .scope(acyclic_observability::observe(
            &operation,
            work,
            crate::StreamError::code,
        ))
        .await
}

/// A failure with a stable, path-free kind to record as `error.kind`.
pub(crate) trait ErrorKind {
    fn kind(&self) -> &'static str;
}

impl ErrorKind for crate::StreamError {
    fn kind(&self) -> &'static str {
        self.code()
    }
}

/// Records `outcome` and, on failure, `error.kind` on the owned operation span.
pub(crate) fn finish<T, E: ErrorKind>(span: &Span, result: Result<T, E>) -> Result<T, E> {
    acyclic_observability::finish(span, result, E::kind)
}

/// Records a failure of `kind` on the owned operation span.
pub(crate) fn failed(span: &Span, kind: &'static str) {
    acyclic_observability::record_outcome(span, Some(kind));
}
