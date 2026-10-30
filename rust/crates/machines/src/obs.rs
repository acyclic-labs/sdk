//! Span helpers for the gRPC client; see `docs/observability.md`.

use crate::ProviderError;
#[cfg(not(target_arch = "wasm32"))]
use tracing::Span;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::field::Empty;

/// Records `outcome` and, on failure, `error.kind` on the current span. A
/// success means every RPC in it returned `OK`.
pub(crate) fn finish<T>(result: Result<T, ProviderError>) -> Result<T, ProviderError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let span = Span::current();
        if result.is_ok() {
            span.record("rpc.code", 0);
        }
        return acyclic_observability::finish(&span, result, ProviderError::kind);
    }
    #[cfg(target_arch = "wasm32")]
    result
}

/// Records a failed RPC's status code on the current span.
pub(crate) fn failed_rpc(_status: &tonic::Status) {
    #[cfg(not(target_arch = "wasm32"))]
    Span::current().record("rpc.code", i32::from(_status.code()));
}
