//! Span helpers for the gRPC host client; see `docs/observability.md`.

use crate::Error;
use tracing::Span;

/// Adapts original RPC status evidence to the shared completion observer.
pub(crate) fn outcome<T>(span: &Span, result: &Result<T, Error>) -> Option<&'static str> {
    match result {
        Ok(_) => {
            span.record("rpc.code", 0);
        }
        Err(Error::Observation(status)) => {
            span.record("rpc.code", i32::from(status.code()));
        }
        Err(_) => {}
    }
    result.as_ref().err().map(Error::kind)
}
