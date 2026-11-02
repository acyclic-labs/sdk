//! Span helpers for the gRPC host client; see `docs/observability.md`.

use crate::Error;
use tracing::Span;

macro_rules! operation {
    ($name:literal, $work:expr) => {
        acyclic_observability::async_operation!(
            @tracing::Level::INFO,
            $name,
            span,
            |result| $crate::obs::outcome(&span, result),
            acyclic_native_runtime::obs::OperationSpan::new,
            $work,
            rpc.code = tracing::field::Empty
        )
    };
}
pub(crate) use operation;

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
