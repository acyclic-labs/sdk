//! Span helpers for the gRPC host client; see `docs/observability.md`.

use crate::Error;
use tracing::Span;

pub(crate) use tracing::field::Empty;

/// Records `outcome`, `error.kind` and, for an RPC failure, `rpc.code` on the
/// current span. A success means every RPC in it returned `OK`.
pub(crate) fn finish<T>(result: Result<T, Error>) -> Result<T, Error> {
    let span = Span::current();
    match &result {
        Ok(_) => span.record("outcome", "ok").record("rpc.code", 0),
        Err(error) => {
            if let Error::Observation(status) = error {
                span.record("rpc.code", i32::from(status.code()));
            }
            span.record("outcome", "err")
                .record("error.kind", error.kind())
        }
    };
    result
}
