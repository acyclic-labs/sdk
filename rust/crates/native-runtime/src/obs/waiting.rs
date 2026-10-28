use super::{OperationSpan, Scoped};
use std::{fmt::Display, future::Future};

/// Names the primitive and opaque operation key a future is waiting for.
///
/// The span lives with the future, including cancellation. This adds no task,
/// timer, wake or completion flag: only the producer determines readiness.
/// Keys must identify operations, never contain prompts or credentials.
pub fn waiting<F: Future>(primitive: &'static str, key: impl Display, future: F) -> Scoped<F> {
    OperationSpan::new(tracing::info_span!(
        target: "acyclic.wait",
        "waiting",
        primitive,
        key = %key,
    ))
    .scope(future)
}
