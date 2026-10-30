//! Operation telemetry is a projection of the operation's existing tracing span.
//!
//! Libraries never install a subscriber. Without one, there is no clock read,
//! metric storage, allocation, lock, task or syscall on an operation's path.
//! The host installs [`Operations`] on the same registry as its other tracing
//! layers. Completion records one latency sample and one outcome, exactly once,
//! even when descendants retain the span after the operation has returned.
#![deny(unsafe_code)]

#[cfg(not(target_arch = "wasm32"))]
mod operations;
#[cfg(not(target_arch = "wasm32"))]
pub use operations::{
    Completion, Metrics, Operations, Outcome, Query, QueryError, Sample, Sink, Snapshot, finish,
    observe, observe_output, record_outcome,
};

mod slo;
pub use slo::{LatencySlo, Slo, SloError, Verdict};
#[cfg(not(target_arch = "wasm32"))]
mod slo_query;
#[cfg(not(target_arch = "wasm32"))]
pub use slo_query::{SloQuery, SloView};

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub use tracing as __tracing;

/// Observe an asynchronous primitive using its shared owned operation context.
/// `context` is the shared context constructor, so polling, cancellation and drop
/// retain the originating subscriber. No second copy of the producer is retained.
#[macro_export]
macro_rules! async_operation {
    ($name:literal, $kind:expr, $context:path, $work:expr) => {{ $crate::operation_future!($name, $kind, $context, $work).await }};
}

/// Own and observe a single future without a forwarding async frame or box.
#[macro_export]
macro_rules! operation_future {
    ($name:literal, $kind:expr, $context:path, $work:expr) => {
        $crate::operation_output_future!(
            $name,
            |result| result.as_ref().err().map($kind),
            $context,
            $work
        )
    };
}

/// Observe an unchanged future output, including separately retained physical
/// and observer errors, using the same span and completion owner.
#[macro_export]
macro_rules! operation_output_future {
    ($name:literal, $error:expr, $context:path, $work:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            let operation = $context($crate::__tracing::info_span!(
                $name,
                outcome = $crate::__tracing::field::Empty,
                error.kind = $crate::__tracing::field::Empty,
            ));
            operation.scope($crate::observe_output(&operation, $work, $error))
        }
        #[cfg(target_arch = "wasm32")]
        ($work)
    }};
}

/// Observe one synchronous primitive operation without evaluating fields or
/// retaining tracing dependencies on wasm32. The error classifier returns only
/// a stable variant name; arguments, returned values and errors are never formatted.
#[macro_export]
macro_rules! operation {
    ($name:literal, $kind:expr, $work:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            let span = $crate::__tracing::info_span!(
                $name,
                outcome = $crate::__tracing::field::Empty,
                error.kind = $crate::__tracing::field::Empty,
            );
            let completion = $crate::Completion::new(&span);
            span.in_scope(|| completion.finish(($work)(), $kind))
        }
        #[cfg(target_arch = "wasm32")]
        ($work)()
    }};
}
