//! Operation telemetry is a projection of the operation's existing tracing span.
//!
//! Libraries never install a subscriber. Without one, there is no clock read,
//! metric storage, allocation, lock, task or syscall on an operation's path.
//! The host installs [`Operations`] on the same registry as its other tracing
//! layers. Completion records one latency sample and one outcome, exactly once,
//! even when descendants retain the span after the operation has returned.
#![deny(unsafe_code)]

#[cfg(not(target_arch = "wasm32"))]
mod capture_retirement;
#[cfg(not(target_arch = "wasm32"))]
pub use capture_retirement::CaptureRetirement;

#[cfg(not(target_arch = "wasm32"))]
mod durability;
#[cfg(not(target_arch = "wasm32"))]
pub use durability::{DurabilityAttempt, DurabilityRequest, DurabilitySyscall};

#[cfg(not(target_arch = "wasm32"))]
mod native_task;
#[cfg(not(target_arch = "wasm32"))]
pub use native_task::{NativeTask, NativeTaskTerminal};

#[cfg(not(target_arch = "wasm32"))]
mod publication;
#[cfg(not(target_arch = "wasm32"))]
pub use publication::{Publication, PublicationControl, PublicationData, PublicationPurpose};

#[cfg(not(target_arch = "wasm32"))]
mod work;
#[cfg(not(target_arch = "wasm32"))]
pub use work::WorkSummary;

#[cfg(not(target_arch = "wasm32"))]
mod binding;
#[cfg(not(target_arch = "wasm32"))]
pub use binding::{Binding, BindingError};

#[cfg(all(feature = "research", not(target_arch = "wasm32")))]
mod interval;
#[cfg(all(feature = "research", not(target_arch = "wasm32")))]
pub use interval::{IntervalCoordinates, IntervalError, IntervalOrigin, OperationInterval};

#[cfg(not(target_arch = "wasm32"))]
mod operations;
#[cfg(not(target_arch = "wasm32"))]
pub use operations::{
    Completion, Metrics, Operations, Outcome, OwnedSample, Query, QueryError, Sample, Sink,
    Snapshot, finish, observe, observe_output, record_outcome,
};
#[cfg(not(target_arch = "wasm32"))]
mod export;
#[cfg(not(target_arch = "wasm32"))]
pub use export::{Export, ExportReceiver, ExportRecord, ExportStats};

#[cfg(not(target_arch = "wasm32"))]
mod output;
#[cfg(not(target_arch = "wasm32"))]
pub use output::{OutputFailure, OutputRefusal, TextOutput, TextReceiver, TextRecord, TextStats};

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
/// The named-span form accepts a level, output classifier and additional fields;
/// its work and classifier may borrow that span until the operation completes.
#[macro_export]
macro_rules! async_operation {
    ($name:literal, $kind:expr, $context:path, $work:expr) => {{ $crate::operation_future!($name, $kind, $context, $work).await }};
    (@$level:path, $name:expr, $span:ident, $error:expr, $context:path, $work:expr $(, $($field:ident).+ = $value:expr)* $(,)?) => {
        $crate::operation_output_future!(@scope [.await] $level, $name, $span, $error, $context, $work $(, $($field).+ = $value)*)
    };
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
    ($name:literal, $error:expr, $context:path, $work:expr) => {
        $crate::operation_output_future!(@scope [] $crate::__tracing::Level::INFO, $name, _operation, $error, $context, $work)
    };
    (@scope [$($finish:tt)*] $level:path, $name:expr, $span:ident, $error:expr, $context:path, $work:expr $(, $($field:ident).+ = $value:expr)* $(,)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            let $span = $context($crate::__tracing::span!(
                $level,
                $name,
                outcome = $crate::__tracing::field::Empty,
                $($($field).+ = $value,)*
                error.kind = $crate::__tracing::field::Empty,
            ));
            $span.scope($crate::observe_output(&$span, $work, $error)) $($finish)*
        }
        #[cfg(target_arch = "wasm32")]
        {
            let $span = ();
            ($work) $($finish)*
        }
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
