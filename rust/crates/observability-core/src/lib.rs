//! Operation telemetry is a projection of the operation's existing tracing span.
//!
//! Libraries never install a subscriber. Without one, there is no clock read,
//! metric storage, allocation, lock, task or syscall on an operation's path.
//! The host installs [`Operations`] on the same registry as its other tracing
//! layers. Completion records one latency sample and one outcome, exactly once,
//! even when descendants retain the span after the operation has returned.
#![deny(unsafe_code)]

#[cfg(not(target_arch = "wasm32"))]
fn record_os_error(slot: &mut Option<i32>, name: &str, value: impl TryInto<i32>) {
    if name == "error.os" && slot.is_none() {
        *slot = value.try_into().ok();
    }
}

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
pub use interval::{
    IntervalCoordinates, IntervalError, IntervalOrigin, OperationInterval, UnixClockAnchor,
    UnixIntervalEstimate,
};

#[cfg(not(target_arch = "wasm32"))]
mod operations;
#[cfg(not(target_arch = "wasm32"))]
pub use operations::{
    Completion, Metrics, Operations, Outcome, OwnedSample, Query, QueryError, Sample, Sink,
    Snapshot, finish, observe, observe_output, record_outcome,
};

mod slo;
pub use slo::{LatencySlo, Slo, SloError, Verdict};
#[cfg(not(target_arch = "wasm32"))]
mod slo_query;
#[cfg(not(target_arch = "wasm32"))]
pub use slo_query::{SloQuery, SloView};
