//! Process initialization and scoped dispatch for span-capture tests.
//!
//! Captures always use scoped subscribers. Process-global subscribers are
//! reserved for run-alone, opt-in diagnostics whose OS callback threads cannot
//! inherit a dispatcher: `ACYCLIC_PROJFS_PROFILE_TRACING` in the ProjFS profile,
//! `E2E_TRACE` in harness scenarios, `NATIVE_BENCH_TRACING=1` in conformance's
//! write-validation bench, and `ACYCLIC_BENCH_TRACE` in conformance's filesystem
//! bench and filesystem's native_capture, projfs_paths, support/lease_paths and
//! support/object_batches benchmark binaries. None asserts on captured spans.
use std::sync::OnceLock;
use tracing::{Dispatch, Subscriber};

static REGISTRARS: OnceLock<[Dispatch; 2]> = OnceLock::new();

/// Call from a test binary's constructor, before libtest starts any threads.
/// Both registrars remain live until process exit, so callsite registration
/// always uses tracing-core's locked multi-dispatch path. They never become
/// a thread's default subscriber and therefore cannot collect another test.
pub fn initialize() {
    REGISTRARS.get_or_init(|| {
        [
            Dispatch::new(tracing_subscriber::registry()),
            Dispatch::new(tracing_subscriber::registry()),
        ]
    });
}

/// Create a scoped capture dispatcher after the binary's constructor ran.
pub fn dispatch(subscriber: impl Subscriber + Send + Sync + 'static) -> Dispatch {
    assert!(
        REGISTRARS.get().is_some(),
        "span capture requires its pre-main constructor"
    );
    Dispatch::new(subscriber)
}

#[cfg(test)]
mod tests;
