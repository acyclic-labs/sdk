//! Deterministic model scripts for harness consumers and scenario tests.

#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub use crate::hooks::identity_test_support::assert_canonical_tool_consumers;

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) mod accounted;
#[cfg(all(
    any(test, all(feature = "test-support", feature = "filesystem-local")),
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod crash_cuts;
mod policy;
mod scripted;
pub use policy::FixedToolPolicy;

pub use scripted::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn, read_request_files,
};

/// Reads child-fixture controls while refusing ambient user directory discovery.
/// Available wherever this module is (unit tests on every target, or `test-support`).
pub fn environment(name: &str) -> Option<std::ffi::OsString> {
    crate::native_environment::variable_with_user_locations(name, false)
}

/// Holds automatic advancement of a paused Tokio clock during native I/O.
///
/// The test can still advance protocol deadlines explicitly. Dropping the
/// returned sender releases the blocking job, including on errors or unwinds.
#[cfg(not(target_arch = "wasm32"))]
pub fn hold_io_clock() -> tokio::sync::oneshot::Sender<()> {
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    // Tokio tracks its blocking pool as outstanding work. Native sockets and
    // the shared I/O service alone do not prevent virtual-time auto-advancement.
    drop(tokio::task::spawn_blocking(move || {
        let _ = wait.blocking_recv();
    }));
    release
}

/// A unique Streams idempotency key for a fixture write that has no retry
/// identity of its own. Stream helpers require a key; fixtures never replay.
#[cfg(test)]
pub(crate) fn fresh_key() -> Result<acyclic_stream::IdempotencyKey, acyclic_stream::StreamError> {
    acyclic_stream::IdempotencyKey::new(uuid::Uuid::new_v4().as_bytes().to_vec())
}
