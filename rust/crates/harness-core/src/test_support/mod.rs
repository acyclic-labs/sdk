//! Deterministic model scripts for harness consumers and scenario tests.

#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub use crate::hooks::identity_test_support::assert_canonical_tool_consumers;

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) mod accounted;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod admission_work;
#[cfg(all(
    any(test, all(feature = "test-support", feature = "filesystem-local")),
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod crash_cuts;
mod scripted;
#[cfg(feature = "test-support")]
mod task_volume_work;

pub use scripted::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn, read_request_files,
};
#[cfg(feature = "test-support")]
pub use task_volume_work::measure as measure_task_volume_work;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use task_volume_work::{Files as TaskVolumeFiles, examined as task_volume_item_examined};

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
