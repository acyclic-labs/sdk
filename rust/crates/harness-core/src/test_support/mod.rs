//! Deterministic model scripts for harness consumers and scenario tests.

#[cfg(feature = "hooks")]
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
