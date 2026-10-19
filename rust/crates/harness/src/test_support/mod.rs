//! Deterministic model scripts for harness consumers and scenario tests.

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) mod accounted;
#[cfg(feature = "test-support")]
pub(crate) mod admission_work;
mod client_views;
pub use client_views::empty_client_views;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod crash_cuts;
mod scripted;
mod search;
#[cfg(feature = "test-support")]
mod task_volume_work;
pub use search::{ScriptedSearch, ScriptedSearchProvider};

pub use scripted::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn, read_request_files,
};
#[cfg(feature = "test-support")]
pub use task_volume_work::measure as measure_task_volume_work;
#[cfg(feature = "test-support")]
pub(crate) use task_volume_work::{
    Files as TaskVolumeFiles, examined as task_volume_item_examined,
};

/// Local test composition with temporary state and settings, independent of the per-user store, HOME and CODEX_HOME.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_harness_config(
    workspace: impl Into<std::path::PathBuf>,
    model: (
        crate::model::Model,
        std::sync::Arc<dyn crate::model::ModelProvider>,
    ),
) -> crate::Result<crate::LocalHarnessConfig> {
    let workspace = workspace.into();
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let settings = crate::native_settings::SettingsConfig::for_test(&workspace)?;
    // Store opening supplies temporary state only when no explicit root was selected.
    let config = crate::LocalHarnessConfig::new(workspace, model).hermetic();
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(settings);
    Ok(config)
}

/// The composition's actual task provider, for scenario wire-frame assertions.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_task_stream(
    harness: &crate::LocalHarness,
) -> std::sync::Arc<acyclic_stream::LocalStream> {
    harness.task_stream()
}

#[cfg(feature = "test-support")]
pub(crate) mod model_view_work;
#[cfg(feature = "test-support")]
pub use model_view_work::measure as measure_model_view_work;

/// Reads child-fixture controls while refusing ambient user directory discovery.
/// Available wherever this module is (unit tests on every target, or `test-support`).
pub fn environment(name: &str) -> Option<std::ffi::OsString> {
    crate::native_environment::variable_with_user_locations(name, false)
}
