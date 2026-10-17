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

pub use scripted::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn};
#[cfg(feature = "test-support")]
pub use task_volume_work::measure as measure_task_volume_work;
#[cfg(feature = "test-support")]
pub(crate) use task_volume_work::{
    Files as TaskVolumeFiles, examined as task_volume_item_examined,
};

/// Fuzzes scheduler framing and checks event/digest projection round trips.
pub fn check_scheduler_intent_frame(data: &[u8]) -> crate::Result<()> {
    crate::distributed::verification::check(data)
}

/// Local test composition with temporary settings, independent of HOME and CODEX_HOME.
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
    let config = crate::LocalHarnessConfig::new(workspace, model);
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
