//! Deterministic model scripts for harness consumers and scenario tests.

pub use acyclic_harness_core::test_support::*;
pub use acyclic_harness_search::test_support::{ScriptedSearch, ScriptedSearchProvider};

mod client_views;
pub use client_views::empty_client_views;
#[cfg(feature = "test-support")]
pub(crate) mod model_view_work;
#[cfg(feature = "test-support")]
pub use model_view_work::measure as measure_model_view_work;

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
    #[cfg(feature = "native-execution")]
    let config = {
        let mut config = config;
        config.environment_platform = crate::native_environment::PlatformPolicy::Fixture {
            os: std::env::consts::OS.into(),
            shell: "bash".into(),
        };
        config
    };
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

#[cfg(all(
    test,
    feature = "filesystem-local",
    any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
))]
mod tests {
    // The local test composition must select the isolated settings snapshot,
    // never the user's settings (see `native_settings` isolation properties).
    #[test]
    fn local_test_config_selects_isolated_settings() {
        let workspace = std::path::PathBuf::from("workspace");
        let config = super::local_harness_config(
            workspace,
            (
                crate::model::Model::new(
                    "test".into(),
                    "settings".into(),
                    "1".into(),
                    serde_json::Value::Null,
                )
                .unwrap(),
                std::sync::Arc::new(super::ScriptedModelProvider::new([])),
            ),
        )
        .unwrap();
        assert!(
            config.settings.is_some(),
            "test configuration must select settings"
        );
    }
}
