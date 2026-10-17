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

/// Exercises local owner configuration and credential parsers without I/O.
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub fn fuzz_local_owner_input(bytes: &[u8]) {
    crate::external_executor::fuzz_local_owner_input(bytes);
}

/// Exercises native Codex transcript ancestry without opening files or processes.
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub fn fuzz_codex_hook_parent(bytes: &[u8]) {
    crate::external_executor::fuzz_codex_parent(bytes);
}

/// Exercises native file input using the shared file JSONL codec and task policy.
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub fn fuzz_native_file_input(bytes: &[u8]) {
    crate::external_executor::fuzz_file_input(bytes);
}

/// Exercises native namespace normalization and request file round trips without I/O.
#[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
pub fn fuzz_native_hook_request(
    host: crate::external_executor::ExternalHost,
    event: &str,
    input: serde_json::Value,
) {
    if let Ok((request, _)) = crate::external_executor::fuzz_prepare_hook(host, event, input) {
        let task = crate::task_volume::Component::new(request.task_id().to_string()).unwrap();
        if let Ok(file) = request.to_file(task.clone()) {
            assert_eq!(
                crate::external_executor::ExternalRequest::from_file(&task, &file).unwrap(),
                request
            );
        }
    }
}

/// Exercises the typed native-to-stock command decoder without dispatching effects.
#[cfg(feature = "filesystem")]
pub fn fuzz_external_command(request: &crate::external_executor::ExternalRequest) {
    crate::external_executor::fuzz_command(request);
    if crate::external_executor::launch::requested(request)
        && let Ok(bytes) = serde_json::to_vec(&crate::external_executor::launch::Launch {
            parent: request.task_id(),
            request,
        })
    {
        crate::external_executor::launch::fuzz(&bytes);
    }
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    crate::external_executor::fuzz_request_record(request);
}

/// Exercises the immutable native parent/label/child provenance codec.
#[cfg(feature = "filesystem")]
pub fn fuzz_native_child_binding(bytes: &[u8]) {
    crate::external_executor::fuzz_child_binding(bytes);
}

/// Exercises retained native executor declarations without launching a process.
#[cfg(feature = "filesystem")]
pub fn fuzz_native_launch(bytes: &[u8]) {
    crate::external_executor::launch::fuzz(bytes);
}

/// Exercises native session file parsing and exact context-cut round trips.
#[cfg(feature = "filesystem")]
pub fn fuzz_native_context_file(bytes: &[u8]) {
    crate::external_executor::native_context::fuzz(bytes);
}
#[cfg(feature = "test-support")]
pub(crate) use task_volume_work::{
    Files as TaskVolumeFiles, examined as task_volume_item_examined,
};

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
