//! Integration behaviour tests.
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(not(loom))]
mod file_tools_contract;
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod history_selection;
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod model_tool_call;

#[cfg(all(
    not(loom),
    target_os = "linux",
    not(target_arch = "wasm32"),
    feature = "test-support"
))]
mod virtiofs;

#[cfg(all(loom, feature = "model-checking"))]
mod e2e_progress_model;
