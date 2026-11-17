//! Harness composition with real model provider adapters.
#![cfg(not(target_arch = "wasm32"))]
// Compile-time depth headroom. This crate composes the deepest stack (tools,
// Git engine, filesystem, publication and objects over concrete backends).
// Each layer erases its futures at its boundary, so query depth stays well
// below the default 128; a by-value chain across layers fails here first.
#![recursion_limit = "112"]
use acyclic_harness_providers::Adapter;
mod benchmark_catalog;
#[cfg(loom)]
#[path = "../src/sync.rs"]
mod sync;
#[cfg(not(loom))]
#[path = "../src/sync/loom.rs"]
mod sync;
mod verification_models;

mod integration_helpers;
use integration_helpers::TestResult;
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness/tests/e2e/local_workspace.rs"]
mod local_workspace;
#[path = "../../harness/tests/support/scenario_matchers.rs"]
mod matchers;
#[path = "integration/mcp_identity.rs"]
mod mcp_identity;
#[path = "../../harness/tests/support/progress_guard.rs"]
mod progress_guard;
#[path = "../../harness/tests/support/progress_registry.rs"]
mod progress_registry;
#[path = "integration/provider_case.rs"]
mod provider_case;
#[path = "integration/provider_http.rs"]
mod provider_http;
#[path = "integration/providers.rs"]
mod providers;
#[path = "../../harness/tests/support/quiescent.rs"]
mod quiescent;
#[path = "../../harness/tests/support/scenario_data.rs"]
mod scenario_data;
#[path = "../../harness/tests/e2e/stall.rs"]
mod stall;

async fn run_scenario(root: std::path::PathBuf, path: std::path::PathBuf) -> TestResult {
    let mut value = scenario_data::load_value(&root, &path)?;
    assert_eq!(
        value.as_object_mut().ok_or("scenario")?.remove("driver"),
        Some("provider".into())
    );
    let case = serde_json::from_value(value)?;
    stall::guarded(&path, providers::run(case))
        .await?
        .map_err(|error| format!("{}: {error}", path.display()))?;
    println!("PASS {}", path.strip_prefix(&root)?.display());
    Ok(())
}

#[tokio::test]
async fn provider_scenarios() -> TestResult {
    use futures::{StreamExt as _, TryStreamExt as _};
    let root = acyclic_test_support::crate_path!("tests/scenarios");
    let mut paths = Vec::new();
    scenario_data::discover(&root, &mut paths)?;
    paths.sort();
    assert!(!paths.is_empty());
    futures::stream::iter(paths)
        .map(|path| run_scenario(root.clone(), path))
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}
