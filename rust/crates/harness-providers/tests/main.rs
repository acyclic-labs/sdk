//! Harness composition with real model provider adapters.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness_providers::Adapter;
mod benchmark_catalog;
#[cfg(loom)]
#[path = "../src/sync.rs"]
mod sync;
#[cfg(not(loom))]
#[path = "../src/sync/loom.rs"]
mod sync;
mod verification_models;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness/tests/e2e/local_workspace.rs"]
mod local_workspace;
#[path = "../../harness/tests/support/scenario_matchers.rs"]
mod matchers;
#[path = "integration/mcp_identity.rs"]
mod mcp_identity;
#[path = "../../harness/tests/support/ordered_matcher.rs"]
mod ordered_matcher;
#[path = "../../harness/tests/support/progress_guard.rs"]
mod progress_guard;
#[path = "../../harness/tests/support/progress_registry.rs"]
mod progress_registry;
#[path = "integration/provider_case.rs"]
mod provider_case;
#[path = "integration/provider_http.rs"]
mod provider_http;
#[path = "integration/provider_tests.rs"]
mod provider_tests;
#[path = "integration/providers.rs"]
mod providers;
#[path = "../../harness/tests/support/quiescent.rs"]
mod quiescent;
#[path = "../../harness/tests/support/scenario_data.rs"]
mod scenario_data;
#[path = "../../harness/tests/e2e/stall.rs"]
mod stall;

#[tokio::test]
async fn provider_scenarios() -> TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut paths = Vec::new();
    scenario_data::discover(&root, &mut paths)?;
    paths.sort();
    assert!(!paths.is_empty());
    for path in paths {
        run_provider_scenario(&root, &path).await?;
        println!("PASS {}", path.strip_prefix(&root)?.display());
    }
    Ok(())
}

async fn run_provider_scenario(root: &std::path::Path, path: &std::path::Path) -> TestResult {
    let mut value = scenario_data::load_value(root, path)?;
    assert_eq!(
        value.as_object_mut().ok_or("scenario")?.remove("driver"),
        Some("provider".into())
    );
    let case = serde_json::from_value(value)?;
    stall::guarded(path, providers::run(case)).await??;
    Ok(())
}

#[tokio::test]
async fn exhausted_provider_retries_settle_failed() -> TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    run_provider_scenario(&root, &root.join("providers/anthropic/exhausted-500.json")).await
}

#[tokio::test]
async fn refused_provider_request_settles_failed() -> TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    run_provider_scenario(&root, &root.join("providers/anthropic/refused-400.json")).await
}
