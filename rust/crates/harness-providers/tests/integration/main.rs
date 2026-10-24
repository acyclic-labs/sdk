//! Harness composition with real model provider adapters.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness::{
    Result,
    client::{CommandCatalog, CommandResolution},
};
use client_files::Fixture;
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[path = "../../../harness/tests/support/client_files.rs"]
mod client_files;
mod client_mention_provider;
#[path = "../../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../../harness/tests/support/frame_oracle.rs"]
mod frame_oracle;
#[path = "../../../harness/tests/e2e/local_admission.rs"]
mod local_admission;
#[path = "../../../harness/tests/e2e/local_workspace.rs"]
mod local_workspace;
#[path = "../../../harness/tests/support/scenario_matchers.rs"]
mod matchers;
mod mcp_identity;
mod native_hook_http;
#[path = "../../../harness/tests/support/ordered_matcher.rs"]
mod ordered_matcher;
#[path = "../../../harness/tests/support/progress_guard.rs"]
mod progress_guard;
#[path = "../../../harness/tests/support/progress_registry.rs"]
mod progress_registry;
mod provider_case;
mod provider_failure_exit;
mod provider_http;
mod provider_tests;
mod providers;
#[path = "../../../harness/tests/support/quiescent.rs"]
mod quiescent;
#[path = "../../../harness/tests/support/scenario_data.rs"]
mod scenario_data;
#[path = "../../../harness/tests/e2e/stall.rs"]
mod stall;

#[tokio::test]
async fn provider_scenarios() -> TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut paths = Vec::new();
    scenario_data::discover(&root, &mut paths)?;
    paths.sort();
    assert!(!paths.is_empty());
    for path in paths {
        let mut value = scenario_data::load_value(&root, &path)?;
        assert_eq!(
            value.as_object_mut().ok_or("scenario")?.remove("driver"),
            Some("provider".into())
        );
        let case = serde_json::from_value(value)?;
        stall::guarded(&path, providers::run(case)).await??;
        println!("PASS {}", path.strip_prefix(&root)?.display());
    }
    Ok(())
}
