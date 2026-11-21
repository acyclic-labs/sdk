//! Harness composition with real model provider adapters.
#![cfg(test)]
#![cfg(not(target_arch = "wasm32"))]
// Compile-time depth headroom. This crate composes the deepest stack (tools,
// Git engine, filesystem, publication and objects over concrete backends).
// Each layer erases its futures at its boundary, so query depth stays well
// below the default 128; a by-value chain across layers fails here first.
#![recursion_limit = "112"]
use acyclic_harness_providers::Adapter;
use futures::{StreamExt as _, stream};
mod benchmark_catalog;
// The models explore every interleaving, so their primitives are always Loom's.
mod sync {
    pub use loom::sync::*;
}
mod verification_models;

mod integration_helpers;
use integration_helpers::TestResult;
#[path = "../../harness-e2e/tests/e2e/case_workers.rs"]
mod case_workers;
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness-e2e/tests/e2e/local_workspace.rs"]
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
#[path = "../../harness/tests/support/stall.rs"]
mod stall;

fn scenario_admission<I>(
    cases: I,
    capacity: usize,
) -> impl futures::Stream<Item = <I::Item as std::future::Future>::Output>
where
    I: Iterator,
    I::Item: std::future::Future,
{
    assert!(capacity > 0, "scenario capacity is zero");
    stream::iter(cases).buffer_unordered(capacity)
}

/// One worker per available CPU, bounded so a large host does not oversubscribe.
fn scenario_worker_budget() -> usize {
    std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(2, 32)
}

fn scenario_runtime(workers: usize) -> TestResult<tokio::runtime::Runtime> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err("provider scenarios must start before the async runtime".into());
    }
    acyclic_native_runtime::initialize_native_io_services()?;
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(16)
        .enable_all()
        .build()?)
}

/// Each scenario owns thread-affine state, so only its path and result cross
/// threads: every driver constructs, polls and drops its own scenarios.
fn provider_scenarios(protocol: &str) -> TestResult {
    let root = acyclic_test_support::crate_path!("tests/scenarios");
    let mut paths = Vec::new();
    scenario_data::discover(&root.join("providers").join(protocol), &mut paths)?;
    paths.sort();
    assert!(!paths.is_empty());
    let capacity = scenario_worker_budget();
    let (services, drivers) = case_workers::layout(capacity, paths.len());
    let runtime = scenario_runtime(services)?;
    let mut guard = progress_guard::Guard::new();
    let io = guard.io.clone();
    let registry = guard.registry.clone();
    let root = &root;
    let results = case_workers::run(
        &runtime,
        paths,
        capacity,
        drivers,
        &mut guard,
        |path| {
            stall::run(io.clone(), registry.clone(), path.clone(), async move {
                let mut value = scenario_data::load_value(root, &path)?;
                assert_eq!(
                    value.as_object_mut().ok_or("scenario")?.remove("driver"),
                    Some("provider".into())
                );
                let case = serde_json::from_value(value)?;
                providers::run(case)
                    .await
                    .map_err(|error| format!("{}: {error}", path.display()))?;
                println!("PASS {}", path.strip_prefix(root)?.display());
                TestResult::Ok(())
            })
        },
        |_| {},
    )?;
    results.into_iter().collect()
}

macro_rules! provider_suites {
    ($($protocol:ident),+ $(,)?) => {
        $(
            #[test]
            fn $protocol() -> TestResult {
                provider_scenarios(stringify!($protocol))
            }
        )+
    };
}

provider_suites!(anthropic, chat, gemini, responses);

#[path = "live_cache.rs"]
mod live_cache;
#[path = "prefix_cache_cases.rs"]
mod prefix_cache_cases;
