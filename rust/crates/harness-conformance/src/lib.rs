//! Substrate-parametric execution of the unchanged harness scenario corpus.
//! Expectations remain on the client; hosts receive only fixture inputs.

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

mod composer;
mod native_schema;

#[path = "../../harness/tests/support/scenario_data.rs"]
mod case_data;
#[path = "../../harness/tests/support/discovery.rs"]
mod discovery;
mod execution;
#[path = "../../harness/tests/e2e/expectations.rs"]
mod expectations;
#[path = "../../harness/tests/support/ordered_matcher.rs"]
mod ordered_matcher;
#[path = "../../harness/tests/e2e/scenario.rs"]
mod scenario;
#[path = "../../harness/tests/support/scenario_matchers.rs"]
mod scenario_matchers;
mod matchers {
    pub use crate::{ordered_matcher::ordered, scenario_matchers::subset};
}

pub use composer::Capture as ComposerObservation;
pub use execution::observation;
/// Raw outputs from each portable driver, without a host-side verdict.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "driver", content = "observation", rename_all = "snake_case")]
pub enum CaseObservation {
    /// Stock executor observations.
    Executor(observation::Observation),
    /// Command catalog, completion, mention and fs observations.
    Composer(ComposerObservation),
}

/// Collects raw fixture observations using the host's resolved providers.
pub async fn collect_on<P, A, O>(
    recipe: serde_json::Value,
    paths: Vec<String>,
    filesystem: acyclic_fs::Fs<A, O>,
    stream: std::sync::Arc<P>,
) -> TestResult<CaseObservation>
where
    P: acyclic_stream::StreamProvider,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    match recipe.get("driver").and_then(serde_json::Value::as_str) {
        None | Some("executor") => Ok(CaseObservation::Executor(
            observation::collect_on(recipe, paths, filesystem, stream).await?,
        )),
        Some("composer") => Ok(CaseObservation::Composer(
            composer::collect(recipe, paths, filesystem, stream).await?,
        )),
        _ => Err("unknown portable fixture driver".into()),
    }
}
/// Corpus discovery and inherited-default resolution used by the native suite.
pub mod scenario_data {
    use std::path::{Path, PathBuf};
    /// Appends every JSON case under `root`, excluding `_defaults.json`.
    pub fn discover(root: &Path, paths: &mut Vec<PathBuf>) -> std::io::Result<()> {
        super::case_data::discover(root, paths)
    }
    /// Resolves platform overrides and inherited defaults without changing cases.
    pub fn load_value(root: &Path, path: &Path) -> Result<serde_json::Value, String> {
        super::case_data::load_value(root, path)
    }
}
/// Deterministic case accounting and client-side assertions.
pub mod runner;
/// Public hosting and immutable-volume adapters.
pub mod substrate;

/// Original direct driver, retained as an independent parity oracle.
pub async fn check_local(recipe: serde_json::Value) -> TestResult {
    execution::run(scenario::Scenario::parse(&serde_json::to_vec(&recipe)?)?).await
}

/// Native composer driver, retained as the direct parity oracle.
pub async fn check_local_composer(recipe: serde_json::Value) -> TestResult {
    composer::Composer::parse(recipe)?.run().await
}

/// Original journal trace, available for detailed cross-substrate comparisons.
pub async fn local_trace(recipe: serde_json::Value) -> TestResult<Vec<serde_json::Value>> {
    let outcome = execution::run_on(
        scenario::Scenario::from_value(recipe)?,
        std::sync::Arc::new(acyclic_stream::MemoryStream::default()),
    )
    .await?;
    outcome.trace().await
}
