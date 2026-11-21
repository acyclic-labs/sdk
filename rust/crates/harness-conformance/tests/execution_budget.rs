//! Scheduling consumes no execution fuel; only the shared executor spends steps.
use acyclic_harness_conformance::{
    CaseObservation, runner, scenario_data,
    substrate::{self, Substrate},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    runtime::{Builder, Handle},
    sync::OnceCell,
    task::JoinSet,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn corpus() -> PathBuf {
    acyclic_test_support::source_path("rust/crates/harness-e2e/tests/scenarios")
}

struct Descheduled {
    execution: Handle,
    descheduled: OnceCell<()>,
}

impl Substrate for Descheduled {
    fn capabilities(&self) -> BTreeSet<String> {
        substrate::Local.capabilities()
    }

    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, Result<CaseObservation>> {
        Box::pin(async move {
            self.descheduled
                .get_or_init(|| async {
                    // Stall admission once, past the old 55-second corpus deadline.
                    tokio::time::advance(Duration::from_secs(60)).await;
                })
                .await;
            // Only the runner sees the simulated stall. Fixture work stays on a
            // regular executor, with task custody retained across cancellation.
            let mut tasks = JoinSet::new();
            tasks.spawn_on(
                async move { substrate::Local.execute(recipe, paths).await },
                &self.execution,
            );
            tasks.join_next().await.ok_or("fixture task missing")??
        })
    }
}

#[test]
fn complete_corpus_verdict_does_not_depend_on_elapsed_time() -> Result {
    let execution = Builder::new_multi_thread().enable_all().build()?;
    let clock = Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?;
    let root = corpus();
    let mut paths = Vec::new();
    scenario_data::discover(&root, &mut paths)?;
    let report = clock.block_on(async {
        let start = tokio::time::Instant::now();
        let substrate = Arc::new(Descheduled {
            execution: execution.handle().clone(),
            descheduled: OnceCell::new(),
        });
        let parallelism = std::thread::available_parallelism()?.get();
        let report = runner::run(&root, substrate, None, parallelism).await?;
        assert!(start.elapsed() >= Duration::from_secs(60));
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(report)
    })?;
    println!(
        "{} passed, {} failed, {} declared native capabilities",
        report.passed, report.failed, report.skipped
    );
    assert_eq!(report.records.len(), paths.len());
    let failures: Vec<_> = report
        .records
        .iter()
        .filter(|record| record.status == runner::Status::Fail)
        .collect();
    assert!(
        report.success(),
        "{} failures: {failures:?}",
        failures.len(),
    );
    Ok(())
}
