//! Scheduling consumes no execution fuel; only the shared executor spends steps.
use acyclic_harness_conformance::{
    CaseObservation, runner, scenario_data,
    substrate::{self, Substrate},
};
use acyclic_stream::BoxProviderFuture;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn corpus() -> PathBuf {
    acyclic_test_support::source_path("rust/crates/harness/tests/scenarios")
}

struct Descheduled<S>(S);

impl<S: Substrate> Substrate for Descheduled<S> {
    fn capabilities(&self) -> BTreeSet<String> {
        self.0.capabilities()
    }

    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, Result<CaseObservation>> {
        Box::pin(async move {
            // A controlled scheduler stall crosses the old 55-second deadline
            // without a real sleep or depending on the machine's current load.
            tokio::time::advance(Duration::from_secs(60)).await;
            self.0.execute(recipe, paths).await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn complete_corpus_verdict_does_not_depend_on_elapsed_time() -> Result {
    let root = corpus();
    let mut paths = Vec::new();
    scenario_data::discover(&root, &mut paths)?;
    let report = runner::run(&root, Arc::new(Descheduled(substrate::Local)), None, 1).await?;
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
        "{} failures, first three: {:?}",
        failures.len(),
        failures.iter().take(3).collect::<Vec<_>>()
    );
    Ok(())
}
