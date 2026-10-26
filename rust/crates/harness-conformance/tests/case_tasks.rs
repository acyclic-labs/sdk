//! Case admission owns runnable work and aborts it with the report future.
use acyclic_harness_conformance::{
    CaseObservation, runner,
    substrate::{self, Substrate},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn corpus() -> Result<tempfile::TempDir> {
    let directory = tempfile::tempdir()?;
    for name in ["first.json", "second.json"] {
        std::fs::write(
            directory.path().join(name),
            include_bytes!("../../harness/tests/scenarios/core/text.json"),
        )?;
    }
    Ok(directory)
}

struct Observed {
    tasks: Arc<Mutex<BTreeSet<tokio::task::Id>>>,
}

impl Substrate for Observed {
    fn capabilities(&self) -> BTreeSet<String> {
        substrate::Local.capabilities()
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, Result<CaseObservation>> {
        Box::pin(async move {
            self.tasks
                .lock()
                .map_err(|_| "task observations poisoned")?
                .insert(tokio::task::try_id().ok_or("case owns a task")?);
            substrate::Local.execute(recipe, paths).await
        })
    }
}

#[tokio::test]
async fn admitted_cases_own_distinct_tasks_and_keep_every_record() -> Result {
    let directory = corpus()?;
    let tasks = Arc::new(Mutex::new(BTreeSet::new()));
    let report = runner::run(
        directory.path(),
        Arc::new(Observed {
            tasks: tasks.clone(),
        }),
        None,
        2,
    )
    .await?;
    assert!(report.success(), "{report:?}");
    assert_eq!(report.records.len(), 2);
    assert_eq!(report.passed, 2);
    assert_eq!(
        tasks
            .lock()
            .map_err(|_| "task observations poisoned")?
            .len(),
        2,
        "admitted cases shared one parent poller"
    );
    assert_eq!(
        report
            .records
            .iter()
            .map(|record| record.case.as_str())
            .collect::<Vec<_>>(),
        ["first.json", "second.json"]
    );
    Ok(())
}

struct Pending {
    active: Arc<AtomicUsize>,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    stopped: tokio::sync::mpsc::UnboundedSender<()>,
}

struct OwnedCase<'a>(&'a Pending);
impl Drop for OwnedCase<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        let _ = self.0.stopped.send(());
    }
}

impl Substrate for Pending {
    fn capabilities(&self) -> BTreeSet<String> {
        substrate::Local.capabilities()
    }
    fn execute(&self, _: Value, _: Vec<String>) -> BoxProviderFuture<'_, Result<CaseObservation>> {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _owned = OwnedCase(self);
            self.started
                .send(())
                .map_err(|_| "admission observation receiver dropped")?;
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn cancelling_the_report_aborts_every_admitted_case() -> Result {
    let directory = corpus()?;
    let root = directory.path().to_path_buf();
    let active = Arc::new(AtomicUsize::new(0));
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let (stopped, mut stops) = tokio::sync::mpsc::unbounded_channel();
    let substrate = Arc::new(Pending {
        active: active.clone(),
        started,
        stopped,
    });
    let report = tokio::spawn(async move { runner::run(&root, substrate, None, 2).await });
    for _ in 0..2 {
        starts.recv().await.ok_or("case was never admitted")?;
    }
    assert_eq!(active.load(Ordering::SeqCst), 2);
    report.abort();
    assert!(
        report
            .await
            .expect_err("report cancellation")
            .is_cancelled()
    );
    for _ in 0..2 {
        stops
            .recv()
            .await
            .ok_or("case outlived report cancellation")?;
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    Ok(())
}
