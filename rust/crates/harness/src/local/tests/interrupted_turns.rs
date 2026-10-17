//! Interrupted head turns settle or resume deterministically after reopen.
use super::*;
use crate::TaskId;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Blocking {
    executed: Arc<AtomicUsize>,
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ToolExecutor for Blocking {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.executed.fetch_add(1, Ordering::SeqCst);
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        Box::pin(futures::future::pending())
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn blocking(executed: &Arc<AtomicUsize>) -> (Tool, tokio::sync::oneshot::Receiver<()>) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut tool = tool("test.blocking");
    tool.executor = Arc::new(Blocking {
        executed: Arc::clone(executed),
        started: Mutex::new(Some(sender)),
    });
    (tool, receiver)
}

fn call() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
            "blocking".into(),
            "test.blocking".into(),
            json!({}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

fn admitted(
    admission: Admission<crate::runtime::RuntimeTask<crate::executor::TurnOutput>>,
) -> TaskId {
    match admission {
        Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) => task_id,
        Admission::Rejected { reason } => panic!("rejected: {reason}"),
        _ => panic!("not durably admitted"),
    }
}

fn rejected(
    admission: &Admission<crate::runtime::RuntimeTask<crate::executor::TurnOutput>>,
) -> bool {
    matches!(admission, Admission::Rejected { reason } if reason == "local agent already has an admitted turn")
}

async fn outcome(
    harness: &LocalHarness,
    task: TaskId,
) -> Option<crate::Outcome<serde_json::Value>> {
    harness.runtime.task_host().outcome(task).await.unwrap()
}

struct Killed {
    directory: tempfile::TempDir,
    project: VolumeRef,
    executed: Arc<AtomicUsize>,
    provider: Arc<ScriptedModelProvider>,
    task: TaskId,
}

impl Killed {
    async fn reopen(&self) -> Result<LocalHarness> {
        let (tool, _) = blocking(&self.executed);
        LocalHarness::open(
            volume_config(
                self.directory.path(),
                self.project.clone(),
                self.provider.clone(),
            )?
            .tools(vec![tool]),
        )
        .await
    }
}

/// Kills the owning process while the first turn's tool effect is in flight.
async fn killed_mid_tool() -> Result<Killed> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let executed = Arc::new(AtomicUsize::new(0));
    let provider = scripted([call(), ScriptedTurn::Text("after".into())]);
    let (tool, started) = blocking(&executed);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project.clone(), provider.clone())?.tools(vec![tool]),
    )
    .await?;
    let task = admitted(harness.submit(OperationId::new(), "start").await?);
    match futures::future::select(harness.resume(task), started).await {
        futures::future::Either::Right((Ok(()), execution)) => drop(execution),
        _ => panic!("no tool boundary"),
    }
    drop(harness);
    Ok(Killed {
        directory,
        project,
        executed,
        provider,
        task,
    })
}

#[tokio::test]
async fn next_prompt_settles_a_killed_tool_turn_without_running_it_again() -> Result<()> {
    let killed = killed_mid_tool().await?;
    let reopened = killed.reopen().await?;
    // Negative control: the reopened head is unsettled, which bricked admission.
    assert_eq!(outcome(&reopened, killed.task).await, None);
    let next = admitted(reopened.submit(OperationId::new(), "next").await?);
    assert_eq!(
        outcome(&reopened, killed.task).await,
        Some(crate::Outcome::Cancelled)
    );
    assert_eq!(reopened.resume(next).await?.text, "after");
    assert!(matches!(
        reopened.resume(killed.task).await,
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled
        ))
    ));
    assert_eq!(killed.executed.load(Ordering::SeqCst), 1, "tool ran again");
    assert_eq!(killed.provider.requests().len(), 2, "model reissued");
    Ok(())
}

#[tokio::test]
async fn a_driven_turn_is_never_superseded_until_its_driver_is_gone() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let executed = Arc::new(AtomicUsize::new(0));
    let provider = scripted([call(), ScriptedTurn::Text("after".into())]);
    let (tool, started) = blocking(&executed);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?.tools(vec![tool]),
    )
    .await?;
    let task = admitted(harness.submit(OperationId::new(), "start").await?);
    // A submitted turn belongs to its submitter until it drives it.
    assert!(rejected(
        &harness.submit(OperationId::new(), "early").await?
    ));
    let mut driving = harness.resume(task);
    match futures::future::select(driving.as_mut(), started).await {
        futures::future::Either::Right((Ok(()), _)) => {}
        _ => panic!("no tool boundary"),
    }
    assert!(rejected(
        &harness.submit(OperationId::new(), "while driven").await?
    ));
    assert_eq!(outcome(&harness, task).await, None);
    // Dropping the driver in-process orphans the turn exactly like a kill.
    drop(driving);
    let next = admitted(harness.submit(OperationId::new(), "next").await?);
    assert_eq!(
        outcome(&harness, task).await,
        Some(crate::Outcome::Cancelled)
    );
    assert_eq!(harness.resume(next).await?.text, "after");
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn an_indeterminate_resume_releases_its_reservation_to_the_next_prompt() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let (started, boundary) = tokio::sync::oneshot::channel();
    let started = Mutex::new(Some(started));
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("next".into()),
        ])
        .with_context_capacity(capacity())
        .with_dispatch_observer(move |_| {
            if let Some(started) = started.lock().unwrap().take() {
                started.send(()).unwrap();
            }
        }),
    );
    let harness = LocalHarness::open(volume_config(
        directory.path(),
        project.clone(),
        provider.clone(),
    )?)
    .await?;
    let task = admitted(harness.submit(OperationId::new(), "start").await?);
    match futures::future::select(harness.resume(task), boundary).await {
        futures::future::Either::Right((Ok(()), execution)) => drop(execution),
        _ => panic!("no model boundary"),
    }
    drop(harness);
    let reopened =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    // Resume cannot recover the unrecorded model response: the turn stays
    // Indeterminate and its reservation is retained for reconciliation.
    assert!(matches!(
        reopened.resume(task).await,
        Err(Error::Indeterminate(_))
    ));
    assert!(matches!(
        outcome(&reopened, task).await,
        Some(crate::Outcome::Indeterminate { .. })
    ));
    let next = admitted(reopened.submit(OperationId::new(), "next").await?);
    assert_eq!(
        outcome(&reopened, task).await,
        Some(crate::Outcome::Cancelled)
    );
    // With concurrency 1 this completes only if settlement released the reservation.
    assert_eq!(reopened.resume(next).await?.text, "next");
    assert_eq!(
        provider.requests().len(),
        2,
        "interrupted model call was reissued"
    );
    Ok(())
}
