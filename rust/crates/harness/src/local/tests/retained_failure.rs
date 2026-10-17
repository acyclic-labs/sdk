use super::*;

#[tokio::test]
async fn resume_preserves_a_retained_failure_without_dispatching_the_model() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id: task, .. }) =
        harness.submit(operation, "retained failure").await?
    else {
        return Err(Error::Conflict("stock admission rejected".into()));
    };
    let worker = crate::distributed::Worker {
        id: "failure-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        return Err(Error::Conflict("task was not claimed".into()));
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    harness
        .runtime
        .task_host()
        .settle_task(
            task,
            &crate::scheduler::LeaseFence::from(&lease.reservation),
            crate::Outcome::Failed {
                message: "retained diagnostic".into(),
            },
            &mut None,
        )
        .await?;
    assert!(matches!(harness.resume(task).await,
        Err(Error::TaskFailed { task: failed, message }) if failed == task && message == "retained diagnostic"));
    assert_eq!(provider.requests().len(), 0);
    Ok(())
}
