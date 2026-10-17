use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_task_can_progress_before_its_workspace_polls_the_join() -> Result<()> {
    let (started, observed) = std::sync::mpsc::sync_channel(0);
    let worker = start(async move {
        started.send(()).unwrap();
    });
    // Deliberately do not poll the returned join: the old inline driver could
    // not start any task while discovery or a sibling's poll occupied it.
    let progressed = observed.recv_timeout(std::time::Duration::from_secs(1));
    drop(observed);
    worker.await?;
    assert!(
        progressed.is_ok(),
        "task still needs its workspace to poll it"
    );
    Ok(())
}
