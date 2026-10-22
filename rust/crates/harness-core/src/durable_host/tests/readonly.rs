//! Protected readonly owners of tasks that never prepared a native view.
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

fn work(cancellation: &CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[tokio::test]
async fn an_unprepared_task_opens_readonly_from_its_retained_or_released_fence() -> Result<()> {
    let f = fixture::fixture().await?;
    let cancellation = CancellationToken::new();
    let task = TaskId::from_bytes([1; 16]);

    // Admitted but never started: the admitted reservation has no started origin.
    f.host.admit(f.admission(1, json!(0))).await?;
    let unstarted = f
        .host
        .readonly_journal_owner(task, &work(&cancellation)?)
        .await;
    assert!(
        unstarted.is_err(),
        "an unstarted reservation is not an original fence"
    );

    let lease = f.claim(1, "original").await?;
    let original = crate::scheduler::LeaseFence::from(&lease.reservation);
    let running = f
        .host
        .readonly_journal_owner(task, &work(&cancellation)?)
        .await?;
    assert_eq!(running.task_binding(), (task, &original));
    drop(running);

    f.host
        .suspend_workflow(task, &original, (1, None), None, None, &mut None)
        .await?;
    let suspended = f
        .host
        .readonly_journal_owner(task, &work(&cancellation)?)
        .await?;
    assert_eq!(suspended.task_binding(), (task, &original));

    // A task the coordinator never declared has neither journal nor fence.
    let unknown = f
        .host
        .readonly_journal_owner(TaskId::from_bytes([2; 16]), &work(&cancellation)?)
        .await;
    assert!(unknown.is_err());
    Ok(())
}
