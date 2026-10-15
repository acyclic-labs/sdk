use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct MarkDropped(Arc<AtomicBool>);
impl Drop for MarkDropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn grandchildren_retain_the_original_cancellation_parent() {
    let root = TaskGroup::new(2, None).unwrap();
    let parent = root.child(2, None).unwrap();
    let grandchild = parent.child(1, None).unwrap();
    drop(parent);
    root.cancel();
    assert!(matches!(
        grandchild.spawn(OperationId::new(), async { 1_u64 }).await,
        Admission::Rejected { .. }
    ));
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_groups_retain_parent_vector_custody_until_drop() -> crate::Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget {
        allocation_operations: 64,
        peak_allocation_bytes: 65_536,
        ..Default::default()
    };
    let root = TaskGroup::new(2, None).unwrap();
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).map_err(|failure| {
            crate::Error::NativeWork {
                cause: failure.0,
                custody: None,
            }
        })?;
    let baseline = work.live_allocation_bytes();
    let parent = root.child(2, Some(&work))?;
    let grandchild = parent.child(1, Some(&work))?;
    drop(parent);
    root.cancel();
    assert!(matches!(
        grandchild.spawn(OperationId::new(), async { 1_u64 }).await,
        Admission::Rejected { .. }
    ));
    drop(grandchild);
    assert!(work.live_allocation_bytes() > baseline);
    drop(root);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn named_admission_rejects_active_duplicates_and_allows_settled_retry() {
    let group = TaskGroup::new(2, None).unwrap();
    let operation = OperationId::new();
    let Admission::Accepted(handle) = group.spawn(operation, std::future::pending::<u64>()).await
    else {
        panic!("first admission rejected")
    };
    assert_eq!(*handle.id(), operation);
    assert!(matches!(
        group.spawn(operation, async { 99_u64 }).await,
        Admission::Rejected { .. }
    ));
    handle.cancel();
    assert_eq!(handle.result().await, Outcome::Cancelled);
    let Admission::Accepted(retried) = group.spawn(operation, async { 7_u64 }).await else {
        panic!("settled retry rejected")
    };
    assert_eq!(*retried.id(), operation);
    assert_eq!(retried.result().await, Outcome::Succeeded(7));
    group.close();
    assert!(matches!(
        group.spawn(operation, async { 9_u64 }).await,
        Admission::Rejected { .. }
    ));
}

#[tokio::test]
async fn dropping_an_owned_wait_cancels_only_its_task() -> Result<(), Box<dyn std::error::Error>> {
    let group = TaskGroup::new(2, None).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let (started, observed) = tokio::sync::oneshot::channel();
    let Admission::Accepted(handle) = group
        .spawn(OperationId::new(), async move {
            let _guard = MarkDropped(flag);
            let _ = started.send(());
            std::future::pending::<u64>().await
        })
        .await
    else {
        panic!("task rejected");
    };
    observed.await?;
    let Admission::Accepted(other) = group
        .spawn(OperationId::new(), std::future::pending::<u64>())
        .await
    else {
        panic!("task rejected");
    };
    // Cancellation must also work when the returned wait was never polled.
    drop(handle.result());
    for _ in 0..16 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(dropped.load(Ordering::SeqCst));
    let abort_other = other.abort_handle();
    let other_result = other.result();
    futures::pin_mut!(other_result);
    assert!(futures::poll!(&mut other_result).is_pending());
    abort_other.abort();
    assert_eq!(other_result.await, Outcome::Cancelled);
    let Admission::Accepted(next) = group.spawn(OperationId::new(), async { 11_u64 }).await else {
        panic!("task rejected");
    };
    assert_eq!(next.result().await, Outcome::Succeeded(11));
    Ok(())
}

#[tokio::test]
async fn cancellation_is_explicit() {
    let Admission::Accepted(handle) = TaskGroup::new(1, None)
        .unwrap()
        .spawn(OperationId::new(), std::future::pending::<u64>())
        .await
    else {
        panic!("task rejected");
    };
    handle.cancel();
    assert_eq!(handle.result().await, Outcome::Cancelled);
}

#[tokio::test]
async fn first_success_observes_without_implicitly_cancelling_losers()
-> Result<(), Box<dyn std::error::Error>> {
    let dropped = Arc::new(AtomicBool::new(false));
    let (started, observed) = tokio::sync::oneshot::channel();
    let flag = Arc::clone(&dropped);
    let group = TaskGroup::new(2, None).unwrap();
    let Admission::Accepted(loser) = group
        .spawn(OperationId::new(), async move {
            let _guard = MarkDropped(flag);
            let _ = started.send(());
            std::future::pending::<u64>().await
        })
        .await
    else {
        panic!("task rejected");
    };
    observed.await?;
    let Admission::Accepted(winner) = group.spawn(OperationId::new(), async { 7_u64 }).await else {
        panic!("task rejected");
    };
    assert_eq!(first_success(vec![loser, winner]).await, Ok(7));
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(!dropped.load(Ordering::SeqCst));
    group.cancel();
    for _ in 0..16 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(dropped.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn ordered_reduction_uses_admission_order() -> Result<(), Box<dyn std::error::Error>> {
    let group = TaskGroup::new(2, None).unwrap();
    let (release, wait) = tokio::sync::oneshot::channel();
    let Admission::Accepted(first) = group
        .spawn(OperationId::new(), async move {
            let _ = wait.await;
            1_u64
        })
        .await
    else {
        panic!("task rejected");
    };
    let Admission::Accepted(second) = group.spawn(OperationId::new(), async { 2_u64 }).await else {
        panic!("task rejected");
    };
    release.send(()).map_err(|_| "first task stopped waiting")?;
    let values = ordered_reduce(vec![first, second], Vec::new(), |mut values, outcome| {
        values.push(outcome);
        values
    })
    .await;
    assert_eq!(values, vec![Outcome::Succeeded(1), Outcome::Succeeded(2)]);
    Ok(())
}
