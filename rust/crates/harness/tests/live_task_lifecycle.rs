//! Public task lifetime semantics on the native executor and browser event loop.
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

use acyclic_harness::{Admission, OperationId, Outcome, live::TaskGroup};
use tokio::sync::oneshot;

struct DropSignal(Option<oneshot::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
async fn cancellation_releases_named_admission_and_queued_permit() {
    let group = TaskGroup::new(1);
    let operation = OperationId::new();
    let (started, observed) = oneshot::channel();
    let Admission::Accepted(first) = group
        .try_spawn_with_operation(operation, async move {
            let _ = started.send(());
            std::future::pending::<u64>().await
        })
        .await
    else {
        panic!("first admission rejected")
    };
    observed.await.expect("first task starts");
    assert!(matches!(
        group
            .try_spawn_with_operation(operation, async { 99_u64 })
            .await,
        Admission::Rejected { .. }
    ));
    let queued = group.spawn(async { 11_u64 }).await;
    first.cancel();
    assert_eq!(first.result().await, Outcome::Cancelled);
    assert_eq!(queued.result().await, Outcome::Succeeded(11));
    let Admission::Accepted(retry) = group
        .try_spawn_with_operation(operation, async { 7_u64 })
        .await
    else {
        panic!("settled operation retains active admission")
    };
    assert_eq!(retry.result().await, Outcome::Succeeded(7));
    group.close();
    assert!(matches!(
        group.spawn(async { 9_u64 }).await.result().await,
        Outcome::Failed { .. }
    ));
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
async fn unpolled_owned_wait_cancels_only_its_task() {
    let group = TaskGroup::new(2);
    let (dropped, observe_drop) = oneshot::channel();
    let (started, observed) = oneshot::channel();
    let task = group
        .spawn(async move {
            let _guard = DropSignal(Some(dropped));
            let _ = started.send(());
            std::future::pending::<u64>().await
        })
        .await;
    observed.await.expect("task starts");
    let (release, wait) = oneshot::channel();
    let sibling = group
        .spawn(async move { wait.await.expect("sibling is not cancelled") })
        .await;
    drop(task.result_owned());
    observe_drop
        .await
        .expect("owned wait drops its running future");
    release.send(13_u64).expect("sibling remains admitted");
    assert_eq!(sibling.result().await, Outcome::Succeeded(13));
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
async fn observation_detaches_but_parent_cancellation_reaches_descendants() {
    let parent = TaskGroup::new(1);
    let child = parent.child(1);
    let (dropped, mut observe_drop) = oneshot::channel();
    let (started, observed) = oneshot::channel();
    let task = child
        .spawn(async move {
            let _guard = DropSignal(Some(dropped));
            let _ = started.send(());
            std::future::pending::<u64>().await
        })
        .await;
    observed.await.expect("child starts");
    drop(task);
    assert!(matches!(
        observe_drop.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    parent.cancel();
    observe_drop.await.expect("parent cancels detached child");
    assert!(matches!(
        child.try_spawn(async { 3_u64 }).await,
        Admission::Rejected { .. }
    ));
    let cancelled_before_poll = TaskGroup::new(1)
        .spawn(std::future::pending::<u64>())
        .await;
    cancelled_before_poll.cancel();
    assert_eq!(cancelled_before_poll.result().await, Outcome::Cancelled);
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test::wasm_bindgen_test]
async fn browser_tasks_retain_event_loop_owned_future() {
    use std::{cell::Cell, rc::Rc};
    let value = Rc::new(Cell::new(0_u64));
    let retained = Rc::clone(&value);
    let (release, wait) = oneshot::channel();
    let task = TaskGroup::new(1)
        .spawn(async move {
            let value = wait.await.expect("caller releases browser-owned task");
            retained.set(value);
            retained.get()
        })
        .await;
    release.send(17_u64).expect("task retains receiver");
    assert_eq!(task.result().await, Outcome::Succeeded(17));
    assert_eq!(value.get(), 17);
}
