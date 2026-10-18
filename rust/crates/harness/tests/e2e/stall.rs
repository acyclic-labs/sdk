//! Task-local attribution backed by the runner-owned, modelled registry.
use super::progress_registry::{Registration, Registry};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};
tokio::task_local! { static CURRENT: Registration; }

async fn admitted<F: Future>(registry: Arc<Registry>, path: &Path, scenario: F) -> F::Output {
    let mut scenario = std::pin::pin!(scenario);
    let mut wakers: Option<(std::task::Waker, std::task::Waker)> = None;
    let observed = std::future::poll_fn(|cx| {
        CURRENT.with(|registration| {
            // Cache the wrapper while the executor's parent waker is unchanged.
            if wakers
                .as_ref()
                .is_none_or(|(parent, _)| !parent.will_wake(cx.waker()))
            {
                wakers = Some((cx.waker().clone(), registration.observed_waker(cx.waker())));
            }
            let _polling = registration.polling();
            scenario.as_mut().poll(&mut std::task::Context::from_waker(
                &wakers.as_ref().unwrap().1,
            ))
        })
    });
    CURRENT.scope(registry.register(path), observed).await
}

pub(super) async fn waiting<F: Future>(on: &'static str, future: F) -> F::Output {
    let _waiting = CURRENT
        .try_with(|registration| registration.waiting(on))
        .ok();
    future.await
}

pub(super) async fn run<F>(
    io: acyclic_native_runtime::testing::Schedule,
    registry: Arc<Registry>,
    path: PathBuf,
    scenario: F,
) -> Result<F::Output, tokio::task::JoinError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task = io
        .scope(async move {
            acyclic_native_runtime::testing::spawn(async move {
                admitted(registry, &path, scenario).await
            })
        })
        .await;
    // A raw JoinHandle detaches on drop. The runner must abort the actual
    // tracked task when its shared guard drops this waiting caller.
    struct Abort(tokio::task::AbortHandle);
    impl Drop for Abort {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _abort = Abort(task.abort_handle());
    task.await
}

#[tokio::test(start_paused = true)]
async fn permanently_stalled_follow_is_rejected_without_external_termination() {
    let mut guard = super::progress_guard::Guard::new();
    let registry = guard.registry.clone();
    let stalled = run(
        guard.io.clone(),
        registry.clone(),
        PathBuf::from("negative-control/stalled-follow.json"),
        async {
            waiting("status follow", std::future::pending::<()>()).await;
        },
    );
    let error = guard
        .run(stalled)
        .await
        .expect_err("accepted a permanently missing follow");
    assert_eq!(
        error,
        "stalled: negative-control/stalled-follow.json at status follow"
    );
    assert!(
        registry.snapshot().1.is_empty(),
        "cancelled caller retained a registration"
    );
}

#[tokio::test(start_paused = true)]
async fn panic_and_abort_retire_the_actual_task_local_registration() {
    for panic in [false, true] {
        let registry = Arc::new(Registry::default());
        let worker = registry.clone();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(admitted(worker, Path::new("cancelled"), async move {
            waiting("status follow", async move {
                entered.send(()).unwrap();
                if panic {
                    panic!("scenario panic control");
                }
                std::future::pending::<()>().await;
            })
            .await;
        }));
        ready.await.unwrap();
        if !panic {
            task.abort();
        }
        let error = task.await.unwrap_err();
        assert_eq!(error.is_panic(), panic);
        assert!(registry.snapshot().1.is_empty());
    }
}

#[path = "../support/progress_runnable_tests.rs"]
mod runnable_tests;

#[tokio::test]
async fn completed_or_cancelled_waits_restore_the_current_operation() {
    let registry = Arc::new(Registry::default());
    admitted(registry.clone(), Path::new("labels"), async {
        waiting("completed follow", std::future::ready(())).await;
        assert_eq!(registry.snapshot().1[0].1, "admitted");
        let mut pending = Box::pin(waiting("cancelled follow", std::future::pending::<()>()));
        assert!(
            pending
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        assert_eq!(registry.snapshot().1[0].1, "cancelled follow");
        drop(pending);
        assert_eq!(registry.snapshot().1[0].1, "admitted");
    })
    .await;
    assert!(registry.snapshot().1.is_empty());
}
