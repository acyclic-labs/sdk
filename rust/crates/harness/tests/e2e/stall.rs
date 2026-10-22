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
            let observed = match &mut wakers {
                Some((parent, observed)) if parent.will_wake(cx.waker()) => observed,
                stale => {
                    &mut stale
                        .insert((cx.waker().clone(), registration.observed_waker(cx.waker())))
                        .1
                }
            };
            let _polling = registration.polling();
            scenario
                .as_mut()
                .poll(&mut std::task::Context::from_waker(observed))
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
) -> F::Output
where
    F: Future,
{
    // Poll async drivers directly. Their Rc state stays on this event loop;
    // only physical native operations acquire native completion owners.
    io.scope(admitted(registry, &path, scenario)).await
}

pub(super) async fn guarded<F: Future>(path: &Path, future: F) -> Result<F::Output, String> {
    if CURRENT.try_with(|_| ()).is_ok() {
        return Ok(future.await);
    }
    let mut guard = super::progress_guard::Guard::new();
    let work = run(
        guard.io.clone(),
        guard.registry.clone(),
        path.into(),
        future,
    );
    guard.run(work).await
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

fn label(registry: &Registry) -> Option<&'static str> {
    registry.snapshot().1.first().map(|(_, label)| *label)
}

#[tokio::test]
async fn completed_or_cancelled_waits_restore_the_current_operation() {
    let registry = Arc::new(Registry::default());
    admitted(registry.clone(), Path::new("labels"), async {
        waiting("completed follow", std::future::ready(())).await;
        assert_eq!(label(&registry), Some("admitted"));
        let mut pending = Box::pin(waiting("cancelled follow", std::future::pending::<()>()));
        assert!(
            pending
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        assert_eq!(label(&registry), Some("cancelled follow"));
        drop(pending);
        assert_eq!(label(&registry), Some("admitted"));
    })
    .await;
    assert!(registry.snapshot().1.is_empty());
}
