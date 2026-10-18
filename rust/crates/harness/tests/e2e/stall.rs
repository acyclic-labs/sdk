//! Task-local attribution backed by the runner-owned, modelled registry.
use super::progress_registry::{Registration, Registry};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};
tokio::task_local! { static CURRENT: Registration; }

async fn admitted<F: Future>(registry: Arc<Registry>, path: &Path, scenario: F) -> F::Output {
    CURRENT.scope(registry.register(path), scenario).await
}

pub(super) fn waiting(on: &'static str) {
    let _ = CURRENT.try_with(|registration| registration.waiting(on));
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
            waiting("status follow");
            std::future::pending::<()>().await;
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
            waiting("status follow");
            entered.send(()).unwrap();
            if panic {
                panic!("scenario panic control");
            }
            std::future::pending::<()>().await;
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
