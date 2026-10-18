use std::future::Future;
#[tokio::test(start_paused = true)]
async fn a_runnable_scenario_may_be_delayed_beyond_the_idle_budget() {
    let mut guard = crate::progress_guard::Guard::new();
    let registry = guard.registry.clone();
    let mut scenario = std::pin::pin!(super::admitted(
        registry.clone(),
        std::path::Path::new("delayed-ready"),
        async {
            tokio::task::yield_now().await;
            42
        }
    ));
    // Adversarial executor: poll the actual scenario wrapper once, then retain
    // its scheduled wake for twenty virtual seconds before polling it again.
    assert!(
        scenario
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    let value = guard
        .run(async {
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            scenario.await
        })
        .await
        .unwrap();
    assert_eq!(value, 42);
    assert!(registry.snapshot().1.is_empty());
}
