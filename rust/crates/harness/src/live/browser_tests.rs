use super::*;
use acyclic_stream::UnixMillisClock as _;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn browser_live_admission_rejection_and_owned_cancellation_need_no_tokio_runtime() {
    let group = TaskGroup::new(1, None).unwrap();
    let handle = group.spawn(async { 17_u32 }).await;
    assert!(matches!(handle.result().await, Outcome::Succeeded(17)));
    let pending = group.spawn(futures::future::pending::<u32>()).await;
    let original = *pending.id();
    let owned = pending.result();
    drop(owned);
    sleep_millis(1).await.unwrap();
    assert!(!group.state.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).active.contains_key(&original));
    group.close();
    let rejected = group.spawn(async { 18_u32 }).await;
    assert!(matches!(rejected.result().await, Outcome::Failed { .. }));
}

#[wasm_bindgen_test]
async fn browser_live_deadlines_use_actual_sdk_clock_and_cancel_the_wait() {
    let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
    assert!(now > 0);
    assert_eq!(timeout_until(now + 1_000, async { 19_u32 }).await.unwrap(), 19);
    assert!(timeout_until(now + 10, futures::future::pending::<u32>()).await.is_err());
    let calls = std::cell::Cell::new(0);
    assert!(timeout_until(now - 1, async { calls.set(calls.get() + 1); }).await.is_err());
    assert_eq!(calls.get(), 0);
}
