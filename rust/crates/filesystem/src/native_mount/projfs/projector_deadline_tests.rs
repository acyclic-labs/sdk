use super::*;
use crate::deadline_test_support::paused_clock_oracle;

async fn settlement_oracle(control: bool) {
    paused_clock_oracle(
        control,
        (true, true, 1, vec![HELD_SETTLE_LIMIT]),
        |clock| async move {
            let projector = clock.enter(Projector::new);
            lock_recover(&projector.state).pending.insert(
                MountPath::root().child(b"held".to_vec()),
                WrittenEntry {
                    owned: None,
                    facts: None,
                    basis: None,
                },
            );
            let (projector, typed, unlocked) = std::thread::spawn(move || {
                let error = projector.settle().unwrap_err();
                let typed = matches!(error, NativeMountError::Driver(ref cause)
                if cause.contains("superseded placeholder is held open"));
                let unlocked = projector.state.try_lock().is_ok();
                (projector, typed, unlocked)
            })
            .join()
            .unwrap();
            let pending = lock_recover(&projector.state).pending.len();
            (typed, unlocked, pending, clock.advances())
        },
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn held_placeholder_settlement_expires_on_its_captured_clock_after_scope_exit() {
    settlement_oracle(false).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "host deadline escaped virtual time")]
async fn held_placeholder_restored_host_deadline_negative_control() {
    settlement_oracle(true).await;
}
