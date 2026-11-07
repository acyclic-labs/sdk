use super::*;
use crate::probe::probe;
use loom::sync::atomic::{AtomicBool, Ordering};
use std::future::Future;

fn snapshot_race(broken: bool) {
    loom::model(move || {
        let changed = Change::new();
        let mut subscription = changed.subscribe().unwrap();
        let value = Arc::new(AtomicBool::new(false));
        let reading = Arc::clone(&value);
        let (count, wake) = probe();
        let poll_wake = wake.clone();
        let consumer = loom::thread::spawn(move || {
            let mut next = Box::pin(async move {
                let snapshot = async || {
                    let found = reading.load(Ordering::SeqCst);
                    loom::thread::yield_now();
                    Ok(found.then_some(7))
                };
                if broken {
                    loop {
                        let value = snapshot().await?;
                        subscription.mark()?; // Deliberately acknowledge after reading.
                        if let Some(value) = value {
                            return Ok(value);
                        }
                        subscription.changed().await?;
                    }
                } else {
                    subscription.next(snapshot).await
                }
            });
            let first = next.as_mut().poll(&mut Context::from_waker(&poll_wake));
            (next, first)
        });
        value.store(true, Ordering::SeqCst);
        changed.notify().unwrap();
        let (mut next, first) = consumer.join().unwrap();
        if first.is_pending() {
            assert!(
                count.count.load(Ordering::SeqCst) > 0,
                "snapshot handoff must not lose a publication wakeup"
            );
            assert_eq!(
                next.as_mut().poll(&mut Context::from_waker(&wake)),
                Poll::Ready(Ok(7))
            );
        } else {
            assert_eq!(first, Poll::Ready(Ok(7)));
        }
    });
}
#[test]
fn snapshot_then_follow_has_no_lost_wakeup() {
    snapshot_race(false);
}
#[test]
#[should_panic(expected = "snapshot handoff must not lose a publication wakeup")]
fn snapshot_negative_control_marks_after_the_read() {
    snapshot_race(true);
}

fn cancellation_race(broken: bool) {
    loom::model(move || {
        let changed = Change::new();
        let mut cancelled = changed.subscribe().unwrap();
        let mut kept = changed.subscribe().unwrap();
        let (_, wake) = probe();
        let (count, kept_wake) = probe();
        assert!(
            cancelled
                .poll_changed(&mut Context::from_waker(&wake))
                .is_pending()
        );
        assert!(
            kept.poll_changed(&mut Context::from_waker(&kept_wake))
                .is_pending()
        );
        let removing = changed.clone();
        let cancel = loom::thread::spawn(move || {
            if broken {
                let old = std::mem::take(&mut removing.state.lock().unwrap().waiters);
                drop(old);
            }
            drop(cancelled);
        });
        changed.notify().unwrap();
        cancel.join().unwrap();
        assert_eq!(
            count.count.load(Ordering::SeqCst),
            1,
            "cancellation must preserve the independent subscription"
        );
        assert_eq!(
            kept.poll_changed(&mut Context::from_waker(&kept_wake)),
            Poll::Ready(Ok(()))
        );
        assert!(changed.state.lock().unwrap().waiters.is_empty());
    });
}
#[test]
fn cancellation_cannot_remove_another_subscribers_wakeup() {
    cancellation_race(false);
}
#[test]
#[should_panic(expected = "cancellation must preserve the independent subscription")]
fn cancellation_negative_control_clears_all_registrations() {
    cancellation_race(true);
}

fn notification(broken: bool) {
    loom::model(move || {
        let changed = Change::new();
        let mut subscription = changed.subscribe().unwrap();
        let (count, wake) = probe();
        assert!(
            subscription
                .poll_changed(&mut Context::from_waker(&wake))
                .is_pending()
        );
        if broken {
            let mut state = changed.state.lock().unwrap();
            state.revision = Some(1);
            // Catch inside the guard's scope so the deliberate assertion cannot
            // poison the mutex and abort later subscription cleanup.
            let violation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for wake in std::mem::take(&mut state.waiters).into_values() {
                    wake.wake();
                }
            }));
            drop(state);
            if let Err(violation) = violation {
                std::panic::resume_unwind(violation);
            }
        } else {
            changed.notify().unwrap();
        }
        assert_eq!(count.count.load(Ordering::SeqCst), 1);
    });
}
#[test]
fn publication_notifies_outside_its_lock() {
    notification(false);
}
#[test]
#[should_panic(expected = "wake must run outside the publication lock")]
fn notification_negative_control_wakes_while_locked() {
    notification(true);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn counters_never_wrap_or_reuse_a_subscription(
        revision in proptest::prop_oneof![proptest::prelude::any::<u64>(), (u64::MAX-2)..=u64::MAX],
        id in proptest::prop_oneof![proptest::prelude::any::<u64>(), (u64::MAX-2)..=u64::MAX],
    ) {
        loom::model(move || {
            let changed = Change::new();
            { let mut state = changed.state.lock().unwrap(); state.revision = Some(revision); state.next_id = id; }
            let first = changed.subscribe();
            assert_eq!(first.is_ok(), id < u64::MAX);
            if let Ok(first) = first {
                assert_eq!(first.id, id);
                if let Ok(second) = changed.subscribe() { assert!(second.id > first.id); }
            }
            assert_eq!(changed.notify().is_ok(), revision < u64::MAX);
            assert_eq!(changed.state.lock().unwrap().revision, revision.checked_add(1));
        });
    }
}
