//! Real waiter locks/parks, instrumented through the native-runtime sync shim.
use super::*;

fn notification(broken: bool) {
    loom::model(move || {
        let mut waiter = Waiter::new();
        waiter.forget_notification = broken;
        let waiter = Arc::new(waiter);
        let producer = waiter.clone();
        let thread = loom::thread::spawn(move || producer.notify());
        thread.join().unwrap();
        // The production latch must retain a notification that precedes park.
        assert!(
            waiter.state.lock().unwrap().notified,
            "notification was erased before park"
        );
        waiter.wait(None);
    });
}
#[test]
fn notifications_survive_the_complete_before_park_interleaving() {
    notification(false);
}
#[test]
#[should_panic(expected = "notification was erased before park")]
fn erased_notification_fails_the_same_oracle() {
    notification(true);
}

#[test]
fn predicate_notification_and_park_have_no_lost_wakeup() {
    loom::model(|| {
        let waiter = Arc::new(Waiter::new());
        // Predicate lock is released before waiting. The generation ticket is
        // captured before acquiring it; no waiter->predicate nesting exists.
        let predicate = Arc::new(Mutex::new(false));
        let producer = waiter.clone();
        let state = predicate.clone();
        let thread = loom::thread::spawn(move || {
            *state.lock().unwrap() = true;
            producer.notify_all();
        });
        loop {
            let generation = waiter.listen();
            if *predicate.lock().unwrap() {
                break;
            }
            waiter.wait_after(generation, None);
        }
        thread.join().unwrap();
    });
}

#[test]
fn notification_during_park_has_no_lost_wakeup() {
    loom::model(|| {
        let waiter = Arc::new(Waiter::new());
        let producer = waiter.clone();
        let thread = loom::thread::spawn(move || producer.notify());
        waiter.wait(None);
        thread.join().unwrap();
    });
}
