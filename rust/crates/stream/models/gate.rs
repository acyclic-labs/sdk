#[test]
fn admission_registration_cannot_lose_a_wakeup() {
    registration(0);
}
#[test]
fn replacing_waiter_retires_previous_outside_lock() {
    loom::model(|| {
        let gate = super::Arc::new(Gate::new());
        let inspected = super::Arc::clone(&gate);
        super::waiter_replacement(
            |wake| gate.poll_changed(false, &mut Context::from_waker(wake)),
            || gate.set(true).unwrap(),
            move || inspected.state.try_lock().is_ok(),
        );
    });
}
#[test]
#[should_panic(expected = "a parked follower must be woken")]
fn admission_negative_control_omits_notification() {
    registration(1);
}
fn registration(broken: u8) {
    loom::model(move || {
        let gate = super::Arc::new(Gate::new());
        let probe = super::Arc::clone(&gate);
        let (count, wake) = super::wake_probe(move || {
            assert!(
                probe.state.try_lock().is_ok(),
                "wake must run outside the admission lock"
            );
        });
        let reader = super::Arc::clone(&gate);
        let reader = loom::thread::spawn(move || {
            reader
                .poll_changed(false, &mut Context::from_waker(&wake))
                .is_ready()
        });
        if broken != 0 {
            let mut state = gate.state.lock().unwrap();
            state.0 = true;
            let wake = state.1.take();
            if broken == 2
                && let Some(wake) = wake
            {
                wake.wake();
            }
        } else {
            gate.set(true).unwrap();
        }
        let ready = reader.join().unwrap();
        assert!(
            ready || count.0.load(super::Ordering::SeqCst) > 0,
            "a parked follower must be woken"
        );
        assert!(
            gate.poll_changed(
                false,
                &mut Context::from_waker(&futures::task::noop_waker())
            )
            .is_ready()
        );
    });
}

#[test]
#[should_panic(expected = "wake must run outside the admission lock")]
fn notification_negative_control_wakes_while_locked() {
    registration(2);
}
