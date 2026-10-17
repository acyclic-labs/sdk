#[test]
fn delivery_registration_cannot_lose_a_wakeup() {
    registration(0);
}
#[test]
#[should_panic(expected = "a parked receiver must be woken")]
fn delivery_negative_control_omits_notification() {
    registration(1);
}
fn registration(broken: u8) {
    loom::model(move || {
        let signal = super::Arc::new(Signal::new());
        let probe = super::Arc::clone(&signal);
        let (count, wake) = super::wake_probe(move || {
            assert!(
                probe.state.try_lock().is_ok(),
                "wake must run outside the delivery lock"
            );
        });
        let reader = super::Arc::clone(&signal);
        let reader =
            loom::thread::spawn(move || match reader.poll(&mut Context::from_waker(&wake)) {
                Poll::Ready(value) => Some(value.unwrap()),
                Poll::Pending => None,
            });
        if broken != 0 {
            let mut state = signal.state.lock().unwrap();
            state.0.push_back(7_u8);
            let wake = state.1.take();
            if broken == 2 {
                if let Some(wake) = wake {
                    wake.wake();
                }
            }
        } else {
            signal.send(7_u8).unwrap();
        }
        let received = reader.join().unwrap();
        assert!(
            received.is_some() || count.0.load(super::Ordering::SeqCst) > 0,
            "a parked receiver must be woken"
        );
        assert_eq!(
            received.unwrap_or_else(|| loom::future::block_on(signal.next()).unwrap()),
            7
        );
    });
}

#[test]
#[should_panic(expected = "wake must run outside the delivery lock")]
fn notification_negative_control_wakes_while_locked() {
    registration(2);
}
