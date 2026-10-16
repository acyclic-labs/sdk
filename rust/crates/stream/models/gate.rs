#[test]
fn admission_registration_cannot_lose_a_wakeup() {
    registration(0);
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
            if broken == 2 {
                if let Some(wake) = wake {
                    wake.wake();
                }
            }
        } else {
            gate.set(true).unwrap();
        }
        let ready = reader.join().unwrap();
        assert!(
            ready || count.0.load(super::Ordering::SeqCst) > 0,
            "a parked follower must be woken"
        );
        assert!(gate.current().unwrap());
    });
}

#[test]
fn a_saturated_channel_cannot_block_an_independent_follow() {
    saturated(false);
}
#[test]
#[should_panic(expected = "the reserved slot must enable the healthy channel")]
fn saturation_negative_control_applies_one_global_gate() {
    saturated(true);
}
fn saturated(broken: bool) {
    use futures::{StreamExt, stream};
    use std::future::Future;
    for latest in [false, true] {
        loom::model(move || {
            let slow = super::Arc::new(Gate::new());
            let healthy = super::Arc::new(Gate::new());
            let records = super::Arc::new(super::signal::Signal::new());
            // Two blocked records leave exactly the reserved third slot.
            let room = super::policy::room(3, 1000, 2, 200, 100, true);
            slow.set(super::policy::enabled(
                latest,
                if latest { 3 } else { 1 },
                room,
                false,
            ))
            .unwrap();
            assert!(!slow.current().unwrap());
            let reader_gate = super::Arc::clone(&healthy);
            let reader_records = super::Arc::clone(&records);
            let (count, wake) = super::wake_count();
            let reader = loom::thread::spawn(move || {
                let mut source = stream::unfold(reader_records, |records| async {
                    Some((records.next().await.unwrap(), records))
                })
                .boxed();
                let mut read = Box::pin(reader_gate.next(&mut source));
                let result = read.as_mut().poll(&mut Context::from_waker(&wake));
                if let Poll::Ready(result) = result {
                    result.unwrap()
                } else {
                    None
                }
            });
            let room = if broken {
                (room.0, room.0, room.2)
            } else {
                room
            };
            healthy
                .set(super::policy::enabled(false, 0, room, true))
                .unwrap();
            records.send(7_u8).unwrap();
            let received = reader.join().unwrap();
            assert!(
                healthy.current().unwrap(),
                "the reserved slot must enable the healthy channel"
            );
            if received.is_none() {
                assert!(count.0.load(super::Ordering::SeqCst) > 0);
                let mut source = stream::once(async { records.next().await.unwrap() }).boxed();
                assert_eq!(
                    loom::future::block_on(healthy.next(&mut source)).unwrap(),
                    Some(7)
                );
            } else {
                assert_eq!(received, Some(7));
            }
        });
    }
}

#[test]
#[should_panic(expected = "wake must run outside the admission lock")]
fn notification_negative_control_wakes_while_locked() {
    registration(2);
}

#[test]
fn disabling_an_idle_source_preserves_its_next_record() {
    disabling(false);
}
#[test]
#[should_panic(expected = "a disabled source must not consume a record")]
fn disabling_negative_control_keeps_polling_the_provider() {
    disabling(true);
}
fn disabling(broken: bool) {
    use futures::{StreamExt, stream};
    use std::future::Future;
    loom::model(move || {
        let gate = super::Arc::new(Gate::new());
        gate.set(true).unwrap();
        let records = super::Arc::new(super::signal::Signal::new());
        let source = super::Arc::clone(&records);
        let mut source = stream::once(async { source.next().await.unwrap() }).boxed();
        let mut read = Box::pin(async {
            if broken {
                Ok(source.next().await)
            } else {
                gate.next(&mut source).await
            }
        });
        let (_, wake) = super::wake_count();
        let mut context = Context::from_waker(&wake);
        assert!(read.as_mut().poll(&mut context).is_pending());
        let disabled = super::Arc::clone(&gate);
        let disabled = loom::thread::spawn(move || disabled.set(false).unwrap());
        let appended = super::Arc::clone(&records);
        let appended = loom::thread::spawn(move || appended.send(7_u8).unwrap());
        disabled.join().unwrap();
        appended.join().unwrap();
        assert!(
            read.as_mut().poll(&mut context).is_pending(),
            "a disabled source must not consume a record"
        );
        drop(read);
        assert_eq!(loom::future::block_on(records.next()).unwrap(), 7);
    });
}
