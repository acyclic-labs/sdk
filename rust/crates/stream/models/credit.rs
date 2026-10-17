#[test]
fn discarded_and_consumed_deliveries_return_credit_exactly_once() {
    disposal(false);
}
#[test]
#[should_panic(expected = "all discarded capacity must be returned")]
fn disposal_negative_control_discards_without_returning_credit() {
    disposal(true);
}
fn disposal(broken: bool) {
    loom::model(move || {
        let sender = super::Arc::new(super::signal::Signal::new());
        // Test receiver storage is never locked while dropping a delivery or
        // taking the production signal/cursor lock. No nested lock ranks.
        let storage = super::Arc::new(super::Mutex::new(
            (0..2)
                .map(|id| {
                    Credit::new(
                        super::Arc::clone(&sender),
                        Released {
                            id,
                            generation: 1,
                            node: "node".into(),
                            epoch: 1,
                            bytes: 100 + id,
                        },
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let consuming = super::Arc::clone(&storage);
        let consuming = loom::thread::spawn(move || {
            let delivery = consuming.lock().unwrap().pop();
            drop(delivery);
        });
        let dropping = super::Arc::clone(&storage);
        let dropping = loom::thread::spawn(move || {
            let mut discarded = std::mem::take(&mut *dropping.lock().unwrap());
            if broken {
                for delivery in &mut discarded {
                    delivery.value.take();
                }
            }
            drop(discarded);
        });
        consuming.join().unwrap();
        dropping.join().unwrap();
        let mut ids = Vec::new();
        let mut bytes = 0;
        for _ in 0..2 {
            let (_, wake) = super::wake_count();
            let std::task::Poll::Ready(Ok(released)) =
                sender.poll(&mut std::task::Context::from_waker(&wake))
            else {
                panic!("all discarded capacity must be returned");
            };
            assert_eq!(
                (released.generation, released.epoch, released.node.as_str()),
                (1, 1, "node")
            );
            bytes += released.bytes;
            ids.push(released.id);
        }
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1]);
        assert_eq!(bytes, 201);
        let (_, wake) = super::wake_count();
        assert!(
            sender
                .poll(&mut std::task::Context::from_waker(&wake))
                .is_pending()
        );
        assert!(storage.lock().unwrap().is_empty());
    });
}
