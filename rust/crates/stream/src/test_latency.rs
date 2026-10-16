use std::future::Future;

pub(crate) async fn sample<A, D>(
    append: impl Future<Output = A>,
    delivery: impl Future<Output = D>,
    now: impl FnOnce() -> u128,
) -> (A, D, u128) {
    let (append, (delivery, elapsed)) = futures::join!(append, async {
        let delivery = delivery.await;
        (delivery, now())
    });
    (append, delivery, elapsed)
}

#[test]
fn delivery_is_observed_before_the_acknowledgement() {
    probe(0);
}

#[test]
#[should_panic(expected = "delivery was gated on the acknowledgement")]
fn negative_control_waits_for_ack_before_receiving() {
    probe(1);
}

#[test]
#[should_panic(expected = "timestamp includes the acknowledgement")]
fn negative_control_timestamps_after_both_futures_complete() {
    probe(2);
}

fn probe(broken: u8) {
    use std::{cell::Cell, pin::Pin, task::Context};
    let tick = Cell::new(0);
    let delivered = Cell::new(false);
    let acknowledged = Cell::new(false);
    let (release, ack) = futures::channel::oneshot::channel();
    let append = async {
        ack.await.expect("delivery releases the acknowledgement");
        acknowledged.set(true);
        tick.set(2);
    };
    let delivery = async {
        delivered.set(true);
        tick.set(1);
        release.send(()).expect("append is still active");
        7
    };
    let now = || tick.get();
    let mut measured: Pin<Box<dyn Future<Output = ((), i32, u128)> + '_>> = match broken {
        1 => Box::pin(async {
            let append = append.await;
            let delivery = delivery.await;
            (append, delivery, now())
        }),
        2 => Box::pin(async {
            let (append, delivery) = futures::join!(append, delivery);
            (append, delivery, now())
        }),
        _ => Box::pin(sample(append, delivery, now)),
    };
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    let first = measured.as_mut().poll(&mut context);
    assert!(delivered.get(), "delivery was gated on the acknowledgement");
    let result = if first.is_pending() {
        // Delivery releases the one-shot after append's first pending poll.
        measured.as_mut().poll(&mut context)
    } else {
        first
    };
    assert!(acknowledged.get());
    assert_eq!(
        result,
        std::task::Poll::Ready(((), 7, 1)),
        "timestamp includes the acknowledgement"
    );
}
