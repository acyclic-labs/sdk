use std::future::Future;

pub(crate) async fn sample<A, D, E>(
    append: impl Future<Output = Result<A, E>>,
    delivery: impl Future<Output = Result<D, E>>,
    now: impl FnOnce() -> u128,
) -> Result<(A, D, u128), E> {
    let (append, (delivery, elapsed)) = futures::try_join!(append, async {
        let delivery = delivery.await?;
        Ok((delivery, now()))
    })?;
    Ok((append, delivery, elapsed))
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
        Ok::<_, ()>(())
    };
    let delivery = async {
        delivered.set(true);
        tick.set(1);
        release.send(()).expect("append is still active");
        Ok::<_, ()>(7)
    };
    let now = || tick.get();
    let mut measured: Pin<Box<dyn Future<Output = Result<((), i32, u128), ()>> + '_>> = match broken
    {
        1 => Box::pin(async {
            let append = append.await?;
            let delivery = delivery.await?;
            Ok((append, delivery, now()))
        }),
        2 => Box::pin(async {
            let (append, delivery) = futures::try_join!(append, delivery)?;
            Ok((append, delivery, now()))
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
        std::task::Poll::Ready(Ok(((), 7, 1))),
        "timestamp includes the acknowledgement"
    );
}

#[test]
fn either_failure_ends_sampling_while_the_other_future_is_pending() {
    failure_probe(false, true);
    failure_probe(false, false);
}

#[test]
#[should_panic(expected = "sampling waited for the pending peer after an error")]
fn negative_control_join_waits_for_delivery_after_append_failure() {
    failure_probe(true, true);
}

fn failure_probe(broken: bool, append_fails: bool) {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    type Operation = Pin<Box<dyn Future<Output = Result<(), &'static str>>>>;
    let failed: Operation = Box::pin(async { Err("original failure") });
    let pending: Operation = Box::pin(futures::future::pending());
    let (append, delivery) = if append_fails {
        (failed, pending)
    } else {
        (pending, failed)
    };
    let mut measured: Pin<Box<dyn Future<Output = Result<((), (), u128), &'static str>>>> =
        if broken {
            Box::pin(async {
                let (append, delivery) = futures::join!(append, delivery);
                append?;
                delivery?;
                Ok(((), (), 0))
            })
        } else {
            Box::pin(sample(append, delivery, || 0))
        };
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert_eq!(
        measured.as_mut().poll(&mut context),
        Poll::Ready(Err("original failure")),
        "sampling waited for the pending peer after an error"
    );
}
