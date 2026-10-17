use futures::task::{ArcWake, AtomicWaker, waker_ref};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Default)]
struct WakeCount {
    wakes: AtomicUsize,
    caller: AtomicWaker,
}

impl ArcWake for WakeCount {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.wakes.fetch_add(1, Ordering::SeqCst);
        wake.caller.wake();
    }
}

pub(super) async fn until_idle<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Option<F::Output> {
    let wake = Arc::new(WakeCount::default());
    futures::future::poll_fn(|caller| {
        wake.caller.register(caller.waker());
        let before = wake.wakes.load(Ordering::SeqCst);
        let waker = waker_ref(&wake);
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(output) => Poll::Ready(Some(output)),
            Poll::Pending if wake.wakes.load(Ordering::SeqCst) == before => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

#[test]
fn quiescence_drives_every_self_wake_and_detects_delayed_delivery() {
    for pending in 0..4 {
        for duplicate in [false, true] {
            let mut polls = 0;
            let mut future = Box::pin(futures::future::poll_fn(|context| {
                polls += 1;
                if polls <= pending {
                    context.waker().wake_by_ref();
                    Poll::Pending
                } else if duplicate {
                    Poll::Ready("duplicate")
                } else {
                    Poll::Pending
                }
            }));
            assert_eq!(
                futures::executor::block_on(until_idle(future.as_mut())),
                duplicate.then_some("duplicate")
            );
            drop(future);
            assert_eq!(polls, pending + 1);
        }
    }
}
