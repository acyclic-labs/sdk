use super::*;
use crate::probe::probe;
use futures::StreamExt;
use loom::sync::atomic::Ordering;
use std::{
    cell::RefCell,
    future::Future,
    rc::Rc,
    task::{Context, Poll},
};

fn source() -> (
    crate::query_sync::watch::Sender<Option<Vec<u8>>>,
    FollowFrames,
) {
    let (send, mut receive) = crate::query_sync::watch::channel(None);
    let frames = futures::stream::once(async move {
        loop {
            let value = receive.borrow_and_update().clone();
            if let Some(value) = value {
                return Ok(value);
            }
            receive
                .changed()
                .await
                .map_err(|_| crate::StreamError::Unavailable)?;
        }
    })
    .boxed_local();
    (send, frames)
}

#[test]
fn delivery_racing_close_never_restores_a_closed_cursor() {
    loom::model(|| {
        let (send, frames) = source();
        let state = FollowState::new(frames);
        let (_, wake) = probe();
        let producer = loom::thread::spawn(move || {
            send.send_replace(Some(vec![7]));
        });
        let mut next = Box::pin(state.next());
        let first = next.as_mut().poll(&mut Context::from_waker(&wake));
        state.close();
        producer.join().unwrap();
        if first.is_pending() {
            assert_eq!(
                next.as_mut().poll(&mut Context::from_waker(&wake)),
                Poll::Ready(Ok(None))
            );
        } else {
            assert_eq!(first, Poll::Ready(Ok(Some(vec![7]))));
        }
        drop(next);
        assert_eq!(loom::future::block_on(Box::pin(state.next())), Ok(None));
        assert!(state.stream.lock().unwrap().is_none());
    });
}

fn close_wakeup(broken: bool) {
    loom::model(move || {
        let (_send, frames) = source();
        let state = FollowState::new(frames);
        let (count, wake) = probe();
        let mut next = Box::pin(state.next());
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&wake))
                .is_pending()
        );
        if broken {
            // Former close without its cancellation publication.
            let stream = state.stream.lock().unwrap().take();
            drop(stream);
        } else {
            state.close();
        }
        assert!(
            count.count.load(Ordering::SeqCst) > 0,
            "close stranded a pending next"
        );
        assert_eq!(
            next.as_mut().poll(&mut Context::from_waker(&wake)),
            Poll::Ready(Ok(None))
        );
    });
}
#[test]
fn close_wakes_a_pending_next() {
    close_wakeup(false);
}
#[test]
#[should_panic(expected = "close stranded a pending next")]
fn cursor_negative_control_omits_cancellation_publication() {
    close_wakeup(true);
}

fn source_drop(broken: bool) {
    struct DropCheck(Rc<RefCell<std::rc::Weak<FollowState>>>);
    impl Drop for DropCheck {
        fn drop(&mut self) {
            if let Some(state) = self.0.borrow().upgrade() {
                assert!(
                    state.stream.try_lock().is_ok(),
                    "cursor dropped a source under its lock"
                );
            }
        }
    }
    loom::model(move || {
        let owner = Rc::new(RefCell::new(std::rc::Weak::new()));
        let check = DropCheck(Rc::clone(&owner));
        let frames = futures::stream::poll_fn(move |_| {
            let _check = &check;
            Poll::<Option<Result<Vec<u8>, crate::StreamError>>>::Pending
        })
        .boxed_local();
        let state = Rc::new(FollowState::new(frames));
        *owner.borrow_mut() = Rc::downgrade(&state);
        if broken {
            let mut slot = state.stream.lock().unwrap();
            let violation =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(slot.take())));
            drop(slot);
            if let Err(violation) = violation {
                std::panic::resume_unwind(violation);
            }
        } else {
            state.close();
        }
    });
}
#[test]
fn closing_drops_the_source_after_unlocking() {
    source_drop(false);
}
#[test]
#[should_panic(expected = "cursor dropped a source under its lock")]
fn cursor_lock_order_negative_control_drops_while_locked() {
    source_drop(true);
}
