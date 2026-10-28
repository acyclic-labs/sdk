use super::*;
use std::{
    cell::Cell,
    rc::Rc,
    task::{Context, Poll},
};

struct Dropped(Rc<Cell<usize>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

// Internal cursor vectors, not external provider response fixtures.
#[test]
fn one_pull_has_one_item_credit_and_terminal_errors_are_visible() {
    futures::executor::block_on(async {
        let polls = Rc::new(Cell::new(0));
        let drops = Rc::new(Cell::new(0));
        let (count, guard) = (Rc::clone(&polls), Dropped(Rc::clone(&drops)));
        let stream = futures::stream::poll_fn(move |_| {
            let _guard = &guard;
            count.set(count.get() + 1);
            Poll::Ready(Some(if count.get() < 3 {
                Ok(count.get())
            } else {
                Err("original source failure")
            }))
        })
        .boxed_local();
        let cursor = PullCursor::new(stream);
        assert_eq!(polls.get(), 0);
        assert_eq!(cursor.next().await, Ok(Some(1)));
        assert_eq!(polls.get(), 1);
        assert_eq!(cursor.next().await, Ok(Some(2)));
        assert_eq!(polls.get(), 2);
        assert_eq!(
            cursor.next().await,
            Err(PullError::Source("original source failure"))
        );
        assert_eq!(cursor.status(), CursorStatus::Failed);
        assert_eq!(drops.get(), 1);
        assert_eq!(cursor.next().await, Ok(None));
        cursor.close();
        assert_eq!(cursor.status(), CursorStatus::Failed);
        assert_eq!(polls.get(), 3);
        assert_eq!(drops.get(), 1);
    });
}

#[test]
fn overlapping_next_does_not_steal_or_close_the_pending_pull() {
    futures::executor::block_on(async {
        let drops = Rc::new(Cell::new(0));
        let guard = Dropped(Rc::clone(&drops));
        let stream = futures::stream::poll_fn(move |_| {
            let _guard = &guard;
            Poll::<Option<Result<(), ()>>>::Pending
        })
        .boxed_local();
        let cursor = PullCursor::new(stream);
        let mut next = Box::pin(cursor.next());
        assert!(matches!(futures::poll!(&mut next), Poll::Pending));
        assert_eq!(cursor.next().await, Err(PullError::Busy));
        assert_eq!(cursor.status(), CursorStatus::Open);
        assert_eq!(drops.get(), 0);
        cursor.close();
        assert_eq!(next.await, Ok(None));
        assert_eq!(cursor.status(), CursorStatus::Closed);
        cursor.close();
        assert_eq!(drops.get(), 1);
    });
}

#[test]
fn abandoning_pending_next_drops_upstream_once() {
    let drops = Rc::new(Cell::new(0));
    let guard = Dropped(Rc::clone(&drops));
    let stream = futures::stream::poll_fn(move |_| {
        let _guard = &guard;
        Poll::<Option<Result<(), ()>>>::Pending
    })
    .boxed_local();
    let cursor = PullCursor::new(stream);
    let mut next = Box::pin(cursor.next());
    let wake = futures::task::noop_waker();
    assert!(std::future::Future::poll(next.as_mut(), &mut Context::from_waker(&wake)).is_pending());
    drop(next);
    assert_eq!(drops.get(), 1);
    assert_eq!(cursor.status(), CursorStatus::Closed);
    cursor.close();
    assert_eq!(drops.get(), 1);
}

#[test]
fn normal_eof_stays_completed_after_close() {
    futures::executor::block_on(async {
        let cursor = PullCursor::new(futures::stream::empty::<Result<(), ()>>().boxed_local());
        assert_eq!(cursor.next().await, Ok(None));
        assert_eq!(cursor.status(), CursorStatus::Completed);
        cursor.close();
        assert_eq!(cursor.status(), CursorStatus::Completed);
    });
}
