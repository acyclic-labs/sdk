use super::PullCursor;
use futures::{Future, StreamExt};
use std::{
    cell::Cell,
    rc::Rc,
    task::{Context, Poll},
};

#[test]
fn first_pull_does_not_collect_history_or_wait_for_source_eof() {
    let polled = Rc::new(Cell::new(0));
    let source = || {
        let polled = Rc::clone(&polled);
        futures::stream::iter(0..100_000)
            .map(move |value| {
                polled.set(polled.get() + 1);
                Ok::<_, ()>(value)
            })
            .chain(futures::stream::pending())
            .boxed_local()
    };
    // Negative control: the former collector cannot expose even its first item.
    let mut collected = Box::pin(source().collect::<Vec<_>>());
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(collected.as_mut().poll(&mut context).is_pending());
    assert_eq!(polled.get(), 100_000);
    drop(collected);
    polled.set(0);

    let cursor = PullCursor::new(source(), ());
    assert_eq!(polled.get(), 0);
    let mut next = Box::pin(cursor.next());
    assert_eq!(next.as_mut().poll(&mut context), Poll::Ready(Ok(Some(0))));
    drop(next);
    assert_eq!(polled.get(), 1);
    cursor.close();
    assert_eq!(futures::executor::block_on(cursor.next()), Ok(None));
    assert_eq!(polled.get(), 1);
}
