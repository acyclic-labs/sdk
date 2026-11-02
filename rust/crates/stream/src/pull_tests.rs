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

#[test]
fn failed_pull_ownership_is_not_eof_and_close_drops_the_original_source() {
    let retained = Rc::new(());
    let source = futures::stream::once({
        let retained = Rc::clone(&retained);
        async move {
            drop(retained);
            Ok::<_, &str>(1)
        }
    })
    .boxed_local();
    let cursor = PullCursor::new(source, "original ownership unavailable");
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _owner = cursor.stream.lock().unwrap();
        panic!("original pull owner failed");
    }));
    assert!(failure.is_err());
    assert_eq!(
        futures::executor::block_on(cursor.next()),
        Err("original ownership unavailable")
    );
    assert_eq!(Rc::strong_count(&retained), 2, "source was never polled");
    cursor.close();
    assert_eq!(
        Rc::strong_count(&retained),
        1,
        "close drops the same source"
    );
    assert_eq!(
        futures::executor::block_on(cursor.next()),
        Err("original ownership unavailable")
    );
}

#[test]
fn outstanding_and_abandoned_pulls_cannot_establish_eof() {
    let (sender, source) = futures::channel::mpsc::unbounded();
    let cursor = PullCursor::new(source.boxed_local(), "original pull unavailable");
    let mut first = Box::pin(cursor.next());
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert_eq!(
        futures::executor::block_on(cursor.next()),
        Err("original pull unavailable")
    );
    sender.unbounded_send(Ok(7)).unwrap();
    assert_eq!(futures::executor::block_on(first), Ok(Some(7)));
    drop(sender);
    assert_eq!(futures::executor::block_on(cursor.next()), Ok(None));
    assert_eq!(futures::executor::block_on(cursor.next()), Ok(None));

    let cursor = PullCursor::new(
        futures::stream::pending::<Result<u8, &str>>().boxed_local(),
        "original pull unavailable",
    );
    let mut abandoned = Box::pin(cursor.next());
    assert!(abandoned.as_mut().poll(&mut context).is_pending());
    drop(abandoned);
    assert_eq!(
        futures::executor::block_on(cursor.next()),
        Err("original pull unavailable")
    );
    cursor.close();
    assert_eq!(futures::executor::block_on(cursor.next()), Ok(None));
}
