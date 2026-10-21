use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

macro_rules! poll {
    ($future:expr) => {
        $future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    };
}
macro_rules! admitted {
    ($make:expr) => {{
        let lock = $make;
        let mut writer = Box::pin(lock.write());
        let Poll::Ready(held) = poll!(writer) else {
            panic!("initial writer")
        };
        let mut first = Box::pin(lock.read());
        assert!(poll!(first).is_pending());
        drop(held);
        let mut second = Box::pin(lock.read());
        let Poll::Ready(second) = poll!(second) else {
            panic!("granted reader excluded a compatible reader")
        };
        let Poll::Ready(first) = poll!(first) else {
            panic!("lost granted reader")
        };
        drop((first, second));
        assert!(lock.try_write().is_ok());
    }};
}
macro_rules! cancellations {
    ($make:expr) => {{
        // Cancel an unpolled handed-off read: the queued writer must progress.
        let lock = $make;
        let mut writer = Box::pin(lock.write());
        let Poll::Ready(held) = poll!(writer) else {
            panic!("writer")
        };
        let mut reader = Box::pin(lock.read());
        assert!(poll!(reader).is_pending());
        let mut next = Box::pin(lock.write());
        assert!(poll!(next).is_pending());
        drop(held);
        let mut later = Box::pin(lock.read());
        assert!(poll!(later).is_pending(), "reader bypassed FIFO writer");
        drop(reader);
        let Poll::Ready(next) = poll!(next) else {
            panic!("cancelled read retained reservation")
        };
        drop(next);
        assert!(poll!(later).is_ready());

        // Cancel a pending writer while another reader still holds the lock.
        let lock = $make;
        let mut initial = Box::pin(lock.read());
        let Poll::Ready(initial) = poll!(initial) else {
            panic!("reader")
        };
        let mut writer = Box::pin(lock.write());
        assert!(poll!(writer).is_pending());
        let mut reader = Box::pin(lock.read());
        assert!(poll!(reader).is_pending());
        drop(writer);
        let Poll::Ready(reader) = poll!(reader) else {
            panic!("cancelled writer excluded reader")
        };
        drop((initial, reader));
        assert!(lock.try_write().is_ok());

        // Cancel a handed-off writer: all consecutive queued readers progress.
        let lock = $make;
        let mut initial = Box::pin(lock.read());
        let Poll::Ready(initial) = poll!(initial) else {
            panic!("reader")
        };
        let mut writer = Box::pin(lock.write());
        assert!(poll!(writer).is_pending());
        let mut first = Box::pin(lock.read());
        let mut second = Box::pin(lock.read());
        assert!(poll!(first).is_pending());
        assert!(poll!(second).is_pending());
        drop(initial);
        assert!(poll!(first).is_pending());
        drop(writer);
        let Poll::Ready(first) = poll!(first) else {
            panic!("cancelled writer retained reservation")
        };
        let Poll::Ready(second) = poll!(second) else {
            panic!("reader batch stalled")
        };
        drop((first, second));
        assert!(lock.try_write().is_ok());
    }};
}

pub(crate) fn run() {
    admitted!(tokio::sync::RwLock::new(()));
    cancellations!(tokio::sync::RwLock::new(()));
    admitted!(super::RwLock::new(()));
    cancellations!(super::RwLock::new(()));
}

#[cfg(acyclic_loom)]
pub(crate) fn negative() {
    admitted!(super::RwLock::stalled_reader_control(()));
}

#[cfg(test)]
#[cfg(not(acyclic_loom))]
#[test]
fn admission_and_cancellation_match_tokio() {
    run();
}
