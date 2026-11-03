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

#[cfg(loom)]
pub(crate) fn negative() {
    admitted!(super::RwLock::stalled_reader_control(()));
}

// The same bounded operation traces run against Tokio and the substituted
// watch, including cancellation of pending changed futures and publisher loss.
#[cfg(loom)]
pub(crate) fn watch() {
    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Changed(Poll<bool>),
        Value(u8),
        Receivers(usize),
    }
    macro_rules! trace {
        ($channel:path, $steps:expr) => {{
            let (sender, mut first) = $channel(0u8);
            let mut second = sender.subscribe();
            let mut spare = Some(sender.clone());
            let mut sender = Some(sender);
            let mut events = Vec::new();
            let mut steps = $steps;
            for _ in 0..4 {
                match steps & 3 {
                    0 => {
                        let mut changed = Box::pin(first.changed());
                        events.push(Event::Changed(poll!(changed).map(|value| value.is_ok())));
                    }
                    1 => {
                        if let Some(sender) = sender.as_ref().or(spare.as_ref()) {
                            sender.send_modify(|value| *value += 1);
                        }
                    }
                    2 => {
                        if let Some(sender) = sender.as_ref().or(spare.as_ref()) {
                            second = sender.subscribe();
                        }
                        let mut changed = Box::pin(second.changed());
                        events.push(Event::Changed(poll!(changed).map(|value| value.is_ok())));
                    }
                    _ => {
                        if spare.is_some() {
                            drop(spare.take());
                        } else {
                            drop(sender.take());
                        }
                    }
                }
                if let Some(sender) = sender.as_ref().or(spare.as_ref()) {
                    events.push(Event::Value(*sender.borrow()));
                    events.push(Event::Receivers(sender.receiver_count()));
                }
                steps >>= 2;
            }
            drop((sender, spare));
            // An unseen final publication precedes the terminal result. A
            // cancelled changed future cannot consume that publication.
            for receiver in [&mut first, &mut second] {
                for _ in 0..2 {
                    let mut changed = Box::pin(receiver.changed());
                    events.push(Event::Changed(poll!(changed).map(|value| value.is_ok())));
                }
                events.push(Event::Value(*receiver.borrow_and_update()));
            }
            events
        }};
    }
    for steps in 0u16..256 {
        assert_eq!(
            trace!(super::watch::channel, steps),
            trace!(tokio::sync::watch::channel, steps),
            "watch trace {steps} differs from Tokio"
        );
    }
}

#[cfg(test)]
#[cfg(not(loom))]
#[test]
fn admission_and_cancellation_match_tokio() {
    run();
}
