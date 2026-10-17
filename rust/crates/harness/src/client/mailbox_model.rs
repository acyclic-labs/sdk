use super::*;

pub fn verify_mailbox(omit_wake: bool) {
    loom::model(move || {
        use crate::sync::atomic::{AtomicUsize, Ordering};
        use futures::{
            executor::block_on,
            task::{ArcWake, waker},
        };
        struct Wake(AtomicUsize);
        impl ArcWake for Wake {
            fn wake_by_ref(wake: &std::sync::Arc<Self>) {
                wake.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (sender, mut receiver) = channel();
        receiver.0.lock().unwrap().omit_wake = omit_wake;
        assert_eq!(block_on(sender.send(1)), Ok(()));
        let wake = std::sync::Arc::new(Wake(AtomicUsize::new(0)));
        let waker = waker(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let reading = loom::thread::spawn(move || {
            assert_eq!(block_on(receiver.recv()), Some(1));
            receiver
        });
        let mut second = Box::pin(sender.send(2));
        let first_poll = second.as_mut().poll(&mut cx);
        let pending = first_poll.is_pending();
        receiver = reading.join().unwrap();
        if pending {
            assert!(
                wake.0.load(Ordering::SeqCst) > 0,
                "mailbox handoff lost its wakeup"
            );
        }
        if pending {
            assert_eq!(second.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        } else {
            assert_eq!(first_poll, Poll::Ready(Ok(())));
        }
        drop(second);
        let mut cancelled = Box::pin(sender.send(3));
        let mut last = Box::pin(sender.send(4));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        assert!(last.as_mut().poll(&mut cx).is_pending());
        drop(cancelled);
        assert_eq!(block_on(receiver.recv()), Some(2));
        assert_eq!(last.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        drop(last);
        assert_eq!(block_on(receiver.recv()), Some(4));
        let mut closed = Box::pin(receiver.recv());
        assert!(closed.as_mut().poll(&mut cx).is_pending());
        drop(sender);
        assert_eq!(closed.as_mut().poll(&mut cx), Poll::Ready(None));
        drop(closed);
        let (reply, waiting) = channel();
        drop(waiting);
        assert!(reply.is_closed());
        assert_eq!(reply.reply(7), Err(()));
        let (sender, mut receiver) = channel();
        assert_eq!(block_on(sender.send(1)), Ok(()));
        sender.close();
        assert_eq!(block_on(receiver.recv()), None);
        assert_eq!(block_on(sender.send(2)), Err(()));
    });
}
