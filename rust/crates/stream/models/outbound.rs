use super::{Arc, Ordering, wake_probe};
use std::future::Future as _;

fn saturation(broken: bool) {
    loom::model(move || {
        let stalled = Arc::new(Outbound::new(1, 2));
        let healthy = Arc::new(Outbound::new(1, 2));
        stalled.push(1_u8, 2).unwrap();
        let cancelling = Arc::clone(&stalled);
        let cancelling = loom::thread::spawn(move || cancelling.close().unwrap());
        let mut dispatch = Box::pin(async {
            if broken {
                // Former dispatcher: queue admission waits for a peer that is
                // not consuming, preventing every subsequent dispatch action.
                futures::future::pending::<()>().await;
            } else {
                assert!(matches!(
                    stalled.push(2, 1),
                    Err(StreamError::Capacity | StreamError::Unavailable)
                ));
            }
            healthy.push(3, 1).unwrap();
        });
        let (_, wake) = super::wake_count();
        let first = dispatch.as_mut().poll(&mut Context::from_waker(&wake));
        cancelling.join().unwrap();
        assert!(
            first.is_ready(),
            "a blocked node must never park independent dispatch"
        );
        assert_eq!(stalled.push(4, 1), Err(StreamError::Unavailable));
        assert_eq!(loom::future::block_on(healthy.next()).unwrap(), 3);
    });
}
#[test]
fn saturated_write_and_cancellation_do_not_park_other_nodes() {
    saturation(false);
}
#[test]
#[should_panic(expected = "a blocked node must never park independent dispatch")]
fn negative_control_awaits_a_stalled_peer() {
    saturation(true);
}

fn registration(close: bool, broken: u8) {
    loom::model(move || {
        let queue = Arc::new(Outbound::new(1, 2));
        let inspected = Arc::clone(&queue);
        let (count, wake) = wake_probe(move || {
            assert!(
                inspected.state.try_lock().is_ok(),
                "outbound wakes must run outside its lock"
            );
        });
        let reading = Arc::clone(&queue);
        let reading = loom::thread::spawn(move || reading.poll(&mut Context::from_waker(&wake)));
        if broken != 0 {
            let mut state = queue.state.lock().unwrap();
            state.closed = true;
            let wake = state.reader.take();
            if broken == 2
                && let Some(wake) = wake
            {
                wake.wake();
            }
        } else if close {
            queue.close().unwrap();
        } else {
            queue.push(7_u8, 2).unwrap();
        }
        let first = reading.join().unwrap();
        assert!(
            first.is_ready() || count.0.load(Ordering::SeqCst) > 0,
            "a cancelled or writable outbound stream must be woken"
        );
        if close {
            assert!(matches!(
                first,
                Poll::Ready(Err(StreamError::Unavailable)) | Poll::Pending
            ));
        } else {
            let value = match first {
                Poll::Ready(value) => value.unwrap(),
                Poll::Pending => loom::future::block_on(queue.next()).unwrap(),
            };
            assert_eq!(value, 7);
        }
    });
}
#[test]
fn enqueue_and_wait_registration_cannot_lose_wakes() {
    registration(false, 0);
}
#[test]
fn cancellation_and_wait_registration_cannot_lose_wakes() {
    registration(true, 0);
}
#[test]
#[should_panic(expected = "a cancelled or writable outbound stream must be woken")]
fn negative_control_cancels_without_waking_the_writer() {
    registration(true, 1);
}
#[test]
#[should_panic(expected = "outbound wakes must run outside its lock")]
fn negative_control_wakes_while_holding_the_outbound_lock() {
    registration(true, 2);
}

#[test]
fn closing_a_link_wakes_its_response_reader_without_nesting_locks() {
    loom::model(|| {
        let queue = Arc::new(Outbound::<u8>::new(1, 2));
        let inspected = Arc::clone(&queue);
        let (count, wake) = wake_probe(move || {
            assert!(
                inspected.state.try_lock().is_ok(),
                "outbound wakes must run outside its lock"
            );
        });
        let reading = Arc::clone(&queue);
        let reading = loom::thread::spawn(move || {
            let mut cancelled = Box::pin(reading.cancelled());
            cancelled.as_mut().poll(&mut Context::from_waker(&wake))
        });
        queue.close().unwrap();
        let first = reading.join().unwrap();
        assert!(
            first.is_ready() || count.0.load(Ordering::SeqCst) > 0,
            "closing a node must cancel its pending response read"
        );
        loom::future::block_on(queue.cancelled()).unwrap();
    });
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn outbound_admission_preserves_fifo_and_exact_byte_bounds(
        operations in proptest::collection::vec((0_u8..3, 0_usize..7), 0..32)
    ) {
        loom::model(move || {
            let queue = Outbound::new(2, 5);
            let mut expected = VecDeque::new();
            let (mut used, mut closed) = (0, false);
            let (_, wake) = super::wake_count();
            for (operation, bytes) in &operations {
                match operation {
                    0 => {
                        let result = queue.push(*bytes, *bytes);
                        if closed { assert_eq!(result, Err(StreamError::Unavailable)); }
                        else if expected.len() == 2 || bytes > &(5 - used) { assert_eq!(result, Err(StreamError::Capacity)); }
                        else { assert!(result.is_ok()); expected.push_back(*bytes); used += bytes; }
                    }
                    1 => {
                        let result = queue.poll(&mut Context::from_waker(&wake));
                        if closed { assert_eq!(result, Poll::Ready(Err(StreamError::Unavailable))); }
                        else if let Some(value) = expected.pop_front() { used -= value; assert_eq!(result, Poll::Ready(Ok(value))); }
                        else { assert!(result.is_pending()); }
                    }
                    _ => { queue.close().unwrap(); expected.clear(); used = 0; closed = true; }
                }
                let state = queue.state.lock().unwrap();
                assert_eq!(state.frames.len(), expected.len());
                assert_eq!(state.bytes, used);
                assert!(used <= 5);
            }
        });
    }
}
