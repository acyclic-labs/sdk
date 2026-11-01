use super::*;
use futures::{Future as _, Stream as _, stream::FusedStream as _, task::ArcWake};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(original: &Arc<Self>) {
        original.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn stalled_source(end: u8, omit_cancellation_event: bool) {
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 4_000_000),
        &acyclic_fs::CancellationToken::new(),
    )
    .expect("original work");
    let baseline = work.live_allocation_bytes();
    let (producer, mut receiver) = futures::channel::oneshot::channel::<()>();
    let polls = AtomicUsize::new(0);
    let original_polls = &polls;
    let mut stream = NativeModelStream::new(&work, || {
        futures::stream::poll_fn(move |cx| {
            original_polls.fetch_add(1, Ordering::SeqCst);
            match Pin::new(&mut receiver).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(_) => Poll::Ready(Some(Err(Error::NativeBoundary {
                    code: Storage,
                    reason: "original model event producer dropped",
                }))),
            }
        })
    })
    .unwrap();
    if omit_cancellation_event {
        // Restore the former remaining()-then-source-poll mechanism.
        drop(stream.cancelled.take());
    }
    let wake = Arc::new(WakeCount::default());
    let waker = futures::task::waker_ref(&wake);
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut stream).poll_next(&mut cx).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    let mut producer = Some(producer);
    let expected = match end {
        0 => {
            work.cancellation().cancel();
            assert!(
                wake.0.load(Ordering::SeqCst) > 0,
                "cancellation did not wake the stalled stream"
            );
            crate::wire::ErrorCode::Cancelled
        }
        1 => {
            drop(producer.take());
            assert!(wake.0.load(Ordering::SeqCst) > 0);
            Storage
        }
        _ => {
            drop(stream);
            assert!(producer.unwrap().is_canceled());
            assert_eq!(work.live_allocation_bytes(), baseline);
            return;
        }
    };
    let Poll::Ready(Some(Err(error))) = Pin::new(&mut stream).poll_next(&mut cx) else {
        panic!("the original event did not terminate its pending stream");
    };
    assert_eq!(error.code(), expected);
    if end == 0 {
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    } else {
        assert!(matches!(
            error,
            Error::NativeBoundary {
                reason: "original model event producer dropped",
                ..
            }
        ));
    }
    assert!(stream.is_terminated());
    let terminal_polls = polls.load(Ordering::SeqCst);
    for _ in 0..2 {
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(None)
        ));
    }
    assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
    drop(stream);
    if let Some(producer) = producer {
        assert!(producer.is_canceled());
    }
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[test]
fn pending_native_streams_keep_original_cancellation_and_producer_events() {
    for end in 0..3 {
        stalled_source(end, false);
    }
}

#[test]
#[should_panic(expected = "cancellation did not wake the stalled stream")]
fn negative_control_rejects_the_former_poll_only_cancellation_check() {
    stalled_source(0, true);
}

#[test]
fn unfunded_cancellation_refuses_before_constructing_the_stream() {
    let work = NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: 4096,
            ..Default::default()
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .expect("original work");
    let baseline = work.live_allocation_bytes();
    let mut built = false;
    let result = NativeModelStream::new(&work, || {
        built = true;
        futures::stream::empty::<Result<(ModelEvent, NativeAllocation)>>()
    });
    assert!(matches!(result, Err(Error::NativeWork { .. })));
    assert!(!built);
    drop(result);
    assert_eq!(work.live_allocation_bytes(), baseline);
}
