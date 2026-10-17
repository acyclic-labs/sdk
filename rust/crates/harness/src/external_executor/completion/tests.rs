use super::*;
use crate::Error;
use proptest::prelude::*;
use std::cell::Cell;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Exact cached bytes and uncertain reads prevent even constructing a child
    // effect, regardless of the child's current state or delivery wake count.
    #[test]
    fn settled_delivery_never_replays_a_later_child_effect(
        bytes in prop::collection::vec(any::<u8>(),0..512),
        cached in any::<bool>(), uncertain in any::<bool>(), wakes in 0usize..4,
    ) {
        verify_order(bytes, cached, uncertain, wakes, false);
    }
}

#[test]
#[should_panic(expected = "settled request constructed a child effect")]
fn negative_control_detects_completion_before_reading_the_stored_outcome() {
    verify_order(vec![1], true, false, 1, true);
}

fn verify_order(bytes: Vec<u8>, cached: bool, uncertain: bool, wakes: usize, broken: bool) {
    let calls = Cell::new(0);
    let polls = Cell::new(0);
    let delivery = std::future::poll_fn(|cx| {
        let polled = polls.get();
        polls.set(polled + 1);
        if polled < wakes {
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        std::task::Poll::Ready(if uncertain {
            Err(Error::Storage("uncertain read".into()))
        } else {
            Ok(cached.then(|| bytes.clone()))
        })
    });
    let complete = || {
        calls.set(calls.get() + 1);
        async { Ok(Some(vec![255])) }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        if broken {
            // The former ordering invoked completion before inspecting delivery.
            complete().await?;
            delivery.await
        } else {
            after_delivery(delivery, complete).await
        }
    });
    assert_eq!(
        calls.get(),
        usize::from(!cached && !uncertain),
        "settled request constructed a child effect"
    );
    if uncertain {
        assert!(matches!(result, Err(Error::Storage(_))));
    } else {
        assert_eq!(
            result.unwrap(),
            Some(if cached { bytes } else { vec![255] })
        );
    }
}
