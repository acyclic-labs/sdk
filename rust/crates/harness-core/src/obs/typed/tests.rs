use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

const CREATED: u32 = 1;
const POLLED: u32 = 2;
const POLL_SCOPE: u32 = 4;
const DROPPED: u32 = 8;
const DROP_SCOPE: u32 = 16;

fn producer_scope() -> bool {
    tracing::Span::current()
        .metadata()
        .is_some_and(|metadata| metadata.name() == "acyclic.harness.test.typed_producer")
}

struct Probe {
    seen: Arc<AtomicU32>,
    ready: bool,
    failed: bool,
}

impl Future for Probe {
    type Output = crate::Result<u8>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.seen.fetch_or(
            POLLED | if producer_scope() { POLL_SCOPE } else { 0 },
            Ordering::Relaxed,
        );
        if !self.ready {
            return Poll::Pending;
        }
        Poll::Ready(if self.failed {
            Err(crate::Error::Invalid("probe".into()))
        } else {
            Ok(5)
        })
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.seen.fetch_or(
            DROPPED | if producer_scope() { DROP_SCOPE } else { 0 },
            Ordering::Relaxed,
        );
    }
}

fn ownership(seen: &AtomicU32, polled: bool) {
    let expected = if polled {
        CREATED | POLLED | POLL_SCOPE | DROPPED | DROP_SCOPE
    } else {
        DROPPED
    };
    assert_eq!(
        seen.load(Ordering::Relaxed),
        expected,
        "producer escaped its polling or destruction context"
    );
}

proptest! {
    // Every pending/ready/error/drop cut retains the lazy operation context.
    // The atomics only make the single-threaded fixture Send; there is no race.
    #[test]
    fn producer_context_covers_poll_and_drop(polled in any::<bool>(), ready in any::<bool>(), failed in any::<bool>()) {
        crate::obs::capture::sync(|_capture| {
        let seen = Arc::new(AtomicU32::new(0));
        let creation = seen.clone();
        let mut future = Box::pin(traced_future(move || {
            creation.fetch_or(CREATED, Ordering::Relaxed);
            obs_span!(INFO, "acyclic.harness.test.typed_producer")
        }, || Probe { seen: seen.clone(), ready, failed }));
        prop_assert_eq!(seen.load(Ordering::Relaxed), 0, "span initialized during construction");
        if polled {
            let result = future.as_mut().poll(&mut Context::from_waker(futures::task::noop_waker_ref()));
            prop_assert_eq!(result.is_ready(), ready);
            prop_assert_eq!(seen.load(Ordering::Relaxed) & DROPPED != 0, ready,
                "a completed producer must be destroyed before returning its result");
            if let Poll::Ready(result) = result { prop_assert_eq!(result.is_err(), failed); }
        }
        drop(future);
        ownership(&seen, polled);
        Ok(())
        })?;
    }
}

#[test]
fn borrowing_the_producer_loses_its_drop_context_negative_control() {
    crate::obs::capture::sync(|_capture| {
        let seen = Arc::new(AtomicU32::new(0));
        let creation = seen.clone();
        let mut producer = Box::pin(Probe {
            seen: seen.clone(),
            ready: false,
            failed: false,
        });
        let borrowed = producer.as_mut();
        let mut future = Box::pin(traced_future(
            move || {
                creation.fetch_or(CREATED, Ordering::Relaxed);
                obs_span!(INFO, "acyclic.harness.test.typed_producer")
            },
            || borrowed,
        ));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
                .is_pending()
        );
        drop(future);
        drop(producer);
        assert!(
            std::panic::catch_unwind(|| ownership(&seen, true)).is_err(),
            "borrowed producer escaped the same ownership assertion"
        );
    });
}
