//! Monotonic, same-ledger samples without resetting shared dispatch counters.
use super::{RequestLedger, Requests};
use std::time::Duration;
use web_time::Instant;

/// A completed interval at one logical provider-dispatch boundary.
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// Monotonic elapsed time; browser/worker hosts require `performance.now`.
    pub elapsed: Duration,
    /// Every dispatched call, including calls returning an error.
    pub requests: Requests,
}

/// One interval borrowing its original ledger; completion cannot select another.
#[must_use = "finish the sample after observing the original operation result"]
pub struct SampleStart<'a> {
    ledger: &'a RequestLedger,
    before: Requests,
    started: Instant,
}

impl RequestLedger {
    /// Begin an on-demand interval. Unrelated concurrent work needs its own ledger.
    /// Native hosts use `std::time::Instant`; browser/worker hosts use Performance.
    pub fn start_sample(&self) -> SampleStart<'_> {
        SampleStart {
            ledger: self,
            before: self.requests(),
            started: Instant::now(),
        }
    }
}

impl SampleStart<'_> {
    /// Finish after the original operation settles, retaining error-path counts.
    /// This reads six counters and a clock; it never waits or resets the ledger.
    pub fn finish(self) -> Sample {
        Sample {
            elapsed: self.started.elapsed(),
            requests: self.ledger.requests().since(self.before),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::testing::RequestClass;
    use std::task::{Context, Poll};

    #[test]
    fn sampling_has_no_per_interval_allocation() {
        let ledger = RequestLedger::new();
        let allocations = acyclic_test_support::measure(|| {
            let sample = ledger.start_sample();
            ledger.record(RequestClass::Data);
            std::hint::black_box(sample.finish());
        });
        assert_eq!(
            allocations.count_total, 0,
            "sampling allocated per interval"
        );
        assert_eq!(allocations.bytes_total, 0);
    }

    #[test]
    fn producer_loss_settles_the_original_wait_and_retains_failed_dispatch() {
        let ledger = RequestLedger::new();
        ledger.record(RequestClass::Token);
        let (producer, consumer) = futures::channel::oneshot::channel::<()>();
        let mut operation = Box::pin(async {
            let sample = ledger.start_sample();
            ledger.record(RequestClass::Data);
            let result = consumer.await;
            (result, sample.finish())
        });
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        assert!(operation.as_mut().poll(&mut context).is_pending());
        drop(producer);
        let Poll::Ready((result, sample)) = operation.as_mut().poll(&mut context) else {
            panic!("sampling waited after the original producer was dropped");
        };
        assert!(result.is_err());
        assert_eq!(sample.requests.data(), 1);
        assert_eq!(sample.requests.control(), 0);
        assert_eq!(ledger.requests().class(RequestClass::Token), 1);
    }

    #[test]
    fn samples_keep_nested_control_calls_and_do_not_reset_other_handles() {
        let ledger = RequestLedger::new();
        let inner = ledger.clone();
        let sample = ledger.start_sample();
        inner.record(RequestClass::Data);
        inner.record(RequestClass::Head);
        let sample = sample.finish();
        assert_eq!(sample.requests.data(), 1);
        assert_eq!(sample.requests.control(), 1);
        assert_eq!(sample.requests, inner.requests());
        let next = ledger.start_sample().finish();
        assert_eq!(next.requests, Requests::default());
        assert_eq!(inner.requests(), sample.requests);
    }
}
