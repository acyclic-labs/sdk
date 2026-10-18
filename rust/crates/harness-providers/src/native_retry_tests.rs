//! Every native retry, for generation and counting, funds its timer on the issued ledger.
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::NativeFutureBundle;
use proptest::prelude::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct RetryTransport {
    platform: ReqwestTransport,
    sent: AtomicUsize,
    portable: AtomicUsize,
    native: AtomicUsize,
    failures: usize,
    reject_timer: bool,
    count: bool,
}

impl HttpTransport for RetryTransport {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("native exchange must be admitted")
    }

    fn now(&self) -> Duration {
        crate::http::native_now()
    }

    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, ()> {
        self.portable.fetch_add(1, Ordering::SeqCst);
        self.platform.delay(wait)
    }

    fn delay_with_native_work(
        &self,
        wait: Duration,
        work: &NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'static, ()>> {
        self.native.fetch_add(1, Ordering::SeqCst);
        if self.reject_timer {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason: "timer admission refused",
            });
        }
        let before = work.live_allocation_bytes();
        let mut timer = None;
        let allocated = acyclic_test_support::measure(|| {
            timer = Some(self.platform.delay_with_native_work(wait, work).unwrap());
        });
        assert_eq!(allocated.count_total, 1);
        assert_eq!(allocated.bytes_total, work.live_allocation_bytes() - before);
        Ok(timer.unwrap())
    }

    fn send_with_native_work<'a>(
        &'a self,
        _: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse>> {
        NativeFutureBundle::new(work, || async move {
            let attempt = self.sent.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures {
                return Err(crate::transport_failure(
                    true,
                    acyclic_harness::ModelTransportFailure::Connect,
                    "unreachable",
                ));
            }
            let body = if self.count {
                "{\"input_tokens\":7}"
            } else {
                CHAT
            };
            HttpResponse::with_native_body(
                200,
                futures::stream::iter([Ok(Bytes::from_static(body.as_bytes()))]),
                work,
            )
        })
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn native_retry_timers_use_original_ledger_for_every_exchange(
        failures in 0..5usize, attempts in 0..4u8,
        count in any::<bool>(), reject_timer in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time().start_paused(true).build().unwrap();
        runtime.block_on(async {
            let (mut provider, _) = provider(Adapter::Chat, 200);
            Arc::make_mut(&mut provider.config).retry = RetrySpec {
                attempts, base_delay_ms: 2, max_delay_ms: 2,
            };
            let transport = Arc::new(RetryTransport {
                platform: ReqwestTransport::new().unwrap(), sent: AtomicUsize::new(0),
                portable: AtomicUsize::new(0), native: AtomicUsize::new(0),
                failures, reject_timer, count,
            });
            provider.transport = crate::deadline::Bounded::new(transport.clone(), TimeoutSpec::DEFAULT);
            let (request, dispatch) = request(&provider).unwrap();
            let work = super::native_tests::work(&CancellationToken::new()).unwrap();
            let baseline = work.live_allocation_bytes();
            let result = if count {
                provider.count_tokens_with_native_work(&request, &work).unwrap().await.map(drop)
            } else {
                async {
                    let mut stream = provider.generate_with_native_work(&request, dispatch, &work)?.await?;
                    while let Some(event) = stream.next().await { drop(event?); }
                    Ok(())
                }.await
            };
            let retries = failures.min(usize::from(attempts));
            assert_eq!(transport.portable.load(Ordering::SeqCst), 0);
            assert_eq!(transport.native.load(Ordering::SeqCst), if reject_timer { retries.min(1) } else { retries });
            assert_eq!(transport.sent.load(Ordering::SeqCst), if reject_timer && retries > 0 { 1 } else { retries + 1 });
            assert_eq!(result.is_ok(), failures == 0 || (!reject_timer && failures <= usize::from(attempts)));
            drop(result);
            assert_eq!(work.live_allocation_bytes(), baseline);
            // Negative control: the former retry path allocates a portable timer with no claim.
            let allocated = acyclic_test_support::measure(|| {
                drop(transport.delay(Duration::from_millis(1)));
            });
            assert!(allocated.bytes_total > 0);
            assert_eq!(work.live_allocation_bytes(), baseline);
            assert_eq!(transport.portable.load(Ordering::SeqCst), 1);
        });
    }
}
