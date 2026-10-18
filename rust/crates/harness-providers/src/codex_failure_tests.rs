use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use acyclic_harness::{Error, ModelTransportFailure, NativeFutureBundle, wire::ErrorCode};
use std::sync::atomic::{AtomicUsize, Ordering};

fn work() -> NativeMaterializationWork {
    NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
        &CancellationToken::new(),
    )
    .unwrap_or_else(|_| panic!("fixture work"))
}

struct FailingTransport(ModelTransportFailure, AtomicUsize);

impl HttpTransport for FailingTransport {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("native refresh must use its ledger")
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn send_with_native_work<'a>(
        &'a self,
        _: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse>> {
        NativeFutureBundle::new(work, || async move {
            self.1.fetch_add(1, Ordering::SeqCst);
            // Let the second bearer queue on the real refresh owner before it settles.
            tokio::task::yield_now().await;
            Err(crate::transport_failure(true, self.0, "endpoint failure"))
        })
    }
}

#[tokio::test]
async fn native_refresh_failure_keeps_its_class_for_every_queued_caller() -> Result<()> {
    for failure in [
        ModelTransportFailure::Connect,
        ModelTransportFailure::Timeout,
        ModelTransportFailure::Interrupted,
        ModelTransportFailure::Tls,
    ] {
        for native_follower in [false, true] {
            let (_root, login, _) = fixture(&jwt(0), 200)?;
            let fresh = login.fresh();
            let transport = FailingTransport(failure, AtomicUsize::new(0));
            let leader_work = work();
            let follower_work = work();
            {
                let mut leader = leader_work.claim(0, 0, WorkCounters::UNCHARGED)?;
                let mut follower = follower_work.claim(0, 0, WorkCounters::UNCHARGED)?;
                let (left, right) = futures::join!(
                    login.bearer(&transport, None, Some(&mut leader)),
                    fresh.bearer(&transport, None, native_follower.then_some(&mut follower)),
                );
                let left = left.unwrap_err();
                let right = right.unwrap_err();
                assert_eq!(left.code(), right.code(), "{failure:?}");
                assert_eq!(left.to_string(), right.to_string(), "{failure:?}");
                // A transport failure is the provider's, so it settles the task instead of stranding it.
                assert!(matches!(&right, Error::ModelUnavailable { excerpt, .. } if excerpt.is_empty()));
                assert_eq!(transport.1.load(Ordering::SeqCst), 1);
            }
            assert_eq!(
                leader_work.live_allocation_bytes(),
                NativeMaterializationWork::accounting_state_bytes()
            );
            assert_eq!(
                follower_work.live_allocation_bytes(),
                NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn shared_refresh_causes_round_trip_without_reclassification(
        text in "[a-zé ]{0,80}",
        status in prop_oneof![Just(408u16), Just(429), 500u16..600],
    ) {
        let mut errors = vec![Error::Invalid(text.clone()), Error::Unauthorized(text.clone()), Error::Storage(text.clone())];
        for failure in [ModelTransportFailure::Connect, ModelTransportFailure::Timeout,
            ModelTransportFailure::Interrupted, ModelTransportFailure::Tls, ModelTransportFailure::Status(status)] {
            errors.push(crate::transport_failure(true, failure, &text));
            errors.push(Error::ModelUnavailable { failure, excerpt: text.clone() });
        }
        for error in errors {
            let expected_code = error.code();
            let expected_text = error.to_string();
            let mut shared = None;
            prop_assert!(RefreshFailure::share::<()>(Err(error), &mut shared).is_err());
            let shared = shared.unwrap();
            let mut io = CredentialIo::new(None).unwrap();
            let restored = shared.error(&mut io).unwrap();
            prop_assert_eq!(restored.code(), expected_code);
            prop_assert_eq!(restored.to_string(), expected_text);
            let recached = RefreshFailure::from_error(&restored).unwrap();
            let again = recached.error(&mut io).unwrap();
            prop_assert_eq!(restored.code(), again.code());
            prop_assert_eq!(restored.to_string(), again.to_string());
            if let RefreshFailure::Boundary { code, reason } = shared {
                prop_assert_eq!(restored.code(), code);
                prop_assert_eq!(restored.to_string(), reason);
                let measured = acyclic_test_support::measure(|| { std::hint::black_box(shared.error(&mut io).unwrap()); });
                prop_assert_eq!(measured.count_total, 0);
                // Negative control: the old diagnostic fallback recoded native Unavailable
                // as Invalid and prepended "invalid request" to the original cause.
                if code == ErrorCode::Unavailable {
                    let old = Error::Invalid(reason.to_owned());
                    prop_assert_ne!(old.code(), restored.code());
                }
            }
        }
        let work = work();
        let diagnostic = Error::NativeDiagnostic {
            code: ErrorCode::Unavailable,
            message: text,
            custody: work.claim(256, 1, WorkCounters::UNCHARGED).unwrap(),
        };
        // A diagnostic whose transport subtype was already erased stays invocation-local;
        // it must never enter the cache's Invalid reconstruction branch.
        prop_assert!(RefreshFailure::from_error(&diagnostic).is_none());
    }
}
