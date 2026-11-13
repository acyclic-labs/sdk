use crate::RetrySpec;
use acyclic_harness_core::{Error, model::ModelDispatch, wire::ErrorCode};
use std::time::Duration;

const fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

/// One HTTP operation's recovery owner. Only transient transport failures retry.
pub(crate) struct Attempt {
    status: Option<u16>,
    redelivery: Redelivery,
    #[cfg(not(target_arch = "wasm32"))]
    refreshed: bool,
}

impl Attempt {
    pub(crate) fn generation(spec: RetrySpec, dispatch: ModelDispatch) -> Self {
        Self::new(Redelivery::new(spec, dispatch))
    }

    pub(crate) fn count(spec: RetrySpec, request_digest: [u8; 32]) -> Self {
        Self::new(Redelivery::for_request(spec, request_digest))
    }

    fn new(redelivery: Redelivery) -> Self {
        Self {
            status: None,
            redelivery,
            #[cfg(not(target_arch = "wasm32"))]
            refreshed: false,
        }
    }

    /// Record headers before reading a refusal or a successful count body.
    pub(crate) fn observe(&mut self, status: u16) {
        self.status = Some(status);
    }

    pub(crate) fn next(
        &mut self,
        error: &Error,
        retry_after: Option<Duration>,
    ) -> Option<Duration> {
        if error.code() != ErrorCode::Unavailable {
            return None;
        }
        let retryable = match self.status {
            Some(status) if !(200..300).contains(&status) => retryable_status(status),
            _ => match error {
                Error::ModelUnavailable {
                    failure: acyclic_harness_core::ModelTransportFailure::Status(status),
                    ..
                } => retryable_status(*status),
                Error::ModelUnavailable { .. } => true,
                _ => false,
            },
        };
        if !retryable {
            return None;
        }
        let wait = self.redelivery.next(retry_after);
        self.status = None;
        Some(wait)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn refresh(&mut self, error: &Error) -> bool {
        if error.code() != ErrorCode::Unauthorized || self.status != Some(401) || self.refreshed {
            return false;
        }
        self.refreshed = true;
        self.status = None;
        true
    }
}

/// One operation's uncapped backoff schedule. Its owner retains permanent HTTP status.
struct Redelivery {
    spec: RetrySpec,
    /// Jitter seed: concurrent exchanges spread out while one exchange's schedule replays.
    seed: [u8; 32],
    // Saturation bounds the backoff exponent, never the number of waits.
    attempt: u8,
}

impl Redelivery {
    /// The schedule of one model dispatch.
    fn new(spec: RetrySpec, dispatch: ModelDispatch) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness-providers:redelivery:dispatch:v1");
        hasher.update(&dispatch.operation_id.into_bytes());
        hasher.update(&dispatch.step.to_le_bytes());
        Self::for_request(spec, *hasher.finalize().as_bytes())
    }

    /// Uses an existing identity digest only to spread retry waits, never for admission.
    const fn for_request(spec: RetrySpec, seed: [u8; 32]) -> Self {
        Self {
            spec,
            seed,
            attempt: 0,
        }
    }

    /// The wait before redelivering a classified transient failure.
    fn next(&mut self, retry_after: Option<Duration>) -> Duration {
        let wait = match retry_after {
            Some(wait) => wait,
            None => {
                let ceiling = u64::from(self.spec.base_delay_ms)
                    .saturating_mul(1 << self.attempt.min(16))
                    .min(u64::from(self.spec.max_delay_ms));
                // Equal jitter from the exchange's seed.
                let mut hasher = blake3::Hasher::new_keyed(&self.seed);
                hasher.update(&[self.attempt]);
                let sample = hasher
                    .finalize()
                    .as_bytes()
                    .split_first_chunk::<8>()
                    .map_or(0, |(sample, _)| u64::from_le_bytes(*sample));
                let half = ceiling / 2;
                Duration::from_millis(half + sample % (ceiling - half + 1))
            }
        };
        self.attempt = self.attempt.saturating_add(1);
        wait
    }
}

/// Observe the original timer in the model stream, without a parallel log or
/// waiter. The same timer still exclusively determines redelivery readiness.
pub(crate) fn events<'a>(
    transport: &'a dyn crate::HttpTransport,
    error: Error,
    duration: Duration,
    retry_after: bool,
    work: acyclic_fs::NativeWork<'a>,
) -> impl futures::Stream<Item = acyclic_harness_core::Result<acyclic_harness_core::model::ModelEvent>>
+ acyclic_stream::ProviderTask
+ 'a {
    async_stream::try_stream! {
        use acyclic_harness_core::model::{ModelEvent, ModelWait};
        #[cfg(not(target_arch = "wasm32"))]
        let primitive = match &error {
        Error::ModelUnavailable {
            failure: acyclic_harness_core::ModelTransportFailure::Status(429),
            ..
        } => "provider rate limit",
        _ => "provider transport",
        };
        let cause = match error {
            Error::ModelUnavailable { failure, .. } => Some(failure),
            _ => None,
        };
        yield ModelEvent::Wait(ModelWait::Started { cause, delay: duration, retry_after });
        let started = transport.now();
        let timer = crate::deadline::timer(transport, duration, work)?;
    #[cfg(not(target_arch = "wasm32"))]
    {
    let waiting = acyclic_native_runtime::obs::waiting(
        primitive,
        format_args!("for {duration:?}"),
        timer,
    );
    waiting.await?;
    }
    #[cfg(target_arch = "wasm32")]
    { timer.await?; }
        yield ModelEvent::Wait(ModelWait::Finished {
            elapsed: transport.now().saturating_sub(started),
        });
    }
}

#[cfg(any(test, feature = "fuzzing"))]
pub(crate) use acyclic_harness_core::http::retry_after_at;

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness_core::{ModelTransportFailure, OperationId};
    fn dispatch() -> ModelDispatch {
        ModelDispatch {
            operation_id: OperationId::from_bytes([3; 16]),
            step: 1,
            request_digest: [0; 32],
        }
    }
    fn unavailable(failure: ModelTransportFailure) -> Error {
        Error::ModelUnavailable {
            failure,
            excerpt: String::new(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn recovery_timer_excludes_observer_latency_and_does_not_require_a_wall_clock() {
        use crate::{HttpRequest, HttpResponse, HttpTransport};
        use acyclic_stream::BoxProviderFuture;
        use futures::StreamExt as _;
        struct Clock {
            origin: tokio::time::Instant,
            wall: bool,
        }
        impl HttpTransport for Clock {
            fn send(
                &self,
                _: HttpRequest,
            ) -> BoxProviderFuture<'_, acyclic_harness_core::Result<HttpResponse>> {
                panic!("timer replay must not issue HTTP")
            }
            fn now(&self) -> Duration {
                self.origin.elapsed()
            }
            fn unix_ms(&self) -> acyclic_harness_core::Result<u64> {
                if self.wall {
                    Ok(1_000_000)
                } else {
                    Err(crate::invalid("wall clock unavailable"))
                }
            }
            fn delay(
                &self,
                duration: Duration,
            ) -> BoxProviderFuture<'static, acyclic_harness_core::Result<()>> {
                Box::pin(async move {
                    tokio::time::sleep(duration).await;
                    Ok(())
                })
            }
            fn delay_with_native_work(
                &self,
                duration: Duration,
                work: &acyclic_fs::NativeMaterializationWork,
            ) -> acyclic_harness_core::Result<
                acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>,
            > {
                acyclic_fs::NativeFutureBundle::new(work, || self.delay(duration))
            }
        }
        for native in [false, true] {
            for wall in [false, true] {
                tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap().block_on(async {
                    let clock = Clock { origin: tokio::time::Instant::now(), wall };
                    let work = crate::tests::native_tests::work(&acyclic_fs::CancellationToken::new()).unwrap();
                    let baseline = work.live_allocation_bytes();
                    let mut stream = Box::pin(events(&clock, unavailable(ModelTransportFailure::Connect), Duration::from_secs(3), false, native.then_some(&work)));
                    assert!(matches!(stream.next().await, Some(Ok(acyclic_harness_core::model::ModelEvent::Wait(
                        acyclic_harness_core::model::ModelWait::Started { .. }
                    )))));
                    // The original observer may take time to commit its start.
                    tokio::time::advance(Duration::from_secs(7)).await;
                    assert!(matches!(stream.next().await, Some(Ok(acyclic_harness_core::model::ModelEvent::Wait(
                        acyclic_harness_core::model::ModelWait::Finished { elapsed }
                    ))) if elapsed == Duration::from_secs(3)));
                    assert_eq!(clock.now(), Duration::from_secs(10));
                    drop(stream);
                    assert_eq!(work.live_allocation_bytes(), baseline);
                });
            }
        }
    }
    #[test]
    fn every_transient_class_waits_without_a_cap_and_permanent_errors_do_not() {
        for failure in [
            ModelTransportFailure::Status(408),
            ModelTransportFailure::Status(429),
            ModelTransportFailure::Status(503),
            ModelTransportFailure::Connect,
            ModelTransportFailure::Interrupted,
            ModelTransportFailure::Timeout,
            ModelTransportFailure::Tls,
        ] {
            let mut owner = Attempt::generation(RetrySpec::DEFAULT, dispatch());
            for index in 0..300 {
                if let ModelTransportFailure::Status(status) = failure {
                    owner.observe(status);
                }
                let hint = (index == 299).then_some(Duration::from_secs(60));
                let wait = owner
                    .next(&unavailable(failure), hint)
                    .expect("transient failure must wait");
                if let Some(hint) = hint {
                    assert_eq!(wait, hint);
                } else {
                    assert!(!wait.is_zero());
                    assert!(wait <= Duration::from_secs(8));
                }
            }
            for error in [
                Error::Unauthorized("auth".into()),
                Error::Invalid("request".into()),
                Error::Storage("storage".into()),
                Error::ModelDiagnostic {
                    code: ErrorCode::Unavailable,
                    message: "credential owner closed".into(),
                    custody: Default::default(),
                },
                unavailable(ModelTransportFailure::Status(400)),
            ] {
                assert!(owner.next(&error, None).is_none());
            }
        }
        // A final HTTP refusal stays final even if reading its diagnostic is interrupted.
        let mut owner = Attempt::generation(RetrySpec::DEFAULT, dispatch());
        owner.observe(400);
        assert_eq!(
            owner.next(&unavailable(ModelTransportFailure::Interrupted), None),
            None
        );
        // An observed transient refusal can have an opaque diagnostic body.
        let mut owner = Attempt::generation(RetrySpec::DEFAULT, dispatch());
        owner.observe(503);
        assert!(
            owner
                .next(
                    &Error::ModelDiagnostic {
                        code: ErrorCode::Unavailable,
                        message: "provider unavailable".into(),
                        custody: Default::default(),
                    },
                    None,
                )
                .is_some()
        );
        owner.observe(503);
        assert!(
            owner
                .next(&Error::Storage("refusal accounting failed".into()), None)
                .is_none()
        );
    }
    proptest::proptest! {
        #[test]
        fn retry_after_forms_round_trip_without_shortening_provider_waits(at in 0..4_000_000_000u64,now in 0..4_000_000_000u64,delta in proptest::prelude::any::<u64>()) {
            let clock=std::time::UNIX_EPOCH+Duration::from_secs(now);
            let date=httpdate::fmt_http_date(std::time::UNIX_EPOCH+Duration::from_secs(at));
            for (value,expected) in [(date,Duration::from_secs(at.saturating_sub(now))),(delta.to_string(),Duration::from_secs(delta))] {
                let wait=retry_after_at(&value,||clock).unwrap();
                proptest::prop_assert_eq!(wait,expected);
                let mut owner=Attempt::generation(RetrySpec::DEFAULT,dispatch());
                proptest::prop_assert_eq!(owner.next(&unavailable(ModelTransportFailure::Status(503)),Some(wait)),Some(expected));
            }
        }
    }
}
