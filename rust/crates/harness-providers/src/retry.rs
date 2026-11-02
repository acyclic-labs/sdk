use crate::RetrySpec;
use acyclic_harness_core::{Error, model::ModelDispatch, wire::ErrorCode};
use std::time::Duration;

const fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

#[derive(Clone, Copy, PartialEq)]
enum Admission {
    Unknown,
    Refused(u16),
    Accepted,
}

/// One HTTP operation's recovery owner. Generation becomes irreversible at
/// successful headers; a read-only count can recover until its body validates.
pub(crate) struct Attempt {
    admission: Admission,
    count: bool,
    redelivery: Redelivery,
    #[cfg(not(target_arch = "wasm32"))]
    refreshed: bool,
}

impl Attempt {
    pub(crate) fn generation(spec: RetrySpec, dispatch: ModelDispatch) -> Self {
        Self::new(Redelivery::new(spec, dispatch), false)
    }

    pub(crate) fn count(spec: RetrySpec, request_digest: [u8; 32]) -> Self {
        Self::new(Redelivery::for_request(spec, request_digest), true)
    }

    fn new(redelivery: Redelivery, count: bool) -> Self {
        Self {
            admission: Admission::Unknown,
            count,
            redelivery,
            #[cfg(not(target_arch = "wasm32"))]
            refreshed: false,
        }
    }

    /// Record headers before reading a refusal or a successful count body.
    /// A later failure cannot undo generation's acceptance fence.
    pub(crate) fn observe(&mut self, status: u16) {
        if self.admission != Admission::Accepted {
            self.admission = if (200..300).contains(&status) {
                Admission::Accepted
            } else {
                Admission::Refused(status)
            };
        }
    }

    pub(crate) fn next(
        &mut self,
        error: &Error,
        retry_after: Option<Duration>,
    ) -> Option<Duration> {
        let retryable = match self.admission {
            Admission::Unknown => {
                self.count
                    || matches!(
                        error,
                        Error::ModelUnavailable {
                            failure: acyclic_harness_core::ModelTransportFailure::Connect,
                            ..
                        }
                    )
            }
            Admission::Refused(status) => retryable_status(status),
            Admission::Accepted => self.count,
        };
        if !retryable {
            return None;
        }
        let wait = self.redelivery.next(error, retry_after);
        if wait.is_some() {
            self.admission = Admission::Unknown;
        }
        wait
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn refresh(&mut self, error: &Error) -> bool {
        if self.count
            || error.code() != ErrorCode::Unauthorized
            || self.admission != Admission::Refused(401)
            || self.refreshed
        {
            return false;
        }
        self.refreshed = true;
        self.admission = Admission::Unknown;
        true
    }
}

/// One exchange's redelivery schedule. Its owner supplies the operation's
/// completion fence; generation additionally requires refusal or connection failure.
struct Redelivery {
    spec: RetrySpec,
    /// Jitter seed: concurrent exchanges spread out while one exchange's schedule replays.
    seed: [u8; 32],
    attempt: u8,
}

impl Redelivery {
    /// The budget of one model dispatch.
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

    /// The wait before redelivering after `error`, or `None` when `error` is final.
    fn next(&mut self, error: &Error, retry_after: Option<Duration>) -> Option<Duration> {
        if error.code() != ErrorCode::Unavailable || self.attempt >= self.spec.attempts {
            return None;
        }
        let maximum = Duration::from_millis(self.spec.max_delay_ms.into());
        let wait = match retry_after {
            Some(wait) if wait > maximum => return None,
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
        self.attempt += 1;
        Some(wait)
    }
}

/// Both HTTP forms of `Retry-After`, relative to the platform clock.
pub(crate) fn retry_after(value: &str) -> Option<Duration> {
    retry_after_at(value, || {
        #[cfg(not(target_arch = "wasm32"))]
        let now = std::time::SystemTime::now();
        #[cfg(target_arch = "wasm32")]
        let now = std::time::UNIX_EPOCH + Duration::from_secs_f64(js_sys::Date::now() / 1_000.0);
        now
    })
}

pub(crate) fn retry_after_at(
    value: &str,
    now: impl FnOnce() -> std::time::SystemTime,
) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // Overflow is still a delay hint, always beyond our u32-millisecond retry budget.
        // Treating it as absent would retry before the endpoint's requested time.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(date.duration_since(now()).unwrap_or(Duration::ZERO))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness_core::OperationId;

    fn dispatch() -> ModelDispatch {
        ModelDispatch {
            operation_id: OperationId::from_bytes([3; 16]),
            step: 1,
            request_digest: [0; 32],
        }
    }
    fn unavailable() -> Error {
        Error::ModelUnavailable {
            failure: acyclic_harness_core::ModelTransportFailure::Status(503),
            excerpt: String::new(),
        }
    }

    #[test]
    fn redelivery_is_bounded_jittered_and_honors_retry_after() {
        let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        let waits: Vec<_> = (0..4).map(|_| retry.next(&unavailable(), None)).collect();
        assert_eq!(waits.last(), Some(&None));
        for (attempt, wait) in waits.iter().take(3).enumerate() {
            let ceiling = Duration::from_millis(500 << attempt);
            assert!(wait.is_some_and(|wait| wait >= ceiling / 2 && wait <= ceiling));
        }
        // The schedule replays exactly for the same dispatch.
        let mut again = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(
            again.next(&unavailable(), None),
            waits.first().copied().flatten()
        );
        let mut hinted = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(
            hinted.next(&unavailable(), Some(Duration::from_secs(2))),
            Some(Duration::from_secs(2))
        );
        // Negative controls: other classes, an over-long hint, a zero budget.
        let mut fresh = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(fresh.next(&Error::Storage("lost".into()), None), None);
        assert_eq!(fresh.next(&Error::Invalid("bad".into()), None), None);
        let transport = |failure| Error::ModelUnavailable {
            failure,
            excerpt: String::new(),
        };
        // TLS failures meet the same peer again; every other transport class is redelivered.
        assert_eq!(
            fresh.next(
                &transport(acyclic_harness_core::ModelTransportFailure::Tls),
                None
            ),
            None
        );
        for failure in [
            acyclic_harness_core::ModelTransportFailure::Connect,
            acyclic_harness_core::ModelTransportFailure::Timeout,
            acyclic_harness_core::ModelTransportFailure::Interrupted,
        ] {
            let mut each = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            assert!(each.next(&transport(failure), None).is_some());
        }
        assert_eq!(
            fresh.next(&unavailable(), Some(Duration::from_secs(9))),
            None
        );
        let mut none = Redelivery::new(
            RetrySpec {
                attempts: 0,
                ..RetrySpec::DEFAULT
            },
            dispatch(),
        );
        assert_eq!(none.next(&unavailable(), None), None);
        assert_eq!(retry_after(" 7 "), Some(Duration::from_secs(7)));
        assert_eq!(retry_after("not a date"), None);
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(128))]
        #[test]
        fn retry_after_forms_round_trip_and_obey_the_same_budget(
            at in 0..4_000_000_000u64, now in 0..4_000_000_000u64,
            delta in proptest::prelude::any::<u64>(),
            overflow_digits in "[0-9]{20,80}",
        ) {
            let clock = std::time::UNIX_EPOCH + Duration::from_secs(now);
            let date = httpdate::fmt_http_date(std::time::UNIX_EPOCH + Duration::from_secs(at));
            let wait = retry_after_at(&date, || clock).unwrap();
            proptest::prop_assert_eq!(wait, Duration::from_secs(at.saturating_sub(now)));
            proptest::prop_assert_eq!(retry_after_at(&delta.to_string(), || clock), Some(Duration::from_secs(delta)));
            let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            proptest::prop_assert_eq!(retry.next(&unavailable(), Some(wait)),
                (wait <= Duration::from_millis(RetrySpec::DEFAULT.max_delay_ms.into())).then_some(wait));
            // Negative control: the former delta-only parser ignores every date hint.
            proptest::prop_assert_ne!(date.parse::<u64>().ok().map(Duration::from_secs), Some(wait));
            let overflow = format!("1{overflow_digits}");
            let wait = retry_after_at(&overflow, || clock);
            proptest::prop_assert_eq!(wait, Some(Duration::from_secs(u64::MAX)));
            let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            proptest::prop_assert_eq!(retry.next(&unavailable(), wait), None);
            // Negative control: discarding numeric overflow would permit an early retry.
            let mut former = Redelivery::new(RetrySpec::DEFAULT, dispatch());
            proptest::prop_assert!(former.next(&unavailable(), overflow.parse::<u64>().ok().map(Duration::from_secs)).is_some());
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn operation_recovery_preserves_completion_fences_and_budget(
            budget in 0..5u8,
            trace in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY,
                proptest::sample::select(vec![400u16, 401, 403, 408, 413, 429, 503])), 0..64),
        ) {
            let spec = RetrySpec { attempts: budget, ..RetrySpec::DEFAULT };
            let mut attempt = Attempt::generation(spec, dispatch());
            let (mut accepted, mut spent) = (false, false);
            for failure in [acyclic_harness_core::ModelTransportFailure::Interrupted,
                acyclic_harness_core::ModelTransportFailure::Timeout] {
                let error = crate::transport_failure(true, failure, "fixture");
                proptest::prop_assert_eq!(attempt.next(&error, None), None);
            }
            for &(admitted, unauthorized, status) in &trace {
                accepted |= admitted;
                if admitted { attempt.observe(200); }
                if unauthorized { attempt.observe(status); }
                let error = if unauthorized { Error::Unauthorized("fixture".into()) } else { Error::Invalid("fixture".into()) };
                let expected = unauthorized && status == 401 && !accepted && !spent;
                proptest::prop_assert_eq!(attempt.refresh(&error), expected);
                let interruption = crate::transport_failure(true,
                    acyclic_harness_core::ModelTransportFailure::Interrupted, "fixture");
                if accepted { proptest::prop_assert_eq!(attempt.next(&interruption, None), None); }
                spent |= expected;
            }
            // Each path uses one unchanged budget across connect failures,
            // refusal bodies, and (for counts only) accepted-body failures.
            // The same trace can grow without any operation growing its state.
            for count in [false, true] {
                let (mut spent, mut observed) = (0u8, None);
                let mut owner = if count { Attempt::count(spec, [0; 32]) }
                    else { Attempt::generation(spec, dispatch()) };
                for (success, headers, status) in trace.iter().copied()
                    .chain(std::iter::repeat_n((true, true, 200), usize::from(budget) + 1))
                {
                    let failure = if success {
                        acyclic_harness_core::ModelTransportFailure::Interrupted
                    } else {
                        acyclic_harness_core::ModelTransportFailure::Connect
                    };
                    let status = if success { 200 } else { status };
                    if headers {
                        owner.observe(status);
                        if observed.is_none_or(|prior| !(200..300).contains(&prior)) {
                            observed = Some(status);
                        }
                    }
                    let accepted = observed.is_some_and(|status| (200..300).contains(&status));
                    let error = crate::transport_failure(true, failure, "fixture");
                    let retryable = if accepted { count } else {
                        observed.map_or(count || !success, |status| matches!(status, 408 | 429 | 500..=599))
                    };
                    let expected = retryable && spent < budget;
                    let mut actual = None;
                    let allocations = acyclic_test_support::measure(|| { actual = owner.next(&error, None); });
                    proptest::prop_assert_eq!(allocations.count_total, 0);
                    proptest::prop_assert_eq!(actual.is_some(), expected);
                    if expected { spent += 1; observed = None; }
                    if accepted && !count {
                        // Negative control: the old schedule alone has no acceptance fence.
                        let mut unfenced = Redelivery::new(spec, dispatch());
                        proptest::prop_assert_eq!(unfenced.next(&error, None).is_some(), budget > 0);
                    }
                }
                if count && budget > 0 {
                    let error = crate::transport_failure(true,
                        acyclic_harness_core::ModelTransportFailure::Interrupted, "fixture");
                    proptest::prop_assert_eq!(owner.next(&error, None), None);
                    // Negative control: reconstructing the owner replenishes a spent budget.
                    let mut replenished = Attempt::count(spec, [0; 32]);
                    proptest::prop_assert!(replenished.next(&error, None).is_some());
                }
            }
        }
    }
}
