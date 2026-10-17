use crate::RetrySpec;
use acyclic_harness::{Error, model::ModelDispatch, wire::ErrorCode};
use std::time::Duration;

/// One dispatch's redelivery budget. Only a request the endpoint refused before any output is
/// redelivered, so no caller-visible event can repeat.
pub(crate) struct Redelivery {
    spec: RetrySpec,
    dispatch: ModelDispatch,
    attempt: u8,
}

impl Redelivery {
    pub(crate) const fn new(spec: RetrySpec, dispatch: ModelDispatch) -> Self {
        Self {
            spec,
            dispatch,
            attempt: 0,
        }
    }

    /// The wait before redelivering after `error`, or `None` when `error` is final.
    pub(crate) fn next(
        &mut self,
        error: &Error,
        retry_after: Option<Duration>,
        output: bool,
    ) -> Option<Duration> {
        if output || error.code() != ErrorCode::Unavailable || self.attempt >= self.spec.attempts {
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
                // Equal jitter, derived from the dispatch so concurrent tasks spread out while
                // one dispatch's schedule replays exactly.
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"harness-providers:redelivery:v1");
                hasher.update(&self.dispatch.operation_id.into_bytes());
                hasher.update(&self.dispatch.step.to_le_bytes());
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

/// Parses a `Retry-After` header's delta-seconds form; an HTTP date falls back to backoff.
pub(crate) fn retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::OperationId;

    fn dispatch() -> ModelDispatch {
        ModelDispatch {
            operation_id: OperationId::from_bytes([3; 16]),
            step: 1,
            request_digest: [0; 32],
        }
    }
    fn unavailable() -> Error {
        Error::ModelUnavailable {
            status: Some(503),
            excerpt: String::new(),
        }
    }

    #[test]
    fn redelivery_is_bounded_jittered_and_honors_retry_after() {
        let mut retry = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        let waits: Vec<_> = (0..4)
            .map(|_| retry.next(&unavailable(), None, false))
            .collect();
        assert_eq!(waits.last(), Some(&None));
        for (attempt, wait) in waits.iter().take(3).enumerate() {
            let ceiling = Duration::from_millis(500 << attempt);
            assert!(wait.is_some_and(|wait| wait >= ceiling / 2 && wait <= ceiling));
        }
        // The schedule replays exactly for the same dispatch.
        let mut again = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(again.next(&unavailable(), None, false), waits.first().copied().flatten());
        let mut hinted = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(
            hinted.next(&unavailable(), Some(Duration::from_secs(2)), false),
            Some(Duration::from_secs(2))
        );
        // Negative controls: output escaped, other classes, an over-long hint, a zero budget.
        let mut fresh = Redelivery::new(RetrySpec::DEFAULT, dispatch());
        assert_eq!(fresh.next(&unavailable(), None, true), None);
        assert_eq!(
            fresh.next(&Error::Storage("lost".into()), None, false),
            None
        );
        assert_eq!(fresh.next(&Error::Invalid("bad".into()), None, false), None);
        assert_eq!(
            fresh.next(&unavailable(), Some(Duration::from_secs(9)), false),
            None
        );
        let mut none = Redelivery::new(
            RetrySpec {
                attempts: 0,
                ..RetrySpec::DEFAULT
            },
            dispatch(),
        );
        assert_eq!(none.next(&unavailable(), None, false), None);
        assert_eq!(retry_after(" 7 "), Some(Duration::from_secs(7)));
        assert_eq!(retry_after("Wed, 21 Oct 2026 07:28:00 GMT"), None);
    }
}
