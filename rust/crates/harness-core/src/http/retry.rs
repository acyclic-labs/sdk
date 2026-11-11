//! Provider-authored recovery timing, decoded once for every HTTP host.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Both HTTP forms of Retry-After, relative to the platform clock.
pub fn retry_after(value: &str) -> Option<Duration> {
    retry_after_at(value, || {
        #[cfg(not(target_arch = "wasm32"))]
        let now = SystemTime::now();
        #[cfg(target_arch = "wasm32")]
        let now = UNIX_EPOCH + Duration::from_secs_f64(js_sys::Date::now() / 1_000.0);
        now
    })
}

/// Decodes a delay hint relative to the supplied original clock.
pub fn retry_after_at(value: &str, now: impl FnOnce() -> SystemTime) -> Option<Duration> {
    let value = value.trim();
    if integer(value) {
        // An overflowing hint remains a delay, never an invitation to retry early.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    Some(
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now())
            .unwrap_or_default(),
    )
}

/// Retry-After is authoritative. Without it, wait until the provider's latest
/// applicable reset. A matching positive remaining counter excludes that bucket;
/// missing or malformed counters do not discard its reset hint. No padding,
/// jitter or client backoff changes a valid hint.
///
/// RateLimit-Reset and x-ratelimit-reset-* express relative durations;
/// x-ratelimit-reset and anthropic-ratelimit-*-reset express absolute times.
pub fn response_retry_after<N: AsRef<str>, V: AsRef<str>, H: IntoIterator<Item = (N, V)>>(
    headers: impl Fn() -> H,
    now: SystemTime,
) -> Option<Duration> {
    let mut reset = None;
    for (name, value) in headers() {
        let (name, value) = (name.as_ref(), value.as_ref().trim());
        if name.eq_ignore_ascii_case("retry-after") {
            if let Some(wait) = retry_after_at(value, || now) {
                return Some(wait);
            }
        } else {
            let wait = if name.eq_ignore_ascii_case("ratelimit-reset")
                || prefix(name, "x-ratelimit-reset-")
            {
                if integer(value) {
                    Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)))
                } else {
                    humantime::parse_duration(value)
                        .ok()
                        .or_else(|| Duration::try_from_secs_f64(value.parse().ok()?).ok())
                }
            } else if name.eq_ignore_ascii_case("x-ratelimit-reset")
                || (prefix(name, "anthropic-ratelimit-")
                    && name
                        .get(name.len().saturating_sub(6)..)
                        .is_some_and(|end| end.eq_ignore_ascii_case("-reset")))
            {
                if integer(value) {
                    Some(
                        Duration::from_secs(value.parse().unwrap_or(u64::MAX))
                            .saturating_sub(now.duration_since(UNIX_EPOCH).unwrap_or_default()),
                    )
                } else {
                    humantime::parse_rfc3339(value)
                        .ok()
                        .or_else(|| httpdate::parse_http_date(value).ok())
                        .map(|at| at.duration_since(now).unwrap_or_default())
                }
            } else {
                None
            };
            if let Some(wait) = wait {
                // Header order is immaterial. Revisit the borrowed header view rather
                // than allocate an index before native response admission.
                if headers().into_iter().any(|(counter, remaining)| {
                    remaining_for(name, counter.as_ref())
                        && remaining
                            .as_ref()
                            .trim()
                            .parse::<u64>()
                            .is_ok_and(|remaining| remaining > 0)
                }) {
                    continue;
                }
                reset = Some(reset.map_or(wait, |prior: Duration| prior.max(wait)));
            }
        }
    }
    reset
}

fn remaining_for(reset: &str, counter: &str) -> bool {
    let Some(at) = reset
        .as_bytes()
        .windows(b"-reset".len())
        .position(|part| part.eq_ignore_ascii_case(b"-reset"))
    else {
        return false;
    };
    let (before, after) = (&reset[..at], &reset[at + "-reset".len()..]);
    counter.len() == before.len() + "-remaining".len() + after.len()
        && prefix(counter, before)
        && counter
            .get(at..at + "-remaining".len())
            .is_some_and(|part| part.eq_ignore_ascii_case("-remaining"))
        && counter
            .get(at + "-remaining".len()..)
            .is_some_and(|part| part.eq_ignore_ascii_case(after))
}

fn integer(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn prefix(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_hints_keep_their_units_and_retry_after_precedence() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for (name, value, millis) in [
            ("Retry-After", "60", 60_000),
            ("Retry-After", "Tue, 14 Nov 2023 22:14:20 GMT", 60_000),
            ("RateLimit-Reset", "60", 60_000),
            ("X-RateLimit-Reset", "1700000060", 60_000),
            ("x-ratelimit-reset-requests", "1m5s", 65_000),
            ("x-ratelimit-reset-tokens", "60.25", 60_250),
            (
                "anthropic-ratelimit-tokens-reset",
                "2023-11-14T22:14:20Z",
                60_000,
            ),
        ] {
            let expected = Duration::from_millis(millis);
            assert_eq!(
                response_retry_after(|| [(name, value)], now),
                Some(expected)
            );
            assert_eq!(
                response_retry_after(
                    || [
                        ("x-ratelimit-reset", "1700000999"),
                        ("Retry-After", "0"),
                        (name, value)
                    ],
                    now
                ),
                Some(Duration::ZERO)
            );
        }
        assert_eq!(
            response_retry_after(
                || [
                    ("x-ratelimit-reset-requests", "1s"),
                    ("x-ratelimit-reset-tokens", "65s")
                ],
                now
            ),
            Some(Duration::from_secs(65))
        );
        assert_eq!(
            response_retry_after(
                || [("Retry-After", "invalid"), ("RateLimit-Reset", "60")],
                now
            ),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            response_retry_after(|| [("x-ratelimit-reset", "1699999999")], now),
            Some(Duration::ZERO)
        );
        assert_eq!(
            response_retry_after(
                || [("Retry-After", ""), ("RateLimit-Reset", "invalid")],
                now
            ),
            None
        );
    }

    #[test]
    fn unused_buckets_do_not_extend_an_exhausted_limits_reset() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for (request_reset, request_remaining, token_reset, token_remaining) in [
            (
                "X-RateLimit-Reset-Requests",
                "x-ratelimit-remaining-requests",
                "x-ratelimit-reset-tokens",
                "X-RateLimit-Remaining-Tokens",
            ),
            (
                "anthropic-ratelimit-requests-reset",
                "anthropic-ratelimit-requests-remaining",
                "anthropic-ratelimit-tokens-reset",
                "anthropic-ratelimit-tokens-remaining",
            ),
        ] {
            let absolute = request_reset.starts_with("anthropic-");
            let one = if absolute { "1700000001" } else { "1s" };
            let sixty_five = if absolute { "1700000065" } else { "65s" };
            let mut headers = [
                (token_reset, sixty_five),
                (request_remaining, "0"),
                (request_reset, one),
                (token_remaining, "10000"),
            ];
            for reverse in [false, true] {
                if reverse {
                    headers.reverse();
                }
                assert_eq!(
                    response_retry_after(|| headers, now),
                    Some(Duration::from_secs(1))
                );
            }
            for remaining in ["0", "invalid", ""] {
                for header in &mut headers {
                    if header.0 == token_remaining {
                        header.1 = remaining;
                    }
                }
                assert_eq!(
                    response_retry_after(|| headers, now),
                    Some(Duration::from_secs(65))
                );
            }
            assert_eq!(
                response_retry_after(|| [(token_reset, sixty_five), (request_reset, one)], now),
                Some(Duration::from_secs(65))
            );
        }
        for (reset, remaining, at) in [
            ("RateLimit-Reset", "RateLimit-Remaining", "65"),
            ("X-RateLimit-Reset", "X-RateLimit-Remaining", "1700000065"),
        ] {
            assert_eq!(
                response_retry_after(|| [(reset, at), (remaining, "1")], now),
                None
            );
            assert_eq!(
                response_retry_after(|| [(reset, at), (remaining, "0")], now),
                Some(Duration::from_secs(65))
            );
            assert_eq!(
                response_retry_after(
                    || [(reset, at), (remaining, "0"), ("Retry-After", "60")],
                    now
                ),
                Some(Duration::from_secs(60))
            );
        }
    }
}
