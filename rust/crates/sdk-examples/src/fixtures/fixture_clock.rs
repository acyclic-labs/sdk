//! Shared deterministic clock for executable SDK qualification fixtures.
//!
//! Every fixture that exposes provider metadata should use this clock so that
//! collector and server receipts compare semantic values without wall-clock
//! noise. Production providers continue to use their system clock by default.

use acyclic_objects::{Clock, FixedClock};
use std::sync::Arc;

pub const FIXTURE_CLOCK_SECONDS: i64 = 1_700_000_000;
pub const FIXTURE_CLOCK_NANOS: i32 = 123_000_000;

#[derive(Clone, Copy, Debug, Default)]
pub struct FixtureStreamClock;

impl acyclic_stream::UnixMillisClock for FixtureStreamClock {
    fn now_unix_millis(&self) -> u64 {
        (FIXTURE_CLOCK_SECONDS as u64)
            .saturating_mul(1_000)
            .saturating_add((FIXTURE_CLOCK_NANOS.max(0) as u64) / 1_000_000)
    }
}

#[must_use]
pub const fn fixture_clock_parts() -> (i64, i32) {
    (FIXTURE_CLOCK_SECONDS, FIXTURE_CLOCK_NANOS)
}

#[must_use]
pub fn objects_clock() -> Arc<dyn Clock> {
    Arc::new(FixedClock::from_parts(
        FIXTURE_CLOCK_SECONDS,
        FIXTURE_CLOCK_NANOS,
    ))
}

#[must_use]
pub fn stream_clock() -> Arc<dyn acyclic_stream::UnixMillisClock> {
    Arc::new(FixtureStreamClock)
}
