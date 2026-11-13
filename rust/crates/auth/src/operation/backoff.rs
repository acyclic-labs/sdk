//! Scheduling only: the caller retains replay admission, timers and cancellation.

/// Uncapped redelivery with exponential spacing and deterministic equal jitter.
/// Seeds spread independent exchanges; they confer no authority or secrecy.
/// Construct a fresh schedule after successful progress. Dropping the caller's
/// original wait still owns cancellation; this value never starts a timer.
pub struct Backoff {
    ceiling_ms: u32,
    maximum_ms: u32,
    sample: u32,
}

impl Backoff {
    /// Select computed wait bounds and an exchange's reproducible jitter seed.
    /// The maximum bounds spacing, never the number of attempts. A zero base
    /// explicitly selects no computed delay; no fallback policy is introduced.
    #[must_use]
    pub fn new(base_delay_ms: u32, maximum_delay_ms: u32, seed: u32) -> Self {
        Self {
            ceiling_ms: base_delay_ms.min(maximum_delay_ms),
            maximum_ms: maximum_delay_ms,
            sample: seed,
        }
    }

    /// Advance after a positively classified replayable failure. The caller may
    /// replace this spacing with an explicit provider wait without shortening it.
    pub fn delay_ms(&mut self) -> u32 {
        let ceiling = self.ceiling_ms;
        self.ceiling_ms = ceiling.saturating_mul(2).min(self.maximum_ms);
        // A noncryptographic scheduling sampler, with one distinct state per wait.
        self.sample = self.sample.wrapping_add(0x9e37_79b9);
        let mut sample = self.sample;
        sample = (sample ^ (sample >> 16)).wrapping_mul(0x21f0_aaad);
        sample = (sample ^ (sample >> 15)).wrapping_mul(0x735a_2d97);
        sample ^= sample >> 15;
        let lower = ceiling.div_ceil(2);
        lower + sample % (ceiling - lower + 1)
    }
}
