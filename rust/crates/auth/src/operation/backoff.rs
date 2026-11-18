//! Scheduling only: the caller retains replay admission, timers and cancellation.

/// Uncapped redelivery with exponential spacing and deterministic equal jitter.
/// Seeds spread independent exchanges; they confer no authority or secrecy.
/// Reset the schedule after successful progress. Dropping the caller's
/// original wait still owns cancellation; this value never starts a timer.
pub struct Backoff {
    base_ms: u32,
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
            base_ms: base_delay_ms.min(maximum_delay_ms),
            ceiling_ms: base_delay_ms.min(maximum_delay_ms),
            maximum_ms: maximum_delay_ms,
            sample: seed,
        }
    }

    /// Advance after a positively classified replayable failure. The caller may
    /// replace this spacing with an explicit provider wait without shortening it.
    pub fn delay_ms(&mut self) -> u32 {
        // A noncryptographic scheduling sampler, with one distinct state per wait.
        self.sample = self.sample.wrapping_add(0x9e37_79b9);
        let mut sample = self.sample;
        sample = (sample ^ (sample >> 16)).wrapping_mul(0x21f0_aaad);
        sample = (sample ^ (sample >> 15)).wrapping_mul(0x735a_2d97);
        sample ^= sample >> 15;
        self.advance(|_| sample)
    }

    /// Restart spacing after successful progress, retaining the advancing
    /// entropy source so successive recoveries do not repeat one jitter draw.
    pub fn reset(&mut self) {
        self.ceiling_ms = self.base_ms;
    }

    /// Native timer projection of the same schedule.
    pub fn delay(&mut self) -> std::time::Duration {
        std::time::Duration::from_millis(u64::from(self.delay_ms()))
    }

    /// Use the operation owner's entropy, including a simulation's original
    /// seeded source. `draw` receives the number of jitter choices; any unsigned
    /// sample is reduced into that range. This never starts a timer.
    pub fn delay_with(&mut self, draw: impl FnOnce(u32) -> u32) -> std::time::Duration {
        std::time::Duration::from_millis(u64::from(self.advance(draw)))
    }

    /// Compute the capped ceiling after `doublings` failures beyond the first.
    /// Callers retaining their own attempt counter can recover spacing directly.
    #[must_use]
    pub fn ceiling_ms_at(base_ms: u64, maximum_ms: u64, doublings: u32) -> u64 {
        let factor = 1_u64.checked_shl(doublings).unwrap_or(u64::MAX);
        base_ms.saturating_mul(factor).min(maximum_ms)
    }

    /// Project indexed equal jitter for a caller carrying fractional offsets
    /// and a unit-interval draw. The caller retains its timer and draw source.
    /// Bounds are nonnegative milliseconds and the draw is in `0.0..=1.0`.
    #[must_use]
    pub fn fractional_delay_ms_at(
        base_ms: f64,
        maximum_ms: f64,
        doublings: u32,
        unit_sample: f64,
    ) -> f64 {
        let factor = Self::ceiling_ms_at(1, 1 << 52, doublings);
        // A power of two is exact in binary64. Encode its biased exponent
        // directly to preserve fractional millisecond bounds.
        let factor = f64::from_bits((1023 + u64::from(factor.trailing_zeros())) << 52);
        ((base_ms * factor).min(maximum_ms) * (0.5 + unit_sample / 2.0)).ceil()
    }

    /// Preserve the reconnect envelope from the capped ceiling through 125% of
    /// that ceiling. The caller supplies its sample and owns the absolute timer.
    #[must_use]
    pub fn reconnect_delay_ms(ceiling_ms: u64, sample: u64) -> u64 {
        ceiling_ms.saturating_add(sample % (ceiling_ms / 4 + 1))
    }

    fn advance(&mut self, draw: impl FnOnce(u32) -> u32) -> u32 {
        let ceiling = self.ceiling_ms;
        self.ceiling_ms = u32::try_from(Self::ceiling_ms_at(
            u64::from(ceiling),
            u64::from(self.maximum_ms),
            1,
        ))
        .unwrap_or(self.maximum_ms);
        let lower = ceiling.div_ceil(2);
        let choices = ceiling - lower + 1;
        lower + draw(choices) % choices
    }
}
