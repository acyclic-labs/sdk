//! A fixed observation window; the measurement fixture must not retain history.
use acyclic_harness::{Error, Result};
use std::time::Duration;

#[derive(Default)]
pub(super) struct Intervals {
    total: usize,
    count: u32,
    next: usize,
    values: [(Duration, u64); 10],
}
impl Intervals {
    pub(super) fn push(&mut self, elapsed: Duration, flushes: u64) -> Result<()> {
        let total = self
            .total
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("benchmark turn counter overflow".into()))?;
        let slot = self
            .values
            .get_mut(self.next)
            .ok_or_else(|| Error::Invalid("benchmark window position is invalid".into()))?;
        *slot = (elapsed, flushes);
        self.next = (self.next + 1) % self.values.len();
        self.count = (self.count + 1).min(10);
        self.total = total;
        Ok(())
    }
    pub(super) const fn len(&self) -> usize {
        self.total
    }
    pub(super) fn summary(&self) -> (Duration, u64, u32) {
        let (elapsed, flushes) = self
            .values
            .iter()
            .fold((Duration::ZERO, 0), |(total, count), (elapsed, flushes)| {
                (total + *elapsed, count + *flushes)
            });
        (elapsed, flushes, self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_thousand_intervals_keep_only_the_last_ten_observations() -> Result<()> {
        let mut intervals = Intervals::default();
        // Independent former fixture: retained one sample for every tool call.
        let mut history = Vec::new();
        for turn in 1..=1000_u32 {
            let elapsed = Duration::from_nanos(u64::from(turn));
            intervals.push(elapsed, 1)?;
            history.push((elapsed, 1_u64));
            let expected = history.iter().rev().take(10).fold(
                (Duration::ZERO, 0, 0_u32),
                |(time, count, n), (elapsed, flushes)| (time + *elapsed, count + *flushes, n + 1),
            );
            assert_eq!(intervals.summary(), expected);
            assert_eq!(intervals.len(), history.len());
        }
        assert_eq!(intervals.values.len(), 10);
        assert!(
            history.len() > intervals.values.len(),
            "unbounded fixture negative control was not detected"
        );
        Ok(())
    }
}
