use serde::Serialize;

#[derive(Clone, Copy, Serialize)]
pub(super) struct Quantiles {
    pub(super) samples: usize,
    pub(super) p50_ns: f64,
    pub(super) p99_ns: f64,
}

impl Quantiles {
    pub(super) fn new(values: &mut [f64]) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        Self::with_order(values, f64::total_cmp)
    }

    pub(super) fn with_order(
        values: &mut [f64],
        mut compare: impl FnMut(&f64, &f64) -> std::cmp::Ordering,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        if values.is_empty()
            || values
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err("quantiles require nonnegative finite samples".into());
        }
        let rank = |percent: usize| -> std::result::Result<usize, Box<dyn std::error::Error>> {
            Ok(values
                .len()
                .checked_mul(percent)
                .ok_or("sample rank overflow")?
                .div_ceil(100)
                .saturating_sub(1))
        };
        let samples = values.len();
        let (median, tail) = (rank(50)?, rank(99)?);
        // Partition only the requested ranks. The lower partition still
        // contains the median; no complete sample ordering is required.
        let (lower, tail_value, _) = values.select_nth_unstable_by(tail, &mut compare);
        let p99_ns = *tail_value;
        let p50_ns = if median == tail {
            p99_ns
        } else {
            *lower.select_nth_unstable_by(median, compare).1
        };
        Ok(Self {
            samples,
            p50_ns,
            p99_ns,
        })
    }
}
