use super::*;

/// Caller-selected sampling, pinned by the canonical request digest. An absent
/// field delegates that setting to the provider; it is never replaced locally.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelSampling {
    /// Provider sampling hint; this does not promise deterministic generation.
    pub seed: Option<i64>,
    /// Nonnegative temperature in the provider's units.
    pub temperature: Option<f64>,
    /// Probability mass in (0, 1].
    pub top_p: Option<f64>,
}

impl ModelSampling {
    /// Refuses invalid numbers at admission, before hashing or provider I/O.
    pub fn validate(&self) -> Result<()> {
        if self.temperature.is_some_and(|n| !n.is_finite() || n < 0.0)
            || self
                .top_p
                .is_some_and(|n| !n.is_finite() || n <= 0.0 || n > 1.0)
        {
            return Err(Error::Invalid(
                "sampling admission failed: invalid temperature or top_p; recovery: set a finite nonnegative temperature and top_p in (0, 1]".into(),
            ));
        }
        Ok(())
    }

    /// Whether every setting delegates to the provider.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}
