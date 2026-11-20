use crate::Adapter;
use crate::sync::atomic::{AtomicU64, Ordering};
use serde_json::Value;

/// Usage-calibrated estimate owned by one immutable resolved model.
pub struct TokenEstimator {
    adapter: Adapter,
    bytes_per_token: AtomicU64,
}

impl TokenEstimator {
    /// A fresh estimator for ADAPTER, calibrated at two bytes per token until usage is observed.
    pub fn new(adapter: Adapter) -> Self {
        Self {
            adapter,
            bytes_per_token: AtomicU64::new(2.0f64.to_bits()),
        }
    }

    /// The estimated token count of BYTES, or `None` when it exceeds `u32`.
    pub fn tokens(&self, bytes: usize) -> Option<u32> {
        let ratio = f64::from_bits(self.bytes_per_token.load(Ordering::Relaxed));
        to_u32((as_float(bytes as u64) / ratio).ceil())
    }

    /// Tightens the ratio from one generation's reported input, never loosening it.
    pub fn observe(&self, bytes: usize, metadata: &Value) {
        let input = input_tokens(self.adapter, metadata);
        if let Some(input) = input.filter(|input| *input != 0) {
            // Leave ten percent headroom around observed tokenizer/framing behavior.
            let ratio = as_float(bytes as u64) / as_float(input) * 0.9;
            if ratio.is_finite() && ratio > 0.0 {
                // Positive finite IEEE-754 encodings preserve numeric order. A sparse
                // prompt must never loosen the estimate for a later dense prompt.
                self.bytes_per_token
                    .fetch_min(ratio.to_bits(), Ordering::Relaxed);
            }
        }
    }
}

/// The `f64` value of a byte count. `f64` holds every integer below 2^53 exactly; above it the
/// sum rounds, which cannot change whether the estimate fits in `u32`.
fn as_float(bytes: u64) -> f64 {
    let high = u32::try_from(bytes >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(bytes & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

/// The exact `u32` an integral, non-negative `f64` below 2^32 names.
///
/// `f64` holds every integer below 2^53 exactly, so the decimal form of an integral estimate is
/// exactly that integer, and parsing it is exact. The buffer is sized for `u32::MAX`; a larger
/// value overflows it and is rejected, as are fractions, negatives and non-finite values.
fn to_u32(value: f64) -> Option<u32> {
    use std::io::Write as _;
    const CAPACITY: usize = 10;
    let mut buffer = [0u8; CAPACITY];
    let mut cursor = buffer.as_mut_slice();
    write!(cursor, "{value}").ok()?;
    let written = CAPACITY - cursor.len();
    std::str::from_utf8(buffer.get(..written)?)
        .ok()?
        .parse()
        .ok()
}

/// The provider's complete reported input for one generation, in each dialect.
/// Anthropic reports cache reads and writes beside uncached input.
pub fn input_tokens(adapter: Adapter, metadata: &Value) -> Option<u64> {
    let usage = &metadata["usage"];
    match adapter {
        Adapter::Chat => usage["prompt_tokens"].as_u64(),
        Adapter::Gemini => usage["promptTokenCount"].as_u64(),
        Adapter::Responses => usage["input_tokens"].as_u64(),
        Adapter::Anthropic => usage["input_tokens"].as_u64().and_then(|input| {
            let additional = |key| match usage.get(key) {
                None | Some(Value::Null) => Some(0),
                Some(value) => value.as_u64(),
            };
            input
                .checked_add(additional("cache_creation_input_tokens")?)?
                .checked_add(additional("cache_read_input_tokens")?)
        }),
    }
}
