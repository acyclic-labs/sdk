use crate::Adapter;
use crate::sync::atomic::{AtomicU64, Ordering};
use serde_json::Value;

/// Usage-calibrated estimate owned by one immutable resolved model.
pub(crate) struct TokenEstimator {
    adapter: Adapter,
    bytes_per_token: AtomicU64,
}

impl TokenEstimator {
    pub(crate) fn new(adapter: Adapter) -> Self {
        let initial: f64 = match adapter {
            Adapter::Anthropic => 2.0,
            Adapter::Responses => 2.0,
            Adapter::Chat => 2.0,
            Adapter::Gemini => 2.0,
        };
        Self {
            adapter,
            bytes_per_token: AtomicU64::new(initial.to_bits()),
        }
    }

    pub(crate) fn tokens(&self, bytes: usize) -> Option<u32> {
        let ratio = f64::from_bits(self.bytes_per_token.load(Ordering::Relaxed));
        let tokens = (bytes as f64 / ratio).ceil();
        (tokens <= u32::MAX as f64).then_some(tokens as u32)
    }

    pub(crate) fn observe(&self, bytes: usize, metadata: &Value) {
        let usage = &metadata["usage"];
        let input = match self.adapter {
            Adapter::Chat => usage["prompt_tokens"].as_u64(),
            Adapter::Gemini => usage["promptTokenCount"].as_u64(),
            Adapter::Responses => usage["input_tokens"].as_u64(),
            Adapter::Anthropic => usage["input_tokens"].as_u64().and_then(|input| {
                input
                    .checked_add(usage["cache_creation_input_tokens"].as_u64().unwrap_or(0))?
                    .checked_add(usage["cache_read_input_tokens"].as_u64().unwrap_or(0))
            }),
        };
        if let Some(input) = input.filter(|input| *input != 0) {
            // Leave ten percent headroom around observed tokenizer/framing behavior.
            let ratio = bytes as f64 / input as f64 * 0.9;
            if ratio.is_finite() && ratio > 0.0 {
                // Positive finite IEEE-754 encodings preserve numeric order. A sparse
                // prompt must never loosen the estimate for a later dense prompt.
                self.bytes_per_token
                    .fetch_min(ratio.to_bits(), Ordering::Relaxed);
            }
        }
    }
}
