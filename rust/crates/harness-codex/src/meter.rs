//! Spend and step accounting for proxied model calls.

use serde::{Deserialize, Serialize};

/// Token usage of one Responses call, read from `response.completed`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponsesUsage {
    /// Prompt tokens, including cached ones.
    pub input_tokens: u64,
    /// Prompt tokens served from cache.
    pub cached_input_tokens: u64,
    /// Generated tokens, including reasoning.
    pub output_tokens: u64,
}

/// Whether the proxy may forward the next model call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MeterVerdict {
    /// Keep going.
    Continue,
    /// Refuse every later call; the reason is reported in the turn's error.
    Stop {
        /// Why, e.g. `swarm budget exhausted`.
        reason: String,
    },
}

/// Prices and caps proxied model calls. The consumer owns the accounting
/// (cloud maps it onto the swarm's spend reporter and the host's `proceed`).
pub trait UsageMeter: Send + Sync {
    /// Asked before each call is forwarded upstream.
    fn admit(&self, model: &str) -> MeterVerdict;
    /// Told the usage of each call that completed upstream.
    fn record(&self, model: &str, usage: &ResponsesUsage) -> MeterVerdict;
}

/// A meter that never stops a turn. For tests and unmetered local use.
#[derive(Clone, Copy, Debug, Default)]
pub struct Unmetered;

impl UsageMeter for Unmetered {
    fn admit(&self, _: &str) -> MeterVerdict {
        MeterVerdict::Continue
    }

    fn record(&self, _: &str, _: &ResponsesUsage) -> MeterVerdict {
        MeterVerdict::Continue
    }
}
