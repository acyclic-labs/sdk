//! The metered Responses proxy Codex calls instead of `OpenAI` (task A2).
//!
//! Contract, fixed by `tests/proxy.rs` and the 0.155.1 fixtures:
//! - Serves `POST /v1/responses` on `127.0.0.1:0`; other paths get a 404 JSON error.
//! - Forwards with the real key (Codex only holds a dummy) and merges `extra_body`.
//! - Streams SSE through frame by frame and meters `response.completed` usage.
//! - Once the meter or the step cap says stop, answers `429` with
//!   `{"error":{"type":"insufficient_quota"}}`. Codex 0.155.1 retries a 402
//!   six times but ends the turn on this after one request.
//! - Upstream 4xx/5xx pass through unchanged.

use crate::{executor::Upstream, meter::UsageMeter};
use acyclic_harness::{Error, Result};
use std::sync::Arc;

/// Why the proxy stopped forwarding, reported in the executor's error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyStop {
    /// The meter refused a call.
    Budget(String),
    /// The turn's model-step cap was reached.
    StepLimit(u32),
}

/// A running proxy for one turn.
#[derive(Debug)]
pub struct ResponsesProxy {
    base_url: String,
}

impl ResponsesProxy {
    /// Starts the proxy. `max_steps` caps forwarded model calls for the turn.
    ///
    /// # Errors
    /// When the listener cannot bind.
    pub async fn start(
        upstream: Upstream,
        meter: Arc<dyn UsageMeter>,
        max_steps: u32,
    ) -> Result<Self> {
        let _ = (upstream, meter, max_steps);
        Err(Error::Unsupported(
            "codex responses proxy is not built yet (A2)".into(),
        ))
    }

    /// The base URL Codex is configured with, ending in `/v1`.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Model calls forwarded so far: the turn's step count.
    #[must_use]
    pub const fn steps(&self) -> u32 {
        0
    }

    /// Why forwarding stopped, if it has.
    #[must_use]
    pub const fn stopped(&self) -> Option<ProxyStop> {
        None
    }
}
