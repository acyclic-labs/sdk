//! Provider-reported usage anchors later admission without a count request.
use acyclic_harness_core::{Result, model::PreparedModelRequest};

use crate::usage_snapshot::{Snapshot, Usage};

/// The last generation's reported input, owned by one resolved model.
#[derive(Default)]
pub(crate) struct Counter(Snapshot);

impl Counter {
    /// Codex has no public preflight count route; this estimate stands in for it.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn codex_estimate(&self, request: &PreparedModelRequest) -> Result<u64> {
        let snapshot = self.snapshot()?;
        let (tokens, bytes) = match snapshot {
            Some(snapshot) => match request.input_since(snapshot.input)? {
                Some(delta) => (snapshot.tokens, delta),
                None => (0, request.input_bytes()?),
            },
            None => (0, request.input_bytes()?),
        };
        Ok(estimate(tokens, bytes))
    }

    /// An upper bound on the request's input with no count request: the
    /// provider's own reported input for the request's observed prefix, which
    /// already includes every token the provider adds, plus the canonical bytes
    /// appended since. Each token of appended text spans at least one byte.
    /// `None` when no observation anchors this request's prefix, or rendering
    /// can expand or restore content outside the appended canonical bytes.
    pub(crate) fn bound(&self, request: &PreparedModelRequest) -> Result<Option<u64>> {
        let Some(snapshot) = self.snapshot()? else {
            return Ok(None);
        };
        if !request.input_has_byte_bound()? {
            return Ok(None);
        }
        Ok(request
            .input_since(snapshot.input)?
            .map(|delta| snapshot.tokens.saturating_add(delta)))
    }

    pub(crate) fn observe(
        &self,
        adapter: crate::Adapter,
        request: &PreparedModelRequest,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        // The footprint covers the request input, not the generated response.
        // Its output is charged once when it becomes input on a later request.
        if let Some(tokens) = crate::accounting::input_tokens(adapter, metadata) {
            let input = request.observe_input()?;
            self.0
                .set(Usage { input, tokens })
                .map_err(|_| crate::invalid("usage lock poisoned"))?;
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Option<Usage>> {
        self.0
            .get()
            .map_err(|_| crate::invalid("usage lock poisoned"))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn estimate(tokens: u64, bytes: u64) -> u64 {
    tokens.saturating_add(bytes.div_ceil(3))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "usage_count_tests.rs"]
mod tests;
