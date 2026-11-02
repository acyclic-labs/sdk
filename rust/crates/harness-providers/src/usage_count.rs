//! Provider-reported usage anchors later admission without a count request.
use acyclic_harness_core::{Result, model::PreparedModelRequest};
use serde::Serialize as _;

use crate::usage_snapshot::{Snapshot, Usage};

/// The last generation's reported input, owned by one resolved model.
#[derive(Default)]
pub(crate) struct Counter(Snapshot);

impl Counter {
    /// Codex has no public preflight count route; this estimate stands in for it.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn codex_estimate(&self, request: &PreparedModelRequest) -> Result<u64> {
        let snapshot = self.snapshot()?;
        let (prefix, total) = footprint(request, snapshot.map_or(0, |s| s.messages))?;
        Ok(estimate(snapshot, prefix, total))
    }

    /// An upper bound on the request's input with no count request: the
    /// provider's own reported input for the request's observed prefix, which
    /// already includes every token the provider adds, plus the canonical bytes
    /// appended since. Each token of appended text spans at least one byte.
    /// `None` when no observation anchors this request's prefix.
    pub(crate) fn bound(&self, request: &PreparedModelRequest) -> Result<Option<u64>> {
        let Some(snapshot) = self.snapshot()? else {
            return Ok(None);
        };
        let (prefix, total) = footprint(request, snapshot.messages)?;
        Ok((snapshot.digest == prefix && snapshot.bytes <= total)
            .then(|| snapshot.tokens.saturating_add(total - snapshot.bytes)))
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
            let messages = request.request().messages.len();
            let (digest, bytes) = footprint(request, messages)?;
            self.0
                .set(Usage {
                    digest,
                    messages,
                    bytes,
                    tokens,
                })
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

// If a resume, compaction or catalog change replaced the prefix, estimate the
// complete input. Never subtract an unrelated request's byte count.
#[cfg(not(target_arch = "wasm32"))]
fn estimate(snapshot: Option<Usage>, prefix: [u8; 32], total: u64) -> u64 {
    let (tokens, bytes) = snapshot
        .filter(|s| s.digest == prefix && s.bytes <= total)
        .map_or((0, 0), |s| (s.tokens, s.bytes));
    tokens.saturating_add(total.saturating_sub(bytes).div_ceil(3))
}

fn footprint(request: &PreparedModelRequest, prefix: usize) -> Result<([u8; 32], u64)> {
    struct Writer {
        hash: blake3::Hasher,
        bytes: u64,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.hash.update(data);
            self.bytes = self
                .bytes
                .checked_add(data.len() as u64)
                .ok_or_else(|| std::io::Error::other("usage footprint size overflow"))?;
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let input = request.request();
    let mut writer = Writer {
        hash: blake3::Hasher::new(),
        bytes: 0,
    };
    serde_json::to_writer(&mut writer, &(&input.model, &input.tools))
        .map_err(|_| crate::invalid("usage footprint encoding failed"))?;
    let mut digest = *writer.hash.finalize().as_bytes();
    for (index, message) in input.messages.iter().enumerate() {
        message
            .serialize(&mut serde_json::Serializer::new(&mut writer))
            .map_err(|_| crate::invalid("usage footprint encoding failed"))?;
        writer.bytes = writer
            .bytes
            .checked_add(crate::files::extra_bytes(&message.content, u64::MAX)? as u64)
            .ok_or_else(|| crate::invalid("usage footprint size overflow"))?;
        if index + 1 == prefix {
            digest = *writer.hash.finalize().as_bytes();
        }
    }
    Ok((digest, writer.bytes))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "usage_count_tests.rs"]
mod tests;
