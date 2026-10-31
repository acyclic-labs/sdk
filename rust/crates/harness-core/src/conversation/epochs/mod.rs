//! Durable model epochs: a summarized prefix followed by a verbatim suffix.
//!
//! A swap changes the epoch, never the underlying conversation. Old items stay
//! addressable by their original stream cursors. The summary key names the full
//! covered prefix even when preparation reads a previous summary and only its
//! subsequent items.

use super::FileRef;
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

mod kernel;
pub use kernel::{Boundary, Cut, fits, inherited, token_bound};
pub mod protocol;

/// Immutable identity of a shared summary on the source's common ancestor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryKey {
    /// The stream whose original items are summarized.
    pub source: StreamPath,
    /// Summary covers exactly `[0, through)` in `source`.
    pub through: u64,
    /// Explicit version of the deterministic request renderer.
    pub renderer: u32,
    /// Digest of the provider family and model identity.
    pub model: [u8; 32],
}

impl SummaryKey {
    /// Domain-separated content address. Fixed-width integer fields and the
    /// path's explicit byte length make the encoding unambiguous across hosts.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.epoch.summary.v1");
        let path = self.source.as_str().as_bytes();
        hash.update(&(path.len() as u64).to_le_bytes());
        hash.update(path);
        hash.update(&self.through.to_le_bytes());
        hash.update(&self.renderer.to_le_bytes());
        hash.update(&self.model);
        *hash.finalize().as_bytes()
    }
}

/// A committed epoch's immutable model prefix. Publication and the agent's
/// epoch pointer must share one conditional stream commit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Epoch {
    /// Digest of the frozen Header, including its tool definitions.
    pub header: [u8; 32],
    /// Digest of the resolved profile used throughout this epoch.
    pub profile: [u8; 32],
    /// The full summarized source range and renderer/model identity.
    pub key: SummaryKey,
    /// Immutable summary body. A genesis epoch has no summary and cursor zero.
    pub summary: Option<FileRef>,
}

impl Epoch {
    /// The first verbatim item. Access never scans older epochs or items.
    #[must_use]
    pub const fn cursor(&self) -> u64 {
        self.key.through
    }

    /// Refuses silent loss and summaries of items outside the inherited source.
    /// File authority and body verification remain the shared file reader's job.
    pub fn validate(&self, source: &StreamPath, common_prefix: u64) -> Result<(), InvalidEpoch> {
        if &self.key.source != source {
            return Err(InvalidEpoch::Source);
        }
        if !inherited(self.cursor(), common_prefix) {
            return Err(InvalidEpoch::OutsidePrefix);
        }
        if (self.cursor() == 0) != self.summary.is_none() {
            return Err(InvalidEpoch::MissingCoverage);
        }
        Ok(())
    }
}

/// Versioned durable record. An unsupported version is refused, never migrated.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "version", content = "epoch", deny_unknown_fields)]
pub enum EpochRecord {
    /// First epoch format.
    #[serde(rename = "1")]
    V1(Epoch),
}

/// A malformed or inapplicable epoch cannot become a model view.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidEpoch {
    /// A cursor from another stream cannot describe this conversation's past.
    #[error("epoch summary names a different source stream")]
    Source,
    /// The summary reaches beyond the fork's inherited prefix.
    #[error("epoch summary exceeds the common source prefix")]
    OutsidePrefix,
    /// A nonempty covered range has no summary, or an empty one invents one.
    #[error("epoch summary does not match its covered range")]
    MissingCoverage,
}

#[cfg(test)]
mod tests;
