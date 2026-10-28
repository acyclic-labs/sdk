use serde::{Deserialize, Serialize};

/// Stable source sections, in their existing display order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptPart {
    /// Optional reasoning preceding the answer.
    Reasoning,
    #[default]
    /// The retained and streaming answer.
    Answer,
    /// Tool output following the answer.
    Tools,
}

/// A displayed row: the `row`th wrapped row of source line `line`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct RowPosition {
    /// Source line.
    pub line: usize,
    /// Wrapped row within the line.
    pub row: usize,
}

/// A logical row within one immutable followed source file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptOrigin {
    /// Canonical followed file identity, independent of the retained window.
    pub file: [u8; 32],
    /// Logical display line created by that source file.
    pub line: usize,
}

/// Persisted top row, resolved by the host from a scrollbar seek. Absence
/// means follow the live tail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptAnchor {
    /// Source section.
    pub part: TranscriptPart,
    /// Logical source line and physical row within it.
    pub position: RowPosition,
    /// Followed source identity supplied by the host projection.
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "source_origin"
    )]
    pub origin: Option<TranscriptOrigin>,
}

// A persisted anchor must carry provenance; accepting a bare retained-window
// index would silently reintroduce the eviction jump on reopen.
fn source_origin<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<TranscriptOrigin>, D::Error> {
    TranscriptOrigin::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests;
