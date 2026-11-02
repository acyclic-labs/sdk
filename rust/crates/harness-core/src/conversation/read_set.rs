//! Original turn evidence, available without the optional analysis codecs.
use super::*;

/// Coverage established by the original producers represented in an artifact.
/// Other reads remain unknown; this is never a claim of universal coverage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
pub enum ReadSetCoverage {
    /// Successful committed built-in read-tool results only.
    CommittedReadToolResultsOnly,
}

/// Why a settled turn has no closed read-set artifact. Missing evidence and an
/// empty closed artifact are distinct. Cancellation never certifies closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
pub enum ReadSetUnavailable {
    /// The original turn did not supply an authenticated journal cut.
    MissingCut,
    /// A producer failed before artifact closure.
    Incomplete,
    /// The original turn was cancelled.
    Cancelled,
    /// The bound journal or publisher lacks the required capability.
    Unsupported,
}

/// One versioned reference admitted at original turn settlement. The existing
/// settlement checkpoint is the only writer of this association. `None` at the
/// enclosing carrier means no observation was supplied, not an empty read set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct TurnReadSet {
    /// Format identity; readers reject other versions without migration.
    pub version: u32,
    /// Fixed original owning agent identity.
    pub agent: AgentId,
    /// Original execution journal operation.
    pub operation: OperationId,
    /// Original journal sequence through which coverage was closed.
    /// Unknown for unavailable evidence with no authenticated cut.
    pub cut: Option<u64>,
    /// Explicit producer coverage, independent of whether reads were observed.
    pub coverage: ReadSetCoverage,
    /// A closed artifact or a reason closure could not be established.
    pub observation: ReadSetObservation,
}

/// A successful publication always includes its immutable content identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadSetObservation {
    /// The complete declared source cut was consumed before publication.
    Closed {
        /// One immutable JSONL artifact, including header and closure record.
        file: FileRef,
    },
    /// No completed artifact is asserted, even if a producer emitted a prefix.
    Unavailable {
        /// Original failure or unavailable boundary.
        reason: ReadSetUnavailable,
    },
}

impl TurnReadSet {
    /// Validates once at the original settlement boundary.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::Invalid("read-set settlement version is unsupported; recovery: supply a version-1 TurnReadSet".into()));
        }
        if let ReadSetObservation::Closed { file } = &self.observation {
            if self.cut.is_none() || file.descriptor().media_type() != "application/x-ndjson" {
                return Err(Error::Invalid("read-set closure lacks its cut or JSONL descriptor; recovery: publish the complete pinned read_set::source before settlement".into()));
            }
            file.validate(&mut None)?;
        }
        Ok(())
    }
}
