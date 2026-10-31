//! Versioned conversation payloads for the original journal publication.
//!
//! Source positions describe one observed record, never the completion of a
//! filesystem publication or model exchange. Only the owning producer can
//! establish those boundaries. Encoding does not submit another transaction.
use super::wire::WireItem;
use crate::{
    OperationId, Result,
    context::{PinnedContextPath, SkillMetadata},
    executor::PreparationIo,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
struct Version;
impl TryFrom<u32> for Version {
    type Error = &'static str;
    fn try_from(version: u32) -> std::result::Result<Self, Self::Error> {
        if version == 1 {
            Ok(Self)
        } else {
            Err("unsupported conversation envelope version")
        }
    }
}
impl From<Version> for u32 {
    fn from(_: Version) -> Self {
        1
    }
}
impl crate::heap_layout::HeapLayout for Version {
    const LAYOUT: &'static crate::heap_layout::JsonLayout = &crate::heap_layout::JsonLayout::Scalar;
}

/// One admitted context delta. Earlier metadata and pinned bodies remain in
/// their original envelopes; a replacement never rewrites the old prefix.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextChange {
    /// Changed frontmatter with its exact lazy body generation and authority.
    Skill {
        /// Original changed metadata and retained body generation.
        metadata: SkillMetadata,
    },
    /// An original publication removed this exact context path.
    Removed {
        /// Exact removed path under the original authorized root.
        source: PinnedContextPath,
    },
}

/// The single immutable payload stored by the conversation's journal owner.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Payload {
    /// Exact admitted provider bytes, retaining native replay provenance.
    Wire {
        /// Immutable native body and its admitted foreign projections.
        item: WireItem,
    },
    /// All selected changes from one original source cut. The digest identifies
    /// the exact producer checkpoint contributed in the same conditional commit.
    ContextBatch {
        /// Canonical source checkpoint identity, including its original cursor.
        checkpoint: [u8; 32],
        /// Changed metadata only; earlier envelopes retain their original bodies.
        changes: Vec<ContextChange>,
    },
}

/// A versioned item with its original intent and pinned resolved profile.
/// Recovery uses the existing journal's intent/result association; there is no
/// secondary index, cursor, publication key or history search here.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(try_from = "EnvelopeFields")]
#[heap_layout(as = EnvelopeFields)]
pub struct Envelope {
    version: Version,
    intent: OperationId,
    profile: [u8; 32],
    payload: Payload,
}

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct EnvelopeFields {
    version: Version,
    intent: OperationId,
    profile: [u8; 32],
    payload: Payload,
}

impl TryFrom<EnvelopeFields> for Envelope {
    type Error = &'static str;
    fn try_from(fields: EnvelopeFields) -> std::result::Result<Self, Self::Error> {
        let envelope = Self {
            version: fields.version,
            intent: fields.intent,
            profile: fields.profile,
            payload: fields.payload,
        };
        envelope
            .validate_context(&mut None)
            .map_err(|_| "invalid conversation context payload")?;
        Ok(envelope)
    }
}

impl Envelope {
    /// Moves an admitted item into the original intent without copying its
    /// body. Context validation checks metadata only and conveys no authority.
    pub fn new(
        intent: OperationId,
        profile: [u8; 32],
        payload: Payload,
        io: &PreparationIo,
    ) -> Result<Self> {
        let envelope = Self {
            version: Version,
            intent,
            profile,
            payload,
        };
        envelope.validate(io)?;
        Ok(envelope)
    }

    fn validate(&self, io: &PreparationIo) -> Result<()> {
        crate::executor::with_history_io(Some(io), |work| self.validate_context(work))
    }

    fn validate_context(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match &self.payload {
            Payload::ContextBatch { changes, .. } => {
                for change in changes {
                    match change {
                        ContextChange::Skill { metadata } => metadata.validate(work)?,
                        ContextChange::Removed { source } => source.validate(work)?,
                    }
                }
                Ok(())
            }
            Payload::Wire { .. } => Ok(()),
        }
    }

    /// Original operation identity retained across acknowledgement loss/replay.
    pub fn intent(&self) -> OperationId {
        self.intent
    }
    /// Profile pinned at this original conversation position.
    pub fn profile(&self) -> [u8; 32] {
        self.profile
    }
    /// Borrows the retained item; loading cannot manufacture exchange closure.
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// Encodes once on the original ledger. The journal owner includes these
    /// bytes in its existing conditional observation/checkpoint commit.
    pub fn encode(
        &self,
        io: &PreparationIo,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        io.encode(self, maximum_bytes, true)
    }

    /// Restores an item with the original bounded decoder and retains its
    /// allocation reservation until the caller drops both item and hold.
    pub fn decode<'a>(
        bytes: &[u8],
        io: &'a PreparationIo,
    ) -> Result<(Self, super::ContentHold<'a>)> {
        io.decode(
            std::marker::PhantomData::<Self>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )
    }
}

impl crate::filesystem::PreparationRecord for Envelope {}

// Borrow the producer's selected metadata once. This has the same wire shape as
// ContextChange; decoding and replay use the one owned Payload above.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BorrowedChange<'a> {
    Skill { metadata: &'a SkillMetadata },
    Removed { source: &'a PinnedContextPath },
}
#[cfg(not(target_arch = "wasm32"))]
struct BorrowedChanges<'a>(&'a [crate::context::SkillPublicationChange]);
#[cfg(not(target_arch = "wasm32"))]
impl Serialize for BorrowedChanges<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use crate::context::SkillPublicationChange;
        serializer.collect_seq(self.0.iter().map(|change| match change {
            SkillPublicationChange::Skill(metadata) => BorrowedChange::Skill { metadata },
            SkillPublicationChange::Removed(source) => BorrowedChange::Removed { source },
        }))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn encode_context_batch(
    intent: OperationId,
    profile: [u8; 32],
    checkpoint: [u8; 32],
    changes: &[crate::context::SkillPublicationChange],
    io: &PreparationIo,
) -> Result<acyclic_stream::OwnedBytes> {
    #[derive(Serialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    enum BorrowedPayload<'a> {
        ContextBatch {
            checkpoint: [u8; 32],
            changes: BorrowedChanges<'a>,
        },
    }
    #[derive(Serialize)]
    struct BorrowedEnvelope<'a> {
        version: Version,
        intent: OperationId,
        profile: [u8; 32],
        payload: BorrowedPayload<'a>,
    }
    io.encode(
        &BorrowedEnvelope {
            version: Version,
            intent,
            profile,
            payload: BorrowedPayload::ContextBatch {
                checkpoint,
                changes: BorrowedChanges(changes),
            },
        },
        io.remaining()?.bytes_encoded,
        true,
    )
}
