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

/// Original exclusive follow position of the record producing a context delta.
/// This is provenance, not a capability or a publication-completion witness.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(try_from = "PositionFields")]
pub struct SourcePosition {
    stream: String,
    exclusive: u64,
    commit: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionFields {
    stream: String,
    exclusive: u64,
    commit: [u8; 32],
}
impl TryFrom<PositionFields> for SourcePosition {
    type Error = &'static str;
    fn try_from(fields: PositionFields) -> std::result::Result<Self, Self::Error> {
        if fields.exclusive == 0 || acyclic_stream::StreamPath::validate(&fields.stream).is_err() {
            return Err("invalid original context record position");
        }
        Ok(Self {
            stream: fields.stream,
            exclusive: fields.exclusive,
            commit: fields.commit,
        })
    }
}

impl SourcePosition {
    /// Retains the actual follow path and record identity on the original
    /// operation ledger. The next position must be checked by that follow owner.
    pub fn from_record(
        stream: &acyclic_stream::StreamPath,
        exclusive: u64,
        commit: [u8; 32],
        io: &PreparationIo,
    ) -> Result<Self> {
        if exclusive == 0 {
            return Err(crate::executor::stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "a source record requires a nonzero exclusive position",
            ));
        }
        Ok(Self {
            stream: crate::executor::stock_string_copy(stream.as_str(), Some(io))?,
            exclusive,
            commit,
        })
    }
    /// Original stream, without reopening its current head.
    pub fn stream(&self) -> &str {
        &self.stream
    }
    /// Exclusive position of this record, not the containing publication's end.
    pub fn exclusive(&self) -> u64 {
        self.exclusive
    }
    /// Original record's commit identity; grouped members may share it.
    pub fn commit(&self) -> [u8; 32] {
        self.commit
    }
}

/// One admitted context delta. Earlier metadata and pinned bodies remain in
/// their original envelopes; a replacement never rewrites the old prefix.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextChange {
    /// Changed frontmatter with its exact lazy body generation and authority.
    Skill { metadata: SkillMetadata },
    /// An original publication removed this exact context path.
    Removed { source: PinnedContextPath },
}

/// The single immutable payload stored by the conversation's journal owner.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Payload {
    /// Exact admitted provider bytes, retaining native replay provenance.
    Wire { item: WireItem },
    /// Changed context at its original publication observation.
    Context {
        position: SourcePosition,
        change: ContextChange,
    },
}

/// A versioned item with its original intent and pinned resolved profile.
/// Recovery uses the existing journal's intent/result association; there is no
/// secondary index, cursor, publication key or history search here.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    version: Version,
    intent: OperationId,
    profile: [u8; 32],
    payload: Payload,
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
        crate::executor::with_history_io(Some(io), |work| match &self.payload {
            Payload::Context {
                change: ContextChange::Skill { metadata },
                ..
            } => metadata.validate(work),
            Payload::Context {
                change: ContextChange::Removed { source },
                ..
            } => source.validate(work),
            Payload::Wire { .. } => Ok(()),
        })
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
        self.validate(io)?;
        io.encode(self, maximum_bytes, false)
    }

    /// Restores an item with the original bounded decoder and retains its
    /// allocation reservation until the caller drops both item and hold.
    pub fn decode<'a>(
        bytes: &[u8],
        io: &'a PreparationIo,
    ) -> Result<(Self, super::ContentHold<'a>)> {
        let decoded = io.decode(
            std::marker::PhantomData::<Self>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        decoded.0.validate(io)?;
        Ok(decoded)
    }
}

impl crate::filesystem::PreparationRecord for Envelope {}
