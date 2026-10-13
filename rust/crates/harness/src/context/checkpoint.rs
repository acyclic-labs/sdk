use super::*;

use crate::{executor::stock_boundary, wire::ErrorCode};

/// Durable output of an ordinary admitted summary model operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSummary {
    /// Caller-owned operation identity; retries use the same admitted request.
    pub operation_id: crate::OperationId,
    /// Logical parent step of the summary admission.
    pub step: u32,
    /// Number of ordered source messages covered by this summary.
    pub source_messages: u32,
    /// Digest of the exact source projection, including immutable content refs.
    pub source_digest: [u8; 32],
    /// Immutable summary text staged through the execution journal.
    pub output: FileRef,
}

/// Consumer-declared content that compaction must preserve verbatim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct CompactionRetention {
    /// Roles retained in full. Defaults to instructions (`System`).
    #[cfg_attr(feature = "wasm", tsify(type = "ModelRole[]"))]
    pub roles: Vec<ModelRole>,
    /// Retains messages containing native media references in full.
    pub native_media: bool,
}

impl Default for CompactionRetention {
    fn default() -> Self {
        Self {
            roles: vec![ModelRole::System],
            native_media: true,
        }
    }
}

impl CompactionRetention {
    /// Rejects ambiguous repeated role declarations.
    pub(crate) fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        for (position, role) in self.roles.iter().enumerate() {
            if self.roles[..position].contains(role) {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "compaction retention repeats a role",
                ));
            }
        }
        Ok(())
    }
}

/// Immutable reference proving which pre-compaction context was summarized.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactionReference {
    /// BLAKE3 digest of the exact serialized source context.
    pub source_digest: [u8; 32],
    /// Caller-selected projection budget; replay reconstructs the same selection.
    pub maximum_messages: u32,
    /// Exact consumer retention policy used by this admission.
    pub retention: CompactionRetention,
    /// Summary operation and immutable output, when a projection was compressed.
    pub summary: Option<ContextSummary>,
}

/// Ref-only canonical checkpoint envelope pinned by a committed history selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalContextCheckpoint {
    /// Existing execution which admitted the summary and its source.
    pub operation_id: crate::OperationId,
    /// Exact canonical delta and previous checkpoint used by that admission.
    pub selection: crate::conversation::ModelContextSelection,
    /// Immutable source base before stage transformation.
    pub source: FileRef,
    /// Immutable compacted canonical base for later deltas.
    pub retained: FileRef,
    /// Exact compaction proof and admitted summary output provenance.
    pub compaction: FileRef,
}

impl CanonicalContextCheckpoint {
    /// Checks a bounded envelope, sharing the I/O companion's policy and native scratch custody.
    pub fn validate(
        &self,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        if self.selection.conversation_revision == 0
            || self.selection.message_ids.is_empty()
            || self.selection.message_ids.len() > limits.context_messages
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "canonical checkpoint coverage is invalid",
            ));
        }
        // Validate immutable references before the bounded scratch set grows.
        for reference in [&self.source, &self.retained, &self.compaction] {
            crate::executor::with_history_io(io, |work| limits.validate_file(reference, work))?;
            if reference.volume() != self.source.volume()
                || reference.descriptor().media_type() != "application/json"
            {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "canonical checkpoint payload scope is invalid",
                ));
            }
        }
        if let Some(previous) = &self.selection.checkpoint {
            crate::executor::with_history_io(io, |work| limits.validate_file(previous, work))?;
        }
        #[cfg(feature = "filesystem")]
        let mut history = io.map(crate::executor::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = history
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        #[cfg(feature = "filesystem")]
        let _unique_guard = {
            // Logical borrowed entry slots only, never opaque allocator nodes.
            let bytes = self
                .selection
                .message_ids
                .len()
                .checked_mul(std::mem::size_of::<&uuid::Uuid>())
                .ok_or_else(|| {
                    stock_boundary(
                        io,
                        ErrorCode::Invalid,
                        "canonical checkpoint identity storage overflows",
                    )
                })?;
            crate::wire_codec::CodecQuota::new(&mut observed, bytes as u64)?
        };
        let mut unique = std::collections::BTreeSet::new();
        for id in &self.selection.message_ids {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            unique.insert(id);
        }
        if unique.len() != self.selection.message_ids.len() {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "canonical checkpoint repeats source identities",
            ));
        }
        Ok(())
    }

    /// Encodes one bounded envelope for the owning journal's normal content publisher.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        self.validate(limits, None)?;
        // Count the typed envelope before canonical serialization creates its Value tree and byte buffer.
        crate::executor::json::json_byte_length(
            self,
            limits.file_bytes.min(limits.render_bytes),
            &mut None,
        )
        .map(|_| ())?;
        crate::contract::canonical_json_bytes(self)
    }

    /// Binds resolved payloads to the canonical source and existing summary operation.
    pub fn validate_projection(
        &self,
        source: &Context,
        retained: &Context,
        compaction: &CompactionReference,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.validate(limits, io)?;
        validate_projected_context(source, limits, io)?;
        validate_projected_context(retained, limits, io)?;
        validate_compaction(compaction, source, retained, io)?;
        if compaction
            .summary
            .as_ref()
            .is_none_or(|summary| summary.operation_id != self.operation_id)
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "canonical checkpoint summary admission differs",
            ));
        }
        Ok(())
    }
}

impl CompactionReference {
    /// Encodes this proof with the existing Harness canonical JSON serializer.
    pub fn encode(&self) -> Result<Vec<u8>> {
        crate::contract::canonical_json_bytes(self)
    }
}
