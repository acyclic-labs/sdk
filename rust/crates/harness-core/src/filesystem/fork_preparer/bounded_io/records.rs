//! Ownership bounds for closed harness DTOs.
use super::*;
use crate::executor::json::JsonValueSchema as Schema;
use std::mem::size_of;

/// A closed record a preparation decodes under its layout admission.
#[doc(hidden)]
pub trait PreparationRecord: serde::de::DeserializeOwned + crate::HeapLayout {
    /// Bytes the decoded record keeps, when smaller than its admission.
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::executor::json::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> &'static Schema {
        Schema::None
    }
}

#[doc(hidden)]
pub trait UploadRecord: serde::de::DeserializeOwned + crate::HeapLayout {
    fn logical_owned_bytes(&self) -> Result<u64>;
    fn diagnostic_label_bytes() -> u64 {
        crate::executor::json::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> &'static Schema {
        Schema::None
    }
}

pub(super) fn decode_allowance_error(
    reason: &'static str,
    code: Option<crate::wire::ErrorCode>,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(code) = code {
        return Error::NativeBoundary { code, reason };
    }
    let _ = code;
    Error::Invalid(reason.into())
}

macro_rules! records {
    ($($record:ty => $schema:ident),* $(,)?) => {$(impl PreparationRecord for $record { fn json_value_schema() -> &'static Schema { Schema::$schema } })*};
}
records!(
    crate::Capabilities => None,
    crate::context::SummarySource => SummarySource,
    crate::model::ModelRequest => ModelRequest, crate::model::ModelContent => ModelContent, crate::model::ModelContentPart => ModelContentPart,
    crate::fork::InheritedConversationPrefix => None, crate::core::ExtensionConfiguration => None,
    FileRef => None, Vec<crate::conversation::Attachment> => None,
    crate::workflow::WorkflowAdmission => WorkflowAdmission, crate::workflow::MachineCheckpoint => MachineCheckpoint, crate::workflow::WorkflowCommand => None,
);
impl PreparationRecord for crate::executor::ResponseProjection {
    fn json_value_schema() -> &'static Schema {
        Schema::ResponseProjection
    }

    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        let context = self.context.decoded_owned_bytes()?.ok_or_else(|| {
            decode_allowance_error("projection context inventory is missing", None)
        })?;
        let payload = context
            .checked_sub(size_of::<crate::context::Context>() as u64)
            .ok_or_else(|| {
                decode_allowance_error("projection context inventory underflows", None)
            })?;
        // The inline context owns its message/value buffers. Decode scratch is
        // transient and must not remain charged across checkpoint validation.
        crate::core::owned_sum([
            size_of::<Self>() as u64,
            payload,
            self.canonical
                .as_ref()
                .map_or(Ok(0), FileRef::logical_owned_bytes)?,
            self.checkpoint
                .as_ref()
                .map_or(Ok(0), FileRef::logical_owned_bytes)?,
            self.working
                .as_ref()
                .map_or(Ok(0), FileRef::logical_owned_bytes)?,
        ])
        .map(Some)
    }
}
impl PreparationRecord for crate::model::ModelEvent {
    fn json_value_schema() -> &'static Schema {
        Schema::ModelEvent
    }
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        self.retained_owned_bytes().map(Some)
    }
}
impl PreparationRecord for crate::context::Context {
    fn json_value_schema() -> &'static Schema {
        Schema::Context
    }

    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        let bytes = crate::core::owned_sum([
            size_of::<Self>() as u64,
            crate::core::owned_buffer::<crate::model::ModelMessage>(self.messages.capacity())?,
            file_map_owned_bytes(&self.metadata)?,
        ])?;
        self.messages
            .iter()
            .try_fold(bytes, |bytes, message| {
                crate::core::owned_sum([bytes, message.content.retained_payload_bytes()?])
            })
            .map(Some)
    }
}

pub(crate) fn file_map_owned_bytes(
    map: &std::collections::BTreeMap<String, FileRef>,
) -> Result<u64> {
    let nodes = if map.is_empty() {
        0
    } else {
        1 + (map.len() - 1) / 5
    };
    let node = 11 * size_of::<(String, FileRef)>() + 14 * size_of::<usize>();
    let slots = nodes
        .checked_mul(node)
        .ok_or_else(|| decode_allowance_error("file metadata node inventory overflows", None))?
        as u64;
    map.iter().try_fold(slots, |bytes, (key, file)| {
        crate::core::owned_sum([bytes, key.capacity() as u64, file.logical_owned_bytes()?])
    })
}

impl PreparationRecord for crate::context::CompactionReference {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        crate::core::owned_sum([
            size_of::<Self>() as u64,
            crate::core::owned_buffer::<crate::model::ModelRole>(self.retention.roles.capacity())?,
            self.summary
                .as_ref()
                .map_or(Ok(0), |summary| summary.output.logical_owned_bytes())?,
        ])
        .map(Some)
    }
}

impl PreparationRecord for crate::context::CanonicalContextCheckpoint {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        // The selection owns UUID slots and at most one earlier FileRef. All
        // other variable storage belongs to the three immutable file identities.
        // Release serde's decode scratch before authenticating the publication.
        crate::core::owned_sum([
            size_of::<Self>() as u64,
            crate::core::owned_buffer::<uuid::Uuid>(self.selection.message_ids.capacity())?,
            self.selection
                .checkpoint
                .as_ref()
                .map_or(Ok(0), FileRef::logical_owned_bytes)?,
            self.source.logical_owned_bytes()?,
            self.retained.logical_owned_bytes()?,
            self.compaction.logical_owned_bytes()?,
        ])
        .map(Some)
    }
}
impl PreparationRecord for acyclic_fs::access::Policy {}
impl PreparationRecord for acyclic_fs::access::Capability {}
impl PreparationRecord for crate::workflow::WorkflowRecord {
    /// The record keeps what its layout reserves for its decode, walked over
    /// the resident record, so the audit and the reservation share one model.
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        crate::executor::json::serialized_layout_bytes(self, <Self as crate::HeapLayout>::LAYOUT)?
            .checked_add(size_of_val(self) as u64)
            .map(Some)
            .ok_or_else(|| decode_allowance_error("workflow ownership overflows", None))
    }
    fn json_value_schema() -> &'static Schema {
        Schema::WorkflowRecord
    }
}
impl PreparationRecord for crate::Outcome<serde_json::Value> {
    fn json_value_schema() -> &'static Schema {
        Schema::TaskOutcome
    }
}
impl PreparationRecord for serde_json::Value {
    fn diagnostic_label_bytes() -> u64 {
        0
    }
    fn json_value_schema() -> &'static Schema {
        Schema::Root
    }
}
impl UploadRecord for FileRef {
    fn logical_owned_bytes(&self) -> Result<u64> {
        FileRef::logical_owned_bytes(self)
    }
}
impl PreparationRecord for (FileRef, Option<crate::resources::GenerationRef>) {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        self.logical_owned_bytes().map(Some)
    }
}
impl UploadRecord for (FileRef, Option<crate::resources::GenerationRef>) {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.0
            .logical_owned_bytes()?
            .checked_add(
                self.1
                    .as_ref()
                    .map(|generation| generation.logical_owned_bytes())
                    .transpose()?
                    .unwrap_or(0),
            )
            .ok_or_else(|| Error::Invalid("upload generation ownership overflow".into()))
    }
}

impl PreparationRecord for crate::resources::GenerationRef {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        self.logical_owned_bytes().map(Some)
    }
}
