//! Ownership bounds for closed harness DTOs.
use super::*;
use crate::executor::json::JsonValueSchema as Schema;

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
    crate::context::Context => Context, crate::context::SummarySource => SummarySource, crate::context::CanonicalContextCheckpoint => None, crate::context::CompactionReference => None,
    crate::executor::ResponseProjection => ResponseProjection, crate::model::ModelEvent => ModelEvent,
    crate::model::ModelRequest => ModelRequest, crate::model::ModelContent => ModelContent, crate::model::ModelContentPart => ModelContentPart,
    crate::fork::InheritedConversationPrefix => None, crate::core::ExtensionConfiguration => None,
    FileRef => None, Vec<crate::conversation::Attachment> => None,
    crate::workflow::WorkflowAdmission => WorkflowAdmission, crate::workflow::MachineCheckpoint => MachineCheckpoint, crate::workflow::WorkflowCommand => None,
);
impl PreparationRecord for acyclic_fs::access::Policy {}
impl PreparationRecord for acyclic_fs::access::Capability {}
impl PreparationRecord for crate::workflow::WorkflowRecord {
    /// Release transient decode buffering while retaining every resident capacity.
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        self.retained_owned_bytes().map(Some)
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
