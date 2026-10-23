//! Ownership bounds for closed harness DTOs.
use super::*;
use crate::executor::json::{JsonValueSchema as Schema, JsonValueShape};
use std::mem::size_of;

#[doc(hidden)]
pub trait PreparationRecord: serde::de::DeserializeOwned {
    fn diagnostic_source_bytes(shape: JsonValueShape) -> u64 {
        shape.input_bytes()
    }
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    fn maximum_slot_bytes() -> u64 {
        preparation_slot_bytes::<Self>()
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::executor::json::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> &'static Schema {
        Schema::None
    }
    fn container_collection_slots() -> bool {
        false
    }
    fn decode_owned_allowance(
        shape: JsonValueShape,
        scratch: u64,
        input: u64,
        code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let value = if Self::json_value_schema() == Schema::None {
            scratch
                .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())
                .ok_or_else(|| decode_allowance_error("preparation codec base overflow", code))?
        } else {
            scratch.max(
                crate::executor::json::json_value_decode_owned_bytes(shape)
                    .map_err(|error| decode_allowance_with_code(error, code))?,
            )
        };
        let nodes = shape
            .lexical_items()
            .checked_add(shape.virtual_items())
            .ok_or_else(|| decode_allowance_error("preparation DTO item count overflow", code))?;
        let typed = if Self::json_value_schema() == Schema::Root {
            Some(0)
        } else {
            let slots = if Self::container_collection_slots() && !shape.unresolved_raw() {
                shape.lexical_containers().map(|containers| {
                    let content = preparation_content_slot_bytes();
                    nodes.checked_mul(content).and_then(|buffering| {
                        containers
                            .checked_mul(Self::maximum_slot_bytes() - content)
                            .and_then(|typed| buffering.checked_add(typed))
                    })
                })
            } else {
                None
            };
            slots
                .unwrap_or_else(|| nodes.checked_mul(Self::maximum_slot_bytes()))
                .and_then(|bytes| bytes.checked_mul(8))
                .and_then(|bytes| {
                    input
                        .checked_mul(4)
                        .and_then(|strings| bytes.checked_add(strings))
                })
        };
        typed
            .and_then(|bytes| bytes.checked_add(value))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() as u64))
            .ok_or_else(|| decode_allowance_error("preparation DTO ownership overflow", code))
    }
}

#[doc(hidden)]
pub trait UploadRecord: serde::de::DeserializeOwned {
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

pub(super) fn decode_allowance_with_code(
    error: Error,
    code: Option<crate::wire::ErrorCode>,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if let (Some(code), Error::NativeBoundary { reason, .. }) = (code, &error) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = code;
    error
}

pub(crate) fn preparation_slot_bytes<T>() -> u64 {
    let typed = [
        size_of::<T>(),
        size_of::<crate::model::ModelMessage>(),
        size_of::<crate::model::ModelContentPart>(),
        size_of::<crate::model::NativeConfigurationBinding>(),
        size_of::<crate::model::ModelDataPart>(),
        size_of::<crate::model::FileProjectionPolicy>(),
        size_of::<crate::model::ProjectInstruction>(),
        size_of::<crate::model::InstructionIdentity>(),
        size_of::<crate::model::NativeMediaIntent>(),
        size_of::<crate::model::NativeMediaPolicy>(),
        size_of::<crate::tool::ToolDefinition>(),
        size_of::<crate::conversation::Attachment>(),
        size_of::<FileRef>(),
        size_of::<serde_json::Value>(),
        size_of::<(String, serde_json::Value)>(),
        size_of::<(String, FileRef)>(),
        size_of::<crate::workflow::WorkflowCommand>(),
        size_of::<crate::workflow::WorkflowRecord>(),
        size_of::<crate::workflow::MachineCheckpoint>(),
        size_of::<crate::workflow::MachineTransition>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0) as u64;
    typed + preparation_content_slot_bytes()
}

fn preparation_content_slot_bytes() -> u64 {
    let content = size_of::<serde::__private229::de::Content<'static>>().max(size_of::<(
        serde::__private229::de::Content<'static>,
        serde::__private229::de::Content<'static>,
    )>()) as u64;
    2 * 5 * content
}

macro_rules! records {
    ($($record:ty => $schema:ident),* $(,)?) => {$(impl PreparationRecord for $record { fn json_value_schema() -> &'static Schema { Schema::$schema } })*};
}
records!(
    acyclic_fs::access::Policy => None, acyclic_fs::access::Capability => None,
    crate::Capabilities => None,
    crate::context::Context => Context, crate::context::SummarySource => SummarySource, crate::context::CanonicalContextCheckpoint => None, crate::context::CompactionReference => None,
    crate::executor::ResponseProjection => ResponseProjection, crate::model::ModelEvent => ModelEvent,
    crate::model::ModelRequest => ModelRequest, crate::model::ModelContent => ModelContent, crate::model::ModelContentPart => ModelContentPart,
    crate::fork::InheritedConversationPrefix => None, crate::core::ExtensionConfiguration => None,
    FileRef => None, Vec<crate::conversation::Attachment> => None,
    crate::workflow::WorkflowAdmission => WorkflowAdmission, crate::workflow::MachineCheckpoint => MachineCheckpoint, crate::workflow::WorkflowCommand => None,
);
impl PreparationRecord for crate::workflow::WorkflowRecord {
    fn diagnostic_source_bytes(shape: JsonValueShape) -> u64 {
        // The closed workflow envelope's Value fields accept any admitted
        // JSON tree. Only typed metadata can render unexpected string data.
        shape.typed_diagnostic_source_bytes()
    }
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        use crate::executor::json::json_value_retained_owned_bytes as value_bytes;
        use crate::workflow::{MachineStatus, WorkflowCommand};
        // Decode admission covers transient serde buffering. Once decoding has
        // completed, retain only the resident DTO capacities, including sparse
        // BTreeMap nodes in its arbitrary JSON values.
        let overflow = || decode_allowance_error("workflow capacity inventory overflows", None);
        let mut bytes = (size_of::<Self>() as u64)
            .checked_add(self.idempotency_key.0.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(self.prior.machine.name.capacity() as u64))
            .and_then(|bytes| bytes.checked_add(self.prior.machine.version.capacity() as u64))
            .ok_or_else(overflow)?;
        for value in [&self.prior.state, &self.input, &self.transition.state] {
            bytes = bytes
                .checked_add(value_bytes(value)?)
                .ok_or_else(overflow)?;
        }
        bytes = bytes
            .checked_add(
                self.transition
                    .commands
                    .capacity()
                    .checked_mul(size_of::<WorkflowCommand>())
                    .ok_or_else(overflow)? as u64,
            )
            .ok_or_else(overflow)?;
        for command in &self.transition.commands {
            let payload = command.payload.logical_owned_bytes()?;
            bytes = bytes
                .checked_add(command.kind.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(payload))
                .ok_or_else(overflow)?;
        }
        let status = match &self.transition.status {
            MachineStatus::Suspended => 0,
            MachineStatus::Completed { value } => value_bytes(value)?,
            MachineStatus::Failed { message } => message.capacity() as u64,
        };
        Ok(Some(bytes.checked_add(status).ok_or_else(overflow)?))
    }
    fn json_value_schema() -> &'static Schema {
        Schema::WorkflowRecord
    }
    fn container_collection_slots() -> bool {
        // WorkflowCommand and its FileRef are object-shaped; every other
        // collection is a Value tree. Keep Content buffering on every lexical
        // item and the separate Value/string allowance, while charging large
        // typed slots only at parsed object/array openings. Resident values and
        // unresolved RawValue shapes retain the conservative item bound.
        true
    }
}
impl PreparationRecord for crate::Outcome<serde_json::Value> {
    fn maximum_slot_bytes() -> u64 {
        [
            size_of::<Self>(),
            size_of::<serde_json::Value>(),
            size_of::<(String, serde_json::Value)>(),
            size_of::<String>(),
            size_of::<crate::OperationId>(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0) as u64
    }
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
