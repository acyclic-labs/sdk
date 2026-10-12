//! Ownership bounds for closed harness DTOs.
use super::*;
use crate::contract::{JsonValueSchema as Schema, JsonValueShape};
use std::mem::size_of;

pub(crate) trait PreparationRecord: serde::de::DeserializeOwned {
    fn maximum_slot_bytes() -> u64 {
        preparation_slot_bytes::<Self>()
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> Schema {
        Schema::None
    }
    fn decode_owned_allowance(
        shape: JsonValueShape,
        scratch: u64,
        input: u64,
        code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let value = if Self::json_value_schema() == Schema::None {
            scratch
                .checked_add(crate::contract::sdk_json_diagnostic_bytes())
                .ok_or_else(|| decode_allowance_error("preparation codec base overflow", code))?
        } else {
            scratch.max(
                crate::contract::json_value_decode_owned_bytes(shape)
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
            nodes
                .checked_mul(Self::maximum_slot_bytes())
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

pub(crate) trait UploadRecord: serde::de::DeserializeOwned {
    fn logical_owned_bytes(&self) -> Result<u64>;
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> Schema {
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
        return Error::NativeBoundary {
            code,
            reason: *reason,
        };
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
    let content = size_of::<serde::__private229::de::Content<'static>>().max(size_of::<(
        serde::__private229::de::Content<'static>,
        serde::__private229::de::Content<'static>,
    )>()) as u64;
    typed + 2 * 5 * content
}

macro_rules! records {
    ($($record:ty => $schema:ident),* $(,)?) => {$(impl PreparationRecord for $record { fn json_value_schema() -> Schema { Schema::$schema } })*};
}
records!(
    crate::context::Context => Context, crate::context::CanonicalContextCheckpoint => None, crate::context::CompactionReference => None,
    crate::executor::ResponseProjection => ResponseProjection, crate::model::ModelEvent => ModelEvent,
    crate::model::ModelRequest => ModelRequest, crate::model::ModelContent => ModelContent, crate::model::ModelContentPart => ModelContentPart,
    crate::fork::InheritedConversationPrefix => None, crate::core::ExtensionConfiguration => None,
    FileRef => None, Vec<crate::conversation::Attachment> => None, crate::workflow::WorkflowRecord => WorkflowRecord,
    crate::workflow::WorkflowAdmission => WorkflowAdmission, crate::workflow::MachineCheckpoint => MachineCheckpoint, crate::workflow::WorkflowCommand => None,
);
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
    fn json_value_schema() -> Schema {
        Schema::TaskOutcome
    }
}
impl PreparationRecord for serde_json::Value {
    fn diagnostic_label_bytes() -> u64 {
        0
    }
    fn json_value_schema() -> Schema {
        Schema::Root
    }
}
impl UploadRecord for FileRef {
    fn logical_owned_bytes(&self) -> Result<u64> {
        FileRef::logical_owned_bytes(self)
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
