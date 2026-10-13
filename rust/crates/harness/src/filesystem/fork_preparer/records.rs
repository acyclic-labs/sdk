//! Ownership bounds for closed harness records.
use super::*;
use crate::contract::{JsonValueSchema, JsonValueShape};
use std::mem::size_of;

pub(crate) trait PreparationRecord: serde::de::DeserializeOwned {
    fn maximum_slot_bytes() -> u64 {
        preparation_slot_bytes::<Self>()
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> JsonValueSchema {
        JsonValueSchema::None
    }
    fn decode_owned_allowance(
        shape: JsonValueShape,
        scratch: u64,
        input: u64,
        _code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let value = if Self::json_value_schema() == JsonValueSchema::None {
            scratch.checked_add(crate::contract::sdk_json_diagnostic_bytes())
        } else {
            Some(scratch.max(crate::contract::json_value_decode_owned_bytes(shape)?))
        };
        let typed = if Self::json_value_schema() == JsonValueSchema::Root {
            Some(0)
        } else {
            shape
                .lexical_items()
                .checked_add(shape.virtual_items())
                .and_then(|nodes| nodes.checked_mul(Self::maximum_slot_bytes()))
                .and_then(|bytes| bytes.checked_mul(8))
                .and_then(|bytes| {
                    input
                        .checked_mul(4)
                        .and_then(|strings| bytes.checked_add(strings))
                })
        };
        typed
            .and_then(|typed| value.and_then(|value| typed.checked_add(value)))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() as u64))
            .ok_or_else(|| Error::Invalid("record ownership overflow".into()))
    }
}
pub(crate) trait UploadRecord: serde::de::DeserializeOwned {
    fn logical_owned_bytes(&self) -> Result<u64>;
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
    }
    fn json_value_schema() -> JsonValueSchema {
        JsonValueSchema::None
    }
}
pub(crate) fn preparation_slot_bytes<T>() -> u64 {
    let slot = [
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
        size_of::<crate::conversation::FileRef>(),
        size_of::<serde_json::Value>(),
        size_of::<(String, serde_json::Value)>(),
        size_of::<(String, crate::conversation::FileRef)>(),
        size_of::<crate::workflow::WorkflowCommand>(),
        size_of::<crate::workflow::WorkflowRecord>(),
        size_of::<crate::workflow::MachineCheckpoint>(),
        size_of::<crate::workflow::MachineTransition>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let content = size_of::<serde::__private229::de::Content<'static>>().max(size_of::<(
        serde::__private229::de::Content<'static>,
        serde::__private229::de::Content<'static>,
    )>());
    (slot + 10 * content) as u64
}
macro_rules! records {
    ($($record:ty => $schema:ident),* $(,)?) => {$(impl PreparationRecord for $record {
        fn json_value_schema() -> JsonValueSchema { JsonValueSchema::$schema }
    })*};
}
records!(
    crate::context::Context => Context, crate::context::CanonicalContextCheckpoint => None,
    crate::context::CompactionReference => None, crate::executor::ResponseProjection => ResponseProjection,
    crate::model::ModelEvent => ModelEvent, crate::model::ModelRequest => ModelRequest,
    crate::model::ModelContent => ModelContent, crate::model::ModelContentPart => ModelContentPart,
    crate::fork::InheritedConversationPrefix => None, crate::core::ExtensionConfiguration => None,
    crate::conversation::FileRef => None, Vec<crate::conversation::Attachment> => None,
    crate::workflow::WorkflowRecord => WorkflowRecord, crate::workflow::WorkflowAdmission => WorkflowAdmission,
    crate::workflow::MachineCheckpoint => MachineCheckpoint, crate::workflow::WorkflowCommand => None,
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
    fn json_value_schema() -> JsonValueSchema {
        JsonValueSchema::TaskOutcome
    }
}
impl PreparationRecord for serde_json::Value {
    fn diagnostic_label_bytes() -> u64 {
        0
    }
    fn json_value_schema() -> JsonValueSchema {
        JsonValueSchema::Root
    }
}
impl UploadRecord for crate::conversation::FileRef {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Self::logical_owned_bytes(self)
    }
}
impl UploadRecord
    for (
        crate::conversation::FileRef,
        Option<crate::resources::GenerationRef>,
    )
{
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.0.logical_owned_bytes()?,
            self.1
                .as_ref()
                .map(crate::resources::GenerationRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
        ])
    }
}
