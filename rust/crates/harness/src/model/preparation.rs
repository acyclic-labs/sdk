use super::*;
use crate::{
    core::{
        copy_boxed, copy_configuration, copy_file, copy_reference, copy_text, copy_value,
        copy_vector,
    },
    wire_codec::CodecQuota,
};

#[cfg(feature = "filesystem")]
pub(crate) const MODEL_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::contract::preparation_serde_diagnostic_label_bytes() + 84;
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ModelPrefix {
    fn diagnostic_label_bytes() -> u64 {
        MODEL_DIAGNOSTIC_LABEL_BYTES
    }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolResultContent {
    fn diagnostic_label_bytes() -> u64 {
        MODEL_DIAGNOSTIC_LABEL_BYTES
    }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
            .max(<crate::context::Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes())
            .max(std::mem::size_of::<Self>() as u64)
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ToolResultContent
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for ToolResultContent {
    fn diagnostic_label_bytes() -> u64 {
        MODEL_DIAGNOSTIC_LABEL_BYTES
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.payload_bytes()
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ToolResultContent
    }
}

pub(crate) fn vec_with_capacity<T>(capacity: usize, io: Option<&PreparationIo>) -> Result<Vec<T>> {
    let mut values = Vec::new();
    let bytes = slots(capacity, std::mem::size_of::<T>())?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = io.original_native_work() {
            return with_history_io(Some(io), |work| {
                let mut quota = CodecQuota::new(work, 0)?;
                let values = crate::core::reserve_copy_vector(capacity, &mut quota)?;
                original.remaining()?;
                quota.transfer();
                Ok(values)
            });
        }
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    let _ = (io, bytes);
    values
        .try_reserve_exact(capacity)
        .map_err(Error::Allocation)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.retain_owned(
            slots(values.capacity(), std::mem::size_of::<T>())?
                .checked_sub(bytes)
                .ok_or_else(overflow)?,
        )?;
    }
    Ok(values)
}

pub(crate) fn charge_items(io: Option<&PreparationIo>, items: u64) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let charge = acyclic_fs::WorkCounters {
            items_examined: items,
            ..Default::default()
        };
        io.preflight(charge)?;
        io.charge(charge)?;
    }
    let _ = (io, items);
    Ok(())
}
pub(super) fn history_step(work: &mut Option<&mut dyn HistoryIo>, items: u64) -> Result<()> {
    if let Some(work) = work.as_deref_mut() {
        work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items })?;
    }
    Ok(())
}
pub(super) fn invalid(work: &mut Option<&mut dyn HistoryIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|work| work.original_native_work().is_some())
    {
        return Error::NativeBoundary {
            code: Invalid,
            reason,
        };
    }
    match work.as_deref_mut() {
        Some(work) => match work.clone_text(reason) {
            Ok(message) => Error::Invalid(message),
            Err(cause) => cause,
        },
        None => Error::Invalid(reason.into()),
    }
}
fn inspect_value(value: &Value, maximum: u64, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    if let Some(observer) = work.as_deref_mut() {
        #[cfg(feature = "filesystem")]
        let maximum_items = observer.remaining()?.items_examined;
        #[cfg(not(feature = "filesystem"))]
        let maximum_items = u64::MAX;
        crate::contract::json_owned_value_shape_bounded(
            value,
            crate::contract::JsonBounds {
                maximum_bytes: maximum,
                maximum_items,
                maximum_depth: 256,
            },
            crate::contract::JsonValueSchema::None,
            &mut |next| observer.codec_charge(next),
        )?;
    }
    Ok(())
}
pub(super) fn value_bound(
    value: &Value,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    inspect_value(value, maximum, work)?;
    crate::contract::json_byte_length(value, maximum, work).map(|_| ())
}
pub(super) fn result_bound(
    content: &ToolResultContent,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if work.is_some() {
        match content {
            ToolResultContent::Json { value } => inspect_value(value, maximum, work)?,
            ToolResultContent::Parts { parts } => {
                for part in parts {
                    history_step(work, 1)?;
                    match part {
                        ModelDataPart::Text { text } => history_step(work, text.len() as u64)?,
                        ModelDataPart::File {
                            policy: FileProjectionPolicy::Native(policy),
                            ..
                        } => {
                            if let Some(binding) = &policy.configuration {
                                history_step(work, binding.source.authority.id.len() as u64)?;
                                history_step(
                                    work,
                                    binding.configuration.extension.name.len() as u64,
                                )?;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    crate::contract::json_byte_length(content, maximum, work).map(|_| ())
}

pub(crate) fn hash_bytes(bytes: &[u8], io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    #[cfg(feature = "filesystem")]
    let charge = acyclic_fs::WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..Default::default()
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(charge)?;
    }
    let digest = *blake3::hash(bytes).as_bytes();
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.charge(charge)?;
    }
    let _ = io;
    Ok(digest)
}
pub(crate) fn clone_file(file: &FileRef, io: Option<&PreparationIo>) -> Result<FileRef> {
    charge_items(io, 1)?;
    with_history_io(io, |work| file.copy(work))
}
pub(crate) fn clone_value(value: &Value, io: Option<&PreparationIo>) -> Result<Value> {
    with_history_io(io, |work| {
        let mut quota = CodecQuota::new(work, 0)?;
        let value = copy_value(value, &mut quota)?;
        quota.transfer();
        Ok(value)
    })
}
pub(crate) fn clone_content(
    content: &ModelContent,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(ModelContent, u64)> {
    let mut quota = CodecQuota::new(work, 0)?;
    let content = content.copy_fields(&mut quota)?;
    let retained = quota.retained();
    quota.transfer();
    Ok((content, retained))
}

impl FileProjectionPolicy {
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        history_step(quota.work(), 1)?;
        Ok(match self {
            Self::Reference => Self::Reference,
            Self::BoundedFull => Self::BoundedFull,
            Self::Native(policy) => {
                Self::Native(copy_boxed(policy.as_ref(), quota, |policy, quota| {
                    history_step(quota.work(), 1)?;
                    let configuration = policy
                        .configuration
                        .as_ref()
                        .map(|binding| -> Result<_> {
                            history_step(quota.work(), 1)?;
                            Ok(NativeConfigurationBinding {
                                source: copy_reference(&binding.source, quota)?,
                                configuration: copy_configuration(&binding.configuration, quota)?,
                                implementation_digest: binding.implementation_digest,
                            })
                        })
                        .transpose()?;
                    Ok(NativeMediaPolicy {
                        intent: policy.intent.clone(),
                        maximum_bytes: policy.maximum_bytes,
                        maximum_work: policy.maximum_work,
                        configuration,
                    })
                })?)
            }
        })
    }
}
impl ToolResultContent {
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        history_step(quota.work(), 1)?;
        Ok(match self {
            Self::Json { value } => Self::Json {
                value: copy_value(value, quota)?,
            },
            Self::Parts { parts } => Self::Parts {
                parts: copy_vector(parts, quota, |part, quota| {
                    history_step(quota.work(), 1)?;
                    Ok(match part {
                        ModelDataPart::Text { text } => ModelDataPart::Text {
                            text: copy_text(text, quota)?,
                        },
                        ModelDataPart::File { file, policy } => ModelDataPart::File {
                            file: copy_file(file, quota)?,
                            policy: policy.copy_fields(quota)?,
                        },
                    })
                })?,
            },
        })
    }
}
impl ModelContentPart {
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        history_step(quota.work(), 1)?;
        Ok(match self {
            Self::Text { text } => Self::Text {
                text: copy_text(text, quota)?,
            },
            Self::File { file, policy } => Self::File {
                file: copy_file(file, quota)?,
                policy: policy.copy_fields(quota)?,
            },
            Self::ToolCall {
                call_id,
                name,
                arguments,
            } => Self::ToolCall {
                call_id: copy_text(call_id, quota)?,
                name: copy_text(name, quota)?,
                arguments: copy_value(arguments, quota)?,
            },
            Self::ToolResult {
                call_id,
                name,
                content,
            } => Self::ToolResult {
                call_id: copy_text(call_id, quota)?,
                name: copy_text(name, quota)?,
                content: content.copy_fields(quota)?,
            },
        })
    }
}
impl ModelContent {
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        if quota.work().is_none() {
            return Ok(self.clone());
        }
        history_step(quota.work(), 1)?;
        Ok(match self {
            Self::Text(text) => Self::Text(copy_text(text, quota)?),
            Self::Part(part) => Self::Part(part.copy_fields(quota)?),
            Self::Parts(parts) => {
                Self::Parts(copy_vector(parts, quota, ModelContentPart::copy_fields)?)
            }
        })
    }
}
pub(crate) fn clone_messages(
    messages: &[ModelMessage],
    io: Option<&PreparationIo>,
) -> Result<Vec<ModelMessage>> {
    if io.is_none() {
        return Ok(messages.to_vec());
    }
    let mut copied = vec_with_capacity(messages.len(), io)?;
    for message in messages {
        charge_items(io, 1)?;
        let content = with_history_io(io, |work| {
            clone_content(&message.content, work).map(|(copy, _)| copy)
        })?;
        crate::executor::stock_vec_push(
            &mut copied,
            ModelMessage {
                role: message.role,
                content,
            },
            io,
        )?;
    }
    Ok(copied)
}
pub(crate) fn clone_model(model: &Model, io: Option<&PreparationIo>) -> Result<Model> {
    if io.is_none() {
        return Ok(model.clone());
    }
    charge_items(io, 1)?;
    Ok(Model {
        provider: crate::executor::stock_string_copy(&model.provider, io)?,
        name: crate::executor::stock_string_copy(&model.name, io)?,
        revision: crate::executor::stock_string_copy(&model.revision, io)?,
        options: clone_value(&model.options, io)?,
    })
}
pub(crate) fn clone_tool_definition(
    tool: &crate::tool::ToolDefinition,
    io: Option<&PreparationIo>,
) -> Result<crate::tool::ToolDefinition> {
    if io.is_none() {
        return Ok(tool.clone());
    }
    charge_items(io, 1)?;
    Ok(crate::tool::ToolDefinition {
        name: crate::executor::stock_string_copy(&tool.name, io)?,
        revision: crate::executor::stock_string_copy(&tool.revision, io)?,
        description: crate::executor::stock_string_copy(&tool.description, io)?,
        input_schema: clone_value(&tool.input_schema, io)?,
        output_schema: clone_value(&tool.output_schema, io)?,
        projection_schema: clone_value(&tool.projection_schema, io)?,
    })
}
pub(crate) fn clone_request(
    request: &ModelRequest,
    io: Option<&PreparationIo>,
) -> Result<ModelRequest> {
    if io.is_none() {
        return Ok(request.clone());
    }
    charge_items(io, 1)?;
    let model = clone_model(&request.model, io)?;
    let messages = clone_messages(&request.messages, io)?;
    let mut tools = vec_with_capacity(request.tools.len(), io)?;
    for tool in &request.tools {
        crate::executor::stock_vec_push(&mut tools, clone_tool_definition(tool, io)?, io)?;
    }
    Ok(ModelRequest {
        model,
        messages,
        tools,
        max_output_tokens: request.max_output_tokens,
    })
}
