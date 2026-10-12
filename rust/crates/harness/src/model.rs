//! Provider-neutral immutable model values and streaming host contract.

use crate::{Error, OperationId, Result, conversation::FileRef};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::BoxProviderStream as BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};

#[cfg(feature = "filesystem")]
pub(crate) const fn model_serde_diagnostic_label_bytes() -> u64 {
    const LABEL_BYTES: u64 = {
        let labels = ["ModelPrefix", "version", "parent", "binding_digest", "message_digest",
            "total_messages", "messages", "ModelRequestManifest", "version", "request_digest",
            "wire_bytes", "binding_digest", "message_digests"];
        let mut bytes = crate::contract::preparation_serde_diagnostic_label_bytes();
        let mut index = 0;
        while index < labels.len() { bytes += labels[index].len() as u64 + 4; index += 1; }
        bytes
    };
    LABEL_BYTES
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for Model {
    fn diagnostic_label_bytes() -> u64 { model_serde_diagnostic_label_bytes() }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ModelPrefix {
    fn diagnostic_label_bytes() -> u64 { model_serde_diagnostic_label_bytes() }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for Vec<ModelMessage> {
    fn diagnostic_label_bytes() -> u64 { model_serde_diagnostic_label_bytes() }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolResultContent {
    fn diagnostic_label_bytes() -> u64 { model_serde_diagnostic_label_bytes() }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
            .max(<crate::context::Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes())
            .max(std::mem::size_of::<Self>() as u64)
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ToolResultContent
    }

}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolResultContent {
    fn diagnostic_label_bytes() -> u64 { model_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        // The converter supplies the root header; this trait reports deep only.
        ToolResultContent::logical_owned_bytes(self)?
            .checked_sub(std::mem::size_of::<Self>() as u64)
            .ok_or_else(model_owned_overflow)
    }

    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ToolResultContent
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for NativeMediaPolicy {
    fn diagnostic_label_bytes() -> u64 { model_serde_diagnostic_label_bytes() }
    fn maximum_slot_bytes() -> u64 {
        <ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
            .max(std::mem::size_of::<Self>() as u64)
    }
}

// This only allocates concrete Vec backing; every caller copies its known DTO fields directly.
fn model_vec_with_capacity<T>(
    capacity: usize, io: Option<&crate::executor::PreparationIo>,
) -> Result<Vec<T>> {
    let mut values = Vec::new();
    let bytes = model_owned_mul(capacity, std::mem::size_of::<T>())?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            if bytes == 0 { work.remaining()?; return Ok(values); }
            let mut custody = work.claim(bytes)?;
            values.try_reserve_exact(capacity).map_err(Error::Allocation)?;
            let completed = custody.reconcile_capacity(model_owned_mul(values.capacity(), std::mem::size_of::<T>())?);
            io.absorb_native_custody(custody)?;
            completed?;
            return Ok(values);
        }
        io.claim(bytes)?;
        io.allocation_attempt(bytes)?;
        values.try_reserve_exact(capacity).map_err(Error::Allocation)?;
        io.claim(model_owned_mul(values.capacity(), std::mem::size_of::<T>())?.checked_sub(bytes)
            .ok_or_else(model_owned_overflow)?)?;
        return Ok(values);
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = (io, bytes);
    values.try_reserve_exact(capacity).map_err(Error::Allocation)?;
    Ok(values)
}

pub(crate) fn clone_file(
    file: &FileRef, io: Option<&crate::executor::PreparationIo>,
) -> Result<FileRef> {
    model_step(io, 1)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut observer = io.history_io();
        let mut work = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let file = crate::core::copy_file(file, &mut quota)?;
        quota.transfer();
        return Ok(file);
    }
    Ok(file.clone())
}

/// Copies the actual Value representation, including private/raw marker keys.
pub(crate) fn clone_value(
    value: &Value, io: Option<&crate::executor::PreparationIo>,
) -> Result<Value> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut observer = io.history_io();
        let mut work = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let value = crate::core::copy_value(value, &mut quota)?;
        quota.transfer();
        return Ok(value);
    }
    let _ = io;
    Ok(value.clone())
}

/// Copies the closed provider-neutral content graph on the original observer.
pub(crate) fn clone_content(
    content: &ModelContent, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(ModelContent, u64)> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let content = model_content_in_quota(content, &mut quota)?;
    let retained = quota.retained();
    quota.transfer();
    Ok((content, retained))
}

fn model_copy_node(quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
    if let Some(work) = quota.work().as_deref_mut() {
        work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

fn model_media_policy_in_quota(
    policy: &NativeMediaPolicy, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<NativeMediaPolicy> {
    if quota.work().is_none() { return Ok(policy.clone()); }
    model_copy_node(quota)?;
    let intent = match &policy.intent {
        NativeMediaIntent::Image { detail } => NativeMediaIntent::Image { detail: *detail },
        NativeMediaIntent::Audio { maximum_duration_ms } => NativeMediaIntent::Audio { maximum_duration_ms: *maximum_duration_ms },
        NativeMediaIntent::Video { maximum_duration_ms, maximum_frames } => NativeMediaIntent::Video {
            maximum_duration_ms: *maximum_duration_ms, maximum_frames: *maximum_frames,
        },
        NativeMediaIntent::Document { maximum_pages } => NativeMediaIntent::Document { maximum_pages: *maximum_pages },
    };
    let configuration = policy.configuration.as_ref().map(|binding| -> Result<NativeConfigurationBinding> {
        model_copy_node(quota)?;
        Ok(NativeConfigurationBinding {
            source: crate::core::copy_reference(&binding.source, quota)?,
            configuration: crate::core::copy_configuration(&binding.configuration, quota)?,
            implementation_digest: binding.implementation_digest,
        })
    }).transpose()?;
    Ok(NativeMediaPolicy { intent, maximum_bytes: policy.maximum_bytes,
        maximum_work: policy.maximum_work, configuration })
}

fn model_policy_in_quota(
    policy: &FileProjectionPolicy, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<FileProjectionPolicy> {
    if quota.work().is_none() { return Ok(policy.clone()); }
    model_copy_node(quota)?;
    Ok(match policy {
        FileProjectionPolicy::Reference => FileProjectionPolicy::Reference,
        FileProjectionPolicy::BoundedFull => FileProjectionPolicy::BoundedFull,
        FileProjectionPolicy::Native(policy) => FileProjectionPolicy::Native(
            crate::core::copy_boxed(policy.as_ref(), quota, model_media_policy_in_quota)?),
    })
}

fn model_tool_result_in_quota(
    content: &ToolResultContent, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ToolResultContent> {
    if quota.work().is_none() { return Ok(content.clone()); }
    model_copy_node(quota)?;
    Ok(match content {
        ToolResultContent::Json { value } => ToolResultContent::Json { value: crate::core::copy_value(value, quota)? },
        ToolResultContent::Parts { parts } => ToolResultContent::Parts {
            parts: crate::core::copy_vector(parts, quota, |part, quota| {
                model_copy_node(quota)?;
                Ok(match part {
                    ModelDataPart::Text { text } => ModelDataPart::Text { text: crate::core::copy_text(text, quota)? },
                    ModelDataPart::File { file, policy } => ModelDataPart::File {
                        file: crate::core::copy_file(file, quota)?, policy: model_policy_in_quota(policy, quota)?,
                    },
                })
            })?,
        },
    })
}

fn model_part_in_quota(
    part: &ModelContentPart, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ModelContentPart> {
    model_copy_node(quota)?;
    Ok(match part {
        ModelContentPart::Text { text } => ModelContentPart::Text { text: crate::core::copy_text(text, quota)? },
        ModelContentPart::File { file, policy } => ModelContentPart::File {
            file: crate::core::copy_file(file, quota)?, policy: model_policy_in_quota(policy, quota)?,
        },
        ModelContentPart::ToolCall { call_id, name, arguments } => ModelContentPart::ToolCall {
            call_id: crate::core::copy_text(call_id, quota)?, name: crate::core::copy_text(name, quota)?,
            arguments: crate::core::copy_value(arguments, quota)?,
        },
        ModelContentPart::ToolResult { call_id, name, content } => ModelContentPart::ToolResult {
            call_id: crate::core::copy_text(call_id, quota)?, name: crate::core::copy_text(name, quota)?,
            content: model_tool_result_in_quota(content, quota)?,
        },
    })
}

fn model_content_in_quota(
    content: &ModelContent, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ModelContent> {
    if quota.work().is_none() { return Ok(content.clone()); }
    model_copy_node(quota)?;
    Ok(match content {
        ModelContent::Text(text) => ModelContent::Text(crate::core::copy_text(text, quota)?),
        ModelContent::Part(part) => ModelContent::Part(model_part_in_quota(part, quota)?),
        ModelContent::Parts(parts) => ModelContent::Parts(crate::core::copy_vector(parts, quota, model_part_in_quota)?),
    })
}

pub(crate) fn clone_message(
    message: &ModelMessage, io: Option<&crate::executor::PreparationIo>,
) -> Result<ModelMessage> {
    if io.is_none() { return Ok(message.clone()); }
    model_step(io, 1)?;
    Ok(ModelMessage { role: message.role, content: crate::executor::with_history_io(io, |work| clone_content(&message.content, work).map(|(copy, _)| copy))? })
}

pub(crate) fn clone_messages(
    messages: &[ModelMessage], io: Option<&crate::executor::PreparationIo>,
) -> Result<Vec<ModelMessage>> {
    if io.is_none() { return Ok(messages.to_vec()); }
    let mut copied = model_vec_with_capacity(messages.len(), io)?;
    for message in messages { crate::executor::stock_vec_push(&mut copied, clone_message(message, io)?, io)?; }
    Ok(copied)
}

pub(crate) fn clone_model(
    model: &Model, io: Option<&crate::executor::PreparationIo>,
) -> Result<Model> {
    if io.is_none() { return Ok(model.clone()); }
    model_step(io, 1)?;
    Ok(Model {
        provider: crate::executor::stock_string_copy(&model.provider, io)?,
        name: crate::executor::stock_string_copy(&model.name, io)?,
        revision: crate::executor::stock_string_copy(&model.revision, io)?,
        options: clone_value(&model.options, io)?,
    })
}

pub(crate) fn clone_tool_definition(
    tool: &crate::tool::ToolDefinition, io: Option<&crate::executor::PreparationIo>,
) -> Result<crate::tool::ToolDefinition> {
    if io.is_none() { return Ok(tool.clone()); }
    model_step(io, 1)?;
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
    request: &ModelRequest, io: Option<&crate::executor::PreparationIo>,
) -> Result<ModelRequest> {
    if io.is_none() { return Ok(request.clone()); }
    model_step(io, 1)?;
    let model = clone_model(&request.model, io)?;
    let messages = clone_messages(&request.messages, io)?;
    let mut tools = model_vec_with_capacity(request.tools.len(), io)?;
    for tool in &request.tools { crate::executor::stock_vec_push(&mut tools, clone_tool_definition(tool, io)?, io)?; }
    Ok(ModelRequest { model, messages, tools, max_output_tokens: request.max_output_tokens })
}

fn model_step(io: Option<&crate::executor::PreparationIo>, items: u64) -> Result<()> {
    #[cfg(not(feature = "filesystem"))]
    let _ = (io, items);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let work = acyclic_fs::WorkCounters { items_examined: items, ..Default::default() };
        io.preflight(work)?;
        io.charge(work)?;
    }
    Ok(())
}

fn model_validate_limits_with_io(
    limits: crate::conversation::Limits, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut observer = io.history_io();
        return limits.validate_with_work(&mut Some(&mut observer));
    }
    let _ = io;
    limits.validate_with_work(&mut None)
}

fn model_validate_file_shape_with_io(
    file: &FileRef, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut observer = io.history_io();
        return file.validate_with_work(&mut Some(&mut observer));
    }
    let _ = io;
    file.validate_with_work(&mut None)
}

fn model_history_step(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>, items: u64,
) -> Result<()> {
    if let Some(work) = work.as_deref_mut() {
        work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items })?;
    }
    Ok(())
}

fn model_history_invalid(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    match work.as_deref_mut() {
        Some(work) => match work.clone_text(reason) {
            Ok(message) => Error::Invalid(message), Err(cause) => cause,
        },
        None => Error::Invalid(reason.into()),
    }
}

fn model_history_bounds(
    maximum: u64, work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<crate::contract::JsonBounds> {
    #[cfg(feature = "filesystem")]
    let maximum_items = match work.as_deref() {
        Some(work) => work.remaining()?.items_examined, None => u64::MAX,
    };
    #[cfg(not(feature = "filesystem"))]
    let maximum_items = { let _ = work; u64::MAX };
    Ok(crate::contract::JsonBounds { maximum_bytes: maximum, maximum_items, maximum_depth: 256 })
}

fn model_json_bound<T: Serialize>(
    value: &T, maximum: u64, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let unobserved = work.is_none();
    let result = crate::contract::validate_trusted_json_byte_bound(value, maximum, work);
    if unobserved {
        result.map_err(|_| model_history_invalid(work, "model tool projection exceeds render limit"))
    } else { result }
}

fn model_value_json_bound(
    value: &Value, maximum: u64, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_some() {
        let bounds = model_history_bounds(maximum, work)?;
        crate::contract::json_owned_value_shape_bounded(value, bounds,
            crate::contract::JsonValueSchema::None,
            &mut |next| work.as_deref_mut().expect("observed model value").codec_charge(next))?;
    }
    model_json_bound(value, maximum, work)
}

fn model_tool_result_json_bound(
    content: &ToolResultContent, maximum: u64, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_some() {
        match content {
            ToolResultContent::Json { value } => {
                let bounds = model_history_bounds(maximum, work)?;
                crate::contract::json_owned_value_shape_bounded(value, bounds,
                    crate::contract::JsonValueSchema::None,
                    &mut |next| work.as_deref_mut().expect("observed model result").codec_charge(next))?;
            }
            ToolResultContent::Parts { parts } => for part in parts {
                model_history_step(work, 1)?;
                match part {
                    ModelDataPart::Text { text } => model_history_step(work, text.len() as u64)?,
                    ModelDataPart::File { policy, .. } => {
                        if let FileProjectionPolicy::Native(policy) = policy
                            && let Some(binding) = &policy.configuration {
                            model_history_step(work, binding.source.authority.id.len() as u64)?;
                            model_history_step(work, binding.configuration.extension.name.len() as u64)?;
                        }
                    }
                }
            },
        }
    }
    model_json_bound(content, maximum, work)
}

fn model_hash(bytes: &[u8], io: Option<&crate::executor::PreparationIo>) -> Result<[u8; 32]> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    let work = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.preflight(work)?; }
    let digest = *blake3::hash(bytes).as_bytes();
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.charge(work)?; }
    Ok(digest)
}

fn model_digest<T: Serialize>(
    value: &T, io: Option<&crate::executor::PreparationIo>,
) -> Result<[u8; 32]> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let _bytes;
        let bytes = io.encode_json(value, io.remaining()?.bytes_encoded, true)?;
        _bytes = io.guard_bytes(&bytes)?;
        return model_hash(&bytes, Some(io));
    }
    crate::contract::canonical_json_digest(value)
}
/// Immutable provider-owned model selection; there is no global catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Model {
    /// Provider selected by application code.
    pub provider: String,
    /// Provider-owned model name.
    pub name: String,
    /// Immutable model revision or digest.
    pub revision: String,
    /// Provider-specific options retained as typed JSON.
    pub options: Value,
}

impl Model {
    /// Creates a validated immutable model value.
    pub fn new(
        provider: impl Into<String>,
        name: impl Into<String>,
        revision: impl Into<String>,
        options: Value,
    ) -> Result<Self> {
        let value = Self {
            provider: provider.into(),
            name: name.into(),
            revision: revision.into(),
            options,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates constructor and deserialized selections.
    pub fn validate(&self) -> Result<()> {
        if self.provider.trim().is_empty()
            || self.name.trim().is_empty()
            || self.revision.trim().is_empty()
        {
            return Err(Error::Invalid(
                "model provider, name, and revision must be non-empty".into(),
            ));
        }
        Ok(())
    }

}

/// One provider-neutral prompt item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelMessage {
    /// Semantic role (`system`, `user`, `assistant`, or `tool`).
    pub role: ModelRole,
    /// Typed, provider-neutral content; file bytes are never part of this value.
    pub content: ModelContent,
}

/// Closed provider-neutral role set; adapters may reject unsupported combinations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    /// Host instruction.
    System,
    /// Human input.
    User,
    /// Agent or model output.
    Assistant,
    /// Result from a tool call.
    Tool,
}

impl ModelRole {
    /// Stable wire role name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// Transient model-visible content. Canonical conversation storage always retains refs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(
    clippy::large_enum_variant,
    reason = "model parts retain their public untagged content shape"
)]
pub enum ModelContent {
    /// Plain prompt text or bounded projection produced by a resolver.
    Text(String),
    /// One typed content part.
    Part(ModelContentPart),
    /// Ordered multimodal content parts.
    Parts(Vec<ModelContentPart>),
}

impl ModelContent {
    /// Completed root and out-of-line known content storage.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        model_owned_add(std::mem::size_of::<Self>() as u64, model_content_payload_bytes(self)?)
    }

    /// Ordered structured parts; plain text has no structured parts.
    #[must_use]
    pub fn parts(&self) -> &[ModelContentPart] {
        match self {
            Self::Text(_) => &[],
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) => parts,
        }
    }

    /// Applies the same byte and part ceilings to direct inputs and selected history before either can reach a provider or durable request journal.
    pub fn validate_limits(
        &self, limits: crate::conversation::Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        model_history_step(work, 1)?;
        limits.validate_with_work(work)?;
        let parts = match self {
            Self::Text(text) => return if text.len() as u64 <= limits.render_bytes { Ok(()) }
                else { Err(model_history_invalid(work, "model text exceeds render limit")) },
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) => parts.as_slice(),
        };
        if parts.len() > limits.attachments.saturating_add(1) {
            return Err(model_history_invalid(work, "model content exceeds attachment limit"));
        }
        for part in parts {
            model_history_step(work, 1)?;
            match part {
                ModelContentPart::Text { text } if text.len() as u64 > limits.render_bytes =>
                    return Err(model_history_invalid(work, "model text exceeds render limit")),
                ModelContentPart::File { file, policy } => policy.validate_file(file, limits, work)?,
                ModelContentPart::ToolCall { name, arguments, .. } => {
                    model_history_step(work, name.len() as u64)?;
                    if !crate::contract::is_valid_component_label(name) {
                        return Err(model_history_invalid(work, "tool name is invalid"));
                    }
                    model_value_json_bound(arguments, limits.render_bytes, work)?;
                }
                ModelContentPart::ToolResult { name, content, .. } => {
                    model_history_step(work, name.len() as u64)?;
                    if !crate::contract::is_valid_component_label(name) {
                        return Err(model_history_invalid(work, "tool name is invalid"));
                    }
                    content.validate_limits(limits, work)?;
                }
                ModelContentPart::Text { .. } => {}
            }
        }
        let mut references = 0usize;
        self.visit_file_bindings(|_, _| {
            model_history_step(work, 1)?;
            references = references.checked_add(1)
                .ok_or_else(|| model_history_invalid(work, "model content reference count overflow"))?;
            Ok(())
        })?;
        if references > limits.attachments.saturating_add(1) {
            return Err(model_history_invalid(work, "model content reference count exceeds attachment limit"));
        }
        Ok(())
    }

    /// Rejects tool and empty content in a human turn input.
    pub fn validate_user_input(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        model_history_step(work, 1)?;
        let parts = match self {
            Self::Text(text) => return if text.is_empty() {
                Err(model_history_invalid(work, "user input is empty"))
            } else { Ok(()) },
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) if !parts.is_empty() => parts.as_slice(),
            _ => return Err(model_history_invalid(work, "user input part count is invalid")),
        };
        for part in parts {
            model_history_step(work, 1)?;
            match part {
                ModelContentPart::Text { text } if !text.is_empty() => {}
                ModelContentPart::File { file, .. } => file.validate_with_work(work)?,
                _ => return Err(model_history_invalid(work, "user input contains an unsupported part")),
            }
        }
        Ok(())
    }

    pub(crate) fn visit_file_bindings<'a>(
        &'a self, mut visit: impl FnMut(&'a FileRef, Option<&'a NativeConfigurationBinding>) -> Result<()>,
    ) -> Result<()> {
        let mut file = |reference: &'a FileRef, policy: &'a FileProjectionPolicy| -> Result<()> {
            visit(reference, None)?;
            if let FileProjectionPolicy::Native(policy) = policy
                && let Some(binding) = &policy.configuration {
                visit(&binding.configuration.content, Some(binding))?;
            }
            Ok(())
        };
        for part in self.parts() {
            match part {
                ModelContentPart::File { file: reference, policy } => file(reference, policy)?,
                ModelContentPart::ToolResult { content: ToolResultContent::Parts { parts }, .. } => {
                    for part in parts {
                        if let ModelDataPart::File { file: reference, policy } = part { file(reference, policy)?; }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Returns ordered media refs followed by their immutable option refs.
    #[must_use]
    pub fn file_refs(&self) -> Vec<&FileRef> {
        let mut files = Vec::new();
        for part in self.parts() {
            match part {
                ModelContentPart::File { file, policy } => policy.append_refs(file, &mut files),
                ModelContentPart::ToolResult {
                    content: ToolResultContent::Parts { parts },
                    ..
                } => {
                    for part in parts {
                        if let ModelDataPart::File { file, policy } = part {
                            policy.append_refs(file, &mut files);
                        }
                    }
                }
                _ => {}
            }
        }
        files
    }

    /// Detects native media in ordinary data and paired result data.
    #[must_use]
    pub fn contains_native_media(&self) -> bool {
        self.parts().iter().any(|part| match part {
            ModelContentPart::File {
                policy: FileProjectionPolicy::Native(_),
                ..
            } => true,
            ModelContentPart::ToolResult {
                content: ToolResultContent::Parts { parts },
                ..
            } => parts.iter().any(|part| {
                matches!(
                    part,
                    ModelDataPart::File {
                        policy: FileProjectionPolicy::Native(_),
                        ..
                    }
                )
            }),
            _ => false,
        })
    }

    /// Claims that the original authenticated host must verify before dispatch.
    #[must_use]
    pub fn native_configurations(&self) -> Vec<&NativeConfigurationBinding> {
        let mut bindings = Vec::new();
        for part in self.parts() {
            match part {
                ModelContentPart::File {
                    policy: FileProjectionPolicy::Native(policy),
                    ..
                } => {
                    bindings.extend(policy.configuration.iter());
                }
                ModelContentPart::ToolResult {
                    content: ToolResultContent::Parts { parts },
                    ..
                } => {
                    for part in parts {
                        if let ModelDataPart::File {
                            policy: FileProjectionPolicy::Native(policy),
                            ..
                        } = part
                        {
                            bindings.extend(policy.configuration.iter());
                        }
                    }
                }
                _ => {}
            }
        }
        bindings
    }
}

/// One provider-neutral part requiring an explicit provider projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "keep immutable FileRef inline; the larger optional native policy payload is boxed"
)]
pub enum ModelContentPart {
    /// Plain text.
    Text {
        /// Model-visible text.
        text: String,
    },
    /// Immutable file reference with an explicit byte-resolution policy.
    File {
        /// Immutable file identity.
        file: FileRef,
        /// Resolution and rendering policy.
        policy: FileProjectionPolicy,
    },
    /// Assistant tool request.
    ToolCall {
        /// Stable call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Schema-defined arguments.
        arguments: Value,
    },
    /// Tool result with matching call identity.
    ToolResult {
        /// Matching call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Complete model-visible result projection envelope.
        content: ToolResultContent,
    },
}

/// File handling policy selected before provider dispatch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileProjectionPolicy {
    /// Metadata-only file reference, with no byte read.
    Reference,
    /// Verified, bounded text bytes.
    BoundedFull,
    /// Explicit bounded provider-native intent, verified at the adapter boundary.
    Native(Box<NativeMediaPolicy>),
}
/// Data in a tool result cannot contain another call or result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "keep immutable FileRef inline; the larger optional native policy payload is boxed"
)]
pub enum ModelDataPart {
    /// Exact text.
    Text {
        /// Exact model-visible text.
        text: String,
    },
    /// Immutable data with an explicit resolution policy.
    File {
        /// Immutable media identity.
        file: FileRef,
        /// Explicit resolution intent.
        policy: FileProjectionPolicy,
    },
}

/// The complete model-visible projection envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolResultContent {
    /// Explicit JSON projection.
    Json {
        /// Exact schema-defined JSON value.
        value: Value,
    },
    /// Ordered, nonrecursive text and file data.
    Parts {
        /// Ordered text and file data.
        parts: Vec<ModelDataPart>,
    },
}

/// Claim to verify against original task admission and linked implementation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConfigurationBinding {
    /// Original immutable extension admission event.
    pub source: crate::core::EventReference,
    /// Exact registered option schema and immutable JSON file.
    pub configuration: crate::core::ExtensionConfiguration,
    /// Original owner-linked executable implementation.
    pub implementation_digest: [u8; 32],
}

impl NativeConfigurationBinding {
    /// Verifies the claim against the original immutable composition before IO.
    pub(crate) fn verify_original<'a>(
        &self,
        admission: Option<&crate::core::ExtensionAdmission>,
        schemas: Option<&'a crate::core::SchemaRegistry>,
        runtime: Option<&crate::extension::ExtensionRuntime>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(&'a crate::core::SchemaRegistry, [u8; 32])> {
        #[cfg(feature = "filesystem")]
        let _binding_hold;
        #[cfg(feature = "filesystem")]
        let _binding_bytes;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            _binding_bytes = io.encode_json(self, io.remaining()?.bytes_encoded, false)?;
            _binding_hold = io.guard_owned(_binding_bytes.len() as u64)?;
        }
        self.configuration.validate()?;
        let admission = admission.ok_or_else(|| {
            Error::Unsupported("original task extension admission is unavailable".into())
        })?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let _bytes;
            let bytes = io.encode_json(
                &(admission.source(), admission.selected(), admission.configurations()),
                io.remaining()?.bytes_encoded, false,
            )?;
            _bytes = io.guard_bytes(&bytes)?;
        }
        if admission.source() != &self.source
            || !admission.selected().contains(&self.configuration.extension)
            || !admission.configurations().contains(&self.configuration)
        {
            return Err(Error::Unauthorized(
                "native options differ from original task admission".into(),
            ));
        }
        let schemas = schemas.ok_or_else(|| {
            Error::Unsupported("original task extension registry is unavailable".into())
        })?;
        let runtime = runtime.ok_or_else(|| {
            Error::Unsupported("original task linked implementation is unavailable".into())
        })?;
        let extension = &self.configuration.extension;
        let mut linked = None;
        for identity in runtime.selected() {
            model_step(io, (identity.name.len() as u64).saturating_add(1))?;
            if identity.name == extension.name && identity.version == extension.version {
                linked = Some(identity);
                break;
            }
        }
        let linked = linked.ok_or_else(|| {
            Error::Unsupported("original linked extension version is unavailable".into())
        })?;
        let expected = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => schemas.implementation_digest_with_io(&extension.name, extension.version, io)?,
            _ => schemas.implementation_digest(&extension.name, extension.version)?,
        };
        if linked.digest != expected || self.implementation_digest != expected {
            return Err(Error::Conflict(
                "native options implementation binding differs".into(),
            ));
        }
        Ok((schemas, expected))
    }
}

/// Explicit common image quality intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
    /// Select adapter image quality.
    Auto,
    /// Request low image detail.
    Low,
    /// Request high image detail.
    High,
}

/// Requested modality is independent of the stored file's MIME type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeMediaIntent {
    /// Provider-native image with explicit detail.
    Image {
        /// Explicit native image detail.
        detail: ImageDetail,
    },
    /// Entire audio input with a finite duration ceiling.
    Audio {
        /// Positive finite maximum source duration.
        maximum_duration_ms: u64,
    },
    /// Entire video input with finite duration and frame ceilings.
    Video {
        /// Positive finite maximum source duration.
        maximum_duration_ms: u64,
        /// Positive finite maximum source frame count.
        maximum_frames: u32,
    },
    /// Entire document with a finite page ceiling.
    Document {
        /// Positive finite maximum source page count.
        maximum_pages: u32,
    },
}

/// Explicit native input contract; no implicit conversion or fallback.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMediaPolicy {
    /// Common modality and processing intent.
    pub intent: NativeMediaIntent,
    /// Positive finite byte ceiling, also bounded by task content limits.
    pub maximum_bytes: u64,
    /// Positive finite work ceiling interpreted by the selected adapter.
    pub maximum_work: u64,
    /// Optional immutable registered adapter options; never bearer authority.
    pub configuration: Option<NativeConfigurationBinding>,
}

impl NativeMediaPolicy {
    /// Validates declarative bounds before any input or options are read.
    pub fn validate(
        &self, file: &FileRef, limits: crate::conversation::Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let finite = |value: u64, work: &mut Option<&mut dyn crate::store::history::HistoryIo>| -> Result<()> {
            model_history_step(work, 1)?;
            if value == 0 || value > crate::conversation::MAX_EXACT_JS_INTEGER {
                return Err(model_history_invalid(work, "native media ceiling must be positive and exactly portable"));
            }
            Ok(())
        };
        limits.validate_file_with_work(file, work)?;
        finite(self.maximum_bytes, work)?;
        finite(self.maximum_work, work)?;
        model_history_step(work, 1)?;
        if file.descriptor().byte_length() > self.maximum_bytes {
            return Err(model_history_invalid(work, "native media exceeds byte ceiling"));
        }
        model_history_step(work, 1)?;
        match &self.intent {
            NativeMediaIntent::Image { .. } => {}
            NativeMediaIntent::Audio { maximum_duration_ms } => finite(*maximum_duration_ms, work)?,
            NativeMediaIntent::Video { maximum_duration_ms, maximum_frames } => {
                finite(*maximum_duration_ms, work)?;
                model_history_step(work, 1)?;
                if *maximum_frames == 0 || *maximum_frames == u32::MAX {
                    return Err(model_history_invalid(work, "native video frame ceiling must be positive and finite"));
                }
            }
            NativeMediaIntent::Document { maximum_pages } => {
                model_history_step(work, 1)?;
                if *maximum_pages == 0 || *maximum_pages == u32::MAX {
                    return Err(model_history_invalid(work, "native document page ceiling must be positive and finite"));
                }
            }
        }
        model_history_step(work, 1)?;
        if let Some(binding) = &self.configuration {
            limits.validate_file_with_work(&binding.configuration.content, work)?;
            model_history_step(work, 1)?;
            if binding.configuration.content.descriptor().media_type() != "application/json"
                || binding.configuration.content.descriptor().byte_length() > limits.render_bytes {
                return Err(model_history_invalid(work, "native options require bounded JSON"));
            }
        }
        // Shape/bounds do not authenticate the source, configuration or code.
        Ok(())
    }
}

impl FileProjectionPolicy {
    fn validate_file(
        &self, file: &FileRef, limits: crate::conversation::Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Self::Native(policy) = self {
            return policy.validate(file, limits, work);
        }
        limits.validate_file_with_work(file, work)?;
        model_history_step(work, 1)?;
        if matches!(self, Self::BoundedFull) && file.descriptor().byte_length() > limits.render_bytes {
            Err(model_history_invalid(work, "model file exceeds render limit"))
        } else { Ok(()) }
    }

    fn append_refs<'a>(&'a self, file: &'a FileRef, files: &mut Vec<&'a FileRef>) {
        files.push(file);
        if let Self::Native(policy) = self
            && let Some(binding) = &policy.configuration
        {
            files.push(&binding.configuration.content);
        }
    }
}

impl ToolResultContent {
    /// Completed closed payload size, without allocation or cancellation.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let root = std::mem::size_of::<Self>() as u64;
        let payload = match self {
            Self::Json { value } => crate::contract::json_value_logical_owned_bytes(value)?,
            Self::Parts { parts } => {
                let mut bytes = model_owned_mul(parts.capacity(), std::mem::size_of::<ModelDataPart>())?;
                for part in parts {
                    let payload = match part {
                        ModelDataPart::Text { text } => text.capacity() as u64,
                        ModelDataPart::File { file, policy } => {
                            let file = file.logical_owned_bytes()?;
                            let policy = match policy {
                                FileProjectionPolicy::Native(policy) => model_native_policy_owned_bytes(policy)?,
                                FileProjectionPolicy::Reference | FileProjectionPolicy::BoundedFull => 0,
                            };
                            model_owned_add(file, policy)?
                        }
                    };
                    bytes = model_owned_add(bytes, payload)?;
                }
                bytes
            }
        };
        model_owned_add(root, payload)
    }

    pub(crate) fn validate_limits(
        &self, limits: crate::conversation::Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        model_history_step(work, 1)?;
        limits.validate_with_work(work)?;
        if let Self::Parts { parts } = self {
            if parts.len() > limits.attachments.saturating_add(1) {
                return Err(model_history_invalid(work, "tool result exceeds attachment limit"));
            }
            let mut references = 0usize;
            for part in parts {
                model_history_step(work, 1)?;
                match part {
                    ModelDataPart::Text { text } if text.len() as u64 > limits.render_bytes =>
                        return Err(model_history_invalid(work, "tool result text exceeds render limit")),
                    ModelDataPart::File { file, policy } => {
                        policy.validate_file(file, limits, work)?;
                        references = references.checked_add(1).ok_or_else(||
                            model_history_invalid(work, "tool result reference count overflow"))?;
                        if matches!(policy, FileProjectionPolicy::Native(policy) if policy.configuration.is_some()) {
                            references = references.checked_add(1).ok_or_else(||
                                model_history_invalid(work, "tool result reference count overflow"))?;
                        }
                    }
                    ModelDataPart::Text { .. } => {}
                }
            }
            if references > limits.attachments.saturating_add(1) {
                return Err(model_history_invalid(work, "tool result reference count exceeds limit"));
            }
        }
        model_tool_result_json_bound(self, limits.render_bytes, work)
    }
}

fn model_owned_overflow() -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "closed model payload ownership overflow",
    } }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { Error::Invalid("closed model payload ownership overflow".into()) }
}

fn model_owned_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(model_owned_overflow)
}

fn model_owned_mul(capacity: usize, slot: usize) -> Result<u64> {
    (capacity as u64).checked_mul(slot as u64).ok_or_else(model_owned_overflow)
}

fn model_native_policy_owned_bytes(policy: &NativeMediaPolicy) -> Result<u64> {
    let mut bytes = std::mem::size_of::<NativeMediaPolicy>() as u64;
    if let Some(binding) = &policy.configuration {
        bytes = model_owned_add(bytes, binding.source.authority.id.capacity() as u64)?;
        bytes = model_owned_add(bytes, binding.configuration.extension.name.capacity() as u64)?;
        bytes = model_owned_add(bytes, binding.configuration.content.logical_owned_bytes()?)?;
    }
    Ok(bytes)
}

fn model_selection_payload_bytes(model: &Model) -> Result<u64> {
    let mut bytes = crate::contract::json_value_logical_owned_bytes(&model.options)?;
    for capacity in [model.provider.capacity(), model.name.capacity(), model.revision.capacity()] {
        bytes = model_owned_add(bytes, capacity as u64)?;
    }
    Ok(bytes)
}

fn model_file_payload_bytes(file: &FileRef, policy: &FileProjectionPolicy) -> Result<u64> {
    let policy = match policy {
        FileProjectionPolicy::Native(policy) => model_native_policy_owned_bytes(policy)?,
        FileProjectionPolicy::Reference | FileProjectionPolicy::BoundedFull => 0,
    };
    model_owned_add(file.logical_owned_bytes()?, policy)
}

fn model_part_payload_bytes(part: &ModelContentPart) -> Result<u64> {
    match part {
        ModelContentPart::Text { text } => Ok(text.capacity() as u64),
        ModelContentPart::File { file, policy } => model_file_payload_bytes(file, policy),
        ModelContentPart::ToolCall { call_id, name, arguments } => model_owned_add(
            model_owned_add(call_id.capacity() as u64, name.capacity() as u64)?,
            crate::contract::json_value_logical_owned_bytes(arguments)?,
        ),
        ModelContentPart::ToolResult { call_id, name, content } => {
            let content = content.logical_owned_bytes()?.checked_sub(std::mem::size_of::<ToolResultContent>() as u64)
                .ok_or_else(model_owned_overflow)?;
            model_owned_add(model_owned_add(call_id.capacity() as u64, name.capacity() as u64)?, content)
        }
    }
}

fn model_content_payload_bytes(content: &ModelContent) -> Result<u64> {
    match content {
        ModelContent::Text(text) => Ok(text.capacity() as u64),
        ModelContent::Part(part) => model_part_payload_bytes(part),
        ModelContent::Parts(parts) => parts.iter().try_fold(
            model_owned_mul(parts.capacity(), std::mem::size_of::<ModelContentPart>())?,
            |bytes, part| model_owned_add(bytes, model_part_payload_bytes(part)?),
        ),
    }
}

fn model_messages_payload_bytes(messages: &Vec<ModelMessage>) -> Result<u64> {
    messages.iter().try_fold(
        model_owned_mul(messages.capacity(), std::mem::size_of::<ModelMessage>())?,
        |bytes, message| model_owned_add(bytes, model_content_payload_bytes(&message.content)?),
    )
}

fn model_request_payload_bytes(request: &ModelRequest) -> Result<u64> {
    let mut bytes = model_owned_add(model_selection_payload_bytes(&request.model)?,
        model_messages_payload_bytes(&request.messages)?)?;
    bytes = model_owned_add(bytes, model_owned_mul(request.tools.capacity(), std::mem::size_of::<crate::tool::ToolDefinition>())?)?;
    for tool in &request.tools {
        for capacity in [tool.name.capacity(), tool.revision.capacity(), tool.description.capacity()] {
            bytes = model_owned_add(bytes, capacity as u64)?;
        }
        for value in [&tool.input_schema, &tool.output_schema, &tool.projection_schema] {
            bytes = model_owned_add(bytes, crate::contract::json_value_logical_owned_bytes(value)?)?;
        }
    }
    Ok(bytes)
}

/// Largest byte count representable by a model request manifest.
pub const MAX_MODEL_REQUEST_BYTES: u64 = u64::MAX;

/// Complete immutable request to a model provider.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    /// Selected model value.
    pub model: Model,
    /// Ordered reconstructable context.
    pub messages: Vec<ModelMessage>,
    /// Tools visible for this request.
    pub tools: Vec<crate::tool::ToolDefinition>,
    /// Maximum output tokens when constrained.
    pub max_output_tokens: Option<u32>,
}

/// Immutable, admitted input and the exact canonical bytes supplied to a provider.
#[derive(Clone, Debug)]
pub struct PreparedModelRequest {
    request: ModelRequest,
    bytes: acyclic_stream::OwnedBytes,
    manifest: ModelRequestManifest,
}

/// Versioned identity of ordered provider input; counters bound wire bytes, not heap use.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequestManifest {
    /// Canonical construction version.
    pub version: u32,
    /// BLAKE3 digest of the exact supplied bytes.
    pub request_digest: [u8; 32],
    /// Serialized byte count.
    pub wire_bytes: u64,
    /// Exact model and ordered tool definitions, including pinned revisions and schemas.
    pub binding_digest: [u8; 32],
    /// Ordered role and content identities.
    pub message_digests: Vec<[u8; 32]>,
}

impl ModelRequest {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        model_owned_add(std::mem::size_of::<Self>() as u64, model_request_payload_bytes(self)?)
    }

    fn validate_with_io(
        &self, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        use crate::tool::ToolInvocation;
        use std::collections::BTreeMap;

        model_validate_limits_with_io(limits, io)?;
        model_step(io, 1)?;
        model_step(io, (self.model.provider.len() as u64)
            .saturating_add(self.model.name.len() as u64)
            .saturating_add(self.model.revision.len() as u64))?;
        self.model.validate()?;
        if self.messages.is_empty() || self.messages.len() > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if self.max_output_tokens == Some(0) {
            return Err(Error::Invalid(
                "model output token bound must be positive".into(),
            ));
        }
        #[cfg(feature = "filesystem")]
        let _maps = if let Some(io) = io {
            let mut calls = 0u64;
            for message in &self.messages {
                model_step(Some(io), 1)?;
                calls = calls.checked_add(message.content.parts().len() as u64)
                    .ok_or_else(|| Error::Invalid("model validation slot count overflow".into()))?;
            }
            let bytes = (self.tools.len() as u64)
                .checked_mul(std::mem::size_of::<(&String, &crate::tool::ToolDefinition)>() as u64)
                .and_then(|tools| calls.checked_mul(std::mem::size_of::<(&String, &String)>() as u64)
                    .and_then(|pending| tools.checked_add(pending)))
                .ok_or_else(|| Error::Invalid("model validation ownership overflow".into()))?;
            Some(io.reserve_decode(bytes)?)
        } else { None };
        let mut tools = BTreeMap::new();
        for tool in &self.tools {
            model_step(io, 1)?;
            tool.validate_with_io(io)?;
            if tools.insert(&tool.name, tool).is_some() {
                return Err(Error::Invalid("model tool definition is repeated".into()));
            }
        }
        let mut pending = BTreeMap::new();
        for message in &self.messages {
            model_step(io, 1)?;
            crate::executor::with_history_io(io, |work| message.content.validate_limits(limits, work))?;
            let parts = message.content.parts();
            for part in parts {
                model_step(io, 1)?;
                match part {
                    ModelContentPart::ToolCall {
                        call_id,
                        name,
                        arguments,
                    } => {
                        model_step(io, (call_id.len() as u64).saturating_add(name.len() as u64))?;
                        ToolInvocation::validate_identity(call_id, name)?;
                        if message.role != ModelRole::Assistant || pending.contains_key(call_id) {
                            return Err(Error::Invalid(
                                "model tool call role or identity is invalid".into(),
                            ));
                        }
                        let tool = tools
                            .get(name)
                            .ok_or_else(|| Error::Invalid("model tool call is unbound".into()))?;
                        crate::contract::validate_json_schema_value(
                            &tool.input_schema, arguments, "model tool arguments", io,
                        )?;
                        pending.insert(call_id, name);
                    }
                    ModelContentPart::ToolResult {
                        call_id,
                        name,
                        content,
                    } => {
                        model_step(io, (call_id.len() as u64).saturating_add(name.len() as u64))?;
                        ToolInvocation::validate_identity(call_id, name)?;
                        if message.role != ModelRole::Tool || pending.remove(call_id) != Some(name)
                        {
                            return Err(Error::Invalid(
                                "model tool result has no matching call".into(),
                            ));
                        }
                        let tool = tools
                            .get(name)
                            .ok_or_else(|| Error::Invalid("model tool result is unbound".into()))?;
                        match io {
                            #[cfg(feature = "filesystem")]
                            Some(io) => {
                            let _bytes;
                            let bytes = io.encode_json(content, io.remaining()?.bytes_encoded, false)?;
                            _bytes = io.guard_bytes(&bytes)?;
                            let envelope = io.decode_preparation::<Value>(&bytes)?;
                            crate::contract::validate_json_schema_value(
                                &tool.projection_schema, &envelope, "model tool result", Some(io),
                            )?;
                            }
                            _ => {
                            let envelope = serde_json::to_value(content)
                                .map_err(|error| Error::Invalid(error.to_string()))?;
                            crate::contract::validate_json_schema_value(
                                &tool.projection_schema, &envelope, "model tool result", None)?;
                        }
                        }
                    }
                    ModelContentPart::Text { .. } | ModelContentPart::File { .. } => {
                        if message.role == ModelRole::Tool {
                            return Err(Error::Invalid(
                                "tool role requires a paired result".into(),
                            ));
                        }
                    }
                }
            }
            if message.role == ModelRole::Tool && parts.is_empty() {
                return Err(Error::Invalid("tool role requires a paired result".into()));
            }
            if !pending.is_empty() && matches!(message.role, ModelRole::System | ModelRole::User) {
                return Err(Error::Invalid("model tool exchange is interrupted".into()));
            }
        }
        if !pending.is_empty() {
            return Err(Error::Invalid("model tool exchange is incomplete".into()));
        }
        Ok(())
    }
}

impl PreparedModelRequest {
    /// Returned graph excluding the independently guarded encoded bytes.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let bytes = model_owned_add(std::mem::size_of::<Self>() as u64,
            model_request_payload_bytes(&self.request)?)?;
        model_owned_add(bytes, model_owned_mul(self.manifest.message_digests.capacity(), std::mem::size_of::<[u8; 32]>())?)
    }

    /// Decodes exact canonical bytes on the optional preparation ledger.
    pub fn decode(
        bytes: &[u8], limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let request = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.decode_preparation::<ModelRequest>(bytes)?.into_retained(),
            _ => crate::contract::json_from_slice(bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let prepared = Self::prepare_engine(request, limits, io)?;
        model_step(io, bytes.len() as u64)?;
        if prepared.bytes() != bytes {
            return Err(Error::Invalid(
                "retained model request is not exact canonical input".into(),
            ));
        }
        Ok(prepared)
    }

    /// Admits explicit input on the optional preparation ledger.
    pub fn prepare(
        request: ModelRequest, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            #[cfg(not(target_arch = "wasm32"))]
            if io.original_native_work().is_some() {
                return Self::prepare_engine(request, limits, Some(io));
            }
            let before = io.receipt().items_examined;
            let _witness;
            let witness = io.encode_json(&request, io.remaining()?.bytes_encoded, false)?;
            _witness = io.guard_bytes(&witness)?;
            let items = io.receipt().items_examined.checked_sub(before)
                .ok_or_else(|| Error::Invalid("model request witness underflow".into()))?;
            let owned = items.checked_mul(<ModelRequest as crate::filesystem::PreparationRecord>::maximum_slot_bytes())
                .and_then(|slots| slots.checked_mul(8))
                .and_then(|slots| (witness.len() as u64).checked_mul(4)
                    .and_then(|strings| slots.checked_add(strings)))
                .and_then(|owned| owned.checked_add(std::mem::size_of::<ModelRequest>() as u64))
                .ok_or_else(|| Error::Invalid("model request ownership overflow".into()))?;
            let request_hold = io.reserve_decode(owned)?;
            let prepared = Self::prepare_engine(request, limits, Some(io))?;
            request_hold.into_retained();
            return Ok(prepared);
        }
        Self::prepare_engine(request, limits, io)
    }

    fn prepare_engine(
        request: ModelRequest, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.claim((std::mem::size_of::<Self>() - std::mem::size_of::<ModelRequest>()) as u64)?;
        }
        request.validate_with_io(limits, io)?;
        #[cfg(feature = "filesystem")]
        let bytes_hold;
        let bytes: acyclic_stream::OwnedBytes = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode_json(&request, io.remaining()?.bytes_encoded, true)?,
            _ => crate::contract::canonical_json_bytes(&request)?.into(),
        };
        #[cfg(feature = "filesystem")]
        {
            bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        }
        let request_digest = model_hash(&bytes, io)?;
        let binding_digest = model_digest(&(&request.model, &request.tools), io)?;
        #[cfg(feature = "filesystem")]
        let capacity = request.messages.len().checked_mul(std::mem::size_of::<[u8; 32]>())
            .ok_or_else(|| Error::Invalid("model message digest capacity overflow".into()))?;
        #[cfg(feature = "filesystem")]
        let mut digest_hold = if let Some(io) = io {
            io.claim(capacity as u64)?;
            let hold = io.guard_owned(capacity as u64)?;
            io.allocation_attempt(capacity as u64)?;
            Some(hold)
        } else { None };
        let mut message_digests = Vec::with_capacity(request.messages.len());
        for message in &request.messages {
            model_step(io, 1)?;
            message_digests.push(model_digest(message, io)?);
        }
        let manifest = ModelRequestManifest {
            version: 1, request_digest, wire_bytes: bytes.len() as u64,
            binding_digest, message_digests,
        };
        #[cfg(feature = "filesystem")]
        if let Some(hold) = bytes_hold { hold.into_retained(); }
        #[cfg(feature = "filesystem")]
        if let Some(hold) = digest_hold.take() { hold.into_retained(); }
        Ok(Self {
            request,
            bytes,
            manifest,
        })
    }

    /// Read-only admitted values for provider-specific encoding.
    #[must_use]
    pub fn request(&self) -> &ModelRequest {
        &self.request
    }

    /// Exact canonical provider-neutral bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Versioned identity of these bytes and ordered content.
    #[must_use]
    pub fn manifest(&self) -> &ModelRequestManifest {
        &self.manifest
    }

    /// Byte-additive token bound including every pinned immutable file.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn serialized_token_upper_bound_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(crate::context::ModelTokenCount, NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.claim(std::mem::size_of::<crate::context::ModelTokenCount>() as u64)?;
        let mut message_tokens = Vec::new();
        for message in &self.request.messages {
            model_step(Some(&io), 1)?;
            let _hold;
            let bytes = io.encode_json(message, io.remaining()?.bytes_encoded, false)?;
            _hold = io.guard_bytes(&bytes)?;
            let mut tokens = bytes.len() as u64;
            message.content.visit_file_bindings(|file, _| {
                model_step(Some(&io), 1)?;
                tokens = tokens.checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| Error::Invalid("deterministic token count overflows".into()))?;
                Ok(())
            })?;
            let tokens = u32::try_from(tokens)
                .map_err(|_| Error::Invalid("deterministic token count exceeds u32".into()))?;
            crate::executor::stock_vec_push(&mut message_tokens, tokens, Some(&io))?;
        }
        let fixed_tokens = u32::try_from(self.bytes.len())
            .map_err(|_| Error::Invalid("deterministic framing exceeds u32".into()))?;
        let count = crate::context::ModelTokenCount {
            request_digest: self.manifest.request_digest, fixed_tokens, message_tokens,
        };
        let retained = model_owned_add(std::mem::size_of::<crate::context::ModelTokenCount>() as u64,
            model_owned_mul(count.message_tokens.capacity(), std::mem::size_of::<u32>())?)?;
        let custody = io.into_native_custody(retained)?;
        Ok((count, custody))
    }
}

/// One immutable, append-only model prefix segment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPrefix {
    version: u32,
    parent: Option<FileRef>,
    binding_digest: [u8; 32],
    message_digest: [u8; 32],
    total_messages: usize,
    messages: Vec<ModelMessage>,
}

impl ModelPrefix {
    /// Media type distinguishing shared model segments from request artifacts.
    pub const MEDIA_TYPE: &'static str = "application/vnd.acyclic.harness.model-prefix+json";

    /// Selects all admitted messages, or just the exact suffix after a pinned parent.
    pub fn select(
        request: &PreparedModelRequest,
        parent: Option<(&FileRef, &PreparedModelRequest)>,
    ) -> Result<Self> {
        let (parent, offset) = if let Some((reference, prior)) = parent {
            reference.validate()?;
            if reference.descriptor().media_type() != Self::MEDIA_TYPE
                || !request.request.messages.starts_with(&prior.request.messages)
            {
                return Err(Error::Invalid("model prefix changed its parent messages".into()));
            }
            (Some(reference), prior.request.messages.len())
        } else { (None, 0) };
        let messages = request.request.messages.get(offset..)
            .ok_or_else(|| Error::Invalid("model prefix offset is invalid".into()))?;
        if messages.is_empty() {
            return Err(Error::Invalid("model prefix must advance its parent".into()));
        }
        let message_digest = ordered_message_digest(&request.manifest.message_digests)?;
        Ok(Self {
            version: 1, parent: parent.cloned(),
            binding_digest: request.manifest.binding_digest, message_digest,
            total_messages: request.request.messages.len(), messages: messages.to_vec(),
        })
    }

    /// Exact bytes staged once and shared by children through pinned references.
    pub fn canonical_bytes(&self, io: Option<&crate::executor::PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { return io.encode_json(self, io.remaining()?.bytes_encoded, true); }
        let _ = io;
        let bytes = crate::contract::canonical_json_bytes(self)?;
        Ok(bytes.into())
    }

}

fn ordered_message_digest(digests: &[[u8; 32]]) -> Result<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:model-prefix:ordered:v1");
    for digest in digests { hasher.update(digest); }
    Ok(*hasher.finalize().as_bytes())
}

impl PreparedModelRequest {
    /// Resolves the pinned parent chain on the optional preparation ledger.
    pub async fn inherit(
        mut local: ModelRequest, prefix: &FileRef,
        verifier: &dyn crate::conversation::ContentResidencyVerifier,
        limits: crate::conversation::Limits, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        model_validate_limits_with_io(limits, io)?;
        let mut next = Some(clone_file(prefix, io)?);
        let mut segments = Vec::new();
        let mut visited = std::collections::BTreeSet::new();
        let mut expected_count = None;
        let mut total_bytes = 0_u64;
        let mut total_messages = local.messages.len();
        while let Some(reference) = next {
            model_step(io, 1)?;
            model_validate_file_shape_with_io(&reference, io)?;
            let identity = model_digest(&reference, io)?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                // Concrete key slots only; opaque BTree allocator nodes excluded.
                io.reserve_decode(std::mem::size_of::<[u8; 32]>() as u64)?.into_retained();
            }
            if reference.descriptor().media_type() != ModelPrefix::MEDIA_TYPE
                || !visited.insert(identity)
            {
                return Err(Error::Invalid(
                    "model prefix identity or cycle is invalid".into(),
                ));
            }
            total_bytes = total_bytes
                .checked_add(reference.descriptor().byte_length())
                .ok_or_else(|| Error::Invalid("model prefix byte count overflow".into()))?;
            if visited.len() > limits.context_messages {
                return Err(Error::Invalid(
                    "model prefix traversal exceeds limit".into(),
                ));
            }
            #[cfg(feature = "filesystem")]
            let _bytes_hold;
            let bytes = match io {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Some(io) if io.original_native_work().is_some() => {
                    let work = io.original_native_work().expect("native branch retains original work");
                    verifier.verify_with_native_work(&reference, reference.descriptor().byte_length(), work)?.await?;
                    let bytes = verifier.read_with_native_work(
                        &reference, reference.descriptor().byte_length(), work,
                    )?.await?;
                    bytes
                }
                #[cfg(feature = "filesystem")]
                Some(io) => {
                    io.receive_content(verifier.verify_measured(&reference,
                        reference.descriptor().byte_length(), io.remaining()?, &io.cancellation).await)?;
                    io.receive_content(verifier.read_measured(&reference,
                        reference.descriptor().byte_length(), io.remaining()?, &io.cancellation).await)?
                }
                _ => {
                    verifier.verify(&reference).await?;
                    verifier.read(&reference).await?
                }
            };
            #[cfg(feature = "filesystem")]
            {
                _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            }
            model_verify_descriptor(&reference, &bytes, io)?;
            let segment: ModelPrefix = match io {
                #[cfg(feature = "filesystem")]
                Some(io) => io.decode_preparation::<ModelPrefix>(&bytes)?.into_retained(),
                _ => crate::contract::json_from_slice(&bytes)
                    .map_err(|error| Error::Invalid(format!("invalid model prefix: {error}")))?,
            };
            #[cfg(feature = "filesystem")]
            let _canonical_hold;
            let canonical = segment.canonical_bytes(io)?;
            #[cfg(feature = "filesystem")]
            {
                _canonical_hold = io.map(|io| io.guard_bytes(&canonical)).transpose()?.flatten();
            }
            model_step(io, bytes.len() as u64)?;
            if segment.version != 1
                || segment.messages.is_empty()
                || expected_count.is_some_and(|count| count != segment.total_messages)
                || canonical.as_slice() != bytes.as_ref()
            {
                return Err(Error::Invalid(
                    "model prefix contract differs from request".into(),
                ));
            }
            let remaining = segment
                .total_messages
                .checked_sub(segment.messages.len())
                .ok_or_else(|| Error::Invalid("model prefix message count is invalid".into()))?;
            if (segment.parent.is_none()) != (remaining == 0) {
                return Err(Error::Invalid(
                    "model prefix parent count is invalid".into(),
                ));
            }
            total_messages = total_messages
                .checked_add(segment.messages.len())
                .ok_or_else(|| Error::Invalid("model prefix message count overflow".into()))?;
            if total_messages > limits.context_messages {
                return Err(Error::Invalid(
                    "inherited model context exceeds message limit".into(),
                ));
            }
            expected_count = Some(remaining);
            next = segment.parent;
            crate::executor::stock_vec_push(&mut segments, (segment.messages, segment.message_digest), io)?;
        }
        let mut messages = Vec::new();
        let mut message_hasher = blake3::Hasher::new();
        model_hasher_update(&mut message_hasher, b"harness:model-prefix:ordered:v1", io)?;
        for (segment, digest) in segments.into_iter().rev() {
            for message in &segment {
                let digest = model_digest(message, io)?;
                model_hasher_update(&mut message_hasher, &digest, io)?;
            }
            for message in segment { crate::executor::stock_vec_push(&mut messages, message, io)?; }
            if message_hasher.finalize().as_bytes() != &digest {
                return Err(Error::Invalid(
                    "model prefix ordered messages differ".into(),
                ));
            }
        }
        let local_messages = std::mem::replace(&mut local.messages, messages);
        local.validate_with_io(limits, io)?;
        for message in local_messages { crate::executor::stock_vec_push(&mut local.messages, message, io)?; }
        for message in &local.messages {
            model_verify_content_files(&message.content, verifier, io).await?;
        }
        Self::prepare_engine(local, limits, io)
    }
}

fn model_hasher_update(
    hasher: &mut blake3::Hasher, bytes: &[u8], io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    let work = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.preflight(work)?; }
    hasher.update(bytes);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.charge(work)?; }
    Ok(())
}

fn model_verify_descriptor(
    file: &FileRef, bytes: &[u8], io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
        return file.descriptor().verify_with_native_work(bytes, work);
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    let work = acyclic_fs::WorkCounters {
        bytes_hashed: if bytes.len() as u64 == file.descriptor().byte_length() { bytes.len() as u64 } else { 0 },
        ..Default::default()
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.preflight(work)?; }
    let result = file.descriptor().verify(bytes);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.charge(work)?; }
    result
}

async fn model_verify_content_files(
    content: &ModelContent, verifier: &dyn crate::conversation::ContentResidencyVerifier,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let mut files = Vec::new();
    content.visit_file_bindings(|file, _| crate::executor::stock_vec_push(&mut files, file, io))?;
    for file in files {
        model_step(io, 1)?;
        match io {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(io) if io.original_native_work().is_some() => {
                verifier.verify_with_native_work(file, file.descriptor().byte_length(),
                    io.original_native_work().expect("native branch retains original work"))?.await?;
            }
            #[cfg(feature = "filesystem")]
            Some(io) => {
                io.receive_content(verifier.verify_measured(file, file.descriptor().byte_length(),
                    io.remaining()?, &io.cancellation).await)?;
            }
            _ => verifier.verify(file).await?,
        }
    }
    Ok(())
}

/// Ordered event emitted by a model run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelEvent {
    /// User-visible content fragment.
    Content {
        /// Incremental content.
        delta: String,
    },
    /// Model reasoning fragment when the provider exposes one.
    Reasoning {
        /// Incremental reasoning content.
        delta: String,
    },
    /// Complete request to invoke one registered tool.
    ToolCall {
        /// Provider/model-owned call identity.
        #[serde(alias = "callId")]
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Schema-defined arguments.
        arguments: Value,
    },
    /// Model completed this step.
    Completed {
        /// Provider-owned usage and finish metadata.
        metadata: Value,
    },
}

/// Executor-owned identity supplied separately from canonical model-visible bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDispatch {
    /// Admitted execution operation identity.
    pub operation_id: OperationId,
    /// Zero-based logical executor step.
    pub step: u32,
    /// Digest of the exact canonical request supplied with this dispatch.
    pub request_digest: [u8; 32],
}

/// Durable identity and observed prefix of one interrupted model attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelAttempt {
    /// Owning turn execution.
    pub operation_id: OperationId,
    /// Zero-based stock-executor step.
    pub step: u32,
    /// Canonical digest of the complete model request.
    pub request_digest: [u8; 32],
    /// Durable event prefix already observed.
    pub observed: Vec<ModelEvent>,
}

impl ModelAttempt {
    /// Returns the same identity supplied when the attempt was first dispatched.
    #[must_use]
    pub const fn dispatch(&self) -> ModelDispatch {
        ModelDispatch {
            operation_id: self.operation_id,
            step: self.step,
            request_digest: self.request_digest,
        }
    }
}

/// Borrowed durable attempt: native reconciliation never clones its prefix.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug)]
pub struct NativeModelAttempt<'a> {
    /// Owning execution.
    pub operation_id: OperationId,
    /// Zero-based logical step.
    pub step: u32,
    /// Exact dispatched request digest.
    pub request_digest: [u8; 32],
    /// Already retained durable events.
    pub observed: &'a [ModelEvent],
}

/// An actual concrete stream box admitted before invoking its builder.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct NativeModelStream<'a> {
    stream: BoxStream<'a, Result<(ModelEvent, NativeAllocation)>>,
    custody: NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl<'a> NativeModelStream<'a> {
    /// Funds the concrete frame before building or boxing it.
    pub fn new<S, B>(work: &NativeMaterializationWork, build: B) -> Result<Self>
    where
        S: futures::Stream<Item = Result<(ModelEvent, NativeAllocation)>> + Send + 'a,
        B: FnOnce() -> S,
    {
        work.remaining()?;
        let bytes = std::mem::size_of::<S>() as u64;
        let custody = if bytes == 0 { work.retain_owned(0)? } else { work.claim(bytes)? };
        let stream = Box::pin(build());
        Ok(Self { stream, custody })
    }

    /// The original ledger associated with this stream allocation.
    #[must_use]
    pub fn work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl futures::Stream for NativeModelStream<'_> {
    type Item = Result<(ModelEvent, NativeAllocation)>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Err(cause) = this.custody.work().remaining() {
            return std::task::Poll::Ready(Some(Err(cause.into())));
        }
        this.stream.as_mut().poll_next(cx)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_model_unsupported() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unsupported,
        reason: "model provider has no original native-ledger producer",
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ModelEvent {
    /// Actual closed event headers and out-of-line known storage.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let payload = match self {
            Self::Content { delta } | Self::Reasoning { delta } => delta.capacity() as u64,
            Self::ToolCall { call_id, name, arguments } => model_owned_add(
                model_owned_add(call_id.capacity() as u64, name.capacity() as u64)?,
                crate::contract::json_value_logical_owned_bytes(arguments)?,
            )?,
            Self::Completed { metadata } => crate::contract::json_value_logical_owned_bytes(metadata)?,
        };
        model_owned_add(std::mem::size_of::<Self>() as u64, payload)
    }

    /// Produces a paid closed SDK event directly from borrowed provider bytes.
    pub fn decode_with_native_work(bytes: &[u8], work: &NativeMaterializationWork) -> Result<(Self, NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let event = io.decode_preparation::<Self>(bytes)?.into_retained();
        let retained = event.logical_owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((event, custody))
    }

}

/// Replaceable streaming model provider.
pub trait ModelProvider: acyclic_stream::ProviderPlatform {
    /// Pure capability query; custom ordinary providers do not opt in implicitly.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_model(&self, _work: &NativeMaterializationWork) -> Result<bool> {
        _work.remaining()?;
        Ok(false)
    }

    /// Pure capacities for the exact selection, without an ordinary callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn context_capacity_with_native_work(
        &self, _model: &Model, _work: &NativeMaterializationWork,
    ) -> Result<crate::context::ModelContextCapacity> {
        _work.remaining()?;
        Err(native_model_unsupported())
    }

    /// Paid deterministic token DTO for this exact admitted request.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn count_tokens_with_native_work(
        &self, _request: &PreparedModelRequest, _work: &NativeMaterializationWork,
    ) -> Result<(crate::context::ModelTokenCount, NativeAllocation)> {
        _work.remaining()?;
        Err(native_model_unsupported())
    }

    /// Starts only an original-ledger stream of already paid SDK events.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn generate_with_native_work<'a>(
        &'a self, _request: &'a PreparedModelRequest, _dispatch: ModelDispatch,
        _work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeModelStream<'a>>> {
        _work.remaining()?;
        Err(native_model_unsupported())
    }

    /// Reconciles a borrowed durable prefix without cloning or ordinary fallback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self, _attempt: NativeModelAttempt<'a>, _work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Vec<ModelEvent>>, NativeAllocation)>> {
        _work.remaining()?;
        Err(native_model_unsupported())
    }

    /// Advertises actual capacities of the exact immutable selection without effects.
    fn context_capacity(&self, _model: &Model) -> Result<crate::context::ModelContextCapacity> {
        Err(Error::Unsupported(
            "selected model context capacity is unavailable".into(),
        ))
    }

    /// Returns deterministic additive token upper bounds for this exact request.
    fn count_tokens(
        &self,
        _request: &PreparedModelRequest,
    ) -> Result<crate::context::ModelTokenCount> {
        Err(Error::Unsupported(
            "selected model token counting is unavailable".into(),
        ))
    }

    /// Starts one request and yields ordered model events.
    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxStream<'a, Result<ModelEvent>>;

    /// Continues or reconciles an interrupted run without starting another model request.
    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::Limits;
    use serde_json::json;

    #[test]
    fn native_result_traversal_preserves_media_options_and_nonrecursive_shape() -> Result<()> {
        let mut reader = PrefixReader::default();
        let media = store_prefix_bytes(&mut reader, vec![1, 2, 3], "image/png", 90)?;
        let options = store_prefix_bytes(
            &mut reader,
            b"{\"detail\":\"high\"}".to_vec(),
            "application/json",
            91,
        )?;
        let binding = NativeConfigurationBinding {
            source: crate::core::EventReference {
                authority: crate::core::Authority {
                    kind: crate::core::AggregateKind::Agent,
                    id: "native-owner".into(),
                },
                revision: 1,
            },
            configuration: crate::core::ExtensionConfiguration {
                extension: crate::core::ExtensionDependency {
                    name: "native.options".into(),
                    version: 1,
                },
                schema_digest: [7; 32],
                content: options.clone(),
            },
            implementation_digest: [8; 32],
        };
        let policy = NativeMediaPolicy {
            intent: NativeMediaIntent::Image {
                detail: ImageDetail::High,
            },
            maximum_bytes: 64,
            maximum_work: 64,
            configuration: Some(binding.clone()),
        };
        let file = ModelContentPart::File {
            file: media.clone(),
            policy: FileProjectionPolicy::Native(Box::new(policy.clone())),
        };
        let result = ModelContentPart::ToolResult {
            call_id: "native-call".into(),
            name: "native.tool".into(),
            content: ToolResultContent::Parts {
                parts: vec![
                    ModelDataPart::Text {
                        text: "before".into(),
                    },
                    ModelDataPart::File {
                        file: media.clone(),
                        policy: FileProjectionPolicy::Native(Box::new(policy)),
                    },
                    ModelDataPart::Text {
                        text: "after".into(),
                    },
                ],
            },
        };
        for content in [
            ModelContent::Part(file.clone()),
            ModelContent::Parts(vec![file]),
            ModelContent::Part(result.clone()),
            ModelContent::Parts(vec![result]),
        ] {
            content.validate_limits(Limits::default(), &mut None)?;
            assert!(
                content
                    .validate_limits(Limits {
                        attachments: 0,
                        ..Limits::default()
                    }, &mut None)
                    .is_err()
            );
            assert!(content.contains_native_media());
            assert_eq!(content.file_refs(), vec![&media, &options]);
            assert_eq!(content.native_configurations(), vec![&binding]);
            let wire = serde_json::to_value(&content)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let decoded: ModelContent =
                serde_json::from_value(wire).map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(decoded, content);
        }
        for content in [
            ModelContent::Part(ModelContentPart::File {
                file: options.clone(),
                policy: FileProjectionPolicy::Reference,
            }),
            ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "json-call".into(),
                name: "native.tool".into(),
                content: ToolResultContent::Json {
                    value: json!({"file":options}),
                },
            }),
        ] {
            assert!(!content.contains_native_media());
            assert!(content.native_configurations().is_empty());
        }
        assert!(
            serde_json::from_value::<ModelDataPart>(
                json!({"kind":"tool_call","call_id":"nested","name":"native.tool","arguments":{}})
            )
            .is_err()
        );
        assert!(serde_json::from_value::<ToolResultContent>(json!("old bare value")).is_err());
        assert!(serde_json::from_value::<FileProjectionPolicy>(json!("native")).is_err());
        Ok(())
    }

    #[test]
    fn native_intents_reject_nonfinite_bounds_and_bind_exact_request_bytes() -> Result<()> {
        let mut reader = PrefixReader::default();
        let media = store_prefix_bytes(&mut reader, vec![1, 2, 3], "image/png", 92)?;
        for intent in [
            NativeMediaIntent::Image {
                detail: ImageDetail::Low,
            },
            NativeMediaIntent::Audio {
                maximum_duration_ms: 1000,
            },
            NativeMediaIntent::Video {
                maximum_duration_ms: 1000,
                maximum_frames: 24,
            },
            NativeMediaIntent::Document { maximum_pages: 2 },
        ] {
            let valid = NativeMediaPolicy {
                intent,
                maximum_bytes: 64,
                maximum_work: 64,
                configuration: None,
            };
            valid.validate(&media, Limits::default(), &mut None)?;
            for value in [0, crate::conversation::MAX_EXACT_JS_INTEGER + 1, u64::MAX] {
                let mut invalid = valid.clone();
                invalid.maximum_bytes = value;
                assert!(invalid.validate(&media, Limits::default(), &mut None).is_err());
                let mut invalid = valid.clone();
                invalid.maximum_work = value;
                assert!(invalid.validate(&media, Limits::default(), &mut None).is_err());
            }
            let mut short = valid;
            short.maximum_bytes = 2;
            assert!(short.validate(&media, Limits::default(), &mut None).is_err());
        }
        for intent in [
            NativeMediaIntent::Audio {
                maximum_duration_ms: 0,
            },
            NativeMediaIntent::Audio {
                maximum_duration_ms: crate::conversation::MAX_EXACT_JS_INTEGER + 1,
            },
            NativeMediaIntent::Video {
                maximum_duration_ms: u64::MAX,
                maximum_frames: 24,
            },
            NativeMediaIntent::Video {
                maximum_duration_ms: 1000,
                maximum_frames: 0,
            },
            NativeMediaIntent::Document {
                maximum_pages: u32::MAX,
            },
        ] {
            let invalid = NativeMediaPolicy {
                intent,
                maximum_bytes: 64,
                maximum_work: 64,
                configuration: None,
            };
            assert!(invalid.validate(&media, Limits::default(), &mut None).is_err());
        }
        let mut original = request()?;
        original.messages[0].content = ModelContent::Part(ModelContentPart::File {
            file: media,
            policy: FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
                intent: NativeMediaIntent::Image {
                    detail: ImageDetail::Low,
                },
                maximum_bytes: 64,
                maximum_work: 64,
                configuration: None,
            })),
        });
        let prepared = PreparedModelRequest::prepare(original.clone(), Limits::default(), None)?;
        if let ModelContent::Part(ModelContentPart::File {
            policy: FileProjectionPolicy::Native(policy),
            ..
        }) = &mut original.messages[0].content
        {
            policy.intent = NativeMediaIntent::Image {
                detail: ImageDetail::High,
            };
        }
        let changed = PreparedModelRequest::prepare(original, Limits::default(), None)?;
        assert_ne!(
            prepared.manifest().request_digest,
            changed.manifest().request_digest
        );
        Ok(())
    }

    fn request() -> Result<ModelRequest> {
        Ok(ModelRequest {
            model: Model::new("mock", "exact", "pinned", json!({}))?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("é\0🦀\r\n".into()),
            }],
            tools: vec![crate::tool::ToolDefinition {
                name: "echo".into(),
                revision: "schema-2".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"string"}),
                output_schema: json!({"type":"string"}),
                projection_schema: json!({"type":"object","properties":{"kind":{"const":"json"},"value":{"type":"string"}},"required":["kind","value"],"additionalProperties":false}),
            }],
            max_output_tokens: Some(32),
        })
    }

    fn exchange(request: &mut ModelRequest) {
        request.messages.extend([
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "call-1".into(),
                    name: "echo".into(),
                    arguments: json!("é"),
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call-1".into(),
                    name: "echo".into(),
                    content: ToolResultContent::Json { value: json!("é") },
                }),
            },
        ]);
    }

    #[derive(Clone, Default)]
    struct PrefixReader(
        std::collections::BTreeMap<String, acyclic_stream::OwnedBytes>,
        std::collections::BTreeSet<String>,
    );

    impl crate::conversation::ContentResidencyVerifier for PrefixReader {
        fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let capability = file.read_capability()?;
                if !self.1.contains(&capability) {
                    return Err(Error::Unauthorized("prefix read is not granted".into()));
                }
                let bytes = self
                    .0
                    .get(&capability)
                    .ok_or_else(|| Error::NotFound("prefix version is missing".into()))?;
                file.descriptor().verify(bytes)
            })
        }
        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
            Box::pin(async move {
                self.0
                    .get(&file.read_capability()?)
                    .cloned()
                    .ok_or_else(|| Error::NotFound("prefix version is missing".into()))
            })
        }
    }

    fn store_prefix(reader: &mut PrefixReader, prefix: &ModelPrefix, index: u8) -> Result<FileRef> {
        store_prefix_bytes(
            reader,
            prefix.canonical_bytes(None)?,
            ModelPrefix::MEDIA_TYPE,
            index,
        )
    }

    fn store_prefix_bytes(
        reader: &mut PrefixReader,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        media_type: &str,
        index: u8,
    ) -> Result<FileRef> {
        let bytes = bytes.into();
        use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
        let file = FileRef::new(
            VolumeRef::new(
                crate::resources::ProviderRef::new("test", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
            )?,
            format!(".system/inherited-conversation/model-prefix/{index}.json"),
            format!("generation-{index}"),
            FileDescriptor::from_bytes(&bytes, media_type)?,
            "model-prefix.json",
        )?;
        reader.1.insert(file.read_capability()?);
        reader.0.insert(file.read_capability()?, bytes);
        Ok(file)
    }

    #[tokio::test]
    async fn shared_prefix_depth_three_reopens_and_rejects_mutation_or_missing_grants() -> Result<()>
    {
        let limits = Limits::default();
        let mut reader = PrefixReader::default();
        let attachment = store_prefix_bytes(
            &mut reader,
            "attachment é".as_bytes().to_vec(),
            "text/plain",
            77,
        )?;
        let mut root = request()?;
        root.messages[0].content = ModelContent::Parts(vec![
            ModelContentPart::Text {
                text: "é\0🦀\r\n".into(),
            },
            ModelContentPart::File {
                file: attachment.clone(),
                policy: FileProjectionPolicy::Reference,
            },
        ]);
        exchange(&mut root);
        let mut prepared = PreparedModelRequest::prepare(root, limits, None)?;
        let mut head = store_prefix(&mut reader, &ModelPrefix::select(&prepared, None)?, 0)?;
        for depth in 1..=3 {
            let mut next = prepared.request().clone();
            next.messages.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!(
                    "child {depth}: é\0🦀; parent scratch retained"
                )),
            });
            let next = PreparedModelRequest::prepare(next, limits, None)?;
            let segment = ModelPrefix::select(&next, Some((&head, &prepared)))?;
            assert_eq!(segment.messages.len(), 1);
            assert_eq!(segment.parent.as_ref(), Some(&head));
            head = store_prefix(&mut reader, &segment, depth)?;
            prepared = next;
        }
        let reopened = reader.clone();
        for child in ["a", "b"] {
            let mut local = request()?;
            local.messages = vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!(
                    "notification; task {child}; identity; workspace; fresh scratch"
                )),
            }];
            let inherited =
                PreparedModelRequest::inherit(local.clone(), &head, &reopened, limits, None).await?;
            let mut expected = prepared.request().clone();
            expected.messages.extend(local.messages.clone());
            assert_eq!(
                inherited.bytes(),
                PreparedModelRequest::prepare(expected, limits, None)?.bytes()
            );
            let mut changed = local.clone();
            changed.model.revision = "changed".into();
            let changed_request =
                PreparedModelRequest::inherit(changed, &head, &reader, limits, None).await?;
            assert_eq!(changed_request.request().model.revision, "changed");
            assert_eq!(
                changed_request.request().messages,
                inherited.request().messages
            );
            assert_ne!(
                changed_request.manifest().binding_digest,
                inherited.manifest().binding_digest
            );
            // Inherited tool exchanges must still validate against the new schema set.
            let mut incompatible = local.clone();
            incompatible.tools.clear();
            assert!(
                PreparedModelRequest::inherit(incompatible, &head, &reader, limits, None)
                    .await
                    .is_err()
            );
            assert!(
                PreparedModelRequest::inherit(
                    local.clone(),
                    &head,
                    &PrefixReader::default(),
                    limits,
                    None,
                )
                .await
                .is_err()
            );
            let mut denied = reader.clone();
            denied.1.remove(&head.read_capability()?);
            assert!(matches!(
                PreparedModelRequest::inherit(local.clone(), &head, &denied, limits, None).await,
                Err(Error::Unauthorized(_))
            ));
            let mut missing_attachment = reader.clone();
            missing_attachment.0.remove(&attachment.read_capability()?);
            assert!(
                PreparedModelRequest::inherit(local.clone(), &head, &missing_attachment, limits, None)
                    .await
                    .is_err()
            );
            let lower_limits = Limits {
                context_messages: prepared.request().messages.len(),
                ..limits
            };
            assert!(
                PreparedModelRequest::inherit(local.clone(), &head, &reader, lower_limits, None)
                    .await
                    .is_err()
            );
            let mut corrupt = reader.clone();
            corrupt.0.insert(head.read_capability()?, b"{}".to_vec().into());
            assert!(
                PreparedModelRequest::inherit(local, &head, &corrupt, limits, None)
                    .await
                    .is_err()
            );
        }
        let mut changed_parent = prepared.request().clone();
        changed_parent.messages[0].content = ModelContent::Text("later parent mutation".into());
        let changed_parent = PreparedModelRequest::prepare(changed_parent, limits, None)?;
        assert!(ModelPrefix::select(&changed_parent, Some((&head, &prepared))).is_err());
        // An authenticated, canonical parent with the same shape and binding still must contain the exact messages claimed by the child segment.
        let impostor = store_prefix(
            &mut reader,
            &ModelPrefix::select(&changed_parent, None)?,
            88,
        )?;
        let mut descendant = prepared.request().clone();
        descendant.messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("next".into()),
        });
        let descendant = PreparedModelRequest::prepare(descendant, limits, None)?;
        let mislabeled = ModelPrefix::select(&descendant, Some((&impostor, &prepared)))?;
        let mislabeled = store_prefix(&mut reader, &mislabeled, 89)?;
        assert!(
            matches!(PreparedModelRequest::inherit(request()?, &mislabeled, &reader, limits, None).await,
            Err(Error::Invalid(message)) if message == "model prefix ordered messages differ")
        );
        assert_eq!(reopened.0.len() + 2, reader.0.len());
        Ok(())
    }

    #[test]
    fn complete_tool_projection_envelopes_enforce_the_exact_render_bound() -> Result<()> {
        for content in [
            ToolResultContent::Json {
                value: json!("é\0🦀"),
            },
            ToolResultContent::Parts {
                parts: vec![ModelDataPart::Text {
                    text: "é\0🦀".into(),
                }],
            },
        ] {
            let exact = crate::contract::canonical_json_bytes(&content)?.len() as u64;
            content.validate_limits(Limits {
                render_bytes: exact,
                ..Limits::default()
            }, &mut None)?;
            assert!(matches!(content.validate_limits(Limits {
                render_bytes: exact - 1, ..Limits::default()
            }, &mut None), Err(Error::Invalid(message)) if message.contains("render limit")));
        }
        Ok(())
    }

    #[test]
    fn malformed_exchanges_and_bounds_fail_closed() -> Result<()> {
        let base = request()?;
        let mut valid = base.clone();
        exchange(&mut valid);
        PreparedModelRequest::prepare(valid.clone(), Limits::default(), None)?;
        let mut candidates = Vec::new();
        let mut changed = valid.clone();
        changed.messages.pop();
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.messages.swap(1, 2);
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.messages[1].role = ModelRole::User;
        candidates.push(changed);
        let mut changed = valid.clone();
        if let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) =
            &mut changed.messages[2].content
        {
            *content = ToolResultContent::Json { value: json!(42) };
        }
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.tools.push(changed.tools[0].clone());
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.max_output_tokens = None;
        let unconstrained = PreparedModelRequest::prepare(changed, Limits::default(), None)?;
        assert!(unconstrained.request().max_output_tokens.is_none());
        assert_eq!(
            PreparedModelRequest::decode(unconstrained.bytes(), Limits::default(), None)?
                .request()
                .max_output_tokens,
            None
        );
        let mut changed = valid.clone();
        changed.max_output_tokens = Some(0);
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.model.revision.clear();
        candidates.push(changed);
        let mut changed = valid.clone();
        if let ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) =
            &mut changed.messages[1].content
        {
            *arguments = json!(false);
        }
        candidates.push(changed);
        for candidate in candidates {
            assert!(PreparedModelRequest::prepare(candidate, Limits::default(), None).is_err());
        }
        let mut oversized = base;
        let maximum = 64 * 1024 * 1024 + 1;
        oversized.model.options = json!({"large": "x".repeat(maximum)});
        assert!(PreparedModelRequest::prepare(oversized, Limits::default(), None).is_ok());
        Ok(())
    }
}
