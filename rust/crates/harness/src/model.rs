//! Provider-neutral immutable model values and streaming host contract.
//!
//! Measured request admission preserves these canonical bytes and validators.
//! Its receipt bounds cumulative SDK JSON traversal, encoding, hashing, explicit
//! copies/allocations, and conservative live logical DTO reservations. JSON
//! Schema library CPU/graph allocations and allocator nodes/RSS are excluded.

use crate::{Error, OperationId, Result, conversation::FileRef};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::BoxProviderStream as BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;


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

fn model_json_bound<T: Serialize>(
    value: &T, maximum: u64, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let _bytes;
        let bytes = io.encode_json(value, maximum, false)?;
        _bytes = io.guard_owned(bytes.len() as u64)?;
        return Ok(());
    }
    crate::contract::validate_json_byte_bound(value, maximum)
        .map_err(|_| Error::Invalid("model tool projection exceeds render limit".into()))
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
        _bytes = io.guard_owned(bytes.len() as u64)?;
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
    /// Ordered structured parts; plain text has no structured parts.
    #[must_use]
    pub fn parts(&self) -> &[ModelContentPart] {
        match self {
            Self::Text(_) => &[],
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) => parts,
        }
    }

    /// Applies the same byte and part ceilings to direct inputs and selected
    /// history before either can reach a provider or durable request journal.
    pub fn validate_limits(&self, limits: crate::conversation::Limits) -> Result<()> {
        self.validate_limits_with_io(limits, None)
    }

    pub(crate) fn validate_limits_with_io(
        &self, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        model_step(io, 1)?;
        limits.validate()?;
        let parts = match self {
            Self::Text(text) => {
                return if text.len() as u64 <= limits.render_bytes {
                    Ok(())
                } else {
                    Err(Error::Invalid("model text exceeds render limit".into()))
                };
            }
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) => parts.as_slice(),
        };
        if parts.len() > limits.attachments.saturating_add(1) {
            return Err(Error::Invalid(
                "model content exceeds attachment limit".into(),
            ));
        }
        for part in parts {
            model_step(io, 1)?;
            match part {
                ModelContentPart::Text { text } if text.len() as u64 > limits.render_bytes => {
                    return Err(Error::Invalid("model text exceeds render limit".into()));
                }
                ModelContentPart::File { file, policy } => {
                    policy.validate_file_with_io(file, limits, io)?;
                }
                ModelContentPart::ToolCall {
                    name, arguments, ..
                } => {
                    model_step(io, name.len() as u64)?;
                    crate::registry::validate_component_label(name, "tool name")?;
                    model_json_bound(arguments, limits.render_bytes, io)?;
                }
                ModelContentPart::ToolResult { name, content, .. } => {
                    model_step(io, name.len() as u64)?;
                    crate::registry::validate_component_label(name, "tool name")?;
                    content.validate_limits_with_io(limits, io)?;
                }
                ModelContentPart::Text { .. } => {}
            }
        }
        let mut references = 0usize;
        self.visit_file_bindings(|_, _| {
            model_step(io, 1)?;
            references = references.checked_add(1)
                .ok_or_else(|| Error::Invalid("model content reference count overflow".into()))?;
            Ok(())
        })?;
        if references > limits.attachments.saturating_add(1) {
            return Err(Error::Invalid(
                "model content reference count exceeds attachment limit".into(),
            ));
        }
        Ok(())
    }

    /// Rejects tool and empty content in a human turn input.
    pub fn validate_user_input(&self) -> Result<()> {
        let parts = match self {
            Self::Text(text) => {
                return if text.is_empty() {
                    Err(Error::Invalid("user input is empty".into()))
                } else {
                    Ok(())
                };
            }
            Self::Part(part) => std::slice::from_ref(part),
            Self::Parts(parts) if !parts.is_empty() => parts.as_slice(),
            _ => return Err(Error::Invalid("user input part count is invalid".into())),
        };
        for part in parts {
            match part {
                ModelContentPart::Text { text } if !text.is_empty() => {}
                ModelContentPart::File { file, .. } => file.validate()?,
                _ => {
                    return Err(Error::Invalid(
                        "user input contains an unsupported part".into(),
                    ));
                }
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
    /// Option JSON files alone never make a message native media.
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
    /// File authority, effective bounds and authenticated bytes remain caller-owned.
    pub(crate) fn verify_original<'a>(
        &self,
        admission: Option<&crate::core::ExtensionAdmission>,
        schemas: Option<&'a crate::core::SchemaRegistry>,
        runtime: Option<&crate::extension::ExtensionRuntime>,
    ) -> Result<(&'a crate::core::SchemaRegistry, [u8; 32])> {
        self.verify_original_engine(admission, schemas, runtime, None)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_original_with_io<'a>(
        &self,
        admission: Option<&crate::core::ExtensionAdmission>,
        schemas: Option<&'a crate::core::SchemaRegistry>,
        runtime: Option<&crate::extension::ExtensionRuntime>,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<(&'a crate::core::SchemaRegistry, [u8; 32])> {
        self.verify_original_engine(admission, schemas, runtime, Some(io))
    }

    fn verify_original_engine<'a>(
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
            _bytes = io.guard_owned(bytes.len() as u64)?;
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
    pub fn validate(&self, file: &FileRef, limits: crate::conversation::Limits) -> Result<()> {
        fn finite(value: u64) -> Result<()> {
            if value == 0 || value > crate::conversation::MAX_EXACT_JS_INTEGER {
                return Err(Error::Invalid(
                    "native media ceiling must be positive and exactly portable".into(),
                ));
            }
            Ok(())
        }
        limits.validate_file(file)?;
        finite(self.maximum_bytes)?;
        finite(self.maximum_work)?;
        if file.descriptor().byte_length() > self.maximum_bytes {
            return Err(Error::Invalid("native media exceeds byte ceiling".into()));
        }
        match &self.intent {
            NativeMediaIntent::Image { .. } => {}
            NativeMediaIntent::Audio {
                maximum_duration_ms,
            } => finite(*maximum_duration_ms)?,
            NativeMediaIntent::Video {
                maximum_duration_ms,
                maximum_frames,
            } => {
                finite(*maximum_duration_ms)?;
                if *maximum_frames == 0 || *maximum_frames == u32::MAX {
                    return Err(Error::Invalid(
                        "native video frame ceiling must be positive and finite".into(),
                    ));
                }
            }
            NativeMediaIntent::Document { maximum_pages } => {
                if *maximum_pages == 0 || *maximum_pages == u32::MAX {
                    return Err(Error::Invalid(
                        "native document page ceiling must be positive and finite".into(),
                    ));
                }
            }
        }
        if let Some(binding) = &self.configuration {
            limits.validate_file(&binding.configuration.content)?;
            if binding.configuration.content.descriptor().media_type() != "application/json"
                || binding.configuration.content.descriptor().byte_length() > limits.render_bytes
            {
                return Err(Error::Invalid("native options require bounded JSON".into()));
            }
        }
        // Shape/bounds do not authenticate the source, configuration or code.
        Ok(())
    }
}

impl FileProjectionPolicy {
    fn validate_file(&self, file: &FileRef, limits: crate::conversation::Limits) -> Result<()> {
        if let Self::Native(policy) = self {
            return policy.validate(file, limits);
        }
        limits.validate_file(file)?;
        match self {
            Self::BoundedFull if file.descriptor().byte_length() > limits.render_bytes => {
                Err(Error::Invalid("model file exceeds render limit".into()))
            }
            _ => Ok(()),
        }
    }

    fn validate_file_with_io(
        &self, file: &FileRef, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        // Visit all metadata/native option fields before the same declarative
        // checks; those checks borrow and do not clone the input.
        #[cfg(not(feature = "filesystem"))]
        let _ = io;
        #[cfg(feature = "filesystem")]
        let _hold;
        #[cfg(feature = "filesystem")]
        let _metadata;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            _metadata = io.encode_json(&(file, self), io.remaining()?.bytes_encoded, false)?;
            _hold = io.guard_owned(_metadata.len() as u64)?;
        }
        self.validate_file(file, limits)
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
    pub(crate) fn validate_limits(&self, limits: crate::conversation::Limits) -> Result<()> {
        self.validate_limits_with_io(limits, None)
    }

    pub(crate) fn validate_limits_with_io(
        &self, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        model_step(io, 1)?;
        limits.validate()?;
        if let Self::Parts { parts } = self {
            if parts.len() > limits.attachments.saturating_add(1) {
                return Err(Error::Invalid(
                    "tool result exceeds attachment limit".into(),
                ));
            }
            let mut references = 0usize;
            for part in parts {
                model_step(io, 1)?;
                match part {
                    ModelDataPart::Text { text } if text.len() as u64 > limits.render_bytes => {
                        return Err(Error::Invalid(
                            "tool result text exceeds render limit".into(),
                        ));
                    }
                    ModelDataPart::File { file, policy } => {
                        policy.validate_file_with_io(file, limits, io)?;
                        references = references.checked_add(1).ok_or_else(|| {
                            Error::Invalid("tool result reference count overflow".into())
                        })?;
                        if matches!(
                            policy,
                            FileProjectionPolicy::Native(policy) if policy.configuration.is_some()
                        ) {
                            references = references.checked_add(1).ok_or_else(|| {
                                Error::Invalid("tool result reference count overflow".into())
                            })?;
                        }
                    }
                    ModelDataPart::Text { .. } => {}
                }
            }
            if references > limits.attachments.saturating_add(1) {
                return Err(Error::Invalid(
                    "tool result reference count exceeds limit".into(),
                ));
            }
        }
        model_json_bound(self, limits.render_bytes, io)
    }
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
    bytes: Vec<u8>,
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
    fn validate(&self, limits: crate::conversation::Limits) -> Result<()> {
        self.validate_with_io(limits, None)
    }

    fn validate_with_io(
        &self, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        use crate::tool::ToolInvocation;
        use std::collections::BTreeMap;

        limits.validate()?;
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
        // Borrowed entries are the actual SDK-owned map payload. Opaque tree
        // nodes/allocator calls are not asserted by this logical reservation.
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
            message.content.validate_limits_with_io(limits, io)?;
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
                        crate::contract::validate_json_schema_value_with_io(
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
                            _bytes = io.guard_owned(bytes.len() as u64)?;
                            let envelope = io.decode_preparation::<Value>(&bytes)?;
                            crate::contract::validate_json_schema_value_with_io(
                                &tool.projection_schema, &envelope, "model tool result", Some(io),
                            )?;
                            }
                            _ => {
                            let envelope = serde_json::to_value(content)
                                .map_err(|error| Error::Invalid(error.to_string()))?;
                            crate::contract::validate_json_schema_value(
                                &tool.projection_schema, &envelope, "model tool result",
                            )?;
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
    /// Reconstructs exact retained canonical bytes; unknown/defaulted fields or
    /// alternative encodings cannot silently change an admitted request.
    pub fn decode(bytes: &[u8], limits: crate::conversation::Limits) -> Result<Self> {
        Self::decode_engine(bytes, limits, None)
    }

    #[cfg(feature = "filesystem")]
    /// Same canonical request engine, with one cumulative finite ledger.
    pub(crate) fn decode_with_io(
        bytes: &[u8], limits: crate::conversation::Limits,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        Self::decode_engine(bytes, limits, Some(io))
    }

    fn decode_engine(
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

    /// Admits explicit input without retrieval, compaction or truncation.
    /// Providers are trusted to honor the token ceiling and native encoding.
    pub fn prepare(request: ModelRequest, limits: crate::conversation::Limits) -> Result<Self> {
        Self::prepare_engine(request, limits, None)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn prepare_with_io(
        request: ModelRequest, limits: crate::conversation::Limits,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        // Admit the owned caller DTO without cloning it. The witness is a
        // conservative logical allowance, not allocator nodes or RSS.
        let before = io.receipt().items_examined;
        let _witness;
        let witness = io.encode_json(&request, io.remaining()?.bytes_encoded, false)?;
        _witness = io.guard_owned(witness.len() as u64)?;
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
        Ok(prepared)
    }

    fn prepare_engine(
        request: ModelRequest, limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        request.validate_with_io(limits, io)?;
        #[cfg(feature = "filesystem")]
        let bytes_hold;
        let bytes = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode_json(&request, io.remaining()?.bytes_encoded, true)?,
            _ => crate::contract::canonical_json_bytes(&request)?,
        };
        #[cfg(feature = "filesystem")]
        {
            bytes_hold = io.map(|io| io.guard_owned(bytes.len() as u64)).transpose()?;
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
}

/// One immutable, append-only model prefix segment. Persist using the existing
/// content host and retain its pinned `FileRef` in the authoritative fork.
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

    /// Selects all admitted messages, or just the exact suffix after a pinned
    /// parent. Publication must authenticate the parent reference against its
    /// authoritative fork; supplying a `FileRef` alone does not establish that.
    pub fn select(
        request: &PreparedModelRequest,
        parent: Option<(&FileRef, &PreparedModelRequest)>,
    ) -> Result<Self> {
        let (parent, offset) = if let Some((reference, prior)) = parent {
            reference.validate()?;
            if reference.descriptor().media_type() != Self::MEDIA_TYPE
                || !request
                    .request
                    .messages
                    .starts_with(&prior.request.messages)
            {
                return Err(Error::Invalid(
                    "model prefix changed its parent messages".into(),
                ));
            }
            (Some(reference.clone()), prior.request.messages.len())
        } else {
            (None, 0)
        };
        let messages = request
            .request
            .messages
            .get(offset..)
            .ok_or_else(|| Error::Invalid("model prefix offset is invalid".into()))?;
        if messages.is_empty() {
            return Err(Error::Invalid(
                "model prefix must advance its parent".into(),
            ));
        }
        Ok(Self {
            version: 1,
            parent,
            binding_digest: request.manifest.binding_digest,
            message_digest: ordered_message_digest(&request.manifest.message_digests),
            total_messages: request.request.messages.len(),
            messages: messages.to_vec(),
        })
    }

    /// Exact bytes staged once and shared by children through pinned references.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let bytes = crate::contract::canonical_json_bytes(self)?;
        Ok(bytes)
    }
}

fn message_prefix_hasher() -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:model-prefix:ordered:v1");
    hasher
}

fn ordered_message_digest(digests: &[[u8; 32]]) -> [u8; 32] {
    let mut hasher = message_prefix_hasher();
    for digest in digests {
        hasher.update(digest);
    }
    *hasher.finalize().as_bytes()
}

impl PreparedModelRequest {
    /// Resolves only the direct-parent chain, in order, then appends explicit
    /// local messages. No retrieval, summarization, history scan or truncation.
    /// The verifier must enforce the reader's exact generation-pinned grants.
    /// The original binding digest is provenance, not a restriction on future
    /// models. The resulting request validates against its selected tools/model.
    pub async fn inherit(
        mut local: ModelRequest,
        prefix: &FileRef,
        verifier: &dyn crate::conversation::ContentResidencyVerifier,
        limits: crate::conversation::Limits,
    ) -> Result<Self> {
        limits.validate()?;
        let mut next = Some(prefix.clone());
        let mut segments = Vec::new();
        let mut visited = std::collections::BTreeSet::new();
        let mut expected_count = None;
        let mut total_bytes = 0_u64;
        let mut total_messages = local.messages.len();
        while let Some(reference) = next {
            reference.validate()?;
            if reference.descriptor().media_type() != ModelPrefix::MEDIA_TYPE
                || !visited.insert(reference.read_capability()?)
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
            verifier.verify(&reference).await?;
            let bytes = verifier.read(&reference).await?;
            reference.descriptor().verify(&bytes)?;
            let segment: ModelPrefix = crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Invalid(format!("invalid model prefix: {error}")))?;
            if segment.version != 1
                || segment.messages.is_empty()
                || expected_count.is_some_and(|count| count != segment.total_messages)
                || segment.canonical_bytes()? != bytes
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
            segments.push((segment.messages, segment.message_digest));
        }
        let mut messages = Vec::with_capacity(total_messages);
        let mut message_hasher = message_prefix_hasher();
        for (segment, digest) in segments.into_iter().rev() {
            for message in &segment {
                message_hasher.update(&crate::contract::canonical_json_digest(message)?);
            }
            messages.extend(segment);
            if message_hasher.finalize().as_bytes() != &digest {
                return Err(Error::Invalid(
                    "model prefix ordered messages differ".into(),
                ));
            }
        }
        let inherited = ModelRequest {
            model: local.model.clone(),
            tools: local.tools.clone(),
            max_output_tokens: local.max_output_tokens,
            messages,
        };
        inherited.validate(limits)?;
        let mut messages = inherited.messages;
        messages.append(&mut local.messages);
        local.messages = messages;
        for message in &local.messages {
            for file in message.content.file_refs() {
                verifier.verify(file).await?;
            }
        }
        Self::prepare(local, limits)
    }
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

/// Replaceable streaming model provider.
pub trait ModelProvider: acyclic_stream::ProviderPlatform {
    /// Advertises actual capacities of the exact immutable selection without effects.
    fn context_capacity(&self, _model: &Model) -> Result<crate::context::ModelContextCapacity> {
        Err(Error::Unsupported(
            "selected model context capacity is unavailable".into(),
        ))
    }

    /// Returns deterministic additive token upper bounds for this exact request.
    /// Unsupported media/counting must fail explicitly; the SDK never guesses tokens.
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
    #[allow(
        clippy::too_many_lines,
        reason = "one ordered native wire scenario checks traversal, limits and negative controls"
    )]
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
            content.validate_limits(Limits::default())?;
            assert!(
                content
                    .validate_limits(Limits {
                        attachments: 0,
                        ..Limits::default()
                    })
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
    #[allow(
        clippy::too_many_lines,
        reason = "one ordered native wire scenario checks traversal, limits and negative controls"
    )]
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
            valid.validate(&media, Limits::default())?;
            for value in [0, crate::conversation::MAX_EXACT_JS_INTEGER + 1, u64::MAX] {
                let mut invalid = valid.clone();
                invalid.maximum_bytes = value;
                assert!(invalid.validate(&media, Limits::default()).is_err());
                let mut invalid = valid.clone();
                invalid.maximum_work = value;
                assert!(invalid.validate(&media, Limits::default()).is_err());
            }
            let mut short = valid;
            short.maximum_bytes = 2;
            assert!(short.validate(&media, Limits::default()).is_err());
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
            assert!(invalid.validate(&media, Limits::default()).is_err());
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
        let prepared = PreparedModelRequest::prepare(original.clone(), Limits::default())?;
        if let ModelContent::Part(ModelContentPart::File {
            policy: FileProjectionPolicy::Native(policy),
            ..
        }) = &mut original.messages[0].content
        {
            policy.intent = NativeMediaIntent::Image {
                detail: ImageDetail::High,
            };
        }
        let changed = PreparedModelRequest::prepare(original, Limits::default())?;
        assert_ne!(
            prepared.manifest().request_digest,
            changed.manifest().request_digest
        );
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    fn model_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            bytes_hashed: 10_000_000, bytes_copied: 10_000_000,
            bytes_encoded: 10_000_000, source_bytes_read: 10_000_000,
            items_examined: 10_000_000, allocation_operations: 10_000,
            peak_allocation_bytes: 10_000_000, ..Default::default()
        }
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn measured_request_exact_quota_and_cancellation_preserve_prefix() -> Result<()> {
        use crate::filesystem::PreparationIo;
        use acyclic_fs::{CancellationToken, WorkCounters};
        let mut input = request()?;
        input.tools.clear();
        let ordinary = PreparedModelRequest::prepare(input, Limits::default())?;
        let cancellation = CancellationToken::new();
        let baseline = PreparationIo::from_budget(model_budget(), &cancellation)?;
        let measured = PreparedModelRequest::decode_with_io(ordinary.bytes(), Limits::default(), &baseline)?;
        assert_eq!(measured.bytes(), ordinary.bytes());
        assert_eq!(measured.manifest(), ordinary.manifest());
        let exact = baseline.receipt();
        assert!(exact.bytes_hashed > 0 && exact.bytes_encoded > 0);
        assert!(exact.source_bytes_read > 0 && exact.items_examined > 0);
        assert!(exact.peak_allocation_bytes > 0 && exact.allocation_operations > 0);
        let admitted = PreparationIo::from_budget(exact, &cancellation)?;
        assert_eq!(
            PreparedModelRequest::decode_with_io(ordinary.bytes(), Limits::default(), &admitted)?.manifest(),
            ordinary.manifest(),
        );
        assert_eq!(admitted.receipt(), exact);
        for budget in [
            acyclic_fs::WorkBudget { bytes_hashed: exact.bytes_hashed - 1, ..exact },
            acyclic_fs::WorkBudget { peak_allocation_bytes: exact.peak_allocation_bytes - 1, ..exact },
            acyclic_fs::WorkBudget { allocation_operations: 0, ..exact },
        ] {
            let denied = PreparationIo::from_budget(budget, &cancellation)?;
            assert!(PreparedModelRequest::decode_with_io(ordinary.bytes(), Limits::default(), &denied).is_err());
            denied.receipt().verify(budget).map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(denied.receipt().source_bytes_read > 0);
        }
        let cancelled = PreparationIo::from_budget(model_budget(), &cancellation)?;
        cancellation.cancel();
        assert!(matches!(
            PreparedModelRequest::decode_with_io(ordinary.bytes(), Limits::default(), &cancelled),
            Err(Error::Storage(_)),
        ));
        assert_eq!(cancelled.receipt(), WorkCounters::default());
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn measured_request_schema_projection_and_canonical_controls() -> Result<()> {
        use crate::filesystem::PreparationIo;
        use acyclic_fs::CancellationToken;
        let cancellation = CancellationToken::new();
        let mut input = request()?;
        exchange(&mut input);
        let ordinary = PreparedModelRequest::prepare(input.clone(), Limits::default())?;
        let io = PreparationIo::from_budget(model_budget(), &cancellation)?;
        let measured = PreparedModelRequest::decode_with_io(ordinary.bytes(), Limits::default(), &io)?;
        assert_eq!(measured.bytes(), include_bytes!("../fixtures/model-request.json"));
        assert_eq!(measured.manifest(), ordinary.manifest());
        let mut alternate = ordinary.bytes().to_vec();
        alternate.push(b' ');
        let mut unknown = serde_json::to_value(&input).map_err(|error| Error::Invalid(error.to_string()))?;
        unknown["discarded"] = Value::Bool(true);
        for bytes in [
            alternate, crate::contract::canonical_json_bytes(&unknown)?, b"{".to_vec(),
        ] {
            let denied = PreparationIo::from_budget(model_budget(), &cancellation)?;
            assert!(PreparedModelRequest::decode_with_io(&bytes, Limits::default(), &denied).is_err());
            assert!(denied.receipt().source_bytes_read > 0);
        }
        let mut invalid = input.clone();
        invalid.tools[0].input_schema = json!({"type":"integer"});
        assert!(PreparedModelRequest::prepare(invalid.clone(), Limits::default()).is_err());
        let denied = PreparationIo::from_budget(model_budget(), &cancellation)?;
        assert!(PreparedModelRequest::prepare_with_io(invalid, Limits::default(), &denied).is_err());
        assert!(denied.receipt().bytes_copied > 0);
        let mut invalid_projection = input.clone();
        if let ModelContent::Part(ModelContentPart::ToolResult {
            content: ToolResultContent::Json { value }, ..
        }) = &mut invalid_projection.messages[2].content {
            *value = json!(42);
        }
        let denied = PreparationIo::from_budget(model_budget(), &cancellation)?;
        assert!(PreparedModelRequest::prepare_with_io(invalid_projection, Limits::default(), &denied).is_err());
        input.tools[0].revision = "schema-3".into();
        input.model.options = json!({"temperature":0});
        let changed_ordinary = PreparedModelRequest::prepare(input.clone(), Limits::default())?;
        let changed_io = PreparationIo::from_budget(model_budget(), &cancellation)?;
        let changed = PreparedModelRequest::prepare_with_io(input, Limits::default(), &changed_io)?;
        assert_eq!(changed.manifest(), changed_ordinary.manifest());
        assert_ne!(changed.manifest().request_digest, measured.manifest().request_digest);
        assert_ne!(changed.manifest().binding_digest, measured.manifest().binding_digest);
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

    #[test]
    fn canonical_bytes_pin_unicode_order_schemas_and_parent_mutation() -> Result<()> {
        let mut original = request()?;
        exchange(&mut original);
        let prepared = PreparedModelRequest::prepare(original.clone(), Limits::default())?;
        assert_eq!(
            prepared.bytes(),
            include_bytes!("../fixtures/model-request.json")
        );
        assert_eq!(
            PreparedModelRequest::decode(prepared.bytes(), Limits::default())?.bytes(),
            prepared.bytes()
        );
        let mut unknown = serde_json::to_value(prepared.request())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        unknown["discarded"] = Value::Bool(true);
        assert!(
            PreparedModelRequest::decode(
                &crate::contract::canonical_json_bytes(&unknown)?,
                Limits::default()
            )
            .is_err()
        );
        assert_eq!(
            ModelPrefix::select(&prepared, None)?.canonical_bytes()?,
            include_bytes!("../fixtures/model-prefix.json")
        );
        assert_eq!(
            crate::contract::json_from_slice::<ModelRequest>(prepared.bytes())
                .map_err(|e| Error::Invalid(e.to_string()))?,
            original
        );
        assert_eq!(
            prepared.manifest().request_digest,
            *blake3::hash(prepared.bytes()).as_bytes()
        );
        assert_eq!(
            prepared.manifest().wire_bytes,
            prepared.bytes().len() as u64
        );
        original.messages[0].content = ModelContent::Text("parent changed".into());
        assert_ne!(
            PreparedModelRequest::prepare(original, Limits::default())?
                .manifest()
                .request_digest,
            prepared.manifest().request_digest
        );
        let mut changed = prepared.request().clone();
        changed.tools[0].revision = "schema-3".into();
        assert_ne!(
            PreparedModelRequest::prepare(changed, Limits::default())?
                .manifest()
                .binding_digest,
            prepared.manifest().binding_digest
        );
        Ok(())
    }

    #[derive(Clone, Default)]
    struct PrefixReader(
        std::collections::BTreeMap<String, Vec<u8>>,
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
        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
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
            prefix.canonical_bytes()?,
            ModelPrefix::MEDIA_TYPE,
            index,
        )
    }

    fn store_prefix_bytes(
        reader: &mut PrefixReader,
        bytes: Vec<u8>,
        media_type: &str,
        index: u8,
    ) -> Result<FileRef> {
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
        let mut prepared = PreparedModelRequest::prepare(root, limits)?;
        let mut head = store_prefix(&mut reader, &ModelPrefix::select(&prepared, None)?, 0)?;
        for depth in 1..=3 {
            let mut next = prepared.request().clone();
            next.messages.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!(
                    "child {depth}: é\0🦀; parent scratch retained"
                )),
            });
            let next = PreparedModelRequest::prepare(next, limits)?;
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
                PreparedModelRequest::inherit(local.clone(), &head, &reopened, limits).await?;
            let mut expected = prepared.request().clone();
            expected.messages.extend(local.messages.clone());
            assert_eq!(
                inherited.bytes(),
                PreparedModelRequest::prepare(expected, limits)?.bytes()
            );
            let mut changed = local.clone();
            changed.model.revision = "changed".into();
            let changed_request =
                PreparedModelRequest::inherit(changed, &head, &reader, limits).await?;
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
                PreparedModelRequest::inherit(incompatible, &head, &reader, limits)
                    .await
                    .is_err()
            );
            assert!(
                PreparedModelRequest::inherit(
                    local.clone(),
                    &head,
                    &PrefixReader::default(),
                    limits
                )
                .await
                .is_err()
            );
            let mut denied = reader.clone();
            denied.1.remove(&head.read_capability()?);
            assert!(matches!(
                PreparedModelRequest::inherit(local.clone(), &head, &denied, limits).await,
                Err(Error::Unauthorized(_))
            ));
            let mut missing_attachment = reader.clone();
            missing_attachment.0.remove(&attachment.read_capability()?);
            assert!(
                PreparedModelRequest::inherit(local.clone(), &head, &missing_attachment, limits)
                    .await
                    .is_err()
            );
            let lower_limits = Limits {
                context_messages: prepared.request().messages.len(),
                ..limits
            };
            assert!(
                PreparedModelRequest::inherit(local.clone(), &head, &reader, lower_limits)
                    .await
                    .is_err()
            );
            let mut corrupt = reader.clone();
            corrupt.0.insert(head.read_capability()?, b"{}".to_vec());
            assert!(
                PreparedModelRequest::inherit(local, &head, &corrupt, limits)
                    .await
                    .is_err()
            );
        }
        let mut changed_parent = prepared.request().clone();
        changed_parent.messages[0].content = ModelContent::Text("later parent mutation".into());
        let changed_parent = PreparedModelRequest::prepare(changed_parent, limits)?;
        assert!(ModelPrefix::select(&changed_parent, Some((&head, &prepared))).is_err());
        // An authenticated, canonical parent with the same shape and binding
        // still must contain the exact messages claimed by the child segment.
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
        let descendant = PreparedModelRequest::prepare(descendant, limits)?;
        let mislabeled = ModelPrefix::select(&descendant, Some((&impostor, &prepared)))?;
        let mislabeled = store_prefix(&mut reader, &mislabeled, 89)?;
        assert!(
            matches!(PreparedModelRequest::inherit(request()?, &mislabeled, &reader, limits).await,
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
            })?;
            assert!(matches!(content.validate_limits(Limits {
                render_bytes: exact - 1, ..Limits::default()
            }), Err(Error::Invalid(message)) if message.contains("render limit")));
        }
        Ok(())
    }

    #[test]
    fn malformed_exchanges_and_bounds_fail_closed() -> Result<()> {
        let base = request()?;
        let mut valid = base.clone();
        exchange(&mut valid);
        PreparedModelRequest::prepare(valid.clone(), Limits::default())?;
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
        let unconstrained = PreparedModelRequest::prepare(changed, Limits::default())?;
        assert!(unconstrained.request().max_output_tokens.is_none());
        assert_eq!(
            PreparedModelRequest::decode(unconstrained.bytes(), Limits::default())?
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
            assert!(PreparedModelRequest::prepare(candidate, Limits::default()).is_err());
        }
        let mut oversized = base;
        let maximum = 64 * 1024 * 1024 + 1;
        oversized.model.options = json!({"large": "x".repeat(maximum)});
        assert!(PreparedModelRequest::prepare(oversized, Limits::default()).is_ok());
        Ok(())
    }
}
