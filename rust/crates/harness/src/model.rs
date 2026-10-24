//! Provider-neutral immutable model values and streaming host contract.

use crate::{Error, OperationId, Result, conversation::FileRef};
use futures::{future::BoxFuture, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
        if value.provider.trim().is_empty()
            || value.name.trim().is_empty()
            || value.revision.trim().is_empty()
        {
            return Err(Error::Invalid(
                "model provider, name, and revision must be non-empty".into(),
            ));
        }
        Ok(value)
    }
}

/// One provider-neutral prompt item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelMessage {
    /// Semantic role (`system`, `user`, `assistant`, or `tool`).
    pub role: ModelRole,
    /// Typed provider-neutral content, including bounded transient inline images.
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
    /// Applies the same byte and part ceilings to direct inputs and selected
    /// history before either can reach a provider or durable request journal.
    pub fn validate_limits(&self, limits: crate::conversation::Limits) -> Result<()> {
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
        if parts.len() > limits.attachments + 1 {
            return Err(Error::Invalid(
                "model content exceeds attachment limit".into(),
            ));
        }
        for part in parts {
            match part {
                ModelContentPart::Image { image } => {
                    image.validate()?;
                    let padding = image
                        .data_base64
                        .bytes()
                        .rev()
                        .take_while(|byte| *byte == b'=')
                        .count();
                    let byte_length = image.data_base64.len() / 4 * 3 - padding;
                    if byte_length as u64 > limits.file_bytes {
                        return Err(Error::Invalid("model image exceeds file limit".into()));
                    }
                }
                ModelContentPart::Text { text } if text.len() as u64 > limits.render_bytes => {
                    return Err(Error::Invalid("model text exceeds render limit".into()));
                }
                ModelContentPart::File { file, policy } => {
                    limits.validate_file(file)?;
                    if *policy == FileProjectionPolicy::BoundedFull
                        && file.descriptor().byte_length() > limits.render_bytes
                    {
                        return Err(Error::Invalid("model file exceeds render limit".into()));
                    }
                }
                ModelContentPart::ToolCall {
                    name, arguments, ..
                } => {
                    crate::registry::validate_component_label(name, "tool name")?;
                    if crate::contract::canonical_json_bytes(arguments)?.len() as u64
                        > limits.render_bytes
                    {
                        return Err(Error::Invalid(
                            "model tool projection exceeds render limit".into(),
                        ));
                    }
                }
                ModelContentPart::ToolResult { name, value, .. } => {
                    crate::registry::validate_component_label(name, "tool name")?;
                    if crate::contract::canonical_json_bytes(value)?.len() as u64
                        > limits.render_bytes
                    {
                        return Err(Error::Invalid(
                            "model tool projection exceeds render limit".into(),
                        ));
                    }
                }
                ModelContentPart::Text { .. } => {}
            }
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
            Self::Parts(parts) if !parts.is_empty() && parts.len() <= 1_024 => parts.as_slice(),
            _ => return Err(Error::Invalid("user input part count is invalid".into())),
        };
        for part in parts {
            match part {
                ModelContentPart::Text { text } if !text.is_empty() => {}
                ModelContentPart::File { file, .. } => file.validate()?,
                ModelContentPart::Image { image } => image.validate()?,
                _ => {
                    return Err(Error::Invalid(
                        "user input contains an unsupported part".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Returns every immutable file referenced by one typed input.
    #[must_use]
    pub fn file_refs(&self) -> Vec<&FileRef> {
        match self {
            Self::Part(ModelContentPart::File { file, .. }) => vec![file],
            Self::Text(_) | Self::Part(_) => Vec::new(),
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ModelContentPart::File { file, .. } => Some(file),
                    _ => None,
                })
                .collect(),
        }
    }
}

/// One provider-neutral part requiring an explicit provider projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelContentPart {
    /// Verified inline PNG or JPEG bytes for provider-native vision.
    Image {
        /// Bounded encoded image.
        image: ModelImage,
    },
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
        /// Model-visible result projection.
        value: Value,
    },
}

/// Bounded provider-neutral inline image, kept out of text/JSON projections.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelImage {
    /// Exact media type: image/png or image/jpeg.
    pub media_type: String,
    /// Standard padded base64 encoding of the original image bytes.
    pub data_base64: String,
}

impl std::fmt::Debug for ModelImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelImage")
            .field("media_type", &self.media_type)
            .field("encoded_bytes", &self.data_base64.len())
            .field("data_base64", &"[redacted]")
            .finish()
    }
}

impl ModelImage {
    /// Maximum decoded bytes in one image.
    pub const MAX_BYTES: usize = 10 * 1024 * 1024;

    /// Rejects invalid encoding, unsupported media, oversized or mismatched bytes.
    pub fn validate(&self) -> Result<()> {
        use base64::Engine as _;
        if self.data_base64.len() > Self::MAX_BYTES.div_ceil(3) * 4 {
            return Err(Error::Invalid("image exceeds byte limit".into()));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.data_base64)
            .map_err(|_| Error::Invalid("image is not valid base64".into()))?;
        let signature_matches = match self.media_type.as_str() {
            "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            "image/jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
            _ => false,
        };
        if bytes.len() > Self::MAX_BYTES || !signature_matches {
            return Err(Error::Invalid(
                "image bytes do not match supported PNG/JPEG media".into(),
            ));
        }
        Ok(())
    }
}

/// File handling policy selected before provider dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileProjectionPolicy {
    /// Metadata-only file reference, with no byte read.
    Reference,
    /// Verified, bounded text bytes.
    BoundedFull,
    /// Verified provider-native input such as an image.
    Native,
}

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

/// Replaceable streaming model provider.
pub trait ModelProvider: Send + Sync {
    /// Starts one request and yields ordered model events.
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>>;

    /// Continues or reconciles an interrupted run without starting another model request.
    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>>;
}

#[cfg(test)]
mod image_tests {
    use super::*;
    #[test]
    fn inline_images_reject_invalid_encoding_media_and_limits() {
        for (media_type, data_base64) in [
            ("image/png", "%%%"),
            ("image/gif", "iVBORw0KGgo="),
            ("image/jpeg", "iVBORw0KGgo="),
        ] {
            assert!(
                ModelImage {
                    media_type: media_type.into(),
                    data_base64: data_base64.into()
                }
                .validate()
                .is_err()
            );
        }
        let image = ModelImage {
            media_type: "image/png".into(),
            data_base64: "iVBORw0KGgo=".into(),
        };
        assert!(image.validate().is_ok());
        assert!(!format!("{image:?}").contains(&image.data_base64));
        let content = ModelContent::Part(ModelContentPart::Image { image });
        assert!(
            content
                .validate_limits(crate::conversation::Limits {
                    file_bytes: 1,
                    ..Default::default()
                })
                .is_err()
        );
        let image = ModelImage {
            media_type: "image/png".into(),
            data_base64: "A".repeat(ModelImage::MAX_BYTES.div_ceil(3) * 4 + 1),
        };
        assert!(image.validate().is_err());
    }
}
