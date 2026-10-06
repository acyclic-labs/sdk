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

/// Hard aggregate ceiling for the canonical serialized model input, independent of output bounds.
pub const MAX_MODEL_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

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
        use crate::tool::{ToolInvocation, validate_value};
        use std::collections::BTreeMap;

        limits.validate()?;
        self.model.validate()?;
        if self.messages.is_empty() || self.messages.len() > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if self.max_output_tokens.is_none_or(|bound| bound == 0) {
            return Err(Error::Invalid(
                "model output token bound is required".into(),
            ));
        }
        let mut tools = BTreeMap::new();
        for tool in &self.tools {
            tool.validate()?;
            if tools.insert(&tool.name, tool).is_some() {
                return Err(Error::Invalid("model tool definition is repeated".into()));
            }
        }
        let mut pending = BTreeMap::new();
        for message in &self.messages {
            message.content.validate_limits(limits)?;
            let parts = match &message.content {
                ModelContent::Text(_) => &[][..],
                ModelContent::Part(part) => std::slice::from_ref(part),
                ModelContent::Parts(parts) => parts.as_slice(),
            };
            for part in parts {
                match part {
                    ModelContentPart::ToolCall {
                        call_id,
                        name,
                        arguments,
                    } => {
                        ToolInvocation::validate_identity(call_id, name)?;
                        if message.role != ModelRole::Assistant || pending.contains_key(call_id) {
                            return Err(Error::Invalid(
                                "model tool call role or identity is invalid".into(),
                            ));
                        }
                        let tool = tools
                            .get(name)
                            .ok_or_else(|| Error::Invalid("model tool call is unbound".into()))?;
                        validate_value(&tool.input_schema, arguments, "model tool arguments")?;
                        pending.insert(call_id, name);
                    }
                    ModelContentPart::ToolResult {
                        call_id,
                        name,
                        value,
                    } => {
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
                        validate_value(&tool.output_schema, value, "model tool result")?;
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
    /// Admits explicit input without retrieval, compaction or truncation.
    /// Providers are trusted to honor the token ceiling and native encoding.
    pub fn prepare(request: ModelRequest, limits: crate::conversation::Limits) -> Result<Self> {
        request.validate(limits)?;
        let bytes = crate::contract::canonical_json_bytes(&request)?;
        if bytes.len() as u64 > MAX_MODEL_REQUEST_BYTES {
            return Err(Error::Invalid(
                "model request exceeds aggregate byte limit".into(),
            ));
        }
        let manifest = ModelRequestManifest {
            version: 1,
            request_digest: *blake3::hash(&bytes).as_bytes(),
            wire_bytes: bytes.len() as u64,
            binding_digest: crate::contract::canonical_json_digest(&(
                &request.model,
                &request.tools,
            ))?,
            message_digests: request
                .messages
                .iter()
                .map(crate::contract::canonical_json_digest)
                .collect::<Result<_>>()?,
        };
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
                || prior.manifest.binding_digest != request.manifest.binding_digest
                || !request
                    .request
                    .messages
                    .starts_with(&prior.request.messages)
            {
                return Err(Error::Invalid(
                    "model prefix changed its parent or binding".into(),
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
        if bytes.len() as u64 > MAX_MODEL_REQUEST_BYTES {
            return Err(Error::Invalid("model prefix exceeds byte limit".into()));
        }
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
    pub async fn inherit(
        mut local: ModelRequest,
        prefix: &FileRef,
        verifier: &dyn crate::conversation::ContentResidencyVerifier,
        limits: crate::conversation::Limits,
    ) -> Result<Self> {
        limits.validate()?;
        let binding = crate::contract::canonical_json_digest(&(&local.model, &local.tools))?;
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
            if total_bytes > MAX_MODEL_REQUEST_BYTES || visited.len() > limits.context_messages {
                return Err(Error::Invalid(
                    "model prefix traversal exceeds limit".into(),
                ));
            }
            verifier.verify(&reference).await?;
            let bytes = verifier.read(&reference).await?;
            reference.descriptor().verify(&bytes)?;
            let segment: ModelPrefix = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(format!("invalid model prefix: {error}")))?;
            if segment.version != 1
                || segment.binding_digest != binding
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
    fn generate<'a>(&'a self, request: PreparedModelRequest) -> BoxStream<'a, Result<ModelEvent>>;

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

    fn request() -> Result<ModelRequest> {
        Ok(ModelRequest {
            model: Model::new("mock", "exact", "pinned", json!({}))?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("é\0🦀\r\n".into()),
            }],
            tools: vec![crate::tool::ToolDefinition {
                name: "echo".into(),
                revision: "schema-1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"string"}),
                output_schema: json!({"type":"string"}),
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
                    value: json!("é"),
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
            include_bytes!("../../../../fixtures/harness/v2/model-request.json")
        );
        assert_eq!(
            ModelPrefix::select(&prepared, None)?.canonical_bytes()?,
            include_bytes!("../../../../fixtures/harness/v2/model-prefix.json")
        );
        assert_eq!(
            serde_json::from_slice::<ModelRequest>(prepared.bytes())
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
        changed.tools[0].revision = "schema-2".into();
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
            assert!(
                PreparedModelRequest::inherit(changed, &head, &reader, limits)
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
        if let ModelContent::Part(ModelContentPart::ToolResult { value, .. }) =
            &mut changed.messages[2].content
        {
            *value = json!(42);
        }
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.tools.push(changed.tools[0].clone());
        candidates.push(changed);
        let mut changed = valid.clone();
        changed.max_output_tokens = None;
        candidates.push(changed);
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
        let maximum = usize::try_from(MAX_MODEL_REQUEST_BYTES)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        oversized.model.options = json!({"large": "x".repeat(maximum)});
        assert!(PreparedModelRequest::prepare(oversized, Limits::default()).is_err());
        Ok(())
    }
}
