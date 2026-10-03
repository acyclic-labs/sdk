//! Versioned model-input admission and immutable fork prefixes.
use crate::{
    conversation::{FileRef, Limits},
    model::{
        ModelContent, ModelContentPart, ModelMessage, ModelOptionPolicy, ModelRequest, ModelRole,
    },
    registry::ComponentIdentity,
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Canonical model-input encoding version.
pub const MODEL_INPUT_VERSION: u32 = 3;

/// An ordered message's exact content identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputMessageManifest {
    /// Zero-based request position.
    pub position: usize,
    /// Original semantic role.
    pub role: ModelRole,
    /// Canonical message digest.
    pub digest: [u8; 32],
    /// Immutable file references in content order.
    pub files: Vec<FileRef>,
}

/// Audit identity of the exact provider-neutral request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInputManifest {
    /// Encoding version.
    pub version: u32,
    /// Model and tool-definition identity, independent of the output budget.
    pub binding_digest: [u8; 32],
    /// Complete request digest.
    pub request_digest: [u8; 32],
    /// Registered model-option policy identity used for admission, when one
    /// was supplied by the provider binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_option_policy: Option<ComponentIdentity>,
    /// Canonical digest of the registered model-option schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_option_schema_digest: Option<[u8; 32]>,
    /// Ordered message identities.
    pub messages: Vec<InputMessageManifest>,
    /// Authenticated rejection evidence for malformed tool calls in the
    /// request. This is journal metadata and is intentionally excluded from
    /// the provider request bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejection_evidence: Vec<crate::tool::ToolRejectionFeedback>,
}

/// Admitted input without retrieval, truncation, or compaction.
#[derive(Clone, Debug)]
pub struct PreparedModelInput {
    request: ModelRequest,
    bytes: Vec<u8>,
    manifest: ModelInputManifest,
}
impl PreparedModelInput {
    /// Validates explicit input and freezes its canonical encoding.
    pub fn prepare(request: ModelRequest, limits: Limits) -> Result<Self> {
        Self::prepare_with_policy(request, limits, None)
    }

    /// Prepares input while binding the provider's registered option policy.
    pub fn prepare_with_policy(
        request: ModelRequest,
        limits: Limits,
        policy: Option<&ModelOptionPolicy>,
    ) -> Result<Self> {
        limits.validate()?;
        if let Some(policy) = policy {
            policy.validate(&request.model.options)?;
        } else if !(request.model.options.is_null()
            || request
                .model
                .options
                .as_object()
                .is_some_and(|value| value.is_empty()))
        {
            return Err(Error::Invalid(
                "model options require a registered provider policy".into(),
            ));
        }
        if request.messages.is_empty() || request.messages.len() > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if request.max_output_tokens == Some(0) {
            return Err(Error::Invalid(
                "model output token bound must be positive".into(),
            ));
        }
        let mut names = BTreeSet::new();
        for tool in &request.tools {
            tool.validate()?;
            if !names.insert(&tool.name) {
                return Err(Error::Invalid("duplicate model tool definition".into()));
            }
        }
        validate_exchanges(&request.messages, &request.tools)?;
        let mut messages = Vec::with_capacity(request.messages.len());
        for (position, message) in request.messages.iter().enumerate() {
            message.content.validate_limits(limits)?;
            messages.push(InputMessageManifest {
                position,
                role: message.role,
                digest: crate::contract::canonical_json_digest(message)?,
                files: message.content.file_refs().into_iter().cloned().collect(),
            });
        }
        let bytes = crate::contract::canonical_json_bytes(&request)?;
        if bytes.len() as u64 > limits.file_bytes {
            return Err(Error::Invalid(
                "aggregate model request exceeds byte limit".into(),
            ));
        }
        let option_schema_digest = policy.map(ModelOptionPolicy::schema_digest).transpose()?;
        let manifest = ModelInputManifest {
            version: MODEL_INPUT_VERSION,
            binding_digest: binding_digest(&request, policy, option_schema_digest)?,
            request_digest: *blake3::hash(&bytes).as_bytes(),
            messages,
            model_option_policy: policy.map(|value| value.identity.clone()),
            model_option_schema_digest: option_schema_digest,
            rejection_evidence: Vec::new(),
        };
        Ok(Self {
            request,
            bytes,
            manifest,
        })
    }
    /// Immutable request to dispatch.
    #[must_use]
    pub fn request(&self) -> &ModelRequest {
        &self.request
    }
    /// Exact canonical provider-neutral bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Ordered content and component identities.
    #[must_use]
    pub fn manifest(&self) -> &ModelInputManifest {
        &self.manifest
    }
    /// Attaches authenticated rejection evidence to the durable manifest.
    /// Evidence is checked against the exact request before it can be staged.
    pub fn with_rejection_evidence(
        mut self,
        rejection_evidence: Vec<crate::tool::ToolRejectionFeedback>,
    ) -> Result<Self> {
        validate_rejection_evidence(&self.request, &rejection_evidence)?;
        self.manifest.rejection_evidence = rejection_evidence;
        Ok(self)
    }
    /// Dispatches the admitted values without reconstruction.
    #[must_use]
    pub fn into_request(self) -> ModelRequest {
        self.request
    }
    /// Requires complete paired tool exchanges at a fork boundary.
    pub fn validate_complete_exchange(&self) -> Result<()> {
        validate_exchanges(&self.request.messages, &self.request.tools)
    }
}

/// Persistable byte-perfect conversation prefix. Child context is a suffix.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenModelPrefix {
    version: u32,
    binding_digest: [u8; 32],
    message_bytes: Vec<Vec<u8>>,
    digest: [u8; 32],
}
impl FrozenModelPrefix {
    /// Captures a completed prefix without regenerating any message.
    pub fn capture(input: &PreparedModelInput, count: usize) -> Result<Self> {
        if count == 0 || count > input.request.messages.len() {
            return Err(Error::Invalid(
                "frozen model prefix count is invalid".into(),
            ));
        }
        let messages = input
            .request
            .messages
            .get(..count)
            .ok_or_else(|| Error::Invalid("frozen model prefix is unavailable".into()))?;
        validate_exchanges(messages, &input.request.tools)?;
        let message_bytes = messages
            .iter()
            .map(crate::contract::canonical_json_bytes)
            .collect::<Result<Vec<_>>>()?;
        let binding_digest = input.manifest.binding_digest;
        let digest = prefix_digest(binding_digest, &message_bytes)?;
        Ok(Self {
            version: MODEL_INPUT_VERSION,
            binding_digest,
            message_bytes,
            digest,
        })
    }
    /// Checks persisted integrity and the actual dispatch input.
    pub fn verify(&self, input: &PreparedModelInput) -> Result<()> {
        if self.version != MODEL_INPUT_VERSION {
            return Err(Error::Conflict(
                "frozen model prefix version is unsupported; re-admission is required".into(),
            ));
        }
        if self.message_bytes.is_empty()
            || self.digest != prefix_digest(self.binding_digest, &self.message_bytes)?
        {
            return Err(Error::Invalid(
                "frozen model prefix integrity failed".into(),
            ));
        }
        if self.binding_digest != input.manifest.binding_digest {
            return Err(Error::Conflict(
                "fork changed model or tool definitions".into(),
            ));
        }
        let messages = input
            .request
            .messages
            .get(..self.message_bytes.len())
            .ok_or_else(|| Error::Conflict("fork omitted inherited model messages".into()))?;
        for (message, expected) in messages.iter().zip(&self.message_bytes) {
            if crate::contract::canonical_json_bytes(message)? != *expected {
                return Err(Error::Conflict(
                    "fork rewrote inherited model prefix".into(),
                ));
            }
        }
        validate_exchanges(messages, &input.request.tools)
    }
    /// Ordered message blocks retained for replay.
    #[must_use]
    pub fn message_bytes(&self) -> &[Vec<u8>] {
        &self.message_bytes
    }
    /// Identity to bind to durable fork publication.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}
fn binding_digest(
    request: &ModelRequest,
    policy: Option<&ModelOptionPolicy>,
    option_schema_digest: Option<[u8; 32]>,
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(
        MODEL_INPUT_VERSION,
        &request.model,
        &request.tools,
        policy.map(|value| &value.identity),
        option_schema_digest,
    ))
}
fn prefix_digest(binding: [u8; 32], messages: &[Vec<u8>]) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(MODEL_INPUT_VERSION, binding, messages))
}
#[derive(Clone, Copy)]
struct PendingToolCall<'a> {
    name: &'a str,
    arguments: &'a serde_json::Value,
    malformed: bool,
}

fn validate_exchanges(
    messages: &[ModelMessage],
    tools: &[crate::tool::ToolDefinition],
) -> Result<()> {
    let definitions = tools
        .iter()
        .map(|tool| (tool.name.as_str(), tool))
        .collect::<BTreeMap<_, _>>();
    let mut pending = BTreeMap::new();
    for message in messages {
        let parts = match &message.content {
            ModelContent::Text(_) => &[][..],
            ModelContent::Part(part) => std::slice::from_ref(part),
            ModelContent::Parts(parts) => parts.as_slice(),
        };
        if !pending.is_empty()
            && message.role != ModelRole::Tool
            && message.role != ModelRole::Assistant
        {
            return Err(Error::Invalid(
                "message interrupts unfinished tool exchange".into(),
            ));
        }
        if message.role == ModelRole::Tool
            && (parts.is_empty()
                || parts
                    .iter()
                    .any(|part| !matches!(part, ModelContentPart::ToolResult { .. })))
        {
            return Err(Error::Invalid(
                "tool message contains non-result content".into(),
            ));
        }
        for part in parts {
            match part {
                ModelContentPart::ToolCall {
                    call_id,
                    name,
                    arguments,
                } => {
                    crate::tool::ToolInvocation::validate_identity(call_id, name)?;
                    if message.role != ModelRole::Assistant || pending.contains_key(call_id) {
                        return Err(Error::Invalid(
                            "invalid or duplicate inherited tool call".into(),
                        ));
                    }
                    let Some(definition) = definitions.get(name.as_str()) else {
                        return Err(Error::Invalid(
                            "inherited tool call names an unregistered tool".into(),
                        ));
                    };
                    let malformed = crate::tool::validate_value(
                        &definition.input_schema,
                        arguments,
                        "tool input",
                    )
                    .is_err();
                    pending.insert(
                        call_id,
                        PendingToolCall {
                            name,
                            arguments,
                            malformed,
                        },
                    );
                }
                ModelContentPart::ToolResult {
                    call_id,
                    name,
                    value,
                } => {
                    if message.role != ModelRole::Tool {
                        return Err(Error::Invalid("tool result has invalid role".into()));
                    }
                    let Some(call) = pending.remove(call_id) else {
                        return Err(Error::Invalid("inherited tool result is not paired".into()));
                    };
                    if call.name != name.as_str() {
                        return Err(Error::Conflict("inherited tool result changed tool".into()));
                    }
                    let feedback = crate::tool::ToolRejectionFeedback::from_model_value(value)?;
                    if call.malformed {
                        let Some(feedback) = feedback else {
                            return Err(Error::Invalid(
                                "malformed tool call lacks correlated rejection feedback".into(),
                            ));
                        };
                        let definition = definitions
                            .get(name.as_str())
                            .ok_or_else(|| Error::Storage("tool definition disappeared".into()))?;
                        let error = value
                            .get("error")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| Error::Invalid("rejection error is missing".into()))?;
                        let expected = crate::tool::ToolRejectionFeedback::invalid_arguments(
                            &crate::tool::ToolInvocation {
                                operation_id: crate::OperationId::new(),
                                call_id: call_id.clone(),
                                name: name.clone(),
                                arguments: call.arguments.clone(),
                            },
                            &definition.input_schema,
                            error,
                        )?;
                        if feedback != expected {
                            return Err(Error::Conflict(
                                "tool rejection feedback does not match the rejected call".into(),
                            ));
                        }
                    } else {
                        if feedback.is_some() {
                            return Err(Error::Invalid(
                                "valid tool call cannot carry rejection feedback".into(),
                            ));
                        }
                        let definition = definitions
                            .get(name.as_str())
                            .ok_or_else(|| Error::Storage("tool definition disappeared".into()))?;
                        crate::tool::validate_value(
                            &definition.model_output_schema,
                            value,
                            "tool projection",
                        )?;
                    }
                }
                _ => {}
            }
        }
    }
    if !pending.is_empty() {
        return Err(Error::Invalid(
            "fork boundary has unfinished tool calls".into(),
        ));
    }
    Ok(())
}

/// Durable boundary shared by all children requested in one completed tool batch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedModelBoundary {
    /// Original serialized request with the ordered completed exchange appended.
    pub request: ModelRequest,
    /// Immutable inherited prefix binding every message and definition.
    pub prefix: FrozenModelPrefix,
    /// Provider policy required to re-admit this boundary and its children.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub option_policy: Option<ModelOptionPolicy>,
    /// Typed durable evidence for every recoverable invalid-call exchange in
    /// the request. Keeping this with the boundary prevents a child fork from
    /// reinterpreting a model-visible rejection after restart.
    #[serde(default)]
    pub rejection_evidence: Vec<crate::tool::ToolRejectionFeedback>,
}
impl CompletedModelBoundary {
    /// Refuses incomplete exchanges; does not regenerate the original context.
    pub fn capture(request: ModelRequest, limits: Limits) -> Result<Self> {
        Self::capture_with_policy(request, limits, None, &[])
    }

    /// Captures a completed exchange after checking each malformed call
    /// against typed, durable rejection evidence. A model-visible feedback
    /// envelope alone cannot authorize a boundary.
    pub fn capture_with_rejections(
        request: ModelRequest,
        limits: Limits,
        rejections: &[crate::tool::ToolRejectionFeedback],
    ) -> Result<Self> {
        Self::capture_with_policy(request, limits, None, rejections)
    }

    /// Captures a completed exchange while pinning the provider option policy.
    pub fn capture_with_policy(
        request: ModelRequest,
        limits: Limits,
        policy: Option<&ModelOptionPolicy>,
        rejections: &[crate::tool::ToolRejectionFeedback],
    ) -> Result<Self> {
        let prepared = PreparedModelInput::prepare_with_policy(request, limits, policy)?;
        validate_rejection_evidence(prepared.request(), rejections)?;
        let prefix = FrozenModelPrefix::capture(&prepared, prepared.request().messages.len())?;
        Ok(Self {
            request: prepared.into_request(),
            prefix,
            option_policy: policy.cloned(),
            rejection_evidence: rejections.to_vec(),
        })
    }
    /// Validates a persisted boundary before admitting any child.
    pub fn verify(&self, limits: Limits) -> Result<()> {
        let prepared = PreparedModelInput::prepare_with_policy(
            self.request.clone(),
            limits,
            self.option_policy.as_ref(),
        )?;
        validate_rejection_evidence(prepared.request(), &self.rejection_evidence)?;
        self.prefix.verify(&prepared)
    }
}

fn validate_rejection_evidence(
    request: &ModelRequest,
    rejections: &[crate::tool::ToolRejectionFeedback],
) -> Result<()> {
    let definitions = request
        .tools
        .iter()
        .map(|tool| (tool.name.as_str(), tool))
        .collect::<BTreeMap<_, _>>();
    let mut pending = BTreeMap::new();
    let mut observed = Vec::new();
    for message in &request.messages {
        let parts = match &message.content {
            ModelContent::Text(_) => &[][..],
            ModelContent::Part(part) => std::slice::from_ref(part),
            ModelContent::Parts(parts) => parts.as_slice(),
        };
        for part in parts {
            match part {
                ModelContentPart::ToolCall { call_id, name, arguments } => {
                    crate::tool::ToolInvocation::validate_identity(call_id, name)?;
                    if message.role != ModelRole::Assistant {
                        return Err(Error::Invalid("tool call has invalid model role".into()));
                    }
                    let definition = definitions.get(name.as_str()).ok_or_else(|| {
                        Error::Invalid("tool call names an unregistered tool".into())
                    })?;
                    let malformed = crate::tool::validate_value(
                        &definition.input_schema,
                        arguments,
                        "tool input",
                    )
                    .is_err();
                    if pending
                        .insert(call_id.clone(), (name.clone(), arguments.clone(), malformed))
                        .is_some()
                    {
                        return Err(Error::Conflict(
                            "rejection evidence contains a duplicate pending call identity".into(),
                        ));
                    }
                }
                ModelContentPart::ToolResult { call_id, name, value } => {
                    if message.role != ModelRole::Tool {
                        return Err(Error::Invalid("tool result has invalid model role".into()));
                    }
                    let Some((called_name, arguments, malformed)) = pending.remove(call_id) else {
                        return Err(Error::Invalid("tool result is not paired".into()));
                    };
                    if called_name != *name {
                        return Err(Error::Conflict(
                            "rejection feedback changed the rejected tool".into(),
                        ));
                    }
                    let definition = definitions
                        .get(name.as_str())
                        .ok_or_else(|| Error::Storage("tool definition disappeared".into()))?;
                    let feedback = crate::tool::ToolRejectionFeedback::from_model_value(value)?;
                    let invocation = crate::tool::ToolInvocation {
                        operation_id: crate::OperationId::new(),
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments,
                    };
                    if malformed {
                        let Some(feedback) = feedback else {
                            return Err(Error::Invalid(
                                "malformed tool call lacks typed rejection evidence".into(),
                            ));
                        };
                        let error = value
                            .get("error")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| Error::Invalid("rejection error is missing".into()))?;
                        let expected = crate::tool::ToolRejectionFeedback::invalid_arguments(
                            &invocation,
                            &definition.input_schema,
                            error,
                        )?;
                        if feedback != expected {
                            return Err(Error::Conflict(
                                "rejection feedback does not match the rejected call".into(),
                            ));
                        }
                        observed.push(feedback);
                    } else {
                        if feedback.is_some() {
                            return Err(Error::Invalid(
                                "valid tool call cannot carry rejection feedback".into(),
                            ));
                        }
                        crate::tool::validate_value(
                            &definition.model_output_schema,
                            value,
                            "tool projection",
                        )?;
                    }
                }
                _ => {}
            }
        }
    }
    if !pending.is_empty() {
        return Err(Error::Invalid("rejection evidence has an unfinished tool call".into()));
    }
    if observed.len() != rejections.len() || observed != rejections {
        return Err(Error::Conflict(
            "rejection feedback does not exactly match the pinned malformed calls".into(),
        ));
    }
    Ok(())
}

/// Explicit context stage appending a child's declaration after a frozen prefix.
/// Private scratch is declared in the suffix; inherited scratch is never rewritten.
pub struct InheritedModelContext {
    boundary: CompletedModelBoundary,
    suffix: Vec<ModelMessage>,
}
impl InheritedModelContext {
    /// Pins all inherited values and a caller-declared child-only suffix.
    pub fn new(
        boundary: CompletedModelBoundary,
        suffix: Vec<ModelMessage>,
        limits: Limits,
    ) -> Result<Self> {
        boundary.verify(limits)?;
        if suffix.is_empty() {
            return Err(Error::Invalid(
                "child context suffix must be explicit".into(),
            ));
        }
        let mut request = boundary.request.clone();
        request.messages.extend(suffix.iter().cloned());
        let prepared = PreparedModelInput::prepare_with_policy(
            request,
            limits,
            boundary.option_policy.as_ref(),
        )?;
        prepared.validate_complete_exchange()?;
        boundary.prefix.verify(&prepared)?;
        Ok(Self { boundary, suffix })
    }

    /// Require exactly the frozen inherited request, declared suffix, and all
    /// current authoritative own messages, with no undeclared middle context.
    pub fn verify_composition(
        &self,
        request: &ModelRequest,
        authoritative: &[ModelMessage],
        limits: Limits,
    ) -> Result<()> {
        self.boundary.verify(limits)?;
        let prepared = PreparedModelInput::prepare_with_policy(
            request.clone(),
            limits,
            self.boundary.option_policy.as_ref(),
        )?;
        prepared.validate_complete_exchange()?;
        self.boundary.prefix.verify(&prepared)?;
        let mut expected = self.boundary.request.messages.clone();
        expected.extend(self.suffix.iter().cloned());
        expected.extend(authoritative.iter().cloned());
        if expected != request.messages {
            return Err(Error::Conflict(
                "recursive fork differs from declared inherited composition".into(),
            ));
        }
        Ok(())
    }
}
impl crate::context::ContextStage for InheritedModelContext {
    fn name(&self) -> &str {
        "inherited-model-prefix"
    }
    fn contract(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name(), "revision": MODEL_INPUT_VERSION,
            "prefix_digest": self.boundary.prefix.digest(), "suffix": self.suffix,
        })
    }
    fn apply<'a>(
        &'a self,
        _: &'a crate::context::ContextInput,
        mut context: crate::context::Context,
    ) -> futures::future::BoxFuture<'a, Result<crate::context::Context>> {
        Box::pin(async move {
            let mut messages = self.boundary.request.messages.clone();
            messages.extend(self.suffix.iter().cloned());
            messages.append(&mut context.messages);
            context.messages = messages;
            if !context.rejection_evidence.is_empty()
                && context.rejection_evidence != self.boundary.rejection_evidence
            {
                return Err(Error::Conflict(
                    "recursive context changed inherited rejection evidence".into(),
                ));
            }
            context.rejection_evidence = self.boundary.rejection_evidence.clone();
            Ok(context)
        })
    }
}

/// Provider boundary that enforces an inherited prefix without owning a loop.
///
/// The caller persists the prefix with its fork state and reconstructs this
/// adapter on recovery. Admission is also checked before replay/reconciliation.
pub struct PrefixBoundModelProvider {
    prefix: FrozenModelPrefix,
    limits: Limits,
    option_policy: Option<ModelOptionPolicy>,
    provider: std::sync::Arc<dyn crate::model::ModelProvider>,
}
impl PrefixBoundModelProvider {
    /// Binds an immutable prefix, local bounds, and the actual model adapter.
    pub fn new(
        prefix: FrozenModelPrefix,
        limits: Limits,
        provider: std::sync::Arc<dyn crate::model::ModelProvider>,
    ) -> Result<Self> {
        limits.validate()?;
        if prefix.version != MODEL_INPUT_VERSION
            || prefix.message_bytes.is_empty()
            || prefix.digest != prefix_digest(prefix.binding_digest, &prefix.message_bytes)?
        {
            return Err(Error::Invalid(
                "frozen model prefix integrity failed".into(),
            ));
        }
        let option_policy = provider.model_option_policy().cloned();
        Ok(Self {
            prefix,
            limits,
            option_policy,
            provider,
        })
    }
    fn admit_prepared(&self, prepared: &PreparedModelInput) -> Result<()> {
        self.prefix.verify(prepared)?;
        prepared.validate_complete_exchange()?;
        self.provider.admit(prepared.request())
    }
}
impl crate::model::ModelProvider for PrefixBoundModelProvider {
    fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
        self.option_policy.as_ref()
    }

    fn admit(&self, request: &ModelRequest) -> Result<()> {
        let input = PreparedModelInput::prepare_with_policy(
            request.clone(),
            self.limits,
            self.option_policy.as_ref(),
        )?;
        self.prefix.verify(&input)?;
        input.validate_complete_exchange()?;
        self.provider.admit(request)
    }
    fn generate<'a>(
        &'a self,
        prepared: PreparedModelInput,
    ) -> futures::stream::BoxStream<'a, Result<crate::model::ModelEvent>> {
        if let Err(error) = self.admit_prepared(&prepared) {
            return Box::pin(futures::stream::iter(vec![Err(error)]));
        }
        self.provider.generate(prepared)
    }
    fn reconcile_admitted<'a>(
        &'a self,
        prepared: PreparedModelInput,
        attempt: crate::model::ModelAttempt,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        Box::pin(async move {
            self.admit_prepared(&prepared)?;
            if prepared.manifest().request_digest != attempt.request_digest {
                return Err(Error::Conflict(
                    "reconciliation request digest changed".into(),
                ));
            }
            self.provider.reconcile_admitted(prepared, attempt).await
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: crate::model::ModelAttempt,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "prefix recovery requires the verified original request".into(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use serde_json::json;
    fn text(value: &str) -> ModelMessage {
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(value.into()),
        }
    }
    fn request() -> Result<ModelRequest> {
        Ok(ModelRequest {
            model: Model::new("mock", "swarm", "1", json!({}))?,
            messages: vec![
                text("  \u{03bb} \u{1f980}\r\n"),
                text("task"),
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: "fork-1".into(),
                        name: "fork".into(),
                        arguments: json!({}),
                    }),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "fork-1".into(),
                        name: "fork".into(),
                        value: json!({"child":"a"}),
                    }),
                },
            ],
            tools: vec![crate::tool::ToolDefinition {
                name: "fork".into(),
                revision: "1".into(),
                description: "fork test tool".into(),
                input_schema: json!({"type": "object", "additionalProperties": false}),
                output_schema: json!({}),
                model_output_schema: json!({}),
            }],
            max_output_tokens: Some(100),
        })
    }
    #[test]
    fn tool_messages_require_nonempty_paired_results() -> Result<()> {
        for content in [ModelContent::Text("forged".into()), ModelContent::Parts(Vec::new())] {
            let mut input = request()?;
            input.messages.push(ModelMessage { role: ModelRole::Tool, content });
            assert!(matches!(
                PreparedModelInput::prepare(input, Limits::default()),
                Err(Error::Invalid(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn recursive_composition_rejects_undeclared_context_and_changed_suffix() -> Result<()> {
        let limits = Limits::default();
        let boundary = CompletedModelBoundary::capture(request()?, limits)?;
        let suffix = vec![text("notification; explicit task; fresh scratch")];
        let declaration = InheritedModelContext::new(boundary.clone(), suffix.clone(), limits)?;
        let own = vec![text("authoritative child input \u{03bb}\n"), text("child result")];
        let mut completed = boundary.request.clone();
        completed.messages.extend(suffix);
        completed.messages.extend(own.iter().cloned());
        declaration.verify_composition(&completed, &own, limits)?;
        let mut hidden = completed.clone();
        hidden.messages.insert(
            boundary.request.messages.len() + 1,
            text("undeclared sibling history"),
        );
        assert!(
            declaration
                .verify_composition(&hidden, &own, limits)
                .is_err()
        );
        let changed = InheritedModelContext::new(boundary, vec![text("changed task")], limits)?;
        assert!(
            changed
                .verify_composition(&completed, &own, limits)
                .is_err()
        );
        let mut reordered = own;
        reordered.reverse();
        assert!(
            declaration
                .verify_composition(&completed, &reordered, limits)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn recursive_prefix_survives_persistence_with_suffix_only_context() -> Result<()> {
        let parent = PreparedModelInput::prepare(request()?, Limits::default())?;
        let frozen = FrozenModelPrefix::capture(&parent, 4)?;
        let file = tempfile::NamedTempFile::new().map_err(|e| Error::Storage(e.to_string()))?;
        std::fs::write(
            file.path(),
            serde_json::to_vec(&frozen).map_err(|e| Error::Invalid(e.to_string()))?,
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
        let restored: FrozenModelPrefix = serde_json::from_slice(
            &std::fs::read(file.path()).map_err(|e| Error::Storage(e.to_string()))?,
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
        let mut child = request()?;
        child.messages.push(text("Fork child. Fresh scratch."));
        let child = PreparedModelInput::prepare(child, Limits::default())?;
        restored.verify(&child)?;
        let recursive = FrozenModelPrefix::capture(&child, 5)?;
        let mut grandchild = child.request().clone();
        grandchild
            .messages
            .push(text("Fork grandchild. Fresh scratch."));
        let grandchild = PreparedModelInput::prepare(grandchild, Limits::default())?;
        restored.verify(&grandchild)?;
        recursive.verify(&grandchild)?;
        assert_eq!(restored.message_bytes(), frozen.message_bytes());
        Ok(())
    }
    #[test]
    fn prefix_rejects_content_order_binding_and_corruption() -> Result<()> {
        let parent = PreparedModelInput::prepare(request()?, Limits::default())?;
        let prefix = FrozenModelPrefix::capture(&parent, 4)?;
        let mut changed = request()?;
        changed.messages[0] = text(" \u{03bb} \u{1f980}\r\n");
        assert!(
            prefix
                .verify(&PreparedModelInput::prepare(changed, Limits::default())?)
                .is_err()
        );
        let mut changed = request()?;
        changed.messages.swap(0, 1);
        assert!(prefix
            .verify(&PreparedModelInput::prepare(changed, Limits::default())?)
            .is_err());
        let mut changed = request()?;
        changed.model.revision = "2".into();
        assert!(prefix
            .verify(&PreparedModelInput::prepare(changed, Limits::default())?)
            .is_err());
        let mut corrupt = prefix;
        corrupt.message_bytes[0].push(b' ');
        assert!(corrupt.verify(&parent).is_err());
        Ok(())
    }

    #[test]
    fn option_policy_identity_binds_prepared_input_and_frozen_prefix() -> Result<()> {
        let policy = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "mock.options".into(),
                version: "1".into(),
                digest: [8; 32],
            },
            json!({"type": "object", "additionalProperties": false}),
        )?;
        let changed = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "mock.options".into(),
                version: "1".into(),
                digest: [8; 32],
            },
            json!({
                "type": "object",
                "properties": {"mode": {"type": "string"}},
                "additionalProperties": false,
            }),
        )?;
        let request = request()?;
        let first = PreparedModelInput::prepare_with_policy(
            request.clone(),
            Limits::default(),
            Some(&policy),
        )?;
        let second = PreparedModelInput::prepare_with_policy(
            request,
            Limits::default(),
            Some(&changed),
        )?;
        assert_eq!(first.bytes(), second.bytes());
        assert_ne!(first.manifest().binding_digest, second.manifest().binding_digest);
        assert_eq!(first.manifest().model_option_policy, Some(policy.identity.clone()));
        assert_ne!(
            first.manifest().model_option_schema_digest,
            second.manifest().model_option_schema_digest
        );
        let prefix = FrozenModelPrefix::capture(&first, first.request().messages.len())?;
        assert!(matches!(
            prefix.verify(&second),
            Err(Error::Conflict(message)) if message == "fork changed model or tool definitions"
        ));
        Ok(())
    }

    #[test]
    fn option_policy_mutation_changes_frozen_binding_and_invalidates_admission() -> Result<()> {
        let mut policy = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "mock.options".into(),
                version: "1".into(),
                digest: [8; 32],
            },
            json!({"type": "object", "additionalProperties": false}),
        )?;
        let request = request()?;
        let first = PreparedModelInput::prepare_with_policy(
            request.clone(),
            Limits::default(),
            Some(&policy),
        )?;
        policy.schema = json!({"type": "object", "properties": {"mode": {"type": "string"}}});
        let second = PreparedModelInput::prepare_with_policy(
            request,
            Limits::default(),
            Some(&policy),
        )?;
        assert_ne!(
            first.manifest().model_option_schema_digest,
            second.manifest().model_option_schema_digest
        );
        let prefix = FrozenModelPrefix::capture(&first, first.request().messages.len())?;
        assert!(prefix.verify(&second).is_err());
        policy.schema = json!({"type": "not-a-schema-type"});
        assert!(policy.validate(&json!({})).is_err());
        policy.identity.name.clear();
        assert!(policy.validate(&json!({})).is_err());
        Ok(())
    }

    #[test]
    fn unregistered_model_options_cannot_reach_prepared_request() -> Result<()> {
        let mut request = request()?;
        request.model.options = json!({"credential": "transport-secret"});
        assert!(matches!(
            PreparedModelInput::prepare(request, Limits::default()),
            Err(Error::Invalid(message)) if message == "model options require a registered provider policy"
        ));
        Ok(())
    }

    #[test]
    fn fork_requires_complete_matching_tool_exchange() -> Result<()> {
        let parent = PreparedModelInput::prepare(request()?, Limits::default())?;
        assert!(FrozenModelPrefix::capture(&parent, 3).is_err());
        let mut orphan = request()?;
        orphan.messages.remove(2);
        assert!(
            PreparedModelInput::prepare(orphan, Limits::default())
                .and_then(|prepared| prepared.validate_complete_exchange())
                .is_err()
        );
        let mut mismatch = request()?;
        mismatch.messages[3].content = ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "fork-1".into(),
            name: "other".into(),
            value: json!({}),
        });
        assert!(
            PreparedModelInput::prepare(mismatch, Limits::default())
                .and_then(|prepared| prepared.validate_complete_exchange())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn malformed_historical_call_requires_correlated_versioned_feedback() -> Result<()> {
        let mut malformed = request()?;
        if let ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) =
            &mut malformed.messages[2].content
        {
            *arguments = json!({"unexpected": true});
        }
        assert!(PreparedModelInput::prepare(malformed.clone(), Limits::default()).is_err());

        let invocation = crate::tool::ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "fork-1".into(),
            name: "fork".into(),
            arguments: json!({"unexpected": true}),
        };
        let definition = &malformed
            .tools
            .iter()
            .find(|tool| tool.name == "fork")
            .expect("fork definition");
        let feedback = crate::tool::ToolRejectionFeedback::invalid_arguments(
            &invocation,
            &definition.input_schema,
            "invalid",
        )?;
        malformed.messages[3] = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "fork-1".into(),
                name: "fork".into(),
                value: feedback.to_model_value("invalid")?,
            }),
        };
        PreparedModelInput::prepare(malformed.clone(), Limits::default())?;
        let prepared = PreparedModelInput::prepare(malformed.clone(), Limits::default())?
            .with_rejection_evidence(vec![feedback.clone()])?;
        assert_eq!(
            prepared.bytes(),
            crate::contract::canonical_json_bytes(prepared.request())?
        );
        let restored: ModelInputManifest = serde_json::from_slice(
            &serde_json::to_vec(prepared.manifest())
                .map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(restored.rejection_evidence, vec![feedback.clone()]);
        assert!(CompletedModelBoundary::capture(malformed.clone(), Limits::default()).is_err());
        let boundary = CompletedModelBoundary::capture_with_rejections(
            malformed.clone(),
            Limits::default(),
            std::slice::from_ref(&feedback),
        )?;
        let inherited = InheritedModelContext::new(
            boundary.clone(),
            vec![text("child task; fresh scratch")],
            Limits::default(),
        )?;
        let mut composed = boundary.request.clone();
        composed.messages.push(text("child task; fresh scratch"));
        inherited.verify_composition(&composed, &[], Limits::default())?;

        // A fork cannot make the model-visible rejection self-authenticating
        // by dropping the durable evidence from the inherited boundary.
        let mut forged_boundary = boundary;
        forged_boundary.rejection_evidence.clear();
        assert!(
            InheritedModelContext::new(
                forged_boundary,
                vec![text("child task; fresh scratch")],
                Limits::default(),
            )
            .is_err()
        );

        if let ModelContent::Part(ModelContentPart::ToolResult { value, .. }) =
            &mut malformed.messages[3].content
        {
            value["rejection"]["call_id"] = json!("forged");
        }
        assert!(PreparedModelInput::prepare(malformed, Limits::default()).is_err());
        Ok(())
    }

    #[test]
    fn historical_projection_is_checked_against_the_pinned_model_schema() -> Result<()> {
        let mut forged = request()?;
        forged.tools[0].model_output_schema = json!({
            "type": "object",
            "required": ["child"],
            "additionalProperties": false,
        });
        if let ModelContent::Part(ModelContentPart::ToolResult { value, .. }) =
            &mut forged.messages[3].content
        {
            *value = json!({"other": "forged"});
        }
        assert!(PreparedModelInput::prepare(forged, Limits::default()).is_err());
        Ok(())
    }
    #[test]
    fn aggregate_limit_does_not_silently_truncate() -> Result<()> {
        let mut input = request()?;
        input.messages = vec![text(&"x".repeat(1024)); 3];
        assert!(PreparedModelInput::prepare(
            input,
            Limits {
                file_bytes: 2048,
                render_bytes: 2048,
                ..Limits::default()
            }
        )
        .is_err());
        Ok(())
    }
    #[test]
    fn manifest_binds_exact_dispatch_bytes() -> Result<()> {
        let prepared = PreparedModelInput::prepare(request()?, Limits::default())?;
        let bytes = crate::contract::canonical_json_bytes(prepared.request())?;
        assert_eq!(bytes, prepared.bytes());
        assert_eq!(
            *blake3::hash(&bytes).as_bytes(),
            prepared.manifest().request_digest
        );
        for (message, manifest) in prepared
            .request()
            .messages
            .iter()
            .zip(&prepared.manifest().messages)
        {
            assert_eq!(
                crate::contract::canonical_json_digest(message)?,
                manifest.digest
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn prefix_provider_rejects_before_downstream_dispatch() -> Result<()> {
        use crate::model::{ModelAttempt, ModelEvent, ModelProvider};
        use futures::{future::BoxFuture, stream::BoxStream, StreamExt};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        struct Capture(AtomicUsize);
        impl ModelProvider for Capture {
            fn generate<'a>(&'a self, _: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(futures::stream::empty())
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Ok(None) })
            }
        }
        let input = PreparedModelInput::prepare(request()?, Limits::default())?;
        let prefix = FrozenModelPrefix::capture(&input, input.request().messages.len())?;
        let capture = Arc::new(Capture(AtomicUsize::new(0)));
        let provider = PrefixBoundModelProvider::new(prefix, Limits::default(), capture.clone())?;
        let mut child = input.request().clone();
        child.messages.push(text("explicit child task"));
        assert!(provider.admit(&child).is_ok());
        assert!(provider
            .generate(PreparedModelInput::prepare(child.clone(), Limits::default())?)
            .next()
            .await
            .is_none());
        assert_eq!(capture.0.load(Ordering::SeqCst), 1);
        let attempt = crate::model::ModelAttempt {
            operation_id: crate::OperationId::new(),
            step: 0,
            request_digest: crate::contract::canonical_json_digest(&child)?,
            observed: vec![],
        };
        assert!(provider.reconcile(attempt.clone()).await.is_err());
        assert!(provider
            .reconcile_admitted(
                PreparedModelInput::prepare(child.clone(), Limits::default())?,
                attempt.clone(),
            )
            .await?
            .is_none());
        let mut corrupt_attempt = attempt.clone();
        corrupt_attempt.request_digest = [0; 32];
        assert!(provider
            .reconcile_admitted(
                PreparedModelInput::prepare(child.clone(), Limits::default())?,
                corrupt_attempt,
            )
            .await
            .is_err());
        child.messages[0] = text("changed inherited content");
        assert!(provider
            .reconcile_admitted(
                PreparedModelInput::prepare(child.clone(), Limits::default())?,
                attempt,
            )
            .await
            .is_err());
        assert!(provider.admit(&child).is_err());
        assert!(provider
            .generate(PreparedModelInput::prepare(child, Limits::default())?)
            .next()
            .await
            .unwrap()
            .is_err());
        assert_eq!(capture.0.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn prefix_provider_rejects_same_options_with_a_different_policy_before_dispatch(
    ) -> Result<()> {
        use crate::model::{ModelAttempt, ModelEvent, ModelProvider};
        use futures::{future::BoxFuture, stream::BoxStream, StreamExt};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        struct PolicyProvider {
            policy: ModelOptionPolicy,
            calls: AtomicUsize,
        }
        impl ModelProvider for PolicyProvider {
            fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
                Some(&self.policy)
            }
            fn generate<'a>(&'a self, _: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(futures::stream::empty())
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Ok(None) })
            }
        }

        let schema = json!({
            "type": "object",
            "additionalProperties": false,
        });
        let first_policy = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "test.model-options".into(),
                version: "first".into(),
                digest: [31; 32],
            },
            schema.clone(),
        )?;
        let second_policy = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "test.model-options".into(),
                version: "second".into(),
                digest: [32; 32],
            },
            schema,
        )?;
        let request = request()?;
        let parent = PreparedModelInput::prepare_with_policy(
            request.clone(),
            Limits::default(),
            Some(&first_policy),
        )?;
        let prefix = FrozenModelPrefix::capture(&parent, parent.request().messages.len())?;
        let downstream = Arc::new(PolicyProvider {
            policy: second_policy.clone(),
            calls: AtomicUsize::new(0),
        });
        let provider =
            PrefixBoundModelProvider::new(prefix, Limits::default(), downstream.clone())?;
        let supplied = PreparedModelInput::prepare_with_policy(
            request,
            Limits::default(),
            Some(&second_policy),
        )?;
        let result = provider
            .generate(supplied)
            .next()
            .await
            .expect("prefix rejection event");
        assert!(result.is_err());
        assert_eq!(downstream.calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn prefix_reconciliation_receives_original_prepared_bytes_manifest_and_policy(
    ) -> Result<()> {
        use crate::model::{ModelAttempt, ModelEvent, ModelProvider};
        use futures::{future::BoxFuture, stream::BoxStream};
        use std::sync::{Arc, Mutex};

        struct ReconciliationCapture {
            policy: ModelOptionPolicy,
            seen: Mutex<Vec<(Vec<u8>, ModelInputManifest)>>,
        }
        impl ModelProvider for ReconciliationCapture {
            fn model_option_policy(&self) -> Option<&ModelOptionPolicy> {
                Some(&self.policy)
            }
            fn generate<'a>(&'a self, _: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
                Box::pin(futures::stream::empty())
            }
            fn reconcile_admitted<'a>(
                &'a self,
                prepared: PreparedModelInput,
                attempt: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                let bytes = prepared.bytes().to_vec();
                let manifest = prepared.manifest().clone();
                Box::pin(async move {
                    if manifest.request_digest != attempt.request_digest {
                        return Err(Error::Conflict(
                            "reconciliation request digest changed".into(),
                        ));
                    }
                    self.seen
                        .lock()
                        .map_err(|_| Error::Storage("capture lock poisoned".into()))?
                        .push((bytes, manifest));
                    Ok(None)
                })
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Ok(None) })
            }
        }

        let policy = ModelOptionPolicy::new(
            ComponentIdentity {
                name: "test.restart-options".into(),
                version: "7".into(),
                digest: [41; 32],
            },
            json!({
                "type": "object",
                "additionalProperties": false,
            }),
        )?;
        let prepared =
            PreparedModelInput::prepare_with_policy(request()?, Limits::default(), Some(&policy))?;
        let expected_bytes = prepared.bytes().to_vec();
        let expected_manifest = prepared.manifest().clone();
        let prefix = FrozenModelPrefix::capture(&prepared, prepared.request().messages.len())?;
        let downstream = Arc::new(ReconciliationCapture {
            policy: policy.clone(),
            seen: Mutex::new(Vec::new()),
        });
        let provider =
            PrefixBoundModelProvider::new(prefix, Limits::default(), downstream.clone())?;
        provider
            .reconcile_admitted(
                prepared.clone(),
                ModelAttempt {
                    operation_id: crate::OperationId::new(),
                    step: 0,
                    request_digest: prepared.manifest().request_digest,
                    observed: Vec::new(),
                },
            )
            .await?;
        let seen = downstream
            .seen
            .lock()
            .map_err(|_| Error::Storage("capture lock poisoned".into()))?;
        assert_eq!(seen.as_slice(), &[(expected_bytes, expected_manifest)]);
        assert_eq!(seen[0].1.model_option_policy, Some(policy.identity.clone()));
        assert_eq!(
            seen[0].1.model_option_schema_digest,
            Some(policy.schema_digest()?)
        );
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn production_batch_pins_text_and_all_ordered_results() -> Result<()> {
        use crate::{
            executor::ExecutionEvent,
            filesystem::LocalHarness,
            model::{ModelAttempt, ModelEvent, ModelProvider},
        };
        use futures::{future::BoxFuture, stream::BoxStream};
        use std::sync::{Arc, Mutex};
        struct Script(Mutex<Vec<ModelRequest>>);
        impl ModelProvider for Script {
            fn generate<'a>(&'a self, prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
                let request = prepared.request().clone();
                let mut requests = self.0.lock().unwrap();
                requests.push(request);
                let events = if requests.len() == 1 {
                    vec![
                        ModelEvent::Content {
                            delta: "preserved assistant text".into(),
                        },
                        ModelEvent::ToolCall {
                            call_id: "first".into(),
                            name: "acyclic.stage_file".into(),
                            arguments: json!({"path":"a.txt","text":"a","media_type":"text/plain","display_name":"a.txt"}),
                        },
                        ModelEvent::ToolCall {
                            call_id: "second".into(),
                            name: "acyclic.stage_file".into(),
                            arguments: json!({"path":"b.txt","text":"b","media_type":"text/plain","display_name":"b.txt"}),
                        },
                        ModelEvent::Completed {
                            metadata: json!(null),
                        },
                    ]
                } else {
                    vec![
                        ModelEvent::Content {
                            delta: "done".into(),
                        },
                        ModelEvent::Completed {
                            metadata: json!(null),
                        },
                    ]
                };
                Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Ok(None) })
            }
        }
        let script = Arc::new(Script(Mutex::new(Vec::new())));
        let local =
            LocalHarness::new(Model::new("mock", "batch", "1", json!({}))?, script.clone()).await?;
        let op = crate::OperationId::new();
        let file = local
            .storage()
            .stage(
                op,
                "input.txt",
                b"make two files",
                "text/plain",
                "input.txt",
            )
            .await?;
        local
            .storage()
            .run_conversation(local.bundle(), op, file, vec![], 8)
            .await?;
        let records = local.storage().journal().replay(op).await?;
        let (index, file) = records
            .iter()
            .enumerate()
            .find_map(|(index, record)| match &record.event {
                ExecutionEvent::ToolBatchCompleted { step: 0, boundary } => Some((index, boundary)),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("missing completed batch".into()))?;
        assert_eq!(
            records[..index]
                .iter()
                .filter(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
                .count(),
            2
        );
        let boundary: CompletedModelBoundary =
            serde_json::from_slice(&local.storage().journal().load(file).await?).unwrap();
        boundary.verify(Limits::default())?;
        let requests = script.0.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(boundary.request, requests[1]);
        assert_eq!(
            boundary.request.messages[1].content,
            ModelContent::Text("preserved assistant text".into())
        );
        let mut child = boundary.request.clone();
        child
            .messages
            .push(text("fork notice; fresh scratch; child task"));
        boundary
            .prefix
            .verify(&PreparedModelInput::prepare(child, Limits::default())?)?;
        drop(requests);
        // Recursive publication is exercised through the real typed
        // filesystem/stream fork path in `model_fork_boundary`. Keeping this
        // production-batch test focused on the one durable boundary avoids a
        // synthetic MemoryStorage loop that cannot prove fork authorization.
        assert!(!boundary.prefix.message_bytes().is_empty());
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn dispatched_provider_bytes_match_journaled_manifest() -> Result<()> {
        use crate::{
            executor::ExecutionEvent,
            filesystem::LocalHarness,
            model::{ModelAttempt, ModelEvent, ModelProvider},
        };
        use futures::{future::BoxFuture, stream::BoxStream};
        use std::sync::{Arc, Mutex};
        struct Capture(Mutex<Vec<(ModelRequest, Vec<u8>, [u8; 32])>>);
        impl ModelProvider for Capture {
            fn generate<'a>(&'a self, prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((
                        prepared.request().clone(),
                        prepared.bytes().to_vec(),
                        prepared.manifest().request_digest,
                    ));
                Box::pin(futures::stream::iter(vec![
                    Ok(ModelEvent::Content {
                        delta: "done".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    }),
                ]))
            }
            fn reconcile<'a>(
                &'a self,
                _: ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                Box::pin(async { Ok(None) })
            }
        }
        let provider = Arc::new(Capture(Mutex::new(Vec::new())));
        let local = LocalHarness::new(
            Model::new("mock", "capture", "1", json!({}))?,
            provider.clone(),
        )
        .await?;
        let operation = crate::OperationId::new();
        let content = local
            .storage()
            .stage(
                operation,
                "input.txt",
                "Whitespace:  \u{03bb} \u{1f980}\r\n".as_bytes(),
                "text/plain",
                "input.txt",
            )
            .await?;
        let output = local
            .storage()
            .run_conversation(local.bundle(), operation, content, vec![], 8)
            .await?;
        assert_eq!(output.text, "done");
        let records = local.storage().journal().replay(operation).await?;
        let manifest_file = records
            .iter()
            .find_map(|record| match &record.event {
                ExecutionEvent::ModelInputPrepared { manifest, .. } => Some(manifest),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("input manifest missing".into()))?;
        assert!(matches!(
            local.storage().read(manifest_file).await,
            Err(Error::Unauthorized(_))
        ));
        let bytes = local.storage().journal().load(manifest_file).await?;
        let manifest: ModelInputManifest =
            serde_json::from_slice(&bytes).map_err(|e| Error::Invalid(e.to_string()))?;
        let requests = provider
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 1);
        let (received, wire_bytes, wire_digest) = requests
            .first()
            .ok_or_else(|| Error::Storage("no provider request".into()))?;
        assert_eq!(
            *blake3::hash(wire_bytes).as_bytes(),
            manifest.request_digest
        );
        assert_eq!(*blake3::hash(wire_bytes).as_bytes(), *wire_digest);
        assert_eq!(wire_bytes, &crate::contract::canonical_json_bytes(received)?);
        assert!(!wire_bytes
            .windows(b"metadata".len())
            .any(|window| window == b"metadata"));
        let manifest_index = records
            .iter()
            .position(|record| matches!(&record.event, ExecutionEvent::ModelInputPrepared { .. }));
        let started_index = records
            .iter()
            .position(|record| matches!(&record.event, ExecutionEvent::ModelStarted { .. }));
        assert!(manifest_index < started_index);
        Ok(())
    }
}
