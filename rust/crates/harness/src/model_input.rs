//! Versioned model-input admission and immutable fork prefixes.
use crate::{
    Error, Result,
    conversation::{FileRef, Limits},
    model::{ModelContent, ModelContentPart, ModelMessage, ModelRequest, ModelRole},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Canonical model-input encoding version.
pub const MODEL_INPUT_VERSION: u32 = 1;

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
    /// Ordered message identities.
    pub messages: Vec<InputMessageManifest>,
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
        limits.validate()?;
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
        let manifest = ModelInputManifest {
            version: MODEL_INPUT_VERSION,
            binding_digest: binding_digest(&request)?,
            request_digest: *blake3::hash(&bytes).as_bytes(),
            messages,
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
    /// Dispatches the admitted values without reconstruction.
    #[must_use]
    pub fn into_request(self) -> ModelRequest {
        self.request
    }
    /// Requires complete paired tool exchanges at a fork boundary.
    pub fn validate_complete_exchange(&self) -> Result<()> {
        validate_exchanges(&self.request.messages)
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
        validate_exchanges(messages)?;
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
        if self.version != MODEL_INPUT_VERSION
            || self.message_bytes.is_empty()
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
        validate_exchanges(messages)
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
fn binding_digest(request: &ModelRequest) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(MODEL_INPUT_VERSION, &request.model, &request.tools))
}
fn prefix_digest(binding: [u8; 32], messages: &[Vec<u8>]) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(MODEL_INPUT_VERSION, binding, messages))
}
fn validate_exchanges(messages: &[ModelMessage]) -> Result<()> {
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
        for part in parts {
            match part {
                ModelContentPart::ToolCall { call_id, name, .. } => {
                    if message.role != ModelRole::Assistant
                        || call_id.is_empty()
                        || pending.contains_key(call_id)
                    {
                        return Err(Error::Invalid(
                            "invalid or duplicate inherited tool call".into(),
                        ));
                    }
                    pending.insert(call_id, name);
                }
                ModelContentPart::ToolResult { call_id, name, .. } => {
                    if message.role != ModelRole::Tool || pending.remove(call_id) != Some(name) {
                        return Err(Error::Invalid("inherited tool result is not paired".into()));
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

/// Provider boundary that enforces an inherited prefix without owning a loop.
///
/// The caller persists the prefix with its fork state and reconstructs this
/// adapter on recovery. Admission is also checked before replay/reconciliation.
pub struct PrefixBoundModelProvider {
    prefix: FrozenModelPrefix,
    limits: Limits,
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
        Ok(Self {
            prefix,
            limits,
            provider,
        })
    }
}
impl crate::model::ModelProvider for PrefixBoundModelProvider {
    fn admit(&self, request: &ModelRequest) -> Result<()> {
        let input = PreparedModelInput::prepare(request.clone(), self.limits)?;
        self.prefix.verify(&input)?;
        input.validate_complete_exchange()?;
        self.provider.admit(request)
    }
    fn generate<'a>(
        &'a self,
        request: ModelRequest,
    ) -> futures::stream::BoxStream<'a, Result<crate::model::ModelEvent>> {
        if let Err(error) = self.admit(&request) {
            return Box::pin(futures::stream::iter(vec![Err(error)]));
        }
        self.provider.generate(request)
    }
    fn reconcile<'a>(
        &'a self,
        attempt: crate::model::ModelAttempt,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
        self.provider.reconcile(attempt)
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
                text("  λ 🦀\r\n"),
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
            tools: vec![],
            max_output_tokens: Some(100),
        })
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
        changed.messages[0] = text(" λ 🦀\r\n");
        assert!(
            prefix
                .verify(&PreparedModelInput::prepare(changed, Limits::default())?)
                .is_err()
        );
        let mut changed = request()?;
        changed.messages.swap(0, 1);
        assert!(
            prefix
                .verify(&PreparedModelInput::prepare(changed, Limits::default())?)
                .is_err()
        );
        let mut changed = request()?;
        changed.model.revision = "2".into();
        assert!(
            prefix
                .verify(&PreparedModelInput::prepare(changed, Limits::default())?)
                .is_err()
        );
        let mut corrupt = prefix;
        corrupt.message_bytes[0].push(b' ');
        assert!(corrupt.verify(&parent).is_err());
        Ok(())
    }
    #[test]
    fn fork_requires_complete_matching_tool_exchange() -> Result<()> {
        let parent = PreparedModelInput::prepare(request()?, Limits::default())?;
        assert!(FrozenModelPrefix::capture(&parent, 3).is_err());
        let mut orphan = request()?;
        orphan.messages.remove(2);
        assert!(
            PreparedModelInput::prepare(orphan, Limits::default())?
                .validate_complete_exchange()
                .is_err()
        );
        let mut mismatch = request()?;
        mismatch.messages[3].content = ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "fork-1".into(),
            name: "other".into(),
            value: json!({}),
        });
        assert!(
            PreparedModelInput::prepare(mismatch, Limits::default())?
                .validate_complete_exchange()
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn aggregate_limit_does_not_silently_truncate() -> Result<()> {
        let mut input = request()?;
        input.messages = vec![text(&"x".repeat(1024)); 3];
        assert!(
            PreparedModelInput::prepare(
                input,
                Limits {
                    file_bytes: 2048,
                    render_bytes: 2048,
                    ..Limits::default()
                }
            )
            .is_err()
        );
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
        use futures::{StreamExt, future::BoxFuture, stream::BoxStream};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Capture(AtomicUsize);
        impl ModelProvider for Capture {
            fn generate<'a>(&'a self, _: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
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
        assert!(provider.generate(child.clone()).next().await.is_none());
        assert_eq!(capture.0.load(Ordering::SeqCst), 1);
        child.messages[0] = text("changed inherited content");
        assert!(provider.admit(&child).is_err());
        assert!(provider.generate(child).next().await.unwrap().is_err());
        assert_eq!(capture.0.load(Ordering::SeqCst), 1);
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
        struct Capture(Mutex<Vec<ModelRequest>>);
        impl ModelProvider for Capture {
            fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(request);
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
                "Whitespace:  λ 🦀\r\n".as_bytes(),
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
        let bytes = local.storage().read(manifest_file).await?;
        let manifest: ModelInputManifest =
            serde_json::from_slice(&bytes).map_err(|e| Error::Invalid(e.to_string()))?;
        let requests = provider
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 1);
        let received = requests
            .first()
            .ok_or_else(|| Error::Storage("no provider request".into()))?;
        assert_eq!(
            crate::contract::canonical_json_digest(received)?,
            manifest.request_digest
        );
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
