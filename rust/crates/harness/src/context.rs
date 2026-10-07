//! Ordered function-based context assembly.

use crate::contract::next_revision;
use crate::{
    Result,
    conversation::{ContentResidencyVerifier, FileRef},
    model::{ModelContent, ModelContentPart, ModelMessage, ModelRole},
    projection::SelectedModelContext,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey as StreamIdempotencyKey, MAX_RECORD_BYTES,
    ReadRequest, StreamError, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt as _;

mod selection;
pub use selection::*;

/// Immutable reference proving which pre-compaction context was summarized.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactionReference {
    /// BLAKE3 digest of the exact serialized source context.
    pub source_digest: [u8; 32],
    /// Number of source messages before compaction.
    pub source_messages: u32,
    /// Number of model-visible messages retained after compaction.
    pub retained_messages: u32,
}

/// One versioned durable context revision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextRevision {
    /// Stable record format version.
    pub format_version: u32,
    /// One-based gapless journal revision.
    pub revision: u64,
    /// Logical provider role such as `memory`, `retrieval`, or `skills`.
    pub source: String,
    /// Exact provider implementation revision.
    pub source_revision: String,
    /// Reconstructable context value.
    pub context: Context,
    /// Present only when this revision deterministically compacts another context.
    pub compaction: Option<CompactionReference>,
}

/// Stream-backed context source shared by memory, retrieval, skills, and compaction stages.
pub struct DurableContextProvider {
    provider: Arc<dyn StreamProvider>,
    path: StreamPath,
    source: String,
    source_revision: String,
    maximum_revisions: u32,
    content_verifier: Arc<dyn ContentResidencyVerifier>,
}

impl DurableContextProvider {
    /// Creates a bounded durable provider over one permanent Stream path.
    pub fn new(
        provider: Arc<dyn StreamProvider>,
        path: StreamPath,
        source: impl Into<String>,
        source_revision: impl Into<String>,
        maximum_revisions: u32,
        content_verifier: Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        let source = source.into();
        let source_revision = source_revision.into();
        if source.trim().is_empty() || source_revision.trim().is_empty() || maximum_revisions == 0 {
            return Err(crate::Error::Invalid(
                "durable context source, revision, and bound are required".into(),
            ));
        }
        Ok(Self {
            provider,
            path,
            source,
            source_revision,
            maximum_revisions,
            content_verifier,
        })
    }

    /// Appends one immutable context revision with exact retry and tail-CAS semantics.
    pub async fn append(
        &self,
        expected_revision: u64,
        context: Context,
        compaction: Option<CompactionReference>,
        idempotency_key: impl Into<Bytes>,
    ) -> Result<ContextRevision> {
        validate_context_refs(&context, self.content_verifier.as_ref()).await?;
        let revision = next_revision(expected_revision)?;
        if revision > u64::from(self.maximum_revisions) {
            return Err(crate::Error::Invalid(
                "durable context revision bound exceeded".into(),
            ));
        }
        if let Some(reference) = &compaction {
            let source = self.read_revision(expected_revision).await?;
            validate_compaction(reference, &source.context, &context)?;
        }
        let record = ContextRevision {
            format_version: 2,
            revision,
            source: self.source.clone(),
            source_revision: self.source_revision.clone(),
            context,
            compaction,
        };
        let encoded = crate::contract::canonical_json_bytes(&record)?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(crate::Error::Invalid(
                "durable context record exceeds Stream limit".into(),
            ));
        }
        let key = StreamIdempotencyKey::new(idempotency_key)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        match self
            .provider
            .append(AppendRequest {
                path: self.path.clone(),
                records: vec![Bytes::from(encoded)],
                if_tail: Some(expected_revision),
                idempotency_key: Some(key),
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?
        {
            AppendOutcome::Committed(receipt) if receipt.tail == revision => Ok(record),
            AppendOutcome::Committed(_) => Err(crate::Error::Storage(
                "context append returned an invalid tail".into(),
            )),
            AppendOutcome::TailConflict { actual_tail } => Err(crate::Error::Conflict(format!(
                "context revision {expected_revision} is stale; actual revision is {actual_tail}"
            ))),
        }
    }

    /// Replays and validates the complete bounded revision history.
    pub async fn revisions(&self) -> Result<Vec<ContextRevision>> {
        let tail = match self.provider.tail(self.path.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(crate::Error::Storage(error.to_string())),
        };
        if tail > u64::from(self.maximum_revisions) {
            return Err(crate::Error::Storage(
                "durable context history exceeds configured revision bound".into(),
            ));
        }
        if tail == 0 {
            return Ok(Vec::new());
        }
        let mut stream = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from: 0,
                limit: self.maximum_revisions,
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        let mut revisions: Vec<ContextRevision> = Vec::new();
        while let Some(record) = stream.next().await {
            let record = record.map_err(|error| crate::Error::Storage(error.to_string()))?;
            let revision = self.decode_revision(record.sequence, &record.value).await?;
            if let Some(reference) = &revision.compaction {
                let source = revisions.last().ok_or_else(|| {
                    crate::Error::Storage(
                        "durable context compaction has no preceding source".into(),
                    )
                })?;
                validate_compaction(reference, &source.context, &revision.context)
                    .map_err(|error| crate::Error::Storage(error.to_string()))?;
            }
            revisions.push(revision);
        }
        if revisions.len() as u64 != tail {
            return Err(crate::Error::Storage(
                "durable context tail changed during replay".into(),
            ));
        }
        Ok(revisions)
    }

    async fn decode_revision(&self, sequence: u64, bytes: &[u8]) -> Result<ContextRevision> {
        let revision: ContextRevision = serde_json::from_slice(bytes)
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&revision)? != bytes
            || revision.format_version != 2
            || revision.revision != next_revision(sequence)?
            || revision.source != self.source
            || revision.source_revision != self.source_revision
        {
            return Err(crate::Error::Storage(
                "durable context identity, revision or canonical bytes are invalid".into(),
            ));
        }
        validate_context_refs(&revision.context, self.content_verifier.as_ref()).await?;
        Ok(revision)
    }

    async fn read_revision(&self, revision: u64) -> Result<ContextRevision> {
        if revision == 0 || revision > u64::from(self.maximum_revisions) {
            return Err(crate::Error::Invalid(
                "context revision is outside configured bounds".into(),
            ));
        }
        let mut stream = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from: revision - 1,
                limit: 1,
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        let record = stream
            .next()
            .await
            .ok_or_else(|| crate::Error::Storage("context revision is missing".into()))?
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        if record.sequence != revision - 1 || stream.next().await.is_some() {
            return Err(crate::Error::Storage(
                "context revision read returned an invalid range".into(),
            ));
        }
        self.decode_revision(record.sequence, &record.value).await
    }

    /// Returns a captured latest revision with at most two record reads, independent
    /// of retained revision count. Explicit `revisions()` remains bounded archival replay.
    pub async fn latest(&self) -> Result<Context> {
        let tail = match self.provider.tail(self.path.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(crate::Error::Storage(error.to_string())),
        };
        if tail == 0 {
            return Ok(Context::default());
        }
        let revision = self.read_revision(tail).await?;
        if let Some(reference) = &revision.compaction {
            let source = self.read_revision(tail - 1).await?;
            validate_compaction(reference, &source.context, &revision.context)?;
        }
        Ok(revision.context)
    }

    /// Deterministically compacts a context and returns its immutable source reference.
    pub fn compact(
        context: &Context,
        max_messages: usize,
        summary: Option<ModelMessage>,
    ) -> Result<(Context, CompactionReference)> {
        if max_messages == 0 {
            return Err(crate::Error::Invalid(
                "compaction max_messages must be positive".into(),
            ));
        }
        let source = crate::contract::canonical_json_bytes(context)?;
        let mut compacted = context.clone();
        if compacted.messages.len() > max_messages {
            let keep = max_messages.saturating_sub(usize::from(summary.is_some()));
            let split = compacted.messages.len().saturating_sub(keep);
            let mut retained = compacted.messages.split_off(split);
            if let Some(summary) = summary {
                retained.insert(0, summary);
            }
            compacted.messages = retained;
        }
        let reference = CompactionReference {
            source_digest: *blake3::hash(&source).as_bytes(),
            source_messages: u32::try_from(context.messages.len())
                .map_err(|_| crate::Error::Invalid("too many context messages".into()))?,
            retained_messages: u32::try_from(compacted.messages.len())
                .map_err(|_| crate::Error::Invalid("too many retained messages".into()))?,
        };
        Ok((compacted, reference))
    }
}

fn validate_compaction(
    reference: &CompactionReference,
    source: &Context,
    compacted: &Context,
) -> Result<()> {
    let encoded = crate::contract::canonical_json_bytes(source)?;
    let source_messages = u32::try_from(source.messages.len())
        .map_err(|_| crate::Error::Invalid("too many context messages".into()))?;
    let retained_messages = u32::try_from(compacted.messages.len())
        .map_err(|_| crate::Error::Invalid("too many retained messages".into()))?;
    if reference.source_digest != *blake3::hash(&encoded).as_bytes()
        || reference.source_messages != source_messages
        || reference.retained_messages != retained_messages
        || reference.retained_messages > reference.source_messages
    {
        return Err(crate::Error::Invalid(
            "compaction reference does not bind the source and retained context".into(),
        ));
    }
    Ok(())
}

async fn validate_context_refs(
    context: &Context,
    verifier: &dyn ContentResidencyVerifier,
) -> Result<()> {
    if context.messages.len() > 65_536 || context.metadata.len() > 1_024 {
        return Err(crate::Error::Invalid(
            "durable context exceeds its reference bounds".into(),
        ));
    }
    for message in &context.messages {
        let parts = match &message.content {
            ModelContent::Part(part) => std::slice::from_ref(part),
            ModelContent::Parts(parts) if !parts.is_empty() => parts.as_slice(),
            _ => {
                return Err(crate::Error::Invalid(
                    "durable context contains inline content".into(),
                ));
            }
        };
        for part in parts {
            let ModelContentPart::File { file, .. } = part else {
                return Err(crate::Error::Invalid(
                    "durable context contains inline content".into(),
                ));
            };
            verifier.verify(file).await?;
        }
    }
    for (name, file) in &context.metadata {
        if !name.contains('.')
            || name.len() > crate::COMPONENT_LABEL_MAX_BYTES
            || name.chars().any(char::is_control)
        {
            return Err(crate::Error::Invalid(
                "durable context metadata key is invalid".into(),
            ));
        }
        verifier.verify(file).await?;
    }
    Ok(())
}

impl ContextSource for DurableContextProvider {
    fn load<'a>(&'a self, _: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move { Ok(self.latest().await?.messages) })
    }
}

/// Replaceable memory/retrieval/skill source used by reusable stock stages.
pub trait ContextSource: acyclic_stream::ProviderPlatform {
    /// Resolves model-visible messages for the current step.
    fn load<'a>(&'a self, input: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>>;
}

impl ContextSource for Context {
    fn load<'a>(&'a self, _: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move { Ok(self.messages.clone()) })
    }
}

/// Placement of source messages relative to existing context.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextPlacement {
    /// Insert before current messages.
    Prepend,
    /// Insert after current messages.
    Append,
}

/// Reusable stage for memory, retrieval, or skills.
pub struct SourceStage {
    name: String,
    revision: String,
    source: Arc<dyn ContextSource>,
    placement: ContextPlacement,
}

impl SourceStage {
    /// Creates a named source stage; `memory`, `retrieval`, and `skills` are ordinary names.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        revision: impl Into<String>,
        source: Arc<dyn ContextSource>,
        placement: ContextPlacement,
    ) -> Self {
        Self {
            name: name.into(),
            revision: revision.into(),
            source,
            placement,
        }
    }
}

impl ContextStage for SourceStage {
    fn validate(&self) -> Result<()> {
        crate::contract::validate_component_label(&self.name, "context source")?;
        crate::contract::validate_component_label(&self.revision, "context source revision")
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn contract(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "revision": self.revision,
            "placement": match self.placement {
                ContextPlacement::Prepend => "prepend",
                ContextPlacement::Append => "append",
            },
        })
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            let loaded = self.source.load(input).await?;
            Ok(selection::place_messages(context, loaded, self.placement))
        })
    }
}

/// Deterministic reusable compaction stage retaining a bounded message tail.
pub struct CompactionStage {
    /// Maximum messages retained after compaction.
    pub max_messages: usize,
    /// Optional caller-produced summary prepended when compaction occurs.
    pub summary: Option<ModelMessage>,
}

impl ContextStage for CompactionStage {
    fn validate(&self) -> Result<()> {
        if self.max_messages == 0 {
            return Err(crate::Error::Invalid(
                "compaction max_messages must be positive".into(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "compaction"
    }

    fn contract(&self) -> Value {
        serde_json::json!({
            "name": self.name(),
            "revision": "1",
            "max_messages": self.max_messages,
            "summary": self.summary,
        })
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        mut context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            self.validate()?;
            if context.messages.len() > self.max_messages {
                let keep = self
                    .max_messages
                    .saturating_sub(usize::from(self.summary.is_some()));
                let split = context.messages.len().saturating_sub(keep);
                let mut retained = context.messages.split_off(split);
                if let Some(summary) = &self.summary {
                    retained.insert(0, summary.clone());
                }
                context.messages = retained;
            }
            Ok(context)
        })
    }
}

/// Mutable context assembled for one model step.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Context {
    /// Ordered model-visible messages.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelMessageWire[]"))]
    pub messages: Vec<ModelMessage>,
    /// Stage-owned, namespaced version-pinned metadata files.
    #[cfg_attr(feature = "wasm", tsify(type = "Record<string, WasmFileRefWire>"))]
    pub metadata: BTreeMap<String, FileRef>,
}

/// Inputs visible to every context stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextInput {
    /// Current durable turn input.
    pub input: ModelContent,
    /// Recorded canonical selection; when present it replaces the synthesized
    /// one-message context without changing its provenance.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Zero-based executor step.
    pub step: u32,
    /// Model-visible results accumulated by earlier executor steps.
    pub prior_messages: Vec<ModelMessage>,
}

/// Replaceable ordered context transformation.
pub trait ContextStage: acyclic_stream::ProviderPlatform {
    /// Stable stage name used for diagnostics and composition.
    fn name(&self) -> &str;

    /// Immutable serializable identity included in durable execution binding.
    fn contract(&self) -> Value;

    /// Validates a replacement before installation. Custom implementations may
    /// reject incompatible configuration; rejection never replaces the last valid pipeline.
    fn validate(&self) -> Result<()> {
        crate::contract::validate_component_label(self.name(), "context stage")?;
        if !self.contract().is_object() {
            return Err(crate::Error::Invalid(
                "context stage contract must be an object".into(),
            ));
        }
        Ok(())
    }

    /// Transforms context; stage order is the order supplied by application code.
    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
    ) -> BoxFuture<'a, Result<Context>>;
}

/// Explicit ordered pipeline; no hidden framework stages are inserted.
#[derive(Clone, Default)]
pub struct ContextPipeline(Vec<Arc<dyn ContextStage>>);

impl ContextPipeline {
    /// Validates stage identities before installation or explicit reload.
    /// Implementations must keep their contract and pinned state immutable.
    pub fn validate(&self) -> Result<()> {
        for stage in &self.0 {
            stage.validate()?;
        }
        Ok(())
    }

    /// Installs a structurally valid replacement for future operations only.
    /// Failure leaves this pipeline and its visibly inspectable contracts intact;
    /// existing clones retain their original stage implementations.
    pub fn reload(&self, replacement: Self) -> Result<Self> {
        replacement.validate()?;
        Ok(replacement)
    }

    /// Creates a pipeline in exact execution order.
    #[must_use]
    pub fn new(stages: impl IntoIterator<Item = Arc<dyn ContextStage>>) -> Self {
        Self(stages.into_iter().collect())
    }

    /// Appends one stage and returns the pipeline for code-first composition.
    #[must_use]
    pub fn with(mut self, stage: Arc<dyn ContextStage>) -> Self {
        self.0.push(stage);
        self
    }

    /// Runs every stage in declared order.
    pub async fn run(&self, input: &ContextInput) -> Result<Context> {
        self.run_bounded(input, crate::conversation::Limits::default())
            .await
    }

    /// Bounds the initial view and every intermediate projection, without truncation.
    pub async fn run_bounded(
        &self,
        input: &ContextInput,
        limits: crate::conversation::Limits,
    ) -> Result<Context> {
        limits.validate()?;
        self.validate()?;
        if self.0.len() > limits.context_messages {
            return Err(crate::Error::Invalid(
                "context stage count exceeds limit".into(),
            ));
        }
        input.input.validate_user_input()?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input)?;
        }
        let base = input.selected_context.as_ref().map_or_else(
            || {
                vec![ModelMessage {
                    role: ModelRole::User,
                    content: input.input.clone(),
                }]
            },
            |selected| selected.messages.clone(),
        );
        let mut context = Context {
            messages: base
                .into_iter()
                .chain(input.prior_messages.iter().cloned())
                .collect(),
            metadata: BTreeMap::new(),
        };
        validate_projected_context(&context, limits)?;
        for stage in &self.0 {
            context = stage.apply(input, context).await?;
            validate_projected_context(&context, limits)?;
        }
        Ok(context)
    }

    /// Returns stage names in exact execution order.
    #[must_use]
    pub fn stage_names(&self) -> Vec<&str> {
        self.0.iter().map(|stage| stage.name()).collect()
    }

    /// Returns immutable stage contracts in exact execution order.
    #[must_use]
    pub fn contracts(&self) -> Vec<Value> {
        self.0.iter().map(|stage| stage.contract()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId,
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        model::{FileProjectionPolicy, ModelRole},
        resources::ProviderRef,
    };
    use acyclic_stream::MemoryStream;
    use std::{future::Future, pin::Pin};

    struct RefVerifier;

    struct CountingVerifier(std::sync::atomic::AtomicUsize);
    impl ContentResidencyVerifier for CountingVerifier {
        fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                file.validate()
            })
        }
    }

    #[tokio::test]
    async fn latest_context_work_is_independent_of_retained_revisions() -> Result<()> {
        for retained in [1, 16, 128] {
            let verifier = Arc::new(CountingVerifier(std::sync::atomic::AtomicUsize::new(0)));
            let provider = DurableContextProvider::new(
                Arc::new(MemoryStream::default()),
                StreamPath::new("bounded/context")
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                "instructions",
                "1",
                128,
                verifier.clone(),
            )?;
            let context = Context {
                messages: vec![message(ModelRole::System, "pinned")?],
                metadata: BTreeMap::new(),
            };
            for revision in 0..retained {
                provider
                    .append(
                        revision,
                        context.clone(),
                        None,
                        Bytes::from(format!("revision-{revision}")),
                    )
                    .await?;
            }
            verifier.0.store(0, std::sync::atomic::Ordering::SeqCst);
            let started = std::time::Instant::now();
            assert_eq!(provider.latest().await?, context);
            assert_eq!(verifier.0.load(std::sync::atomic::Ordering::SeqCst), 1);
            eprintln!(
                "context retained={retained} verified_refs=1 active_wire_bytes={} elapsed_us={}",
                crate::contract::canonical_json_bytes(&context)?.len(),
                started.elapsed().as_micros()
            );
        }
        Ok(())
    }

    struct AttributeRenderer;

    impl ContextRenderer for AttributeRenderer {
        fn contract(&self) -> Value {
            serde_json::json!({"revision": "1"})
        }

        fn render<'a>(
            &'a self,
            selection: &'a ContextSelection,
            mode: ContextRenderMode,
            _: &'a ContextInput,
            _: crate::conversation::Limits,
        ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
            Box::pin(async move {
                let ContextSourceValue::Attribute { attribute } = &selection.source else {
                    return Err(crate::Error::Unsupported(
                        "test renderer handles attributes".into(),
                    ));
                };
                Ok(vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(format!(
                        "{mode:?}:{:?}:{}:{}",
                        selection.representation, attribute.state_revision, attribute.value
                    )),
                }])
            })
        }
    }

    fn attribute_pipeline(
        value: &str,
        mode: ContextRenderMode,
        representation: ContextRepresentation,
    ) -> Result<ContextPipeline> {
        let attribute = ContextAttribute::typed(
            "example.role".into(),
            "1".into(),
            serde_json::json!({"type":"string"}),
            value.into(),
            &value,
        )?;
        Ok(ContextPipeline::new([Arc::new(SelectionStage::new(
            "role".into(),
            ContextSelection {
                source: ContextSourceValue::Attribute { attribute },
                extent: ContextExtent::Whole,
                representation,
            },
            Arc::new(AttributeRenderer),
            Arc::new(RefVerifier),
            mode,
            ContextPlacement::Prepend,
            crate::conversation::Limits::default(),
        )?) as Arc<dyn ContextStage>]))
    }

    #[tokio::test]
    async fn attribute_update_reload_and_bounds_use_one_pinned_state() -> Result<()> {
        let input = ContextInput {
            input: ModelContent::Text("task".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let old = attribute_pipeline(
            "old",
            ContextRenderMode::Prompt,
            ContextRepresentation::Full,
        )?;
        let snapshot = old.clone();
        let current = old.reload(attribute_pipeline(
            "new",
            ContextRenderMode::Prompt,
            ContextRepresentation::Full,
        )?)?;
        assert_ne!(snapshot.contracts(), current.contracts());
        let rebuilt = current.run(&input).await?;
        let update = attribute_pipeline(
            "new",
            ContextRenderMode::Update,
            ContextRepresentation::Full,
        )?
        .run(&input)
        .await?;
        assert_eq!(
            rebuilt.messages.first().map(|m| &m.content),
            Some(&ModelContent::Text("Prompt:Full:new:\"new\"".into()))
        );
        assert_eq!(
            update.messages.last().map(|m| &m.content),
            Some(&ModelContent::Text("Update:Full:new:\"new\"".into()))
        );
        for representation in [
            ContextRepresentation::Full,
            ContextRepresentation::Summary,
            ContextRepresentation::Reference,
        ] {
            let view = attribute_pipeline("new", ContextRenderMode::Prompt, representation)?
                .run(&input)
                .await?;
            assert_eq!(
                view.messages.first().map(|m| &m.content),
                Some(&ModelContent::Text(format!(
                    "Prompt:{representation:?}:new:\"new\""
                )))
            );
        }
        assert_eq!(snapshot.run(&input).await?, old.run(&input).await?);
        assert!(
            old.reload(ContextPipeline::new([Arc::new(CompactionStage {
                max_messages: 0,
                summary: None,
            })
                as Arc<dyn ContextStage>]))
                .is_err()
        );
        assert_eq!(old.contracts(), snapshot.contracts());
        assert!(
            ContextAttribute::typed(
                "example.role".into(),
                "1".into(),
                serde_json::json!({"type":"integer"}),
                "2".into(),
                &"invalid"
            )
            .is_err()
        );
        let tiny = SelectionStage::new(
            "role".into(),
            ContextSelection {
                source: ContextSourceValue::Attribute {
                    attribute: ContextAttribute::typed(
                        "example.role".into(),
                        "1".into(),
                        serde_json::json!({"type":"string"}),
                        "1".into(),
                        &"role",
                    )?,
                },
                extent: ContextExtent::Whole,
                representation: ContextRepresentation::Full,
            },
            Arc::new(AttributeRenderer),
            Arc::new(RefVerifier),
            ContextRenderMode::Prompt,
            ContextPlacement::Append,
            crate::conversation::Limits {
                render_bytes: 1,
                ..crate::conversation::Limits::default()
            },
        );
        assert!(tiny.is_err());
        Ok(())
    }

    proptest::proptest! {
        #[test]
        fn file_spans_are_exact_and_bounded(start in 0u64..20, end in 0u64..20) {
            let content = message(ModelRole::User, "1234567890").unwrap();
            let ModelContent::Part(ModelContentPart::File { file, .. }) = content.content else { unreachable!() };
            let selection = ContextSelection { source: ContextSourceValue::File { file },
                extent: ContextExtent::Span { start, end }, representation: ContextRepresentation::Full };
            proptest::prop_assert_eq!(selection.validate(crate::conversation::Limits::default()).is_ok(), start < end && end <= 10);
        }
    }

    impl ContentResidencyVerifier for RefVerifier {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { reference.validate() })
        }
    }

    fn message(role: ModelRole, text: &str) -> Result<ModelMessage> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "context",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
        )?;
        let file = FileRef::new(
            volume,
            format!("context/{text}.txt"),
            "pinned",
            FileDescriptor::from_bytes(text.as_bytes(), "text/plain")?,
            format!("{text}.txt"),
        )?;
        Ok(ModelMessage {
            role,
            content: ModelContent::Part(ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::BoundedFull,
            }),
        })
    }

    #[tokio::test]
    #[allow(
        clippy::indexing_slicing,
        reason = "each index is preceded by an assert_eq! on the corresponding Vec's len(), proving it in-bounds"
    )]
    async fn durable_sources_and_compaction_reopen_exactly() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let path = StreamPath::new("runtime/context/memory")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            2,
            Arc::new(RefVerifier),
        )?;
        assert_eq!(provider.latest().await?, Context::default());
        assert!(
            provider
                .append(
                    0,
                    Context {
                        messages: vec![ModelMessage {
                            role: ModelRole::User,
                            content: ModelContent::Text("secret".into())
                        }],
                        metadata: BTreeMap::new(),
                    },
                    None,
                    Bytes::from_static(b"inline-rejected")
                )
                .await
                .is_err()
        );
        let original = Context {
            messages: vec![
                message(ModelRole::User, "one")?,
                message(ModelRole::Assistant, "two")?,
                message(ModelRole::User, "three")?,
            ],
            metadata: BTreeMap::new(),
        };
        provider
            .append(0, original.clone(), None, Bytes::from_static(b"context-1"))
            .await?;
        let (compacted, reference) = DurableContextProvider::compact(
            &original,
            2,
            Some(message(ModelRole::System, "summary")?),
        )?;
        let mut forged = reference.clone();
        forged.source_digest[0] ^= 1;
        assert!(
            provider
                .append(
                    1,
                    compacted.clone(),
                    Some(forged),
                    Bytes::from_static(b"forged-context"),
                )
                .await
                .is_err()
        );
        provider
            .append(
                1,
                compacted.clone(),
                Some(reference.clone()),
                Bytes::from_static(b"context-2"),
            )
            .await?;
        provider
            .append(
                1,
                compacted.clone(),
                Some(reference.clone()),
                Bytes::from_static(b"context-2"),
            )
            .await?;
        assert!(
            provider
                .append(2, compacted.clone(), None, Bytes::from_static(b"context-3"),)
                .await
                .is_err()
        );

        let reopened = Arc::new(DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            2,
            Arc::new(RefVerifier),
        )?);
        let revisions = reopened.revisions().await?;
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[1].compaction, Some(reference));
        assert_eq!(reopened.latest().await?, compacted);

        let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
            "memory",
            "1",
            reopened,
            ContextPlacement::Prepend,
        )) as Arc<dyn ContextStage>]);
        let assembled = pipeline
            .run(&ContextInput {
                input: ModelContent::Text("current input".into()),
                selected_context: None,
                step: 0,
                prior_messages: vec![message(ModelRole::User, "current")?],
            })
            .await?;
        assert_eq!(assembled.messages.len(), 4);
        assert_eq!(assembled.messages[3], message(ModelRole::User, "current")?);
        let undersized =
            DurableContextProvider::new(stream, path, "memory", "1", 1, Arc::new(RefVerifier))?;
        assert!(undersized.latest().await.is_err());
        Ok(())
    }
}

/// Bounded request-admission model; no unbounded correctness claim.
///
/// Two distinct payloads, one request, exhaustive reachable finite state space.
/// State: current projection, retained admission, storage intact, dispatch
/// count, durable completion. Linearizable journal CAS and immutable
/// authenticated storage are assumptions. Admission claims the attempt before
/// provider dispatch; a crash at that boundary may lose dispatch, never
/// authorize redispatch. Correspondence: `executor::run_model_step`
/// `ModelStarted`/`load_json`/`append_if_tail`/`Model` admission.
#[cfg(test)]
mod admission_model {
    use std::collections::{BTreeSet, VecDeque};

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct State {
        projection: u8,
        admitted: Option<u8>,
        intact: bool,
        dispatches: u8,
        completed: bool,
    }

    fn successors(state: State) -> Vec<State> {
        let mut next = vec![State {
            projection: 1 - state.projection,
            ..state
        }]; // future projection
        if state.admitted.is_none() {
            let claimed = State {
                admitted: Some(state.projection),
                intact: true,
                ..state
            };
            next.push(claimed); // crash after claim
            next.push(State {
                dispatches: 1,
                ..claimed
            }); // claim then dispatch
        } else {
            next.push(State {
                intact: false,
                ..state
            }); // storage fault
            if state.dispatches > 0 && state.intact {
                next.push(State {
                    completed: true,
                    ..state
                }); // durable observation
            }
        }
        next
    }

    fn check(replay: fn(State) -> Option<u8>) -> Result<usize, String> {
        let initial = State {
            projection: 0,
            admitted: None,
            intact: true,
            dispatches: 0,
            completed: false,
        };
        let mut pending = VecDeque::from([initial]);
        let mut seen = BTreeSet::from([initial]);
        while let Some(state) = pending.pop_front() {
            if state.dispatches > 1 {
                return Err("redispatch".to_owned());
            }
            if state.admitted.is_some() {
                let value = replay(state);
                if value != state.admitted.filter(|_| state.intact) {
                    return Err("replay returned other than the retained admission".to_owned());
                }
                if state.completed && value.is_none() && state.intact {
                    return Err("completion without retained admission".to_owned());
                }
            }
            for next in successors(state) {
                if seen.insert(next) {
                    pending.push_back(next);
                }
            }
        }
        Ok(seen.len())
    }

    #[test]
    fn retained_replay_holds_over_every_reachable_state() {
        let retained: fn(State) -> Option<u8> = |state| state.admitted.filter(|_| state.intact);
        assert_eq!(check(retained), Ok(26));
        // Negative controls: replaying the current projection, or ignoring
        // missing storage, must be rejected.
        let fresh: fn(State) -> Option<u8> = |state| state.intact.then_some(state.projection);
        let unchecked: fn(State) -> Option<u8> = |state| state.admitted;
        assert!(check(fresh).is_err());
        assert!(check(unchecked).is_err());
    }
}
