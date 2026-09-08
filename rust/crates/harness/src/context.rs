//! Ordered function-based context assembly.

use crate::{Result, model::ModelMessage};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "host")]
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey as StreamIdempotencyKey, MAX_RECORD_BYTES,
    ReadRequest, StreamError, StreamPath, StreamProvider,
};
#[cfg(feature = "host")]
use bytes::Bytes;
#[cfg(feature = "host")]
use futures::StreamExt as _;

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
#[cfg(feature = "host")]
pub struct DurableContextProvider {
    provider: Arc<dyn StreamProvider>,
    path: StreamPath,
    source: String,
    source_revision: String,
    maximum_revisions: u32,
}

#[cfg(feature = "host")]
impl DurableContextProvider {
    /// Creates a bounded durable provider over one permanent Stream path.
    pub fn new(
        provider: Arc<dyn StreamProvider>,
        path: StreamPath,
        source: impl Into<String>,
        source_revision: impl Into<String>,
        maximum_revisions: u32,
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
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| crate::Error::Invalid("context revision exhausted".into()))?;
        if revision > u64::from(self.maximum_revisions) {
            return Err(crate::Error::Invalid(
                "durable context revision bound exceeded".into(),
            ));
        }
        if let Some(reference) = &compaction {
            let revisions = self.revisions().await?;
            let source = expected_revision
                .checked_sub(1)
                .and_then(|index| revisions.get(index as usize))
                .ok_or_else(|| {
                    crate::Error::Invalid(
                        "compaction must reference the immediately preceding context".into(),
                    )
                })?;
            validate_compaction(reference, &source.context, &context)?;
        }
        let record = ContextRevision {
            format_version: 1,
            revision,
            source: self.source.clone(),
            source_revision: self.source_revision.clone(),
            context,
            compaction,
        };
        let encoded = serde_json::to_vec(&record)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
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
            let revision: ContextRevision = serde_json::from_slice(&record.value)
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let expected = record.sequence + 1;
            if revision.format_version != 1
                || revision.revision != expected
                || revision.source != self.source
                || revision.source_revision != self.source_revision
            {
                return Err(crate::Error::Storage(
                    "durable context history failed identity or revision validation".into(),
                ));
            }
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

    /// Returns the latest reconstructed context, or an empty context before the first append.
    pub async fn latest(&self) -> Result<Context> {
        Ok(self
            .revisions()
            .await?
            .last()
            .map(|revision| revision.context.clone())
            .unwrap_or_default())
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
        let source = serde_json::to_vec(context)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
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

#[cfg(feature = "host")]
fn validate_compaction(
    reference: &CompactionReference,
    source: &Context,
    compacted: &Context,
) -> Result<()> {
    let encoded =
        serde_json::to_vec(source).map_err(|error| crate::Error::Invalid(error.to_string()))?;
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

#[cfg(feature = "host")]
impl ContextSource for DurableContextProvider {
    fn load<'a>(&'a self, _: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move { Ok(self.latest().await?.messages) })
    }
}

/// Replaceable memory/retrieval/skill source used by reusable stock stages.
pub trait ContextSource: Send + Sync {
    /// Resolves model-visible messages for the current step.
    fn load<'a>(&'a self, input: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>>;
}

/// Placement of source messages relative to existing context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
        mut context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            let mut loaded = self.source.load(input).await?;
            match self.placement {
                ContextPlacement::Prepend => {
                    loaded.append(&mut context.messages);
                    context.messages = loaded;
                }
                ContextPlacement::Append => context.messages.append(&mut loaded),
            }
            Ok(context)
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
            if self.max_messages == 0 {
                return Err(crate::Error::Invalid(
                    "compaction max_messages must be positive".into(),
                ));
            }
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
pub struct Context {
    /// Ordered model-visible messages.
    pub messages: Vec<ModelMessage>,
    /// Stage-owned structured metadata that remains reconstructable.
    pub metadata: Value,
}

/// Inputs visible to every context stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextInput {
    /// Current durable turn input.
    pub input: Value,
    /// Zero-based executor step.
    pub step: u32,
    /// Model-visible results accumulated by earlier executor steps.
    pub prior_messages: Vec<ModelMessage>,
}

/// Replaceable ordered context transformation.
pub trait ContextStage: Send + Sync {
    /// Stable stage name used for diagnostics and composition.
    fn name(&self) -> &str;

    /// Immutable serializable identity included in durable execution binding.
    fn contract(&self) -> Value;

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
        let mut context = Context {
            messages: input.prior_messages.clone(),
            metadata: Value::Null,
        };
        for stage in &self.0 {
            context = stage.apply(input, context).await?;
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

#[cfg(all(test, feature = "host"))]
mod tests {
    use super::*;
    use acyclic_stream::MemoryStream;
    use serde_json::json;

    fn message(role: &str, text: &str) -> ModelMessage {
        ModelMessage {
            role: role.into(),
            content: json!(text),
        }
    }

    #[tokio::test]
    async fn durable_sources_and_compaction_reopen_exactly() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let path = StreamPath::new("runtime/context/memory")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(stream.clone(), path.clone(), "memory", "1", 2)?;
        assert_eq!(provider.latest().await?, Context::default());
        let original = Context {
            messages: vec![
                message("user", "one"),
                message("assistant", "two"),
                message("user", "three"),
            ],
            metadata: json!({"source": "test"}),
        };
        provider
            .append(0, original.clone(), None, Bytes::from_static(b"context-1"))
            .await?;
        let (compacted, reference) =
            DurableContextProvider::compact(&original, 2, Some(message("system", "summary")))?;
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
                input: Value::Null,
                step: 0,
                prior_messages: vec![message("user", "current")],
            })
            .await?;
        assert_eq!(assembled.messages.len(), 3);
        assert_eq!(assembled.messages[2], message("user", "current"));
        let undersized = DurableContextProvider::new(stream, path, "memory", "1", 1)?;
        assert!(undersized.latest().await.is_err());
        Ok(())
    }
}
