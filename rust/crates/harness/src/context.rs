//! Ordered function-based context assembly.

use crate::contract::next_revision;
use crate::{
    Result,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, Limits},
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

mod accounting;
pub use accounting::*;
mod selection;
pub use selection::*;
mod discovery;
pub use discovery::*;
mod compaction;
pub use compaction::*;

/// Durable output of an ordinary admitted summary model operation.
/// The operation's execution journal retains its exact request and observations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSummary {
    /// Caller-owned operation identity; retries use the same admitted request.
    pub operation_id: crate::OperationId,
    /// Logical parent step of the summary admission.
    pub step: u32,
    /// Number of ordered source messages covered by this summary.
    pub source_messages: u32,
    /// Digest of the exact source projection, including immutable content refs.
    pub source_digest: [u8; 32],
    /// Immutable summary text staged through the execution journal.
    pub output: FileRef,
}

/// Consumer-declared content that compaction must preserve verbatim.
/// The current input is always retained, regardless of this policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct CompactionRetention {
    /// Roles retained in full. Defaults to instructions (`System`).
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelRole[]"))]
    pub roles: Vec<ModelRole>,
    /// Retains messages containing native media references in full.
    pub native_media: bool,
}

impl Default for CompactionRetention {
    fn default() -> Self {
        Self {
            roles: vec![ModelRole::System],
            native_media: true,
        }
    }
}

impl CompactionRetention {
    /// Rejects ambiguous repeated role declarations.
    pub fn validate(&self) -> Result<()> {
        for (position, role) in self.roles.iter().enumerate() {
            if self
                .roles
                .get(..position)
                .is_some_and(|prior| prior.contains(role))
            {
                return Err(crate::Error::Invalid(
                    "compaction retention repeats a role".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Immutable reference proving which pre-compaction context was summarized.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactionReference {
    /// BLAKE3 digest of the exact serialized source context.
    pub source_digest: [u8; 32],
    /// Caller-selected projection budget; replay reconstructs the same selection.
    pub maximum_messages: u32,
    /// Exact consumer retention policy used by this admission.
    pub retention: CompactionRetention,
    /// Summary operation and immutable output, when a projection was compressed.
    pub summary: Option<ContextSummary>,
}

impl CompactionReference {
    /// Encodes this proof with the existing Harness canonical JSON serializer.
    pub fn encode(&self) -> Result<Vec<u8>> {
        crate::contract::canonical_json_bytes(self)
    }
}

/// Ref-only canonical checkpoint envelope pinned by a committed history selection.
/// The source selection's revision is its coverage watermark; stage output is separate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalContextCheckpoint {
    /// Existing execution which admitted the summary and its source.
    pub operation_id: crate::OperationId,
    /// Exact canonical delta and previous checkpoint used by that admission.
    pub selection: crate::conversation::ModelContextSelection,
    /// Immutable source base before stage transformation.
    pub source: FileRef,
    /// Immutable compacted canonical base for later deltas.
    pub retained: FileRef,
    /// Exact compaction proof and admitted summary output provenance.
    pub compaction: FileRef,
}

impl CanonicalContextCheckpoint {
    /// Checks a bounded envelope. The owning journal must verify publication and read refs.
    pub fn validate(&self, limits: Limits) -> Result<()> {
        limits.validate()?;
        if self.selection.conversation_revision == 0
            || self.selection.message_ids.is_empty()
            || self.selection.message_ids.len() > limits.context_messages
        {
            return Err(crate::Error::Invalid(
                "canonical checkpoint coverage is invalid".into(),
            ));
        }
        let unique = self
            .selection
            .message_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        if unique.len() != self.selection.message_ids.len() {
            return Err(crate::Error::Invalid(
                "canonical checkpoint repeats source identities".into(),
            ));
        }
        for reference in [&self.source, &self.retained, &self.compaction] {
            limits.validate_file(reference)?;
            if reference.volume() != self.source.volume()
                || reference.descriptor().media_type() != "application/json"
            {
                return Err(crate::Error::Invalid(
                    "canonical checkpoint payload scope is invalid".into(),
                ));
            }
        }
        if let Some(previous) = &self.selection.checkpoint {
            limits.validate_file(previous)?;
        }
        Ok(())
    }

    /// Encodes one bounded envelope for the owning journal's normal content publisher.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        self.validate(limits)?;
        let bytes = crate::contract::canonical_json_bytes(self)?;
        if bytes.len() as u64 > limits.file_bytes || bytes.len() as u64 > limits.render_bytes {
            return Err(crate::Error::Invalid(
                "canonical checkpoint envelope exceeds byte bounds".into(),
            ));
        }
        Ok(bytes)
    }

    /// Binds resolved payloads to the canonical source and existing summary operation.
    pub fn validate_projection(
        &self,
        source: &Context,
        retained: &Context,
        compaction: &CompactionReference,
        limits: Limits,
    ) -> Result<()> {
        self.validate(limits)?;
        validate_projected_context(source, limits)?;
        validate_projected_context(retained, limits)?;
        validate_compaction(compaction, source, retained)?;
        if compaction
            .summary
            .as_ref()
            .is_none_or(|summary| summary.operation_id != self.operation_id)
        {
            return Err(crate::Error::Invalid(
                "canonical checkpoint summary admission differs".into(),
            ));
        }
        Ok(())
    }
}

/// Resolved view of one durable context revision. Stream retains only `content`.
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
    /// Immutable canonical context payload retained by the owning content provider.
    pub content: FileRef,
    /// Reconstructable context value.
    pub context: Context,
    /// Present only when this revision deterministically compacts another context.
    pub compaction: Option<CompactionReference>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredContextRevision {
    format_version: u32,
    revision: u64,
    source: String,
    source_revision: String,
    context: FileRef,
    compaction: Option<CompactionReference>,
}

/// Stream-backed context source shared by memory, retrieval, skills, and compaction stages.
pub struct DurableContextProvider {
    provider: Arc<dyn StreamProvider>,
    path: StreamPath,
    source: String,
    source_revision: String,
    maximum_page_records: u32,
    content_verifier: Arc<dyn ContentResidencyVerifier>,
    publisher: Option<Arc<dyn ContentPublisher>>,
    limits: Limits,
}

impl DurableContextProvider {
    /// Creates a durable provider with a positive per-page work bound over one Stream path.
    pub fn new(
        provider: Arc<dyn StreamProvider>,
        path: StreamPath,
        source: impl Into<String>,
        source_revision: impl Into<String>,
        maximum_page_records: u32,
        content_verifier: Arc<dyn ContentResidencyVerifier>,
        limits: Limits,
    ) -> Result<Self> {
        let source = source.into();
        let source_revision = source_revision.into();
        limits.validate()?;
        crate::contract::validate_component_label(&source, "context source")?;
        crate::contract::validate_component_label(&source_revision, "context source revision")?;
        if source.trim().is_empty()
            || source_revision.trim().is_empty()
            || maximum_page_records == 0
        {
            return Err(crate::Error::Invalid(
                "durable context source, revision, and bound are required".into(),
            ));
        }
        Ok(Self {
            provider,
            path,
            source,
            source_revision,
            maximum_page_records,
            content_verifier,
            publisher: None,
            limits,
        })
    }

    /// Installs an owner-bound writer for immutable projection payloads.
    /// A source can reopen with only a reader; publication requires this binding.
    pub fn with_publisher(mut self, publisher: Arc<dyn ContentPublisher>) -> Result<Self> {
        publisher.volume().validate()?;
        self.publisher = Some(publisher);
        Ok(self)
    }

    async fn stage_revision(
        &self,
        revision: u64,
        context: Context,
        compaction: Option<CompactionReference>,
        key: &[u8],
    ) -> Result<(ContextRevision, Bytes)> {
        let publisher = self.publisher.as_ref().ok_or_else(|| {
            crate::Error::Unsupported("durable context publisher is not bound".into())
        })?;
        let bytes = crate::contract::canonical_json_bytes(&context)?;
        if bytes.len() as u64 > self.limits.file_bytes {
            return Err(crate::Error::Invalid(
                "context payload exceeds file limit".into(),
            ));
        }
        let digest = crate::contract::canonical_json_digest(&(
            "harness:context-payload:v1",
            self.path.as_str(),
            &self.source,
            &self.source_revision,
            revision,
            key,
        ))?;
        let mut identity = [0; 16];
        for (destination, byte) in identity.iter_mut().zip(digest.iter()) {
            *destination = *byte;
        }
        let operation = crate::OperationId::from_bytes(identity);
        let content = publisher
            .stage(
                operation,
                &format!("context/projections/{operation}.json"),
                &bytes,
                "application/json",
                "context.json",
            )
            .await?;
        self.limits.validate_file(&content)?;
        if content.volume() != publisher.volume()
            || content.descriptor().media_type() != "application/json"
        {
            return Err(crate::Error::Unauthorized(
                "context publisher returned another binding".into(),
            ));
        }
        content.descriptor().verify(&bytes)?;
        self.content_verifier.verify(&content).await?;
        let stored = StoredContextRevision {
            format_version: 5,
            revision,
            source: self.source.clone(),
            source_revision: self.source_revision.clone(),
            context: content.clone(),
            compaction,
        };
        let encoded = crate::contract::canonical_json_bytes(&stored)?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(crate::Error::Invalid(
                "durable context record exceeds Stream limit".into(),
            ));
        }
        Ok((
            ContextRevision {
                format_version: stored.format_version,
                revision,
                source: stored.source,
                source_revision: stored.source_revision,
                content,
                context,
                compaction: stored.compaction,
            },
            Bytes::from(encoded),
        ))
    }

    /// Appends one immutable context revision with exact retry and tail-CAS semantics.
    pub async fn append(
        &self,
        expected_revision: u64,
        context: Context,
        compaction: Option<CompactionReference>,
        idempotency_key: impl Into<Bytes>,
    ) -> Result<ContextRevision> {
        validate_context_refs(&context, self.content_verifier.as_ref(), self.limits).await?;
        let revision = next_revision(expected_revision)?;
        let key_bytes = idempotency_key.into();
        let key = StreamIdempotencyKey::new(key_bytes.clone())
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        if let Some(reference) = &compaction {
            let source = self.read_revision(expected_revision).await?;
            validate_compaction(reference, &source.context, &context)?;
        }
        let (record, encoded) = self
            .stage_revision(revision, context, compaction, &key_bytes)
            .await?;
        match self
            .provider
            .append(AppendRequest {
                path: self.path.clone(),
                records: vec![encoded],
                if_tail: Some(expected_revision),
                idempotency_key: Some(key),
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?
        {
            AppendOutcome::Committed(receipt)
                if receipt.start == expected_revision
                    && receipt.end == revision
                    && receipt.tail == revision =>
            {
                Ok(record)
            }
            AppendOutcome::Committed(_) => Err(crate::Error::Storage(
                "context append returned an invalid tail".into(),
            )),
            AppendOutcome::TailConflict { actual_tail } => Err(crate::Error::Conflict(format!(
                "context revision {expected_revision} is stale; actual revision is {actual_tail}"
            ))),
        }
    }

    /// Publishes one continuing projection and its exact source in a single CAS.
    /// Neither the uncompressed source nor a projection without its provenance
    /// becomes a visible latest revision. Exact retries reuse the same pair.
    pub async fn append_compaction(
        &self,
        expected_revision: u64,
        source: Context,
        compacted: Context,
        reference: CompactionReference,
        idempotency_key: impl Into<Bytes>,
    ) -> Result<[ContextRevision; 2]> {
        let source_revision = next_revision(expected_revision)?;
        let compacted_revision = next_revision(source_revision)?;
        let key_bytes = idempotency_key.into();
        let key = StreamIdempotencyKey::new(key_bytes.clone())
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        validate_compaction(&reference, &source, &compacted)?;
        validate_context_refs(&source, self.content_verifier.as_ref(), self.limits).await?;
        validate_context_refs(&compacted, self.content_verifier.as_ref(), self.limits).await?;
        let (source_record, source_bytes) = self
            .stage_revision(source_revision, source, None, &key_bytes)
            .await?;
        let (compacted_record, compacted_bytes) = self
            .stage_revision(compacted_revision, compacted, Some(reference), &key_bytes)
            .await?;
        let pair = [source_record, compacted_record];
        match self
            .provider
            .append(AppendRequest {
                path: self.path.clone(),
                records: vec![source_bytes, compacted_bytes],
                if_tail: Some(expected_revision),
                idempotency_key: Some(key),
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?
        {
            AppendOutcome::Committed(receipt)
                if receipt.start == expected_revision
                    && receipt.end == compacted_revision
                    && receipt.tail == compacted_revision =>
            {
                Ok(pair)
            }
            AppendOutcome::Committed(_) => Err(crate::Error::Storage(
                "context append returned an invalid tail".into(),
            )),
            AppendOutcome::TailConflict { actual_tail } => Err(crate::Error::Conflict(format!(
                "context revision {expected_revision} is stale; actual revision is {actual_tail}"
            ))),
        }
    }

    /// Captures an immutable read boundary; later appends do not extend this revision.
    pub async fn tail_revision(&self) -> Result<u64> {
        match self.provider.tail(self.path.clone()).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(crate::Error::Storage(error.to_string())),
        }
    }

    /// Reads one verified page after a cursor through a captured revision.
    /// Work and returned records are bounded by the configured page allowance,
    /// independent of total retained history. A page beginning with compaction
    /// reads its immediate source once in addition to the bounded page.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.context.page",
            level = "debug",
            skip_all,
            fields(rev = through, items = crate::obs::Empty, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn revisions(&self, after: u64, through: u64) -> Result<Vec<ContextRevision>> {
        crate::obs::outcome(self.revisions_inner(after, through).await)
    }

    async fn revisions_inner(&self, after: u64, through: u64) -> Result<Vec<ContextRevision>> {
        if after > through || through > self.tail_revision().await? {
            return Err(crate::Error::Invalid(
                "context page cursor is invalid".into(),
            ));
        }
        let count = (through - after).min(u64::from(self.maximum_page_records));
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut stream = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from: after,
                limit: u32::try_from(count)
                    .map_err(|_| crate::Error::Invalid("context page bound overflow".into()))?,
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        let mut revisions: Vec<ContextRevision> = Vec::new();
        while let Some(record) = stream.next().await {
            let record = record.map_err(|error| crate::Error::Storage(error.to_string()))?;
            if revisions.len() as u64 >= count || record.sequence != after + revisions.len() as u64
            {
                return Err(crate::Error::Storage(
                    "context page returned an invalid range".into(),
                ));
            }
            let revision = self.decode_revision(record.sequence, &record.value).await?;
            if let Some(reference) = &revision.compaction {
                let preceding = if revisions.is_empty() {
                    if after == 0 {
                        return Err(crate::Error::Storage(
                            "context compaction has no preceding source".into(),
                        ));
                    }
                    Some(self.read_revision(after).await?)
                } else {
                    None
                };
                let source = revisions.last().or(preceding.as_ref()).ok_or_else(|| {
                    crate::Error::Storage("context compaction source is absent".into())
                })?;
                validate_compaction(reference, &source.context, &revision.context)
                    .map_err(|error| crate::Error::Storage(error.to_string()))?;
            }
            revisions.push(revision);
        }
        if revisions.len() as u64 != count {
            return Err(crate::Error::Storage("context page is incomplete".into()));
        }
        crate::obs::obs_record!("items" = revisions.len() as u64);
        Ok(revisions)
    }

    async fn decode_revision(&self, sequence: u64, bytes: &[u8]) -> Result<ContextRevision> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(crate::Error::Storage(
                "context record exceeds Stream limit".into(),
            ));
        }
        let revision: StoredContextRevision = crate::contract::json_from_slice(bytes)
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&revision)? != bytes
            || revision.format_version != 5
            || revision.revision != next_revision(sequence)?
            || revision.source != self.source
            || revision.source_revision != self.source_revision
        {
            return Err(crate::Error::Storage(
                "durable context identity, revision or canonical bytes are invalid".into(),
            ));
        }
        self.limits.validate_file(&revision.context)?;
        if revision.context.descriptor().media_type() != "application/json"
            || revision.context.descriptor().byte_length() > self.limits.render_bytes
        {
            return Err(crate::Error::Storage(
                "context payload descriptor is invalid".into(),
            ));
        }
        self.content_verifier.verify(&revision.context).await?;
        let payload = self.content_verifier.read(&revision.context).await?;
        revision.context.descriptor().verify(&payload)?;
        let context: Context = crate::contract::json_from_slice(&payload)
            .map_err(|error| crate::Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&context)? != payload {
            return Err(crate::Error::Storage(
                "context payload is not canonical".into(),
            ));
        }
        validate_context_refs(&context, self.content_verifier.as_ref(), self.limits).await?;
        Ok(ContextRevision {
            format_version: revision.format_version,
            revision: revision.revision,
            source: revision.source,
            source_revision: revision.source_revision,
            content: revision.context,
            context,
            compaction: revision.compaction,
        })
    }

    async fn read_revision(&self, revision: u64) -> Result<ContextRevision> {
        if revision == 0 {
            return Err(crate::Error::Invalid(
                "context revision must be positive".into(),
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

    /// Loads one pinned revision, checking its source proof with at most two
    /// record reads. Later publications do not change this projection.
    pub async fn revision(&self, revision: u64) -> Result<ContextRevision> {
        let record = self.read_revision(revision).await?;
        if let Some(reference) = &record.compaction {
            let source = self.read_revision(revision - 1).await?;
            validate_compaction(reference, &source.context, &record.context)?;
        }
        Ok(record)
    }

    /// Captures the current continuing projection and its immutable revision.
    /// Reads at most two records, independent of retained revision count.
    pub async fn latest_revision(&self) -> Result<Option<ContextRevision>> {
        let tail = self.tail_revision().await?;
        if tail == 0 {
            return Ok(None);
        }
        self.revision(tail).await.map(Some)
    }

    /// Returns the captured latest context. `latest_revision` also retains its
    /// pin; `revisions(after, through)` provides explicit bounded archival pages.
    pub async fn latest(&self) -> Result<Context> {
        Ok(self
            .latest_revision()
            .await?
            .map_or_else(Context::default, |record| record.context))
    }

    /// Deterministically compacts a context and returns its immutable source reference.
    pub fn compact(
        context: &Context,
        max_messages: usize,
        summary: Option<ContextSummary>,
        retention: CompactionRetention,
    ) -> Result<(Context, CompactionReference)> {
        retention.validate()?;
        context.validate_current_input()?;
        if max_messages == 0 {
            return Err(crate::Error::Invalid(
                "compaction max_messages must be positive".into(),
            ));
        }
        let source = crate::contract::canonical_json_bytes(context)?;
        let source_digest = *blake3::hash(&source).as_bytes();
        if let Some(summary) = &summary {
            let covered = context
                .messages
                .get(..summary.source_messages as usize)
                .ok_or_else(|| {
                    crate::Error::Invalid("summary extent exceeds source projection".into())
                })?;
            let summarized = Context {
                messages: covered.to_vec(),
                metadata: context.metadata.clone(),
                current_input_index: context
                    .current_input_index
                    .filter(|index| *index < summary.source_messages),
            };
            if summary.source_messages == 0
                || summary.source_digest != crate::contract::canonical_json_digest(&summarized)?
                || summary.output.descriptor().media_type() != "text/plain"
            {
                return Err(crate::Error::Invalid(
                    "summary does not bind its source prefix".into(),
                ));
            }
        }
        let compacted = if context.messages.len() > max_messages
            || (summary.is_some() && context.messages.len() == max_messages)
        {
            let summary = summary.as_ref().ok_or_else(|| {
                crate::Error::Invalid(
                    "compaction requires a durable summary; context cannot be silently dropped"
                        .into(),
                )
            })?;
            compact_projection(context, max_messages, summary, &retention)?
        } else {
            context.clone()
        };
        let reference = CompactionReference {
            source_digest,
            maximum_messages: u32::try_from(max_messages).map_err(|_| {
                crate::Error::Invalid("compaction budget exceeds portable count".into())
            })?,
            summary,
            retention,
        };
        Ok((compacted, reference))
    }
}

fn compact_projection(
    context: &Context,
    max_messages: usize,
    summary: &ContextSummary,
    retention: &CompactionRetention,
) -> Result<Context> {
    if max_messages < 2 {
        return Err(crate::Error::Invalid(
            "compaction must retain the current input and summary".into(),
        ));
    }
    let mandatory = mandatory_positions(context, summary.source_messages as usize, retention);
    if mandatory.len() >= max_messages {
        return Err(crate::Error::Invalid(
            "mandatory context exceeds compaction message budget".into(),
        ));
    }
    let mut selected = mandatory;
    for position in (0..context.messages.len()).rev() {
        if selected.len() + 1 == max_messages {
            break;
        }
        selected.insert(position);
    }
    retain_compacted_messages(context, selected, summary)
}

pub(super) fn mandatory_positions(
    context: &Context,
    covered: usize,
    retention: &CompactionRetention,
) -> std::collections::BTreeSet<usize> {
    context
        .messages
        .iter()
        .enumerate()
        .filter_map(|(position, message)| {
            let parts = message.content.parts();
            (context.current_input_index.map(|index| index as usize) == Some(position)
                || position + 1 == context.messages.len()
                || position >= covered
                || retention.roles.contains(&message.role)
                || (retention.native_media
                    && parts.iter().any(|part| {
                        matches!(
                            part,
                            ModelContentPart::File {
                                policy: crate::model::FileProjectionPolicy::Native,
                                ..
                            }
                        )
                    })))
            .then_some(position)
        })
        .collect()
}

fn retain_compacted_messages(
    context: &Context,
    selected: std::collections::BTreeSet<usize>,
    summary: &ContextSummary,
) -> Result<Context> {
    let mut calls = std::collections::BTreeSet::new();
    let mut retained = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::File {
            file: summary.output.clone(),
            policy: crate::model::FileProjectionPolicy::BoundedFull,
        }),
    }];
    let mut current_input_index = None;
    for position in selected {
        let message = context
            .messages
            .get(position)
            .ok_or_else(|| crate::Error::Invalid("compaction position is missing".into()))?;
        let parts = message.content.parts();
        for part in parts {
            match part {
                ModelContentPart::ToolCall { call_id, .. } => {
                    calls.insert(call_id);
                }
                ModelContentPart::ToolResult { call_id, .. } if !calls.remove(call_id) => {
                    return Err(crate::Error::Invalid(
                        "compaction boundary splits a tool exchange".into(),
                    ));
                }
                _ => {}
            }
        }
        if context.current_input_index.map(|index| index as usize) == Some(position) {
            current_input_index = Some(u32::try_from(retained.len()).map_err(|_| {
                crate::Error::Invalid("current input index exceeds portable count".into())
            })?);
        }
        retained.push(message.clone());
    }
    if !calls.is_empty() {
        return Err(crate::Error::Invalid(
            "compaction boundary splits a tool exchange".into(),
        ));
    }
    Ok(Context {
        messages: retained,
        metadata: context.metadata.clone(),
        current_input_index,
    })
}

pub(crate) fn validate_compaction(
    reference: &CompactionReference,
    source: &Context,
    compacted: &Context,
) -> Result<()> {
    let encoded = crate::contract::canonical_json_bytes(source)?;
    if reference.source_digest != *blake3::hash(&encoded).as_bytes()
        || DurableContextProvider::compact(
            source,
            reference.maximum_messages as usize,
            reference.summary.clone(),
            reference.retention.clone(),
        )?
        .0 != *compacted
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
    limits: Limits,
) -> Result<()> {
    validate_projected_context(context, limits)?;
    for message in &context.messages {
        for file in message.content.file_refs() {
            verifier.verify(file).await?;
        }
    }
    for (name, file) in &context.metadata {
        if !name.contains('.')
            || name.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
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
            selection::place_messages(context, loaded, self.placement)
        })
    }
}

/// Deterministic reusable compaction stage retaining a bounded message tail.
pub struct CompactionStage {
    /// Maximum messages retained after compaction.
    pub max_messages: usize,
    /// Optional caller-produced summary prepended when compaction occurs.
    pub summary: Option<ContextSummary>,
    /// Consumer-declared mandatory roles and native media retention.
    pub retention: CompactionRetention,
}

impl ContextStage for CompactionStage {
    fn validate(&self) -> Result<()> {
        self.retention.validate()?;
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
            "revision": "3",
            "max_messages": self.max_messages,
            "summary": self.summary,
            "retention": self.retention,
        })
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            self.validate()?;
            Ok(DurableContextProvider::compact(
                &context,
                self.max_messages,
                self.summary.clone(),
                self.retention.clone(),
            )?
            .0)
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
    /// Position of the active turn input. Absent for standalone source projections.
    /// Transformations must preserve this marker when they reorder or replace messages.
    #[cfg_attr(feature = "wasm", tsify(optional))]
    pub current_input_index: Option<u32>,
}

impl Context {
    fn validate_current_input(&self) -> Result<()> {
        if self
            .current_input_index
            .is_some_and(|index| index as usize >= self.messages.len())
        {
            return Err(crate::Error::Invalid(
                "current input index is outside context".into(),
            ));
        }
        Ok(())
    }
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

    /// Builds the canonical selection/current input and in-turn messages before stages.
    /// This boundary contains no stage contributions and performs no source reads.
    pub fn base_context(input: &ContextInput, limits: Limits) -> Result<Context> {
        limits.validate()?;
        input.input.validate_user_input()?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input)?;
        }
        if input
            .selected_context
            .as_ref()
            .is_some_and(|selected| selected.selection.checkpoint.is_some())
        {
            return Err(crate::Error::Unsupported(
                "canonical checkpoint requires owner-resolved base context".into(),
            ));
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
        let current_input_index = u32::try_from(base.len().checked_sub(1).ok_or_else(|| {
            crate::Error::Invalid("current input is missing from context".into())
        })?)
        .map_err(|_| crate::Error::Invalid("current input index exceeds portable count".into()))?;
        let context = Context {
            messages: base
                .into_iter()
                .chain(input.prior_messages.iter().cloned())
                .collect(),
            metadata: BTreeMap::new(),
            current_input_index: Some(current_input_index),
        };
        validate_projected_context(&context, limits)?;
        Ok(context)
    }

    /// Bounds the initial view and every intermediate projection, without truncation.
    pub async fn run_bounded(&self, input: &ContextInput, limits: Limits) -> Result<Context> {
        self.transform_bounded(input, Self::base_context(input, limits)?, limits)
            .await
    }

    /// Applies declared stages to an explicit base, including a retained canonical
    /// checkpoint plus its new history delta. The caller owns that base's provenance;
    /// transforms may replace or reorder it while preserving the current input marker.
    pub async fn transform_bounded(
        &self,
        input: &ContextInput,
        mut context: Context,
        limits: Limits,
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
        validate_projected_context(&context, limits)?;
        if context.current_input_index.is_none() {
            return Err(crate::Error::Invalid(
                "base context has no current input marker".into(),
            ));
        }
        for stage in &self.0 {
            context = stage.apply(input, context).await?;
            validate_projected_context(&context, limits)?;
            if context.current_input_index.is_none() {
                return Err(crate::Error::Invalid(
                    "context stage removed the current input marker".into(),
                ));
            }
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

    #[test]
    fn compaction_keeps_mandatory_messages_and_never_splits_tool_pairs() -> Result<()> {
        let source = Context {
            messages: vec![
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("instructions".into()),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("old".into()),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("old answer".into()),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: "call".into(),
                        name: "example.tool".into(),
                        arguments: Value::Null,
                    }),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "example.tool".into(),
                        value: Value::Null,
                    }),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("current".into()),
                },
            ],
            metadata: BTreeMap::new(),
            current_input_index: None,
        };
        let summary = ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: 6,
            source_digest: crate::contract::canonical_json_digest(&source)?,
            output: message(ModelRole::System, "summary")?
                .content
                .file_refs()
                .into_iter()
                .next()
                .ok_or_else(|| crate::Error::Invalid("summary file is missing".into()))?
                .clone(),
        };
        let prefix = Context {
            messages: source.messages[..3].to_vec(),
            metadata: source.metadata.clone(),
            current_input_index: None,
        };
        let prefix_summary = ContextSummary {
            source_messages: 3,
            source_digest: crate::contract::canonical_json_digest(&prefix)?,
            ..summary.clone()
        };
        let (prefix_compacted, prefix_reference) = DurableContextProvider::compact(
            &source,
            5,
            Some(prefix_summary.clone()),
            CompactionRetention::default(),
        )?;
        assert_eq!(prefix_compacted.messages.get(2..), source.messages.get(3..));
        validate_compaction(&prefix_reference, &source, &prefix_compacted)?;
        assert!(
            DurableContextProvider::compact(
                &source,
                4,
                Some(prefix_summary.clone()),
                CompactionRetention::default(),
            )
            .is_err(),
            "unsummarized messages cannot be dropped to meet the budget"
        );
        for extent in [0, 2, 7] {
            assert!(
                DurableContextProvider::compact(
                    &source,
                    5,
                    Some(ContextSummary {
                        source_messages: extent,
                        ..prefix_summary.clone()
                    }),
                    CompactionRetention::default(),
                )
                .is_err()
            );
        }
        for budget in 1..=6 {
            let result = DurableContextProvider::compact(
                &source,
                budget,
                Some(summary.clone()),
                CompactionRetention::default(),
            );
            if matches!(budget, 1 | 2 | 4) {
                assert!(result.is_err());
                continue;
            }
            let (projected, reference) = result?;
            assert!(projected.messages.contains(&source.messages[0]));
            assert_eq!(projected.messages.last(), source.messages.last());
            assert_eq!(
                projected.messages.contains(&source.messages[3]),
                projected.messages.contains(&source.messages[4])
            );
            validate_compaction(&reference, &source, &projected)?;
            let mut corrupt = projected;
            corrupt
                .messages
                .last_mut()
                .ok_or_else(|| crate::Error::Invalid("empty context".into()))?
                .content = ModelContent::Text("substitution".into());
            assert!(validate_compaction(&reference, &source, &corrupt).is_err());
        }
        assert!(
            DurableContextProvider::compact(&source, 3, None, CompactionRetention::default())
                .is_err()
        );
        let configurable = Context {
            messages: vec![
                source.messages[0].clone(),
                source.messages[1].clone(),
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::File {
                        file: summary.output.clone(),
                        policy: crate::model::FileProjectionPolicy::Native,
                    }),
                },
                source.messages[5].clone(),
            ],
            metadata: BTreeMap::new(),
            current_input_index: None,
        };
        let configured_summary = ContextSummary {
            source_digest: crate::contract::canonical_json_digest(&configurable)?,
            source_messages: 4,
            ..summary
        };
        assert!(
            DurableContextProvider::compact(
                &configurable,
                3,
                Some(configured_summary.clone()),
                CompactionRetention::default(),
            )
            .is_err()
        );
        let retention = CompactionRetention {
            roles: vec![ModelRole::User],
            native_media: false,
        };
        let (projected, mut reference) = DurableContextProvider::compact(
            &configurable,
            3,
            Some(configured_summary),
            retention.clone(),
        )?;
        assert_eq!(
            projected.messages.get(1..),
            Some(
                &[
                    configurable.messages[1].clone(),
                    configurable.messages[3].clone(),
                ][..]
            )
        );
        assert_eq!(reference.retention, retention);
        validate_compaction(&reference, &configurable, &projected)?;
        reference.retention = CompactionRetention::default();
        assert!(validate_compaction(&reference, &configurable, &projected).is_err());
        assert!(
            CompactionRetention {
                roles: vec![ModelRole::User, ModelRole::User],
                native_media: true
            }
            .validate()
            .is_err()
        );
        Ok(())
    }

    struct RefVerifier;

    struct TestPayloadStore {
        volume: VolumeRef,
        files: std::sync::Mutex<BTreeMap<String, (FileRef, Vec<u8>)>>,
        reads: std::sync::atomic::AtomicUsize,
    }

    fn payload_limits() -> Limits {
        Limits {
            file_bytes: 1_048_576,
            render_bytes: 1_048_576,
            context_messages: 4_096,
            attachments: 1_024,
            path_bytes: 4_096,
            ..Limits::default()
        }
    }

    impl TestPayloadStore {
        fn new() -> Result<Self> {
            Ok(Self {
                volume: VolumeRef::new(
                    ProviderRef::new("test", "filesystem", "2")?,
                    "context",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                )?,
                files: std::sync::Mutex::new(BTreeMap::new()),
                reads: std::sync::atomic::AtomicUsize::new(0),
            })
        }
    }

    impl ContentPublisher for TestPayloadStore {
        fn volume(&self) -> &VolumeRef {
            &self.volume
        }

        fn stage<'a>(
            &'a self,
            operation: crate::OperationId,
            path: &'a str,
            bytes: &'a [u8],
            media_type: &'a str,
            display_name: &'a str,
        ) -> BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move {
                let reference = FileRef::new(
                    self.volume.clone(),
                    path,
                    operation.to_string(),
                    FileDescriptor::from_bytes(bytes, media_type)?,
                    display_name,
                )?;
                let mut files = self
                    .files
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some((retained, body)) = files.get(path) {
                    if retained != &reference || body != bytes {
                        return Err(crate::Error::Conflict("payload identity changed".into()));
                    }
                    return Ok(retained.clone());
                }
                files.insert(path.to_owned(), (reference.clone(), bytes.to_vec()));
                Ok(reference)
            })
        }
    }

    impl ContentResidencyVerifier for TestPayloadStore {
        fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move { file.validate() })
        }

        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            Box::pin(async move {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let files = self
                    .files
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (reference, bytes) = files
                    .get(file.path())
                    .ok_or_else(|| crate::Error::Storage("missing context payload".into()))?;
                if file != reference {
                    return Err(crate::Error::Storage(
                        "context payload reference changed".into(),
                    ));
                }
                Ok(bytes.clone())
            })
        }
    }

    struct RemoveInputMarker;

    impl ContextStage for RemoveInputMarker {
        fn name(&self) -> &str {
            "remove-input-marker"
        }

        fn contract(&self) -> Value {
            serde_json::json!({"name": self.name(), "revision": "1"})
        }

        fn apply<'a>(
            &'a self,
            _: &'a ContextInput,
            mut context: Context,
        ) -> BoxFuture<'a, Result<Context>> {
            context.current_input_index = None;
            Box::pin(async move { Ok(context) })
        }
    }

    #[test]
    fn repeated_compaction_replaces_summaries_and_retains_instructions() -> Result<()> {
        let source = Context {
            messages: vec![
                message(ModelRole::System, "instructions")?,
                message(ModelRole::User, "old")?,
                message(ModelRole::Assistant, "old-answer")?,
                message(ModelRole::User, "current")?,
            ],
            metadata: BTreeMap::new(),
            current_input_index: Some(3),
        };
        let make_summary = |source: &Context, label: &str| -> Result<ContextSummary> {
            let output = message(ModelRole::Assistant, label)?;
            Ok(ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: u32::try_from(source.messages.len())
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                source_digest: crate::contract::canonical_json_digest(source)?,
                output: output.content.file_refs()[0].clone(),
            })
        };
        let first_summary = make_summary(&source, "first-summary")?;
        let (mut next, reference) = DurableContextProvider::compact(
            &source,
            3,
            Some(first_summary.clone()),
            CompactionRetention::default(),
        )?;
        validate_compaction(&reference, &source, &next)?;
        assert_eq!(next.messages[0].role, ModelRole::Assistant);
        next.messages.extend([
            message(ModelRole::Assistant, "later-answer")?,
            message(ModelRole::User, "latest")?,
        ]);
        next.current_input_index = Some(4);
        let (compacted, reference) = DurableContextProvider::compact(
            &next,
            3,
            Some(make_summary(&next, "second-summary")?),
            CompactionRetention::default(),
        )?;
        validate_compaction(&reference, &next, &compacted)?;
        assert_eq!(compacted.messages.len(), 3);
        assert!(compacted.messages.contains(&source.messages[0]));
        assert_eq!(compacted.messages.last(), next.messages.last());
        assert_eq!(compacted.current_input_index, Some(2));
        assert!(
            compacted
                .messages
                .iter()
                .all(|message| { !message.content.file_refs().contains(&&first_summary.output) })
        );
        Ok(())
    }

    #[test]
    fn compaction_retains_current_user_before_trailing_tool_results() -> Result<()> {
        let source = Context {
            messages: vec![
                message(ModelRole::System, "instructions")?,
                message(ModelRole::User, "old")?,
                message(ModelRole::Assistant, "old-answer")?,
                message(ModelRole::User, "current")?,
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: "current-call".into(),
                        name: "tool".into(),
                        arguments: Value::Null,
                    }),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "current-call".into(),
                        name: "tool".into(),
                        value: Value::Null,
                    }),
                },
            ],
            metadata: BTreeMap::new(),
            current_input_index: Some(3),
        };
        let summary = ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: 6,
            source_digest: crate::contract::canonical_json_digest(&source)?,
            output: message(ModelRole::Assistant, "summary")?
                .content
                .file_refs()[0]
                .clone(),
        };
        assert!(
            DurableContextProvider::compact(
                &source,
                4,
                Some(summary.clone()),
                CompactionRetention::default(),
            )
            .is_err()
        );
        let (compacted, reference) = DurableContextProvider::compact(
            &source,
            5,
            Some(summary),
            CompactionRetention::default(),
        )?;
        validate_compaction(&reference, &source, &compacted)?;
        assert!(compacted.messages.contains(&source.messages[3]));
        assert_eq!(compacted.messages.last(), source.messages.last());
        assert!(compacted.messages.contains(&source.messages[4]));
        assert!(compacted.messages.contains(&source.messages[5]));
        assert_eq!(compacted.current_input_index, Some(2));
        Ok(())
    }

    struct InputDependentProjection {
        revision: u32,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ContextStage for InputDependentProjection {
        fn name(&self) -> &str {
            "input-dependent-projection"
        }

        fn contract(&self) -> Value {
            serde_json::json!({ "name": self.name(), "revision": self.revision })
        }

        fn apply<'a>(
            &'a self,
            input: &'a ContextInput,
            mut context: Context,
        ) -> BoxFuture<'a, Result<Context>> {
            Box::pin(async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                context.messages.push(ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(format!(
                        "stage:{}:{}",
                        self.revision,
                        serde_json::to_string(&input.input)
                            .map_err(|error| crate::Error::Invalid(error.to_string()))?
                    )),
                });
                context.messages.reverse();
                let length = u32::try_from(context.messages.len())
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?;
                context.current_input_index =
                    context.current_input_index.map(|index| length - 1 - index);
                Ok(context)
            })
        }
    }

    fn retained_input_base(input: ModelContent) -> Context {
        Context {
            messages: vec![
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("older question".into()),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Text("older answer".into()),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: input,
                },
            ],
            current_input_index: Some(2),
            metadata: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn explicit_base_boundary_reruns_input_dependent_custom_transforms_and_reload()
    -> Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipeline = ContextPipeline::new([Arc::new(InputDependentProjection {
            revision: 1,
            calls: calls.clone(),
        }) as Arc<dyn ContextStage>]);
        let mut input = ContextInput {
            input: ModelContent::Text("first input".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let raw = ContextPipeline::base_context(&input, Limits::default())?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(ContextPipeline::default().run(&input).await?, raw);
        // The continuation owner supplies its retained base plus the current delta.
        // No prior stage contribution is saved into this base.
        let retained = retained_input_base(input.input.clone());
        let first = pipeline
            .transform_bounded(&input, retained.clone(), Limits::default())
            .await?;
        assert_eq!(first.current_input_index, Some(1));
        assert_eq!(first.messages[1].content, input.input);
        assert_eq!(
            first.messages[0].content,
            ModelContent::Text("stage:1:\"first input\"".into())
        );
        assert_eq!(retained.messages.len(), 3);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        input.input = ModelContent::Text("later input".into());
        let mut next_base = retained.clone();
        next_base.messages[2].content = input.input.clone();
        let next = pipeline
            .transform_bounded(&input, next_base.clone(), Limits::default())
            .await?;
        assert_eq!(
            next.messages[0].content,
            ModelContent::Text("stage:1:\"later input\"".into())
        );
        let reloaded =
            pipeline.reload(ContextPipeline::new([Arc::new(InputDependentProjection {
                revision: 2,
                calls: calls.clone(),
            })
                as Arc<dyn ContextStage>]))?;
        let next = reloaded
            .transform_bounded(&input, next_base, Limits::default())
            .await?;
        assert_eq!(
            next.messages[0].content,
            ModelContent::Text("stage:2:\"later input\"".into())
        );
        assert_eq!(
            next.messages
                .iter()
                .filter(|message| message.role == ModelRole::System)
                .count(),
            1
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        let invalid = Context {
            current_input_index: None,
            ..retained
        };
        assert!(
            pipeline
                .transform_bounded(&input, invalid, Limits::default())
                .await
                .is_err()
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        Ok(())
    }

    #[tokio::test]
    async fn context_stages_preserve_explicit_input_position() -> Result<()> {
        let input = ContextInput {
            input: ModelContent::Text("active input".into()),
            selected_context: None,
            step: 0,
            prior_messages: vec![ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("prior answer".into()),
            }],
        };
        let source = Arc::new(Context {
            messages: vec![message(ModelRole::User, "additional-facts")?],
            ..Context::default()
        });
        let pipeline = ContextPipeline::new([
            Arc::new(SourceStage::new(
                "prepend",
                "1",
                source.clone(),
                ContextPlacement::Prepend,
            )) as Arc<dyn ContextStage>,
            Arc::new(SourceStage::new(
                "append",
                "1",
                source,
                ContextPlacement::Append,
            )),
        ]);
        let projected = pipeline.run(&input).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(projected.messages[1].content, input.input);
        let summary = ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: 4,
            source_digest: crate::contract::canonical_json_digest(&projected)?,
            output: message(ModelRole::Assistant, "summary")?
                .content
                .file_refs()[0]
                .clone(),
        };
        let (compacted, reference) = DurableContextProvider::compact(
            &projected,
            3,
            Some(summary),
            CompactionRetention::default(),
        )?;
        validate_compaction(&reference, &projected, &compacted)?;
        assert_eq!(compacted.current_input_index, Some(1));
        assert_eq!(compacted.messages[1].content, input.input);
        assert_eq!(compacted.messages.last(), projected.messages.last());
        assert!(
            ContextPipeline::new([Arc::new(RemoveInputMarker) as Arc<dyn ContextStage>])
                .run(&input)
                .await
                .is_err()
        );

        let invalid = Context {
            current_input_index: Some(4),
            ..projected
        };
        assert!(
            validate_projected_context(&invalid, crate::conversation::Limits::default()).is_err()
        );
        assert!(
            DurableContextProvider::compact(&invalid, 4, None, CompactionRetention::default())
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn context_pages_reject_corrupt_identity_and_unproven_compaction() -> Result<()> {
        let context = Context {
            messages: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("pinned".into()),
            }],
            metadata: BTreeMap::new(),
            current_input_index: None,
        };
        let payloads = Arc::new(TestPayloadStore::new()?);
        let payload = payloads
            .stage(
                crate::OperationId::new(),
                "context/projections/pinned.json",
                &crate::contract::canonical_json_bytes(&context)?,
                "application/json",
                "context.json",
            )
            .await?;
        let record = StoredContextRevision {
            format_version: 5,
            revision: 1,
            source: "instructions".into(),
            source_revision: "1".into(),
            context: payload,
            compaction: None,
        };
        let mut records = Vec::new();
        for (field, value) in [
            ("format_version", serde_json::json!(0)),
            ("revision", serde_json::json!(2)),
            ("source", serde_json::json!("another-source")),
            ("source_revision", serde_json::json!("another-revision")),
            ("unknown", serde_json::json!(true)),
        ] {
            let mut corrupt = serde_json::to_value(&record)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            corrupt[field] = value;
            records.push(crate::contract::canonical_json_bytes(&corrupt)?);
        }
        records.push(
            serde_json::to_vec_pretty(&record)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
        );
        records.push(b"not-json".to_vec());
        let mut missing_source = record;
        missing_source.compaction = Some(CompactionReference {
            source_digest: crate::contract::canonical_json_digest(&context)?,
            maximum_messages: 2,
            retention: CompactionRetention::default(),
            summary: None,
        });
        records.push(crate::contract::canonical_json_bytes(&missing_source)?);
        for bytes in records {
            let stream = Arc::new(MemoryStream::default());
            let path = StreamPath::new("corrupt/context")
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            stream
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![Bytes::from(bytes)],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let provider = DurableContextProvider::new(
                stream,
                path,
                "instructions",
                "1",
                1,
                payloads.clone(),
                payload_limits(),
            )?;
            assert!(provider.revisions(0, 1).await.is_err());
            assert!(
                provider
                    .decode_revision(0, &vec![b' '; MAX_RECORD_BYTES + 1])
                    .await
                    .is_err()
            );
        }
        Ok(())
    }

    struct CountingVerifier {
        count: std::sync::atomic::AtomicUsize,
        payloads: Arc<TestPayloadStore>,
    }
    impl ContentResidencyVerifier for CountingVerifier {
        fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                file.validate()
            })
        }

        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            self.payloads.read(file)
        }
    }

    #[tokio::test]
    async fn latest_context_work_is_independent_of_retained_revisions() -> Result<()> {
        for retained in [1, 16, 128] {
            let payloads = Arc::new(TestPayloadStore::new()?);
            let verifier = Arc::new(CountingVerifier {
                count: std::sync::atomic::AtomicUsize::new(0),
                payloads: payloads.clone(),
            });
            let provider = DurableContextProvider::new(
                Arc::new(MemoryStream::default()),
                StreamPath::new("bounded/context")
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                "instructions",
                "1",
                7,
                verifier.clone(),
                payload_limits(),
            )?
            .with_publisher(payloads)?;
            let context = Context {
                messages: vec![message(ModelRole::System, "pinned")?],
                metadata: BTreeMap::new(),
                current_input_index: None,
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
            verifier.count.store(0, std::sync::atomic::Ordering::SeqCst);
            let started = std::time::Instant::now();
            assert_eq!(provider.latest().await?, context);
            assert_eq!(verifier.count.load(std::sync::atomic::Ordering::SeqCst), 2);
            eprintln!(
                "context retained={retained} verified_refs=2 active_wire_bytes={} elapsed_us={}",
                crate::contract::canonical_json_bytes(&context)?.len(),
                started.elapsed().as_micros()
            );
            let through = provider.tail_revision().await?;
            provider
                .append(
                    through,
                    context.clone(),
                    None,
                    Bytes::from_static(b"later-revision"),
                )
                .await?;
            let mut after = 0;
            let mut visited = 0;
            loop {
                let page = provider.revisions(after, through).await?;
                assert!(page.len() <= 7);
                if page.is_empty() {
                    break;
                }
                for revision in &page {
                    assert_eq!(revision.revision, after + 1);
                    assert!(revision.revision <= through);
                    after = revision.revision;
                    visited += 1;
                }
            }
            assert_eq!(visited, retained);
            assert!(provider.revisions(through + 1, through).await.is_err());
            assert!(provider.revisions(0, through + 2).await.is_err());
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
                retention: CompactionRetention::default(),
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
        let payloads = Arc::new(TestPayloadStore::new()?);
        let path = StreamPath::new("runtime/context/memory")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            2,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads.clone())?;
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
                        current_input_index: Some(1),
                    },
                    None,
                    Bytes::from_static(b"invalid-marker-rejected")
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
            current_input_index: None,
        };
        provider
            .append(0, original.clone(), None, Bytes::from_static(b"context-1"))
            .await?;
        let (compacted, reference) = DurableContextProvider::compact(
            &original,
            2,
            Some(ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: 3,
                source_digest: crate::contract::canonical_json_digest(&original)?,
                output: message(ModelRole::System, "summary")?
                    .content
                    .file_refs()
                    .into_iter()
                    .next()
                    .ok_or_else(|| crate::Error::Invalid("summary file is missing".into()))?
                    .clone(),
            }),
            CompactionRetention::default(),
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
        provider
            .append(2, compacted.clone(), None, Bytes::from_static(b"context-3"))
            .await?;

        let reopened = Arc::new(DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            2,
            payloads.clone(),
            payload_limits(),
        )?);
        let revisions = reopened.revisions(0, 2).await?;
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[1].compaction, Some(reference));
        assert_eq!(reopened.revisions(1, 2).await?, revisions[1..]);
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
        let undersized = DurableContextProvider::new(
            stream,
            path,
            "memory",
            "1",
            1,
            payloads,
            payload_limits(),
        )?;
        assert_eq!(undersized.latest().await?, compacted);
        assert_eq!(undersized.revisions(0, 2).await?.len(), 1);
        let second = undersized.revisions(1, 2).await?;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].context, compacted);
        Ok(())
    }

    #[tokio::test]
    async fn continuing_compaction_publishes_and_retries_one_atomic_pair() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(TestPayloadStore::new()?);
        let path = StreamPath::new("runtime/context/continuing")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads.clone())?;
        let source = Context {
            messages: vec![
                message(ModelRole::User, "one")?,
                message(ModelRole::Assistant, "two")?,
                message(ModelRole::User, "three")?,
            ],
            current_input_index: Some(2),
            ..Context::default()
        };
        let mut covered = source.clone();
        covered.messages.truncate(2);
        covered.current_input_index = None;
        let output = message(ModelRole::System, "summary")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Invalid("summary file is missing".into()))?
            .clone();
        let (compacted, reference) = DurableContextProvider::compact(
            &source,
            2,
            Some(ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: 2,
                source_digest: crate::contract::canonical_json_digest(&covered)?,
                output,
            }),
            CompactionRetention::default(),
        )?;
        let key = Bytes::from_static(b"atomic-compaction");
        // Discard the first successful acknowledgement, as a caller whose
        // receipt was lost would. The same admission must remain recoverable.
        provider
            .append_compaction(
                0,
                source.clone(),
                compacted.clone(),
                reference.clone(),
                key.clone(),
            )
            .await?;
        let reopened = DurableContextProvider::new(
            stream,
            path,
            "memory",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads)?;
        let pair = reopened
            .append_compaction(0, source.clone(), compacted.clone(), reference.clone(), key)
            .await?;
        assert_eq!(reopened.tail_revision().await?, 2);
        assert_eq!(pair.first().map(|record| &record.context), Some(&source));
        assert_eq!(pair.last().map(|record| &record.context), Some(&compacted));
        assert_eq!(reopened.latest().await?, compacted);
        let pin = reopened
            .latest_revision()
            .await?
            .ok_or_else(|| crate::Error::Storage("published context revision is missing".into()))?;
        assert_eq!(pin.revision, 2);
        assert_eq!(reopened.revision(pin.revision).await?, pin);
        assert!(reopened.revision(0).await.is_err());
        assert_eq!(reopened.revisions(0, 1).await?, pair[..1]);
        assert_eq!(reopened.revisions(1, 2).await?, pair[1..]);
        assert!(matches!(
            reopened
                .append_compaction(
                    0,
                    source.clone(),
                    compacted.clone(),
                    reference.clone(),
                    Bytes::from_static(b"stale-compaction")
                )
                .await,
            Err(crate::Error::Conflict(_))
        ));
        let mut forged = reference.clone();
        forged.source_digest[0] ^= 1;
        assert!(
            reopened
                .append_compaction(
                    2,
                    source.clone(),
                    compacted.clone(),
                    forged,
                    Bytes::from_static(b"forged-compaction")
                )
                .await
                .is_err()
        );
        assert!(
            reopened
                .append_compaction(
                    u64::MAX - 1,
                    source,
                    compacted,
                    reference,
                    Bytes::from_static(b"overflow-compaction")
                )
                .await
                .is_err()
        );
        assert_eq!(reopened.tail_revision().await?, 2);
        reopened
            .append(
                2,
                Context::default(),
                None,
                Bytes::from_static(b"newer-context"),
            )
            .await?;
        assert_eq!(reopened.latest().await?, Context::default());
        assert_eq!(reopened.revision(pin.revision).await?, pin);
        Ok(())
    }

    #[tokio::test]
    async fn context_payload_missing_corrupt_and_oversized_fail_without_fallback() -> Result<()> {
        for failure in [0, 1, 2] {
            let stream = Arc::new(MemoryStream::default());
            let payloads = Arc::new(TestPayloadStore::new()?);
            let path = StreamPath::new("context/payload-negatives")
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            let provider = DurableContextProvider::new(
                stream.clone(),
                path.clone(),
                "memory",
                "1",
                1,
                payloads.clone(),
                payload_limits(),
            )?
            .with_publisher(payloads.clone())?;
            let context = Context {
                messages: vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("pinned payload".into()),
                }],
                ..Context::default()
            };
            let published = provider
                .append(0, context, None, Bytes::from_static(b"payload"))
                .await?;
            if failure < 2 {
                {
                    let mut files = payloads.files.lock().unwrap();
                    if failure == 0 {
                        files.remove(published.content.path());
                    } else {
                        let (_, bytes) =
                            files.get_mut(published.content.path()).ok_or_else(|| {
                                crate::Error::Storage("payload fixture is missing".into())
                            })?;
                        if let Some(first) = bytes.first_mut() {
                            *first ^= 1;
                        }
                    }
                }
                assert!(provider.latest().await.is_err());
            } else {
                payloads.reads.store(0, std::sync::atomic::Ordering::SeqCst);
                let reader = DurableContextProvider::new(
                    stream,
                    path,
                    "memory",
                    "1",
                    1,
                    payloads.clone(),
                    Limits {
                        render_bytes: 1,
                        ..payload_limits()
                    },
                )?;
                assert!(reader.latest().await.is_err());
                assert_eq!(payloads.reads.load(std::sync::atomic::Ordering::SeqCst), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn context_payload_bounds_and_missing_writer_do_not_publish() -> Result<()> {
        for (file_bytes, render_bytes, writer) in [
            (128, 1_024, true),
            (1_024, 128, true),
            (1_024, 1_024, false),
        ] {
            let payloads = Arc::new(TestPayloadStore::new()?);
            let provider = DurableContextProvider::new(
                Arc::new(MemoryStream::default()),
                StreamPath::new("context/bounds")
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                "memory",
                "1",
                1,
                payloads.clone(),
                Limits {
                    file_bytes,
                    render_bytes,
                    ..payload_limits()
                },
            )?;
            let provider = if writer {
                provider.with_publisher(payloads.clone())?
            } else {
                provider
            };
            let context = Context {
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("x".repeat(512)),
                }],
                current_input_index: Some(0),
                ..Context::default()
            };
            assert!(
                provider
                    .append(0, context, None, Bytes::from_static(b"oversized"))
                    .await
                    .is_err()
            );
            assert_eq!(provider.tail_revision().await?, 0);
            assert!(payloads.files.lock().unwrap().is_empty());
        }
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
