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
pub(crate) use selection::place_messages_with_io;
pub use selection::*;
mod discovery;
pub use discovery::*;
mod compaction;
pub use compaction::*;
pub(crate) use compaction::clone_context_prefix;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use compaction::clone_retention_native;
mod snapshot;
pub use snapshot::*;

/// Durable output of an ordinary admitted summary model operation.
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
    pub(crate) fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.check_cancellation()?; }
        for (position, role) in self.roles.iter().enumerate() {
            if self.roles[..position].contains(role) {
                return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                    "compaction retention repeats a role"));
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
    /// Checks a bounded envelope, sharing the I/O companion's policy and native scratch custody.
    pub fn validate(
        &self, limits: Limits, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        if self.selection.conversation_revision == 0
            || self.selection.message_ids.is_empty()
            || self.selection.message_ids.len() > limits.context_messages
        {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "canonical checkpoint coverage is invalid"));
        }
        // Validate immutable references before the bounded scratch set grows.
        for reference in [&self.source, &self.retained, &self.compaction] {
            crate::executor::with_history_io(io, |work| limits.validate_file(reference, work))?;
            if reference.volume() != self.source.volume()
                || reference.descriptor().media_type() != "application/json"
            {
                return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "canonical checkpoint payload scope is invalid"));
            }
        }
        if let Some(previous) = &self.selection.checkpoint {
            crate::executor::with_history_io(io, |work| limits.validate_file(previous, work))?;
        }
        #[cfg(feature = "filesystem")]
        let mut history = io.map(crate::executor::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> =
            history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        #[cfg(feature = "filesystem")]
        let _unique_guard = {
            // Logical borrowed entry slots only, never opaque allocator nodes.
            let bytes = self.selection.message_ids.len()
                .checked_mul(std::mem::size_of::<&uuid::Uuid>())
                .ok_or_else(|| crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "canonical checkpoint identity storage overflows"))?;
            crate::wire_codec::CodecQuota::new(&mut observed, bytes as u64)?
        };
        let mut unique = std::collections::BTreeSet::new();
        for id in &self.selection.message_ids {
            context_checkpoint_step(io)?;
            unique.insert(id);
        }
        if unique.len() != self.selection.message_ids.len() {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "canonical checkpoint repeats source identities"));
        }
        Ok(())
    }

    /// Encodes one bounded envelope for the owning journal's normal content publisher.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        self.validate(limits, None)?;
        // Count the typed envelope before canonical serialization creates its Value tree and byte buffer.
        crate::contract::validate_json_byte_bound(
            self,
            limits.file_bytes.min(limits.render_bytes),
        )?;
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
        &self, source: &Context, retained: &Context, compaction: &CompactionReference,
        limits: Limits, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.validate(limits, io)?;
        validate_projected_context(source, limits, io)?;
        validate_projected_context(retained, limits, io)?;
        validate_compaction(compaction, source, retained, io)?;
        if compaction
            .summary
            .as_ref()
            .is_none_or(|summary| summary.operation_id != self.operation_id)
        {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "canonical checkpoint summary admission differs"));
        }
        Ok(())
    }
}

fn context_checkpoint_step(io: Option<&crate::executor::PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
    }
    let _ = io;
    Ok(())
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

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for StoredContextRevision {
    fn maximum_slot_bytes() -> u64 {
        (<Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes())
            .max(std::mem::size_of::<Self>() as u64)
    }
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
        limits.validate(&mut None)?;
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
    pub fn with_publisher(mut self, publisher: Arc<dyn ContentPublisher>) -> Result<Self> {
        publisher.volume().validate(&mut None)?;
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
        let path = format!("context/projections/{operation}.json");
        let (content, _content_hold) = publisher
            .stage(
                operation,
                &path,
                &bytes,
                "application/json",
                "context.json",
                None,
            )?
            .await?;
        self.limits.validate_file(&content, &mut None)?;
        if content.volume() != publisher.volume()
            || content.descriptor().media_type() != "application/json"
        {
            return Err(crate::Error::Unauthorized(
                "context publisher returned another binding".into(),
            ));
        }
        content.descriptor().verify(&bytes, &mut None)?;
        verify_file(&content, self.content_verifier.as_ref(), None).await?;
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
        validate_context_refs(&context, self.content_verifier.as_ref(), self.limits, None).await?;
        let revision = next_revision(expected_revision)?;
        let key_bytes = idempotency_key.into();
        let key = StreamIdempotencyKey::new(key_bytes.clone())
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        if let Some(reference) = &compaction {
            let source = self.read_revision(expected_revision, self.limits, None).await?;
            validate_compaction(reference, &source.context, &context, None)?;
        }
        let (record, encoded) = self
            .stage_revision(revision, context, compaction, &key_bytes)
            .await?;
        match self
            .provider
            .append(AppendRequest {
                path: self.path.clone(),
                records: vec![encoded.into()],
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
        validate_projected_context(&source, self.limits, None)?;
        validate_projected_context(&compacted, self.limits, None)?;
        validate_compaction(&reference, &source, &compacted, None)?;
        validate_context_refs(&source, self.content_verifier.as_ref(), self.limits, None).await?;
        validate_context_refs(&compacted, self.content_verifier.as_ref(), self.limits, None).await?;
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
                records: vec![source_bytes.into(), compacted_bytes.into()],
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
    pub async fn tail_revision(&self, io: Option<&crate::executor::PreparationIo>) -> Result<u64> {
        crate::executor::with_history_io(io, |work| work.as_deref_mut().map_or(Ok(()), |work| work.begin_call()))?;
        match self.provider.tail(self.path.clone()).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(context_stream_error(error, io)),
        }
    }

    /// Reads one verified page after a cursor through a captured revision.
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
        crate::obs::outcome(async {
            if after > through || through > self.tail_revision(None).await? {
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
                let revision = self
                    .decode_revision(record.sequence, &record.value, self.limits, None)
                    .await?;
                if let Some(reference) = &revision.compaction {
                    let preceding = if revisions.is_empty() {
                        if after == 0 {
                            return Err(crate::Error::Storage(
                                "context compaction has no preceding source".into(),
                            ));
                        }
                        Some(self.read_revision(after, self.limits, None).await?)
                    } else {
                        None
                    };
                    let source = revisions.last().or(preceding.as_ref()).ok_or_else(|| {
                        crate::Error::Storage("context compaction source is absent".into())
                    })?;
                    validate_compaction(reference, &source.context, &revision.context, None)
                        .map_err(|error| crate::Error::Storage(error.to_string()))?;
                }
                revisions.push(revision);
            }
            if revisions.len() as u64 != count {
                return Err(crate::Error::Storage("context page is incomplete".into()));
            }
            crate::obs::obs_record!("items" = revisions.len() as u64);
            Ok(revisions)
        }.await)
    }

    async fn decode_revision(
        &self, sequence: u64, bytes: &[u8], limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ContextRevision> {
        let limits = restrict_context_limits(self.limits, limits, io)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage,
                "context record exceeds Stream limit"));
        }
        let revision: StoredContextRevision = decode_context_json(
            bytes, MAX_RECORD_BYTES as u64, io,
            "durable context identity, revision or canonical bytes are invalid")?;
        let next = sequence.checked_add(1).ok_or_else(|| crate::executor::stock_boundary(
            io, crate::wire::ErrorCode::Invalid, "revision exhausted"))?;
        if revision.format_version != 5 || revision.revision != next
            || revision.source != self.source || revision.source_revision != self.source_revision
        {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage,
                "durable context identity, revision or canonical bytes are invalid"));
        }
        crate::executor::with_history_io(io, |work| limits.validate_file(&revision.context, work))?;
        if revision.context.descriptor().media_type() != "application/json"
            || revision.context.descriptor().byte_length() > limits.render_bytes
        {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage,
                "context payload descriptor is invalid"));
        }
        verify_file(&revision.context, self.content_verifier.as_ref(), io).await?;
        if !self.content_verifier.supports_read(revision.context.volume(), io)? {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Unsupported,
                "context reader has no bounded capability"));
        }
        let payload = self.content_verifier.read(&revision.context,
            revision.context.descriptor().byte_length(), io)?.await?;
        #[cfg(feature = "filesystem")]
        let _payload_hold = io.map(|io| io.guard_bytes(&payload)).transpose()?;
        crate::executor::with_history_io(io, |work| revision.context.descriptor().verify(&payload, work))?;
        let context = decode_context_json(&payload, limits.render_bytes, io,
            "context payload is not canonical")?;
        validate_context_refs(&context, self.content_verifier.as_ref(), limits, io).await?;
        Ok(ContextRevision {
            format_version: revision.format_version, revision: revision.revision,
            source: revision.source, source_revision: revision.source_revision,
            content: revision.context, context, compaction: revision.compaction,
        })
    }

    async fn read_revision(
        &self, revision: u64, limits: Limits, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ContextRevision> {
        let limits = restrict_context_limits(self.limits, limits, io)?;
        if revision == 0 {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "context revision must be positive"));
        }
        crate::executor::with_history_io(io, |work| work.as_deref_mut().map_or(Ok(()), |work| work.begin_call()))?;
        let mut stream = self.provider.read(ReadRequest {
            path: self.path.clone(), from: revision - 1, limit: 1,
        }).await.map_err(|error| context_stream_error(error, io))?;
        #[cfg(feature = "filesystem")]
        let record_hold;
        let record = stream.next().await.ok_or_else(|| crate::executor::stock_boundary(
            io, crate::wire::ErrorCode::Storage, "context revision is missing"))?
            .map_err(|error| context_stream_error(error, io))?;
        crate::executor::with_history_io(io, |work| work.as_deref_mut().map_or(Ok(()),
            |work| work.receive_record(record.value.len() as u64)))?;
        #[cfg(feature = "filesystem")]
        { record_hold = io.map(|io| io.guard_owned(record.value.len() as u64)).transpose()?; }
        if record.sequence != revision - 1 {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage,
                "context revision read returned an invalid range"));
        }
        #[cfg(feature = "filesystem")]
        let extra_hold;
        if let Some(extra) = stream.next().await {
            let extra = extra.map_err(|error| context_stream_error(error, io))?;
            crate::executor::with_history_io(io, |work| work.as_deref_mut().map_or(Ok(()),
                |work| work.receive_record(extra.value.len() as u64)))?;
            #[cfg(feature = "filesystem")]
            { extra_hold = io.map(|io| io.guard_owned(extra.value.len() as u64)).transpose()?; }
            drop(extra);
            #[cfg(feature = "filesystem")]
            drop(extra_hold);
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage,
                "context revision read returned an invalid range"));
        }
        let result = self.decode_revision(record.sequence, &record.value, limits, io).await?;
        drop(record); drop(stream);
        #[cfg(feature = "filesystem")]
        drop(record_hold);
        Ok(result)
    }

    /// Loads one pinned revision, checking its source proof with at most two record reads.
    pub async fn revision(&self, revision: u64) -> Result<ContextRevision> {
        self.revision_bounded(revision, self.limits, None).await
    }

    async fn revision_bounded(&self, revision: u64, limits: Limits, io: Option<&crate::executor::PreparationIo>) -> Result<ContextRevision> {
        let record = self.read_revision(revision, limits, io).await?;
        if let Some(reference) = &record.compaction {
            #[cfg(feature = "filesystem")]
            let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
            #[cfg(feature = "filesystem")]
            let source_hold;
            let source = self.read_revision(revision - 1, limits, io).await?;
            #[cfg(feature = "filesystem")]
            { source_hold = io.map(|io| io.guard_owned(io.owned_bytes()? - before)).transpose()?; }
            validate_compaction(reference, &source.context, &record.context, io)?;
            drop(source);
            #[cfg(feature = "filesystem")]
            drop(source_hold);
        }
        Ok(record)
    }

    /// Captures the current continuing projection and its immutable revision.
    pub async fn latest_revision(&self) -> Result<Option<ContextRevision>> {
        let tail = self.tail_revision(None).await?;
        if tail == 0 {
            return Ok(None);
        }
        self.revision(tail).await.map(Some)
    }

    /// Returns the captured latest context. `latest_revision` also retains its pin; `revisions(after, through)` provides explicit bounded archival pages.
    pub async fn latest(&self) -> Result<Context> {
        Ok(self
            .latest_revision()
            .await?
            .map_or_else(Context::default, |record| record.context))
    }

    /// Compacts a context on the optional original preparation ledger.
    pub fn compact(
        context: &Context, max_messages: usize, summary: Option<ContextSummary>,
        retention: CompactionRetention, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Context, CompactionReference)> {
        retention.validate(io)?;
        context.validate_current_input(io)?;
        if max_messages == 0 {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "compaction max_messages must be positive"));
        }
        let maximum_messages = u32::try_from(max_messages).map_err(|_| crate::executor::stock_boundary(
            io, crate::wire::ErrorCode::Invalid, "compaction budget exceeds portable count"))?;
        let source_digest = crate::executor::preparation_digest(context, io)?;
        if let Some(summary) = &summary {
            let covered = context.messages.get(..summary.source_messages as usize)
                .ok_or_else(|| crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                    "summary extent exceeds source projection"))?;
            let summarized = BorrowedContext {
                messages: ContextMessages { first: covered, user: None, second: &[] },
                metadata: &context.metadata,
                current_input_index: context.current_input_index.filter(|index| *index < summary.source_messages),
            };
            if summary.source_messages == 0
                || summary.source_digest != crate::executor::preparation_digest(&summarized, io)?
                || summary.output.descriptor().media_type() != "text/plain"
            {
                return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                    "summary does not bind its source prefix"));
            }
        }
        let (compacted, _) = crate::executor::with_history_io(io, |work| {
            if context.messages.len() > max_messages
                || (summary.is_some() && context.messages.len() == max_messages)
            {
                let summary = summary.as_ref().ok_or_else(|| crate::executor::stock_boundary(
                    io, crate::wire::ErrorCode::Invalid,
                    "compaction requires a durable summary; context cannot be silently dropped"))?;
                compaction::compact_context(context, max_messages, summary, &retention, work)
            } else {
                compaction::clone_context(context, work)
            }
        })?;
        Ok((compacted, CompactionReference { source_digest, maximum_messages, summary, retention }))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn clone_summary_native(
    summary: Option<&ContextSummary>, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Option<ContextSummary>, acyclic_fs::NativeAllocation)> {
    match summary {
        Some(summary) => {
            let mut io = crate::store::history::native_work_io(work)?;
            let output = summary.output.copy(&mut Some(&mut io))?;
            let custody = crate::store::history::HistoryIo::take_native_custody(&mut io, output.logical_owned_bytes()?)?;
            Ok((Some(ContextSummary { operation_id: summary.operation_id, step: summary.step,
                source_messages: summary.source_messages, source_digest: summary.source_digest,
                output }), custody))
        }
        None => Ok((None, work.retain_owned(0).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?)),
    }
}

pub(crate) fn checked_compaction_budget(
    policy: &ThresholdCompaction, context: &Context, count: &ModelTokenCount,
    capacity: ModelContextCapacity, maximum_messages: usize,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(usize, usize)> {
    crate::executor::with_history_io(io, |work| {
        let (through, maximum) = policy.projection_budget(context, count, maximum_messages, work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (selected, _pending) = compaction::mandatory_flags(context, through, &policy.retention, &mut quota)?;
        let mandatory_tokens = selected.iter().enumerate().filter(|(_, selected)| **selected)
            .try_fold(u64::from(count.fixed_tokens), |total, (position, _)| {
                if let Some(work) = quota.work().as_deref_mut() {
                    work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                }
                let tokens = count.message_tokens.get(position).ok_or_else(||
                    crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                        "mandatory token position is missing"))?;
                total.checked_add(u64::from(*tokens)).ok_or_else(||
                    crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                        "mandatory token count overflows"))
            })?;
        if policy.needs_compaction(capacity, mandatory_tokens)? {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "mandatory content exceeds selected model capacity"));
        }
        Ok((through, maximum))
    })
}

pub(crate) fn validate_compaction(
    reference: &CompactionReference, source: &Context, compacted: &Context,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let summary = match &reference.summary {
            Some(summary) => Some(ContextSummary {
                operation_id: summary.operation_id, step: summary.step,
                source_messages: summary.source_messages, source_digest: summary.source_digest,
                output: summary.output.clone_in_quota(&mut quota)?,
            }),
            None => None,
        };
        let mut roles = Vec::new();
        let copied = reference.retention.roles.len().checked_mul(std::mem::size_of::<ModelRole>())
            .ok_or_else(|| crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "compaction retention capacity overflows"))? as u64;
        if let Some(work) = quota.work().as_deref() { work.admit_copy(copied)?; }
        compaction::reserve_vector(&mut roles, reference.retention.roles.len(), quota.work())?;
        quota.adopt((roles.capacity() * std::mem::size_of::<ModelRole>()) as u64)?;
        if let Some(work) = quota.work().as_deref_mut() { work.copy_bytes(copied)?; }
        roles.extend_from_slice(&reference.retention.roles);
        let retention = CompactionRetention { roles, native_media: reference.retention.native_media };
        #[cfg(feature = "filesystem")]
        let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        let (reconstructed, proof) = DurableContextProvider::compact(
            source, reference.maximum_messages as usize, summary, retention, io)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { quota.adopt(io.owned_bytes()? - before)?; }
        if reference.source_digest != proof.source_digest || reconstructed != *compacted {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "compaction reference does not bind the source and retained context"));
        }
        Ok(())
    })
}

pub(crate) fn validate_projected_context(
    context: &Context, limits: Limits, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| {
        limits.validate(work)?;
        context.validate_current_input(io)?;
        if context.messages.len() > limits.context_messages || context.metadata.len() > limits.attachments {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "context projection exceeded declared bounds"));
        }
        for message in &context.messages { message.content.validate_limits(limits, work)?; }
        for file in context.metadata.values() { limits.validate_file(file, work)?; }
        crate::contract::validate_trusted_json_byte_bound(context, limits.render_bytes, work)
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn context_contract_native<T: Serialize>(
    value: &T, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo as _;
    let remaining = work.remaining().map_err(native_context_work_error)?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: remaining.bytes_encoded,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let labels = [
        "Contract", "name", "revision", "max_messages", "summary", "retention",
        "roles", "native_media", "operation_id", "step", "source_messages", "source_digest", "output",
        "selection", "renderer", "mode", "placement", "limits", "reference",
        "ContextSelection", "source", "extent", "representation", "ContextSourceValue", "kind", "file",
        "attribute", "ContextAttribute", "type_name", "type_revision", "schema", "state_revision", "value",
        "ContextExtent", "whole", "span", "start", "end", "ContextRepresentation", "full",
        "ContextRenderMode", "prompt", "update", "prepend", "append", "ContextPlacement",
    ].iter().map(|label| label.len() as u64 + 4).sum::<u64>();
    let private_labels = labels.checked_add(std::any::type_name::<T>().len() as u64 + 4)
        .ok_or_else(|| crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
            "context serializer diagnostic witness overflows"))?;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(0, private_labels)?;
    let diagnostic = work.retain_owned(diagnostic_bytes).map_err(native_context_work_error)?;
    let mut io = crate::store::history::native_work_io(work).map_err(native_context_work_error)?;
    let encoded = (|| {
        crate::contract::validate_json_byte_bound_native_trusted(value, bounds,
            crate::wire::ErrorCode::Invalid, &mut |charge| io.codec_charge(charge))?;
        crate::contract::json_value_bounded(value, bounds,
            Some(crate::wire::ErrorCode::Invalid), &mut |charge| io.codec_charge(charge))
    })();
    match encoded {
        Ok(value) => Ok((value, io.into_custody())),
        Err(crate::Error::NativeEncode { code, source, custody: None }) =>
            Err(crate::Error::NativeEncode { code, source, custody: Some(diagnostic) }),
        Err(error) => Err(error),
    }
}

fn context_stream_error(source: StreamError, io: Option<&crate::executor::PreparationIo>) -> crate::Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
        return crate::Error::NativeStream { code: crate::wire::ErrorCode::Storage, source, custody: None };
    }
    let _ = io;
    crate::Error::Storage(source.to_string())
}

fn decode_context_json<T: crate::executor::PreparationRecord>(
    bytes: &[u8], maximum: u64, io: Option<&crate::executor::PreparationIo>,
    reason: &'static str,
) -> Result<T> {
    let value = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<T>, bytes, crate::wire::ErrorCode::Storage)?),
        _ => crate::contract::json_from_slice(bytes)
            .map_err(|error| crate::Error::Storage(error.to_string()))?,
    };
    let canonical = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => {
            let encoded = io.encode(&value, maximum, true)?;
            let _hold = io.guard_bytes(&encoded)?;
            encoded.as_ref() == bytes
        }
        _ => crate::contract::canonical_json_bytes(&value)? == bytes,
    };
    let _ = maximum;
    if !canonical { return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Storage, reason)); }
    Ok(value)
}

pub(super) async fn verify_file(
    file: &FileRef, verifier: &dyn ContentResidencyVerifier,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if !verifier.supports_read(file.volume(), io)? {
        return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Unsupported,
            "context verifier has no bounded capability"));
    }
    verifier.verify(file, file.descriptor().byte_length(), io)?.await
}

async fn validate_context_refs(
    context: &Context, verifier: &dyn ContentResidencyVerifier, limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    validate_projected_context(context, limits, io)?;
    for message in &context.messages {
        for file in message.content.file_refs() { verify_file(file, verifier, io).await?; }
    }
    for (name, file) in &context.metadata {
        if !name.contains('.') || name.len() > crate::COMPONENT_LABEL_MAX_BYTES
            || name.chars().any(char::is_control)
        {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "durable context metadata key is invalid"));
        }
        verify_file(file, verifier, io).await?;
    }
    Ok(())
}

impl ContextSource for DurableContextProvider {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move {
            limits.validate(&mut None)?;
            let tail = self.tail_revision(None).await?;
            if tail == 0 {
                return Ok(Vec::new());
            }
            Ok(self.revision_bounded(tail, limits, None).await?.context.messages)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_native<'a>(
        &'a self, _: &'a ContextInput, limits: Limits,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ModelMessage>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&io), |work| limits.validate(work))?;
            let tail = self.tail_revision(Some(&io)).await?;
            if tail == 0 {
                return Ok((Vec::new(), work.retain_owned(0)
                    .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?));
            }
            let record = self.revision_bounded(tail, limits, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            Ok((record.context.messages, io.into_native_custody(retained)?))
        })
    }
}

/// Replaceable memory/retrieval/skill source used by reusable stock stages.
pub trait ContextSource: acyclic_stream::ProviderPlatform {
    /// Resolves model-visible messages under the original admitted limits.
    fn load<'a>(
        &'a self,
        input: &'a ContextInput,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Vec<ModelMessage>>>;

    /// Reports the explicit original-ledger source capability without constructing a future.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { false }

    /// Resolves paid source messages; ordinary callbacks are never a native fallback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_native<'a>(
        &'a self,
        _: &'a ContextInput,
        _: Limits,
        _: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ModelMessage>, acyclic_fs::NativeAllocation)>> {
        Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
            "context source has no native capability"))
    }
}

impl ContextSource for Context {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move {
            validate_source_messages(&self.messages, limits, None)?;
            Ok(self.messages.clone())
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_native<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ModelMessage>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            validate_source_messages(&self.messages, limits, Some(&io))?;
            let messages = crate::executor::with_history_io(Some(&io), |work|
                compaction::clone_messages(&self.messages, work).map(|(copy, _)| copy))?;
            let retained = io.owned_bytes()?;
            Ok((messages, io.into_native_custody(retained)?))
        })
    }
}

/// A source contributes messages only; its unrelated metadata is not imported.
pub(super) fn validate_source_messages(
    messages: &[ModelMessage], limits: Limits, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| limits.validate(work))?;
    BorrowedContext {
        messages: ContextMessages { first: messages, user: None, second: &[] },
        metadata: &BTreeMap::new(), current_input_index: None,
    }.validate_bounds(limits, io)
}

/// A pinned provider allowance can narrow, never widen, the admitted request.
pub(crate) fn restrict_context_limits(
    pinned: Limits, admitted: Limits, io: Option<&crate::executor::PreparationIo>,
) -> Result<Limits> {
    crate::executor::with_history_io(io, |work| {
        pinned.validate(work)?;
        admitted.validate(work)
    })?;
    Ok(Limits {
        file_bytes: pinned.file_bytes.min(admitted.file_bytes),
        path_bytes: pinned.path_bytes.min(admitted.path_bytes),
        attachments: pinned.attachments.min(admitted.attachments),
        render_bytes: pinned.render_bytes.min(admitted.render_bytes),
        model_steps: pinned.model_steps.min(admitted.model_steps),
        model_events_per_step: pinned
            .model_events_per_step
            .min(admitted.model_events_per_step),
        tool_calls_per_step: pinned.tool_calls_per_step.min(admitted.tool_calls_per_step),
        context_messages: pinned.context_messages.min(admitted.context_messages),
    })
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

    /// Copies native identities before construction; the caller retains the source provider's custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        name: &str,
        revision: &str,
        source: Arc<dyn ContextSource>,
        placement: ContextPlacement,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        work.remaining().map_err(native_context_work_error)?;
        if !crate::is_valid_component_label(name) || !crate::is_valid_component_label(revision) {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "context source identity is invalid"));
        }
        if !source.supports_native() {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                "context source has no native capability"));
        }
        let mut io = crate::store::history::native_work_io(work).map_err(native_context_work_error)?;
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        let name = crate::core::copy_text(name, &mut quota)?;
        let revision = crate::core::copy_text(revision, &mut quota)?;
        quota.transfer();
        drop(quota);
        drop(observed);
        Ok((Self::new(name, revision, source, placement), io.into_custody()))
    }

    fn contract_value(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "revision": self.revision,
            "placement": match self.placement {
                ContextPlacement::Prepend => "prepend",
                ContextPlacement::Append => "append",
            },
        })
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
        self.contract_value()
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            validate_projected_context(&context, limits, None)?;
            let loaded = self.source.load(input, limits).await?;
            selection::place_messages_with_io(context, loaded, self.placement, limits, None)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { self.source.supports_native() }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn contract_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        self.validate_native(work)?;
        let placement = match self.placement {
            ContextPlacement::Prepend => "prepend", ContextPlacement::Append => "append",
        };
        let text_bytes = ["name", "revision", "placement", self.name.as_str(),
            self.revision.as_str(), placement].iter().try_fold(0_u64, |count, text|
                count.checked_add(text.len() as u64)).ok_or_else(||
                crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, "context contract storage overflows"))?;
        let owned = text_bytes.checked_add(3 * std::mem::size_of::<(String, Value)>() as u64)
            .ok_or_else(|| crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "context contract storage overflows"))?;
        // The original map convention retains typed entry slots; opaque std node/RSS layout is excluded.
        let guard = work.retain_owned(owned).map_err(native_context_work_error)?;
        work.charge(acyclic_fs::WorkCounters { bytes_copied: text_bytes, allocation_operations: 6,
            ..acyclic_fs::WorkCounters::default() }).map_err(native_context_work_error)?;
        Ok((self.contract_value(), guard))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !crate::is_valid_component_label(&self.name)
            || !crate::is_valid_component_label(&self.revision)
        {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "context source identity is invalid"));
        }
        if !self.source.supports_native() {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                "context source has no native capability"));
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn apply_native<'a>(
        &'a self,
        input: &'a ContextInput,
        context: (Context, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Context, acyclic_fs::NativeAllocation)>> {
        self.validate_native(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            validate_projected_context(&context.0, limits, Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
            let loaded = self.source.load_native(input, limits, work)?.await?;
            selection::place_messages_native(context, loaded, self.placement, limits, work)
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
        self.retention.validate(None)?;
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn contract_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        self.validate_native(work)?;
        #[derive(Serialize)]
        struct Contract<'a> {
            name: &'static str, revision: &'static str, max_messages: usize,
            summary: &'a Option<ContextSummary>, retention: &'a CompactionRetention,
        }
        context_contract_native(&Contract { name: "compaction", revision: "3",
            max_messages: self.max_messages, summary: &self.summary, retention: &self.retention }, work)
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: Context,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            self.validate()?;
            validate_projected_context(&context, limits, None)?;
            let compacted = DurableContextProvider::compact(&context, self.max_messages, self.summary.clone(), self.retention.clone(), None)?
            .0;
            validate_projected_context(&compacted, limits, None)?;
            Ok(compacted)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.retention.validate(Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
        if self.max_messages == 0 {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Invalid,
                "compaction max_messages must be positive"));
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn apply_native<'a>(
        &'a self, _: &'a ContextInput,
        context: (Context, acyclic_fs::NativeAllocation),
        limits: Limits, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Context, acyclic_fs::NativeAllocation)>> {
        self.validate_native(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            validate_projected_context(&context.0, limits, Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
            let summary = clone_summary_native(self.summary.as_ref(), work)?;
            let retention = clone_retention_native(&self.retention, work)?;
            io.absorb_native_custody(summary.1)?;
            io.absorb_native_custody(retention.1)?;
            let compacted = DurableContextProvider::compact(
                &context.0, self.max_messages, summary.0, retention.0, Some(&io))?.0;
            validate_projected_context(&compacted, limits, Some(&crate::executor::PreparationIo::from_native_work(work)?))?;
            let retained = io.owned_bytes()?;
            Ok((compacted, io.into_native_custody(retained)?))
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
    /// Position of the active turn input.
    #[cfg_attr(feature = "wasm", tsify(optional))]
    pub current_input_index: Option<u32>,
}

impl Context {
    fn validate_current_input(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        if self.current_input_index.is_some_and(|index| index as usize >= self.messages.len()) {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "current input index is outside context"));
        }
        Ok(())
    }
}

/// A borrowed view of exactly the messages that will form a base or continuation.
struct ContextMessages<'a> {
    first: &'a [ModelMessage],
    user: Option<&'a ModelContent>,
    second: &'a [ModelMessage],
}

impl Serialize for ContextMessages<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;

        #[derive(Serialize)]
        struct UserMessage<'a> {
            role: ModelRole,
            content: &'a ModelContent,
        }

        let mut sequence = serializer.serialize_seq(None)?;
        for message in self.first {
            sequence.serialize_element(message)?;
        }
        if let Some(content) = self.user {
            sequence.serialize_element(&UserMessage {
                role: ModelRole::User,
                content,
            })?;
        }
        for message in self.second {
            sequence.serialize_element(message)?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct BorrowedContext<'a> {
    messages: ContextMessages<'a>,
    metadata: &'a BTreeMap<String, FileRef>,
    current_input_index: Option<u32>,
}

impl BorrowedContext<'_> {
    fn validate_bounds(&self, limits: Limits, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        let count = self
            .messages
            .first
            .len()
            .checked_add(usize::from(self.messages.user.is_some()))
            .and_then(|count| count.checked_add(self.messages.second.len()))
            .filter(|count| *count <= limits.context_messages);
        if count.is_none() || self.metadata.len() > limits.attachments {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "context projection exceeded declared bounds"));
        }
        crate::executor::with_history_io(io, |work| {
            for message in self.messages.first.iter().chain(self.messages.second) {
                message.content.validate_limits(limits, work)?;
            }
            if let Some(content) = self.messages.user { content.validate_limits(limits, work)?; }
            crate::contract::validate_trusted_json_byte_bound(self, limits.render_bytes, work)
        })
    }
}

/// Inputs visible to every context stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextInput {
    /// Current durable turn input.
    pub input: ModelContent,
    /// Recorded canonical selection; when present it replaces the synthesized one-message context without changing its provenance.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Zero-based executor step.
    pub step: u32,
    /// Model-visible results accumulated by earlier executor steps.
    pub prior_messages: Vec<ModelMessage>,
}

impl ContextInput {
    pub(crate) fn copy_for_turn(
        turn: &crate::executor::TurnInput,
        step: u32,
        prior: &[ModelMessage],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let input = crate::executor::with_history_io(io, |work| crate::model::clone_content(&turn.input, work).map(|(copy, _)| copy))?;
        let selected_context = turn.selected_context.as_ref().map(|selected| {
            crate::executor::with_history_io(io, |work| {
                if work.is_none() { return Ok::<_, crate::Error>(selected.clone()); }
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let mut message_ids = Vec::new();
                let copied = (selected.selection.message_ids.len() * std::mem::size_of::<uuid::Uuid>()) as u64;
                quota.work().as_deref().expect("observed selection").admit_copy(copied)?;
                compaction::reserve_vector(&mut message_ids, selected.selection.message_ids.len(), quota.work())?;
                quota.adopt((message_ids.capacity() * std::mem::size_of::<uuid::Uuid>()) as u64)?;
                let result = (|| {
                    quota.work().as_deref_mut().expect("observed selection").copy_bytes(copied)?;
                    message_ids.extend_from_slice(&selected.selection.message_ids);
                    let checkpoint = match &selected.selection.checkpoint {
                        Some(file) => Some(file.clone_in_quota(&mut quota)?),
                        None => None,
                    };
                    let (messages, claim) = compaction::clone_messages(&selected.messages, quota.work())?;
                    quota.adopt(claim)?;
                    Ok((checkpoint, messages))
                })();
                let (checkpoint, messages) = match result {
                    Ok(value) => value,
                    Err(error) => { drop(message_ids); return Err(error); }
                };
                quota.transfer();
                Ok(SelectedModelContext {
                    selection: crate::conversation::ModelContextSelection {
                        conversation_revision: selected.selection.conversation_revision,
                        message_ids, checkpoint,
                    },
                    messages,
                })
            })
        }).transpose()?;
        let prior_messages = crate::executor::with_history_io(io, |work|
            compaction::clone_messages(prior, work).map(|(copy, _)| copy))?;
        Ok(Self { input, selected_context, step, prior_messages })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_context_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> crate::Error {
    crate::Error::NativeWork { cause, custody: None }
}

pub(crate) fn copy_context(context: &Context, io: Option<&crate::executor::PreparationIo>) -> Result<Context> {
    crate::executor::with_history_io(io, |work| compaction::clone_context(context, work).map(|(copy, _)| copy))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn reserve_context_vec_native<T>(
    values: &mut Vec<T>, capacity: usize, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::NativeAllocation> {
    let mut guard = work.retain_owned(0).map_err(native_context_work_error)?;
    if capacity <= values.capacity() { return Ok(guard); }
    let old = values.capacity();
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    guard.resize_retained(bytes as u64).map_err(native_context_work_error)?;
    let copied = values.len().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, "context vector copy overflows"))?;
    work.charge(acyclic_fs::WorkCounters { allocation_operations: 1, bytes_copied: copied as u64,
        ..acyclic_fs::WorkCounters::default() }).map_err(native_context_work_error)?;
    values.try_reserve_exact(capacity - values.len()).map_err(|cause| crate::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
    })?;
    let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    if let Err(cause) = guard.reconcile_capacity(actual as u64) {
        return Err(crate::Error::NativeWork { cause, custody: Some(guard) });
    }
    let retired = old.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    if let Err(cause) = guard.release(retired as u64) {
        return Err(crate::Error::NativeWork { cause, custody: Some(guard) });
    }
    if let Err(cause) = work.remaining() {
        return Err(crate::Error::NativeWork { cause, custody: Some(guard) });
    }
    Ok(guard)
}

fn append_context_messages(
    first: &mut Vec<ModelMessage>, second: &mut Vec<ModelMessage>, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let capacity = first.len().checked_add(second.len()).ok_or_else(||
        crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    crate::executor::with_history_io(io, |work| {
        compaction::reserve_vector(first, capacity, work)?;
        if let Some(work) = work.as_deref_mut() {
            let copied = second.len().checked_mul(std::mem::size_of::<ModelMessage>()).ok_or_else(||
                crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "context vector copy overflows"))?;
            work.admit_copy(copied as u64)?;
            work.copy_bytes(copied as u64)?;
        }
        first.append(second);
        if let Some(work) = work.as_deref_mut() {
            let bytes = second.capacity().checked_mul(std::mem::size_of::<ModelMessage>()).ok_or_else(||
                crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
            drop(std::mem::take(second));
            work.release_owned(bytes as u64)?;
        }
        Ok(())
    })
}

/// Replaceable ordered context transformation.
pub trait ContextStage: acyclic_stream::ProviderPlatform {
    /// Stable stage name used for diagnostics and composition.
    fn name(&self) -> &str;

    /// Immutable serializable identity included in durable execution binding.
    fn contract(&self) -> Value;

    /// Constructs the same immutable contract under the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn contract_with_native_work(
        &self, _: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
            "context stage has no native contract capability"))
    }

    /// Validates a replacement before installation.
    fn validate(&self) -> Result<()> {
        crate::contract::validate_component_label(self.name(), "context stage")?;
        if !self.contract().is_object() {
            return Err(crate::Error::Invalid(
                "context stage contract must be an object".into(),
            ));
        }
        Ok(())
    }

    /// Transforms context under the original admitted limits, in application order.
    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Context>>;

    /// Native capability is explicit; an ordinary stage is rejected before boxing or invocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { false }

    /// Validates native pinned state without invoking the allocating ordinary contract callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
            "context stage has no native capability"))
    }

    /// Transforms the actual guarded DTO on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn apply_native<'a>(
        &'a self,
        _: &'a ContextInput,
        _: (Context, acyclic_fs::NativeAllocation),
        _: Limits,
        _: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Context, acyclic_fs::NativeAllocation)>> {
        Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
            "context stage has no native capability"))
    }
}

/// Explicit ordered pipeline; no hidden framework stages are inserted.
#[derive(Clone, Default)]
pub struct ContextPipeline(Vec<Arc<dyn ContextStage>>);

impl ContextPipeline {
    /// Validates stage identities on the optional preparation ledger.
    pub fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            work.remaining().map_err(native_context_work_error)?;
            if self.0.iter().any(|stage| !stage.supports_native()) {
                return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                    "context stage has no native capability"));
            }
            for stage in &self.0 { stage.validate_native(work)?; }
            return Ok(());
        }
        let _ = io;
        for stage in &self.0 { stage.validate()?; }
        Ok(())
    }

    /// Appends to a caller-guarded native pipeline, returning only its actual backing-capacity delta.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn append_with_native_work(
        &mut self, stage: Arc<dyn ContextStage>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeAllocation> {
        work.remaining().map_err(native_context_work_error)?;
        if !stage.supports_native() {
            return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                "context stage has no native capability"));
        }
        stage.validate_native(work)?;
        let capacity = self.0.len().checked_add(1).ok_or_else(||
            crate::executor::native_boundary(crate::wire::ErrorCode::Invalid, "context stage count overflows"))?;
        let guard = reserve_context_vec_native(&mut self.0, capacity, work)?;
        self.0.push(stage);
        Ok(guard)
    }

    /// Installs a structurally valid replacement for future operations only.
    pub fn reload(&self, replacement: Self) -> Result<Self> {
        replacement.validate(None)?;
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

    /// Builds the canonical selection/current input and in-turn messages before stages.
    pub fn base_context(
        input: &ContextInput,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        let metadata = BTreeMap::new();
        let messages = ContextMessages {
            first: input
                .selected_context
                .as_ref()
                .map_or(&[], |selected| selected.messages.as_slice()),
            user: input.selected_context.is_none().then_some(&input.input),
            second: &input.prior_messages,
        };
        let base_count = input
            .selected_context
            .as_ref()
            .map_or(1, |selected| selected.messages.len());
        let current_input_index = u32::try_from(base_count.checked_sub(1).ok_or_else(|| {
            crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "current input is missing from context")
        })?)
        .map_err(|_| crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
            "current input index exceeds portable count"))?;
        let base = BorrowedContext {
            messages,
            metadata: &metadata,
            current_input_index: Some(current_input_index),
        };
        base.validate_bounds(limits, io)?;
        crate::executor::with_history_io(io, |work| {
            input.input.validate_limits(limits, work)?;
            input.input.validate_user_input(work)
        })?;
        if let Some(selected) = &input.selected_context { selected.validate_for_input(&input.input, io)?; }
        if input
            .selected_context
            .as_ref()
            .is_some_and(|selected| selected.selection.checkpoint.is_some())
        {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Unsupported,
                "canonical checkpoint requires owner-resolved base context"));
        }
        let messages = crate::executor::with_history_io(io, |work|
            compaction::clone_base_messages(base.messages.first, base.messages.user,
                base.messages.second, work).map(|(copy, _)| copy))?;
        let context = Context { messages, metadata, current_input_index: Some(current_input_index) };
        validate_projected_context(&context, limits, io)?;
        Ok(context)
    }

    /// Adds an exact new delta to an owner-resolved base before running stages.
    pub fn continue_base(
        mut retained: Context,
        delta: Context,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        validate_projected_context(&retained, limits, io)?;
        if delta.current_input_index.is_none() {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "continued base has no current input"));
        }
        // The retained parent's old active input is historical in this admission.
        retained.current_input_index = None;
        selection::place_context(retained, delta, ContextPlacement::Append, limits, io)
    }

    /// Bounds the initial view and every intermediate projection, without truncation.
    pub async fn run(&self, input: &ContextInput, limits: Limits, io: Option<&crate::executor::PreparationIo>) -> Result<Context> {
        self.transform(input, Self::base_context(input, limits, io)?, limits, io)
            .await
    }

    /// Applies declared stages to an explicit base, including a retained canonical checkpoint plus its new history delta.
    pub async fn transform(
        &self,
        input: &ContextInput,
        mut context: Context,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Context> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = io.and_then(crate::executor::PreparationIo::original_native_work);
        self.validate(io)?;
        if self.0.len() > limits.context_messages {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "context stage count exceeds limit"));
        }
        crate::executor::with_history_io(io, |work| input.input.validate_user_input(work))?;
        if let Some(selected) = &input.selected_context { selected.validate_for_input(&input.input, io)?; }
        validate_projected_context(&context, limits, io)?;
        if context.current_input_index.is_none() {
            return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "base context has no current input marker"));
        }
        for stage in &self.0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let (Some(io), Some(work)) = (io, native) {
                // The operation observer retains the actual input payloads, not this zero transfer guard.
                let guard = work.retain_owned(0).map_err(native_context_work_error)?;
                let (next, guard) = stage.apply_native(input, (context, guard), limits, work)?.await?;
                io.absorb_native_custody(guard)?;
                context = next;
            } else {
                context = stage.apply(input, context, limits).await?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            { context = stage.apply(input, context, limits).await?; }
            validate_projected_context(&context, limits, io)?;
            if context.current_input_index.is_none() {
                return Err(crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid,
                    "context stage removed the current input marker"));
            }
        }
        Ok(context)
    }

    /// Returns immutable stage contracts on the optional preparation ledger.
    pub fn contracts(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Vec<Value>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io && let Some(work) = io.original_native_work() {
            if self.0.iter().any(|stage| !stage.supports_native()) {
                return Err(crate::executor::native_boundary(crate::wire::ErrorCode::Unsupported,
                    "context stage has no native capability"));
            }
            let mut output = Vec::new();
            io.absorb_native_custody(reserve_context_vec_native(&mut output, self.0.len(), work)?)?;
            for stage in &self.0 {
                let (value, custody) = stage.contract_with_native_work(work)?;
                io.absorb_native_custody(custody)?;
                output.push(value);
            }
            return Ok(output);
        }
        let _ = io;
        Ok(self.0.iter().map(|stage| stage.contract()).collect())
    }

    /// Copies stage handles on the optional preparation ledger.
    pub fn copy(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        let mut stages = Vec::new();
        crate::executor::with_history_io(io, |work| {
            compaction::reserve_vector(&mut stages, self.0.len(), work)?;
            let copied = self.0.len().checked_mul(std::mem::size_of::<Arc<dyn ContextStage>>()).ok_or_else(||
                crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "context stage copy overflows"))?;
            if let Some(work) = work.as_deref_mut() {
                work.admit_copy(copied as u64)?;
                work.copy_bytes(copied as u64)?;
            }
            stages.extend(self.0.iter().cloned());
            Ok::<(), crate::Error>(())
        })?;
        Ok(Self(stages))
    }

    pub(crate) fn summary_source(
        source: Context, revision: String, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            if work.is_none() {
                return Ok(Self::new([Arc::new(SourceStage::new(
                    "summary-source", revision, Arc::new(source), ContextPlacement::Prepend)) as Arc<dyn ContextStage>]));
            }
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut stages: Vec<Arc<dyn ContextStage>> = Vec::new();
            let made = (|| {
                let name = quota.work().as_deref_mut().expect("observed summary").clone_text("summary-source")?;
                quota.adopt(name.capacity() as u64)?;
                let bytes = std::mem::size_of::<Context>() as u64;
                quota.work().as_deref_mut().expect("observed summary").admit_allocation(bytes)?;
                quota.work().as_deref_mut().expect("observed summary").retain_owned(bytes)?;
                quota.adopt(bytes)?;
                let source = Arc::new(source);
                let bytes = std::mem::size_of::<SourceStage>() as u64;
                quota.work().as_deref_mut().expect("observed summary").admit_allocation(bytes)?;
                quota.work().as_deref_mut().expect("observed summary").retain_owned(bytes)?;
                quota.adopt(bytes)?;
                let stage: Arc<dyn ContextStage> = Arc::new(SourceStage::new(
                    name, revision, source, ContextPlacement::Prepend));
                compaction::reserve_vector(&mut stages, 1, quota.work())?;
                quota.adopt((stages.capacity() * std::mem::size_of::<Arc<dyn ContextStage>>()) as u64)?;
                stages.push(stage);
                Ok(())
            })();
            if let Err(error) = made { drop(stages); return Err(error); }
            quota.transfer();
            Ok(Self(stages))
        })
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

    #[cfg(feature = "filesystem")]
    fn owned_context_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576, bytes_hashed: 1_048_576,
            items_examined: 4096, allocation_operations: 4096, peak_allocation_bytes: 1_048_576,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn owned_projected_validation_observes_source_without_inventory_or_copy() -> Result<()> {
        let source = Context {
            messages: vec![ModelMessage { role: ModelRole::User,
                content: ModelContent::Text("current input".into()) }],
            current_input_index: Some(0), ..Context::default()
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut readonly = owned_context_budget();
        readonly.bytes_copied = 0;
        readonly.allocation_operations = 0;
        let io = crate::executor::PreparationIo::from_budget(readonly, &cancellation)?;
        validate_projected_context(&source, Limits::default(), Some(&io))?;
        assert_eq!(io.owned_bytes()?, 0);
        let mut budget = owned_context_budget();
        budget.items_examined = 0;
        let denied = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
        assert!(validate_projected_context(&source, Limits::default(), Some(&denied)).is_err());
        assert_eq!(denied.owned_bytes()?, 0);
        let cancelled = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
        cancellation.cancel();
        assert!(validate_projected_context(&source, Limits::default(), Some(&cancelled)).is_err());
        assert_eq!(cancelled.owned_bytes()?, 0);
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn owned_compaction_proof_reconstructs_exactly_and_releases_actual_fields() -> Result<()> {
        let source = Context {
            messages: (0..4).map(|index| ModelMessage {
                role: ModelRole::User, content: ModelContent::Text(format!("message {index}")),
            }).collect(),
            current_input_index: Some(3), ..Context::default()
        };
        let prefix = compaction::clone_context_prefix(&source, 2, &mut None)?.0;
        let output = message(ModelRole::Assistant, "summary")?.content.file_refs().next().unwrap().clone();
        let summary = ContextSummary { operation_id: crate::OperationId::new(), step: 0,
            source_messages: 2, source_digest: crate::contract::canonical_json_digest(&prefix)?, output };
        let (retained, reference) = DurableContextProvider::compact(&source, 3, Some(summary), CompactionRetention::default(), None)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
        validate_compaction(&reference, &source, &retained, Some(&io))?;
        assert_eq!(io.owned_bytes()?, 0);
        let mut changed = retained.clone();
        changed.messages.last_mut().expect("retained current input").content = ModelContent::Text("changed".into());
        assert!(validate_compaction(&reference, &source, &changed, Some(&io)).is_err());
        assert_eq!(io.owned_bytes()?, 0);
        let mut changed = reference.clone();
        changed.summary.as_mut().expect("summary proof").source_digest[0] ^= 1;
        assert!(validate_compaction(&changed, &source, &retained, Some(&io)).is_err());
        assert_eq!(io.owned_bytes()?, 0);
        let mut budget = owned_context_budget();
        budget.items_examined = 0;
        let denied = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
        assert!(validate_compaction(&reference, &source, &retained, Some(&denied)).is_err());
        assert_eq!(denied.owned_bytes()?, 0);
        let cancelled = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
        cancellation.cancel();
        assert!(validate_compaction(&reference, &source, &retained, Some(&cancelled)).is_err());
        assert_eq!(cancelled.owned_bytes()?, 0);
        Ok(())
    }

    use acyclic_stream::MemoryStream;

    fn native_retention_policy() -> FileProjectionPolicy {
        FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
            intent: crate::model::NativeMediaIntent::Document { maximum_pages: 1 },
            maximum_bytes: 1_048_576,
            maximum_work: 1,
            configuration: None,
        }))
    }

    fn native_call_context(
        file: &FileRef,
        results: usize,
        native_in_result: bool,
    ) -> Result<Context> {
        let mut calls: Vec<_> = (0..results)
            .map(|index| ModelContentPart::ToolCall {
                call_id: format!("native-{index}"),
                name: "native.tool".into(),
                arguments: Value::Null,
            })
            .collect();
        if !native_in_result {
            calls.push(ModelContentPart::File {
                file: file.clone(),
                policy: native_retention_policy(),
            });
        }
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("old".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(calls),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text("gap".into()),
            },
        ];
        for index in 0..results {
            messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: format!("native-{index}"),
                    name: "native.tool".into(),
                    content: if native_in_result && index == 0 {
                        crate::model::ToolResultContent::Parts {
                            parts: vec![crate::model::ModelDataPart::File {
                                file: file.clone(),
                                policy: native_retention_policy(),
                            }],
                        }
                    } else {
                        crate::model::ToolResultContent::Json { value: Value::Null }
                    },
                }),
            });
        }
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("current".into()),
        });
        Ok(Context {
            current_input_index: Some(
                u32::try_from(messages.len() - 1)
                    .map_err(|_| crate::Error::Invalid("fixture index exceeds u32".into()))?,
            ),
            messages,
            metadata: BTreeMap::new(),
        })
    }

    #[test]
    fn native_call_retention_closes_tool_pair_before_compaction_budget() -> Result<()> {
        let file: FileRef = message(ModelRole::Assistant, "media reference")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Invalid("media reference missing".into()))?
            .clone();
        for (results, native_in_result) in
            (1..=3).flat_map(|results| [false, true].map(move |nested| (results, nested)))
        {
            let source = native_call_context(&file, results, native_in_result)?;
            let summary = ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: u32::try_from(source.messages.len())
                    .map_err(|_| crate::Error::Invalid("fixture count exceeds u32".into()))?,
                source_digest: crate::contract::canonical_json_digest(&source)?,
                output: file.clone(),
            };
            let (compacted, reference) = DurableContextProvider::compact(&source, results + 3, Some(summary.clone()), CompactionRetention::default(), None)?;
            let mut expected = vec![source.messages[1].clone()];
            expected.extend_from_slice(&source.messages[3..]);
            assert_eq!(compacted.messages.get(1..), Some(expected.as_slice()));
            assert_eq!(
                compacted.current_input_index,
                Some(
                    u32::try_from(results + 2)
                        .map_err(|_| crate::Error::Invalid("fixture index exceeds u32".into()))?
                )
            );
            validate_compaction(&reference, &source, &compacted, None)?;
            assert!(
                DurableContextProvider::compact(&source, results + 2, Some(summary.clone()), CompactionRetention::default(), None)
                .is_err()
            );
            let retention = CompactionRetention {
                native_media: false,
                ..CompactionRetention::default()
            };
            let (without_media, _) =
                DurableContextProvider::compact(&source, 2, Some(summary), retention, None)?;
            assert_eq!(
                without_media.messages.get(1..),
                source.messages.get(source.messages.len() - 1..)
            );
        }
        Ok(())
    }

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
                        content: crate::model::ToolResultContent::Json { value: Value::Null },
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
        let (prefix_compacted, prefix_reference) = DurableContextProvider::compact(&source, 5, Some(prefix_summary.clone()), CompactionRetention::default(), None)?;
        assert_eq!(prefix_compacted.messages.get(2..), source.messages.get(3..));
        validate_compaction(&prefix_reference, &source, &prefix_compacted, None)?;
        assert!(
            DurableContextProvider::compact(&source, 4, Some(prefix_summary.clone()), CompactionRetention::default(), None)
            .is_err(),
            "unsummarized messages cannot be dropped to meet the budget"
        );
        for extent in [0, 2, 7] {
            assert!(
                DurableContextProvider::compact(&source, 5, Some(ContextSummary {
                        source_messages: extent,
                        ..prefix_summary.clone()
                    }), CompactionRetention::default(), None)
                .is_err()
            );
        }
        for budget in 1..=6 {
            let result = DurableContextProvider::compact(&source, budget, Some(summary.clone()), CompactionRetention::default(), None);
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
            validate_compaction(&reference, &source, &projected, None)?;
            let mut corrupt = projected;
            corrupt
                .messages
                .last_mut()
                .ok_or_else(|| crate::Error::Invalid("empty context".into()))?
                .content = ModelContent::Text("substitution".into());
            assert!(validate_compaction(&reference, &source, &corrupt, None).is_err());
        }
        assert!(
            DurableContextProvider::compact(&source, 3, None, CompactionRetention::default(), None)
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
                        policy: native_retention_policy(),
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
            DurableContextProvider::compact(&configurable, 3, Some(configured_summary.clone()), CompactionRetention::default(), None)
            .is_err()
        );
        let retention = CompactionRetention {
            roles: vec![ModelRole::User],
            native_media: false,
        };
        let (projected, mut reference) = DurableContextProvider::compact(&configurable, 3, Some(configured_summary), retention.clone(), None)?;
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
        validate_compaction(&reference, &configurable, &projected, None)?;
        reference.retention = CompactionRetention::default();
        assert!(validate_compaction(&reference, &configurable, &projected, None).is_err());
        assert!(
            CompactionRetention {
                roles: vec![ModelRole::User, ModelRole::User],
                native_media: true
            }
            .validate(None)
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

    struct SourceLimitSpy(std::sync::Mutex<Vec<Limits>>);

    impl ContextSource for SourceLimitSpy {
        fn load<'a>(
            &'a self,
            _: &'a ContextInput,
            limits: Limits,
        ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
            Box::pin(async move {
                self.0
                    .lock().unwrap()
                    .push(limits);
                Ok(vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("source".into()),
                }])
            })
        }
    }

    struct StageLimitSpy(std::sync::Mutex<Vec<Limits>>);

    impl ContextStage for StageLimitSpy {
        fn name(&self) -> &str {
            "limit-spy"
        }
        fn contract(&self) -> Value {
            serde_json::json!({"revision":"1"})
        }
        fn apply<'a>(
            &'a self,
            _: &'a ContextInput,
            context: Context,
            limits: Limits,
        ) -> BoxFuture<'a, Result<Context>> {
            Box::pin(async move {
                self.0
                    .lock().unwrap()
                    .push(limits);
                Ok(context)
            })
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one actual pipeline fixture checks forwarding and both exact placement boundaries"
    )]
    async fn admitted_limits_reach_sources_and_stages_and_bound_message_joins() -> Result<()> {
        let input = ContextInput {
            input: ModelContent::Text("current".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let limits = Limits {
            context_messages: 2,
            render_bytes: 2_048,
            ..payload_limits()
        };
        let source = Arc::new(SourceLimitSpy(std::sync::Mutex::new(Vec::new())));
        let stage = Arc::new(StageLimitSpy(std::sync::Mutex::new(Vec::new())));
        let pipeline = ContextPipeline::new([
            Arc::new(SourceStage::new(
                "source",
                "1",
                source.clone(),
                ContextPlacement::Prepend,
            )) as Arc<dyn ContextStage>,
            stage.clone(),
        ]);
        let projected = pipeline.run(&input, limits, None).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(
            *source
                .0
                .lock().unwrap(),
            vec![limits]
        );
        assert_eq!(
            *stage
                .0
                .lock().unwrap(),
            vec![limits]
        );
        let base = ContextPipeline::base_context(&input, limits, None)?;
        let contribution = vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text("source".into()),
        }];
        for placement in [ContextPlacement::Prepend, ContextPlacement::Append] {
            let joined =
                selection::place_messages_with_io(base.clone(), contribution.clone(), placement, limits, None)?;
            assert_eq!(
                joined.current_input_index,
                Some(u32::from(placement == ContextPlacement::Prepend))
            );
            let exact = crate::contract::canonical_json_bytes(&joined)?.len() as u64;
            assert_eq!(
                selection::place_messages_with_io(
                    base.clone(),
                    contribution.clone(),
                    placement,
                    Limits {
                        render_bytes: exact,
                        ..limits
                    },
                    None,
                )?,
                joined
            );
            for rejected in [
                Limits {
                    context_messages: 1,
                    ..limits
                },
                Limits {
                    render_bytes: exact - 1,
                    ..limits
                },
            ] {
                assert!(matches!(
                    selection::place_messages_with_io(
                        base.clone(),
                        contribution.clone(),
                        placement,
                        rejected,
                        None,
                    ),
                    Err(crate::Error::Invalid(_))
                ));
            }
        }
        let oversized = Context {
            messages: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("x".repeat(2_048)),
            }],
            ..Context::default()
        };
        assert!(matches!(
            oversized.load(&input, limits).await,
            Err(crate::Error::Invalid(_))
        ));
        let too_many = Context {
            messages: projected.messages,
            ..Context::default()
        };
        assert!(matches!(
            too_many
                .load(
                    &input,
                    Limits {
                        context_messages: 1,
                        ..limits
                    }
                )
                .await,
            Err(crate::Error::Invalid(_))
        ));
        let file = message(ModelRole::System, "instruction")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .cloned()
            .ok_or_else(|| crate::Error::Invalid("fixture instruction is missing".into()))?;
        let discovered = DiscoveredContext {
            instructions: vec![file.clone(), file.clone()],
            skills: Vec::new(),
        };
        assert!(matches!(
            discovered
                .load(
                    &input,
                    Limits {
                        context_messages: 1,
                        ..limits
                    }
                )
                .await,
            Err(crate::Error::Invalid(_))
        ));
        assert!(matches!(
            discovered
                .load(
                    &input,
                    Limits {
                        file_bytes: 1,
                        ..limits
                    }
                )
                .await,
            Err(crate::Error::Invalid(_))
        ));
        Ok(())
    }

    struct RendererLimitSpy(std::sync::Mutex<Vec<Limits>>);

    impl ContextRenderer for RendererLimitSpy {
        fn contract(&self) -> Value {
            serde_json::json!({"revision":"1"})
        }
        fn render<'a>(
            &'a self,
            _: &'a ContextSelection,
            _: ContextRenderMode,
            _: &'a ContextInput,
            limits: Limits,
        ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
            Box::pin(async move {
                self.0
                    .lock().unwrap()
                    .push(limits);
                Ok(vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("rendered".into()),
                }])
            })
        }
    }

    #[tokio::test]
    async fn selection_stage_narrows_limits_before_source_verification_and_rendering() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let payloads = Arc::new(TestPayloadStore::new()?);
        let verifier = Arc::new(CountingVerifier {
            count: std::sync::atomic::AtomicUsize::new(0),
            payloads,
        });
        let renderer = Arc::new(RendererLimitSpy(std::sync::Mutex::new(Vec::new())));
        let file = message(ModelRole::System, "0123456789")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .cloned()
            .ok_or_else(|| crate::Error::Invalid("fixture source is missing".into()))?;
        let pinned = Limits {
            render_bytes: 2_048,
            ..payload_limits()
        };
        let stage = SelectionStage::new(
            "selection".into(),
            ContextSelection {
                source: ContextSourceValue::File { file: file.clone() },
                extent: ContextExtent::Whole,
                representation: ContextRepresentation::Full,
            },
            renderer.clone(),
            verifier.clone(),
            ContextRenderMode::Prompt,
            ContextPlacement::Prepend,
            pinned,
        )?;
        let input = ContextInput {
            input: ModelContent::Text("current".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        let admitted = Limits {
            file_bytes: 9,
            render_bytes: 4_096,
            context_messages: 2,
            ..payload_limits()
        };
        let base = ContextPipeline::base_context(&input, admitted, None)?;
        assert!(matches!(
            stage.apply(&input, base.clone(), admitted).await,
            Err(crate::Error::Invalid(_))
        ));
        assert_eq!(verifier.count.load(Ordering::SeqCst), 0);
        assert!(
            renderer
                .0
                .lock().unwrap()
                .is_empty()
        );
        let admitted = Limits {
            file_bytes: 10,
            ..admitted
        };
        let projected = stage.apply(&input, base, admitted).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(verifier.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            *renderer
                .0
                .lock().unwrap(),
            vec![Limits {
                render_bytes: pinned.render_bytes,
                ..admitted
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn durable_source_reopen_applies_admitted_payload_bounds_before_body_reads() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(TestPayloadStore::new()?);
        let path = StreamPath::new("bounded/source-admission")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "source",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads.clone())?;
        let context = Context {
            messages: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("x".repeat(400)),
            }],
            ..Context::default()
        };
        provider
            .append(0, context.clone(), None, Bytes::from_static(b"original"))
            .await?;
        let reopened = DurableContextProvider::new(
            stream,
            path,
            "source",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?;
        let input = ContextInput {
            input: ModelContent::Text("current".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        for limits in [
            Limits {
                file_bytes: 128,
                ..payload_limits()
            },
            Limits {
                render_bytes: 128,
                ..payload_limits()
            },
        ] {
            payloads.reads.store(0, Ordering::SeqCst);
            assert!(reopened.load(&input, limits).await.is_err());
            assert_eq!(payloads.reads.load(Ordering::SeqCst), 0);
        }
        assert_eq!(
            reopened.load(&input, payload_limits()).await?,
            context.messages
        );
        assert_eq!(payloads.reads.load(Ordering::SeqCst), 1);
        Ok(())
    }

    impl TestPayloadStore {
        fn new() -> Result<Self> {
            Ok(Self {
                volume: VolumeRef::new(
                    ProviderRef::new("test", "filesystem", "2")?,
                    "context".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                    &mut None,
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
            display_name: &'a str, io: Option<&'a crate::executor::PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, (FileRef, Option<crate::conversation::ContentHold<'a>>)>> {
            crate::conversation::BoundedContentFuture::new(io, || async move {
                let reference = FileRef::new(
                    self.volume.clone(),
                    path.into(),
                    operation.to_string(),
                    FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
                    display_name.into(),
                    &mut None,
                )?;
                let mut files = self
                    .files
                    .lock().unwrap();
                if let Some((retained, body)) = files.get(path) {
                    if retained != &reference || body != bytes {
                        return Err(crate::Error::Conflict("payload identity changed".into()));
                    }
                    return Ok((retained.clone(), None));
                }
                files.insert(path.to_owned(), (reference.clone(), bytes.to_vec()));
                Ok((reference, None))
            })
        }
    }

    impl ContentResidencyVerifier for TestPayloadStore {
        fn verify<'a>(&'a self, file: &'a FileRef, _maximum_bytes: u64, io: Option<&'a crate::executor::PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
            crate::conversation::BoundedContentFuture::new(io, || async move { file.validate(&mut None) })
        }

        fn read<'a>(&'a self, file: &'a FileRef, _maximum_bytes: u64, io: Option<&'a crate::executor::PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
            crate::conversation::BoundedContentFuture::new(io, || async move {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let files = self
                    .files
                    .lock().unwrap();
                let (reference, bytes) = files
                    .get(file.path())
                    .ok_or_else(|| crate::Error::Storage("missing context payload".into()))?;
                if file != reference {
                    return Err(crate::Error::Storage(
                        "context payload reference changed".into(),
                    ));
                }
                Ok(bytes.clone().into())
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
            _: Limits,
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
                output: output.content.file_refs().next().unwrap().clone(),
            })
        };
        let first_summary = make_summary(&source, "first-summary")?;
        let (mut next, reference) = DurableContextProvider::compact(&source, 3, Some(first_summary.clone()), CompactionRetention::default(), None)?;
        validate_compaction(&reference, &source, &next, None)?;
        assert_eq!(next.messages[0].role, ModelRole::Assistant);
        next.messages.extend([
            message(ModelRole::Assistant, "later-answer")?,
            message(ModelRole::User, "latest")?,
        ]);
        next.current_input_index = Some(4);
        let (compacted, reference) = DurableContextProvider::compact(&next, 3, Some(make_summary(&next, "second-summary")?), CompactionRetention::default(), None)?;
        validate_compaction(&reference, &next, &compacted, None)?;
        assert_eq!(compacted.messages.len(), 3);
        assert!(compacted.messages.contains(&source.messages[0]));
        assert_eq!(compacted.messages.last(), next.messages.last());
        assert_eq!(compacted.current_input_index, Some(2));
        assert!(
            compacted
                .messages
                .iter()
                .all(|message| { !message.content.file_refs().any(|file| file == &first_summary.output) })
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
                        content: crate::model::ToolResultContent::Json { value: Value::Null },
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
                .file_refs().next().unwrap()
                .clone(),
        };
        assert!(
            DurableContextProvider::compact(&source, 4, Some(summary.clone()), CompactionRetention::default(), None)
            .is_err()
        );
        let (compacted, reference) = DurableContextProvider::compact(&source, 5, Some(summary), CompactionRetention::default(), None)?;
        validate_compaction(&reference, &source, &compacted, None)?;
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
            _: Limits,
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
        let raw = ContextPipeline::base_context(&input, Limits::default(), None)?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(ContextPipeline::default().run(&input, Limits::default(), None).await?, raw);
        let retained = retained_input_base(input.input.clone());
        let first = pipeline
            .transform(&input, retained.clone(), Limits::default(), None)
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
            .transform(&input, next_base.clone(), Limits::default(), None)
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
            .transform(&input, next_base, Limits::default(), None)
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
                .transform(&input, invalid, Limits::default(), None)
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
        let projected = pipeline.run(&input, Limits::default(), None).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(projected.messages[1].content, input.input);
        let summary = ContextSummary {
            operation_id: crate::OperationId::new(),
            step: 0,
            source_messages: 4,
            source_digest: crate::contract::canonical_json_digest(&projected)?,
            output: message(ModelRole::Assistant, "summary")?
                .content
                .file_refs().next().unwrap()
                .clone(),
        };
        let (compacted, reference) = DurableContextProvider::compact(&projected, 3, Some(summary), CompactionRetention::default(), None)?;
        validate_compaction(&reference, &projected, &compacted, None)?;
        assert_eq!(compacted.current_input_index, Some(1));
        assert_eq!(compacted.messages[1].content, input.input);
        assert_eq!(compacted.messages.last(), projected.messages.last());
        assert!(
            ContextPipeline::new([Arc::new(RemoveInputMarker) as Arc<dyn ContextStage>])
                .run(&input, Limits::default(), None)
                .await
                .is_err()
        );

        let invalid = Context {
            current_input_index: Some(4),
            ..projected
        };
        assert!(
            validate_projected_context(&invalid, crate::conversation::Limits::default(), None).is_err()
        );
        assert!(
            DurableContextProvider::compact(&invalid, 4, None, CompactionRetention::default(), None)
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
        let (payload, _payload_hold) = payloads
            .stage(
                crate::OperationId::new(),
                "context/projections/pinned.json",
                &crate::contract::canonical_json_bytes(&context)?,
                "application/json",
                "context.json",
                None,
            )?
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
                    records: vec![bytes.into()],
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
                    .decode_revision(0, &vec![b' '; MAX_RECORD_BYTES + 1], payload_limits(), None)
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
        fn verify<'a>(&'a self, file: &'a FileRef, _maximum_bytes: u64, io: Option<&'a crate::executor::PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
            crate::conversation::BoundedContentFuture::new(io, || async move {
                self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                file.validate(&mut None)
            })
        }

        fn read<'a>(&'a self, file: &'a FileRef, _maximum_bytes: u64, io: Option<&'a crate::executor::PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
            self.payloads.read(file, _maximum_bytes, io)
        }
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
        assert_ne!(snapshot.contracts(None)?, current.contracts(None)?);
        let rebuilt = current.run(&input, Limits::default(), None).await?;
        let update = attribute_pipeline(
            "new",
            ContextRenderMode::Update,
            ContextRepresentation::Full,
        )?
        .run(&input, Limits::default(), None)
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
                .run(&input, Limits::default(), None)
                .await?;
            assert_eq!(
                view.messages.first().map(|m| &m.content),
                Some(&ModelContent::Text(format!(
                    "Prompt:{representation:?}:new:\"new\""
                )))
            );
        }
        assert_eq!(snapshot.run(&input, Limits::default(), None).await?, old.run(&input, Limits::default(), None).await?);
        assert!(
            old.reload(ContextPipeline::new([Arc::new(CompactionStage {
                max_messages: 0,
                summary: None,
                retention: CompactionRetention::default(),
            })
                as Arc<dyn ContextStage>]))
                .is_err()
        );
        assert_eq!(old.contracts(None)?, snapshot.contracts(None)?);
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
            proptest::prop_assert_eq!(selection.validate(crate::conversation::Limits::default(), &mut None).is_ok(), start < end && end <= 10);
        }
    }

    impl ContentResidencyVerifier for RefVerifier {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef, _maximum_bytes: u64, io: Option<&'a crate::executor::PreparationIo>) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
            crate::conversation::BoundedContentFuture::new(io, || async move { reference.validate(&mut None) })
        }
    }

    fn message(role: ModelRole, text: &str) -> Result<ModelMessage> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "context".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?;
        let file = FileRef::new(
            volume,
            format!("context/{text}.txt"),
            "pinned".into(),
            FileDescriptor::from_bytes(text.as_bytes(), "text/plain", &mut None)?,
            format!("{text}.txt"),
            &mut None,
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
        let (compacted, reference) = DurableContextProvider::compact(&original, 2, Some(ContextSummary {
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
            }), CompactionRetention::default(), None)?;
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
            }, Limits::default(), None)
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
        let (compacted, reference) = DurableContextProvider::compact(&source, 2, Some(ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: 2,
                source_digest: crate::contract::canonical_json_digest(&covered)?,
                output,
            }), CompactionRetention::default(), None)?;
        let key = Bytes::from_static(b"atomic-compaction");
        // Discard the first successful acknowledgement, as a caller whose receipt was lost would.
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
        assert_eq!(reopened.tail_revision(None).await?, 2);
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
        assert_eq!(reopened.tail_revision(None).await?, 2);
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
            assert_eq!(provider.tail_revision(None).await?, 0);
            assert!(payloads.files.lock().unwrap().is_empty());
        }
        Ok(())
    }
}
