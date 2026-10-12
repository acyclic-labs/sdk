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
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use compaction::{
    clone_base_messages_native, clone_context_native, clone_context_prefix_native, clone_file_native,
    clone_messages_native, clone_retention_native, mandatory_positions_native,
};
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_native(&self) -> Result<()> {
        for (position, role) in self.roles.iter().enumerate() {
            if self.roles[..position].contains(role) {
                return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
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
    pub fn validate(&self, limits: Limits) -> Result<()> {
        self.validate_with_io(limits, None)
    }

    pub(crate) fn validate_with_io(
        &self, limits: Limits, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        validate_context_limits_with_io(limits, io)?;
        #[cfg(not(feature = "filesystem"))]
        limits.validate()?;
        if self.selection.conversation_revision == 0
            || self.selection.message_ids.is_empty()
            || self.selection.message_ids.len() > limits.context_messages
        {
            return Err(context_checkpoint_error(io, "canonical checkpoint coverage is invalid"));
        }
        // Validate immutable references before the bounded scratch set grows.
        for reference in [&self.source, &self.retained, &self.compaction] {
            context_checkpoint_file(limits, reference, io)?;
            if reference.volume() != self.source.volume()
                || reference.descriptor().media_type() != "application/json"
            {
                return Err(context_checkpoint_error(io, "canonical checkpoint payload scope is invalid"));
            }
        }
        if let Some(previous) = &self.selection.checkpoint {
            context_checkpoint_file(limits, previous, io)?;
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
                .ok_or_else(|| context_checkpoint_error(io, "canonical checkpoint identity storage overflows"))?;
            crate::wire_codec::CodecQuota::new(&mut observed, bytes as u64)?
        };
        let mut unique = std::collections::BTreeSet::new();
        for id in &self.selection.message_ids {
            context_checkpoint_step(io)?;
            unique.insert(id);
        }
        if unique.len() != self.selection.message_ids.len() {
            return Err(context_checkpoint_error(io, "canonical checkpoint repeats source identities"));
        }
        Ok(())
    }

    /// Encodes one bounded envelope for the owning journal's normal content publisher.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        self.validate(limits)?;
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
        &self,
        source: &Context,
        retained: &Context,
        compaction: &CompactionReference,
        limits: Limits,
    ) -> Result<()> {
        self.validate_projection_with_io(source, retained, compaction, limits, None)
    }

    pub(crate) fn validate_projection_with_io(
        &self, source: &Context, retained: &Context, compaction: &CompactionReference,
        limits: Limits, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.validate_with_io(limits, io)?;
        validate_projected_context_with_io(source, limits, io)?;
        validate_projected_context_with_io(retained, limits, io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            validate_compaction_native(compaction, source, retained, work)?;
        } else {
            validate_compaction_with_io(compaction, source, retained, io)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        validate_compaction_with_io(compaction, source, retained, io)?;
        if compaction
            .summary
            .as_ref()
            .is_none_or(|summary| summary.operation_id != self.operation_id)
        {
            return Err(context_checkpoint_error(io, "canonical checkpoint summary admission differs"));
        }
        Ok(())
    }
}

fn context_checkpoint_error(io: Option<&crate::executor::PreparationIo>, reason: &'static str) -> crate::Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
        return native_context_boundary(crate::wire::ErrorCode::Invalid, reason);
    }
    let _ = io;
    crate::Error::Invalid(reason.into())
}

fn context_checkpoint_file(limits: Limits, file: &FileRef, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return limits.validate_file_with_work(file, &mut Some(&mut history));
    }
    let _ = io;
    limits.validate_file(file)
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
            validate_compaction_with_io(reference, &source.context, &context, None)?;
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
        validate_projected_context(&source, self.limits)?;
        validate_projected_context(&compacted, self.limits)?;
        validate_compaction_with_io(&reference, &source, &compacted, None)?;
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
    pub async fn tail_revision(&self) -> Result<u64> {
        match self.provider.tail(self.path.clone()).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(crate::Error::Storage(error.to_string())),
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
            let revision = self
                .decode_revision(record.sequence, &record.value, self.limits)
                .await?;
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
                validate_compaction_with_io(reference, &source.context, &revision.context, None)
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

    async fn decode_revision(
        &self,
        sequence: u64,
        bytes: &[u8],
        limits: Limits,
    ) -> Result<ContextRevision> {
        let limits = restrict_context_limits(self.limits, limits)?;
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
        limits.validate_file(&revision.context)?;
        if revision.context.descriptor().media_type() != "application/json"
            || revision.context.descriptor().byte_length() > limits.render_bytes
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
        if crate::contract::canonical_json_bytes(&context)?.as_slice() != payload.as_ref() {
            return Err(crate::Error::Storage(
                "context payload is not canonical".into(),
            ));
        }
        validate_context_refs(&context, self.content_verifier.as_ref(), limits).await?;
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
        self.read_revision_bounded(revision, self.limits).await
    }

    async fn read_revision_bounded(
        &self,
        revision: u64,
        limits: Limits,
    ) -> Result<ContextRevision> {
        limits.validate()?;
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
        self.decode_revision(record.sequence, &record.value, limits)
            .await
    }

    /// Loads one pinned revision, checking its source proof with at most two record reads.
    pub async fn revision(&self, revision: u64) -> Result<ContextRevision> {
        self.revision_bounded(revision, self.limits).await
    }

    async fn revision_bounded(&self, revision: u64, limits: Limits) -> Result<ContextRevision> {
        let record = self.read_revision_bounded(revision, limits).await?;
        if let Some(reference) = &record.compaction {
            let source = self.read_revision_bounded(revision - 1, limits).await?;
            validate_compaction_with_io(reference, &source.context, &record.context, None)?;
        }
        Ok(record)
    }

    /// Captures the current continuing projection and its immutable revision.
    pub async fn latest_revision(&self) -> Result<Option<ContextRevision>> {
        let tail = self.tail_revision().await?;
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn tail_revision_on_original_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<u64> {
        use crate::store::history::HistoryIo as _;
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
        observer.begin_call()?;
        match self.provider.tail(self.path.clone()).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(source) => Err(native_context_stream_error(source)),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn read_revision_on_original_work(
        &self, revision: u64, limits: Limits, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ContextRevision, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let limits = restrict_context_limits_native(self.limits, limits, work)?;
        if revision == 0 {
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
                "context revision must be positive"));
        }
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        observer.begin_call()?;
        let mut stream = self.provider.read(ReadRequest {
            path: self.path.clone(), from: revision - 1, limit: 1,
        }).await.map_err(native_context_stream_error)?;
        let record = stream.next().await.ok_or_else(|| native_context_boundary(
            crate::wire::ErrorCode::Storage, "context revision is missing"))?
            .map_err(native_context_stream_error)?;
        observer.receive_record(record.value.len() as u64)?;
        if record.sequence != revision - 1 {
            return Err(native_context_boundary(crate::wire::ErrorCode::Storage,
                "context revision read returned an invalid range"));
        }
        if let Some(extra) = stream.next().await {
            let extra = extra.map_err(native_context_stream_error)?;
            observer.receive_record(extra.value.len() as u64)?;
            return Err(native_context_boundary(crate::wire::ErrorCode::Storage,
                "context revision read returned an invalid range"));
        }
        if record.value.len() > MAX_RECORD_BYTES {
            return Err(native_context_boundary(crate::wire::ErrorCode::Storage,
                "context record exceeds Stream limit"));
        }
        let stored = io.decode_preparation::<StoredContextRevision>(&record.value)
            .map_err(native_context_decode_storage)?.into_retained();
        let canonical_guard;
        let canonical = io.encode_json(&stored, MAX_RECORD_BYTES as u64, true)?;
        canonical_guard = io.guard_bytes(&canonical)?;
        if canonical.as_slice() != record.value.as_ref() || stored.format_version != 5 || stored.revision != revision
            || stored.source != self.source || stored.source_revision != self.source_revision
        {
            return Err(native_context_boundary(crate::wire::ErrorCode::Storage,
                "durable context identity, revision or canonical bytes are invalid"));
        }
        drop(canonical); drop(canonical_guard);
        limits.validate_file_with_native_work(&stored.context, work)?;
        if stored.context.descriptor().media_type() != "application/json"
            || stored.context.descriptor().byte_length() > limits.render_bytes
        {
            return Err(native_context_boundary(crate::wire::ErrorCode::Storage,
                "context payload descriptor is invalid"));
        }
        verify_context_file_native(&stored.context, self.content_verifier.as_ref(), work).await?;
        if !self.content_verifier.supports_native_read(stored.context.volume(), work)? {
            return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
                "context reader has no original native capability"));
        }
        let payload = self.content_verifier.read_with_native_work(
            &stored.context, stored.context.descriptor().byte_length(), work)?.await?;
        stored.context.descriptor().verify_with_work(payload.as_ref(), &mut Some(&mut observer))?;
        let context = io.decode_preparation::<Context>(payload.as_ref())
            .map_err(native_context_decode_storage)?.into_retained();
        let context_canonical_guard;
        let context_canonical = io.encode_json(&context, limits.render_bytes, true)?;
        context_canonical_guard = io.guard_bytes(&context_canonical)?;
        if context_canonical.as_slice() != payload.as_ref() {
            return Err(native_context_boundary(crate::wire::ErrorCode::Storage,
                "context payload is not canonical"));
        }
        drop(context_canonical); drop(context_canonical_guard);
        validate_context_refs_native(&context, self.content_verifier.as_ref(), limits, work).await?;
        let result = ContextRevision { format_version: stored.format_version, revision,
            source: stored.source, source_revision: stored.source_revision, content: stored.context,
            context, compaction: stored.compaction };
        drop(payload); drop(record); drop(stream); drop(observer);
        let retained = io.owned_bytes()?;
        let guard = io.into_native_custody(retained)?;
        Ok((result, guard))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn revision_on_original_work(
        &self, revision: u64, limits: Limits, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ContextRevision, acyclic_fs::NativeAllocation)> {
        let record = self.read_revision_on_original_work(revision, limits, work).await?;
        if let Some(reference) = &record.0.compaction {
            let source = self.read_revision_on_original_work(revision - 1, limits, work).await?;
            validate_compaction_native(reference, &source.0.context, &record.0.context, work)?;
        }
        Ok(record)
    }

    /// Deterministically compacts a context and returns its immutable source reference.
    pub fn compact(
        context: &Context,
        max_messages: usize,
        summary: Option<ContextSummary>,
        retention: CompactionRetention,
    ) -> Result<(Context, CompactionReference)> {
        Self::compact_with_io(context, max_messages, summary, retention, None)
    }

    pub(crate) fn compact_with_io(
        context: &Context, max_messages: usize, summary: Option<ContextSummary>,
        retention: CompactionRetention, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Context, CompactionReference)> {
        Self::compact_with_io_and_custody(context, max_messages, summary, retention, io)
            .map(|(result, _)| result)
    }

    fn compact_with_io_and_custody(
        context: &Context, max_messages: usize, summary: Option<ContextSummary>,
        retention: CompactionRetention, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<((Context, CompactionReference), u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io
            && let Some(work) = io.original_native_work()
        {
            let compacted = Self::compact_on_original_work(context, max_messages, summary, retention, work)?;
            let claim = compacted.1.bytes();
            io.absorb_native_custody(compacted.1)?;
            return Ok((compacted.0, claim));
        }
        retention.validate()?;
        context.validate_current_input()?;
        if max_messages == 0 {
            return Err(crate::Error::Invalid(
                "compaction max_messages must be positive".into(),
            ));
        }
        let source_digest = crate::executor::preparation_digest(context, io)?;
        if let Some(summary) = &summary {
            let mut observed_claim = 0;
            let digest = (|| {
                #[cfg(feature = "filesystem")]
                let summarized = if let Some(io) = io {
                    let mut history = io.history_io();
                    let (copy, claim) = compaction::clone_context_prefix(
                        context, summary.source_messages as usize, &mut Some(&mut history))?;
                    observed_claim = claim;
                    copy
                } else {
                    compaction::clone_context_prefix(context, summary.source_messages as usize, &mut None)?.0
                };
                #[cfg(not(feature = "filesystem"))]
                let summarized = compaction::clone_context_prefix(
                    context, summary.source_messages as usize, &mut None)?.0;
                crate::executor::preparation_digest(&summarized, io)
            })();
            #[cfg(feature = "filesystem")]
            if let Some(io) = io { io.release(observed_claim)?; }
            let _ = observed_claim;
            if summary.source_messages == 0
                || summary.source_digest != digest?
                || summary.output.descriptor().media_type() != "text/plain"
            {
                return Err(crate::Error::Invalid(
                    "summary does not bind its source prefix".into(),
                ));
            }
        }
        #[cfg(feature = "filesystem")]
        if io.is_some() {
            u32::try_from(max_messages).map_err(|_| context_boundary_with_io(
                io, crate::wire::ErrorCode::Invalid, "compaction budget exceeds portable count"))?;
        }
        let mut produced_claim = 0;
        let compacted = if context.messages.len() > max_messages
            || (summary.is_some() && context.messages.len() == max_messages)
        {
            let summary = summary.as_ref().ok_or_else(|| {
                crate::Error::Invalid(
                    "compaction requires a durable summary; context cannot be silently dropped"
                        .into(),
                )
            })?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let mut history = io.history_io();
                let (context, claim) = compaction::compact_context(context, max_messages, summary, &retention,
                    &mut Some(&mut history))?;
                produced_claim = claim;
                context
            } else {
                compaction::compact_context(context, max_messages, summary, &retention, &mut None)?.0
            }
            #[cfg(not(feature = "filesystem"))]
            { compaction::compact_context(context, max_messages, summary, &retention, &mut None)?.0 }
        } else {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let mut history = io.history_io();
                let (context, claim) = compaction::clone_context(context, &mut Some(&mut history))?;
                produced_claim = claim;
                context
            } else { context.clone() }
            #[cfg(not(feature = "filesystem"))]
            { context.clone() }
        };
        let reference = CompactionReference {
            source_digest,
            maximum_messages: u32::try_from(max_messages).map_err(|_| {
                crate::Error::Invalid("compaction budget exceeds portable count".into())
            })?,
            summary,
            retention,
        };
        Ok(((compacted, reference), produced_claim))
    }

    /// Native continuation of the original compaction policy; every clone is admitted on the caller's original ledger before physical allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn compact_on_original_work(
        context: &Context,
        max_messages: usize,
        summary: Option<ContextSummary>,
        retention: CompactionRetention,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<((Context, CompactionReference), acyclic_fs::NativeAllocation)> {
        retention.validate_native()?;
        context.validate_current_input_native()?;
        if max_messages == 0 {
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
                "compaction max_messages must be positive"));
        }
        let maximum_messages = u32::try_from(max_messages).map_err(|_| native_context_boundary(
            crate::wire::ErrorCode::Invalid, "compaction budget exceeds portable count"))?;
        let source_digest = context_digest_native(context, work)?;
        if let Some(summary) = &summary {
            let covered = context.messages.get(..summary.source_messages as usize)
                .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid,
                    "summary extent exceeds source projection"))?;
            let summarized = BorrowedContext {
                messages: ContextMessages { first: covered, user: None, second: &[] },
                metadata: &context.metadata,
                current_input_index: context.current_input_index.filter(|index| *index < summary.source_messages),
            };
            if summary.source_messages == 0
                || summary.source_digest != context_digest_native(&summarized, work)?
                || summary.output.descriptor().media_type() != "text/plain"
            {
                return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
                    "summary does not bind its source prefix"));
            }
        }
        let compacted = if context.messages.len() > max_messages
            || (summary.is_some() && context.messages.len() == max_messages)
        {
            let summary = summary.as_ref().ok_or_else(|| native_context_boundary(
                crate::wire::ErrorCode::Invalid,
                "compaction requires a durable summary; context cannot be silently dropped"))?;
            let mut io = crate::store::history::native_work_io(work).map_err(native_context_work_error)?;
            let (context, _) = compaction::compact_context(context, max_messages, summary, &retention,
                &mut Some(&mut io))?;
            (context, io.into_custody())
        } else { clone_context_native(context, work)? };
        Ok(((compacted.0, CompactionReference {
            source_digest, maximum_messages, retention, summary,
        }), compacted.1))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn context_digest_native<T: Serialize>(
    value: &T, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<[u8; 32]> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let bytes = io.encode_json(value, io.remaining()?.bytes_encoded, true)?;
    work.charge(acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64,
        ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn clone_summary_native(
    summary: Option<&ContextSummary>, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Option<ContextSummary>, acyclic_fs::NativeAllocation)> {
    match summary {
        Some(summary) => {
            let output = clone_file_native(&summary.output, work)?;
            Ok((Some(ContextSummary { operation_id: summary.operation_id, step: summary.step,
                source_messages: summary.source_messages, source_digest: summary.source_digest,
                output: output.0 }), output.1))
        }
        None => Ok((None, work.retain_owned(0).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?)),
    }
}

pub(super) fn mandatory_positions(
    context: &Context, covered: usize, retention: &CompactionRetention,
) -> Vec<usize> {
    let mut work = None;
    let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)
        .expect("unobserved compaction has no ledger denial");
    let (selected, _) = compaction::mandatory_flags(context, covered, retention, &mut quota)
        .expect("ordinary mandatory projection has no finite ledger failures");
    selected.into_iter().enumerate().filter_map(|(position, selected)| selected.then_some(position)).collect()
}

pub(crate) fn validate_compaction_with_io(
    reference: &CompactionReference, source: &Context, compacted: &Context,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return validate_compaction_native(reference, source, compacted, work);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
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
            .ok_or_else(|| context_boundary_with_io(Some(io), crate::wire::ErrorCode::Invalid,
                "compaction retention capacity overflows"))? as u64;
        quota.work().as_deref().expect("observed compaction proof").admit_copy(copied)?;
        compaction::reserve_vector_with_work(&mut roles, reference.retention.roles.len(), quota.work())?;
        quota.adopt((roles.capacity() * std::mem::size_of::<ModelRole>()) as u64)?;
        quota.work().as_deref_mut().expect("observed compaction proof").copy_bytes(copied)?;
        roles.extend_from_slice(&reference.retention.roles);
        let retention = CompactionRetention { roles, native_media: reference.retention.native_media };
        let ((reconstructed, proof), claim) = DurableContextProvider::compact_with_io_and_custody(
            source, reference.maximum_messages as usize, summary, retention, Some(io))?;
        quota.adopt(claim)?;
        if reference.source_digest != proof.source_digest || reconstructed != *compacted {
            return Err(context_boundary_with_io(Some(io), crate::wire::ErrorCode::Invalid,
                "compaction reference does not bind the source and retained context"));
        }
        return Ok(());
    }
    let _ = io;
    let (reconstructed, proof) = DurableContextProvider::compact(
        source, reference.maximum_messages as usize, reference.summary.clone(), reference.retention.clone())?;
    if reference.source_digest != proof.source_digest || reconstructed != *compacted {
        return Err(crate::Error::Invalid(
            "compaction reference does not bind the source and retained context".into(),
        ));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_compaction_native(
    reference: &CompactionReference, source: &Context, compacted: &Context,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let summary = clone_summary_native(reference.summary.as_ref(), work)?;
    let retention = clone_retention_native(&reference.retention, work)?;
    let reconstructed = DurableContextProvider::compact_on_original_work(
        source, reference.maximum_messages as usize, summary.0, retention.0, work)?;
    if reference.source_digest != reconstructed.0.1.source_digest || reconstructed.0.0 != *compacted {
        return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
            "compaction reference does not bind the source and retained context"));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_projected_context_native(
    context: &Context, limits: Limits, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    limits.validate_with_native_work(work)?;
    context.validate_current_input_native()?;
    if context.messages.len() > limits.context_messages || context.metadata.len() > limits.attachments {
        return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
            "context projection exceeded declared bounds"));
    }
    for message in &context.messages { message.content.validate_limits_with_native_work(limits, work)?; }
    for file in context.metadata.values() { limits.validate_file_with_native_work(file, work)?; }
    let mut observer = crate::store::history::native_work_io(work)
        .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
    crate::contract::validate_trusted_json_byte_bound(
        context, limits.render_bytes, &mut Some(&mut observer))?;
    Ok(())
}

pub(crate) fn validate_projected_context_with_io(
    context: &Context, limits: Limits, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return validate_projected_context_native(context, limits, work);
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        limits.validate_with_work(&mut observed)?;
        context.validate_current_input()?;
        if context.messages.len() > limits.context_messages || context.metadata.len() > limits.attachments {
            return Err(crate::Error::Invalid("context projection exceeded declared bounds".into()));
        }
        for message in &context.messages {
            message.content.validate_limits_with_work(limits, &mut observed)?;
        }
        for file in context.metadata.values() {
            limits.validate_file_with_work(file, &mut observed)?;
        }
        return crate::contract::validate_trusted_json_byte_bound(
            context, limits.render_bytes, &mut observed);
    }
    let _ = io;
    validate_projected_context(context, limits)
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
        .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid,
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_context_stream_error(source: StreamError) -> crate::Error {
    crate::Error::NativeStream { code: crate::wire::ErrorCode::Storage, source, custody: None }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_context_decode_storage(error: crate::Error) -> crate::Error {
    match error {
        crate::Error::NativeDecode { source, custody, .. } => crate::Error::NativeDecode {
            code: crate::wire::ErrorCode::Storage, source, custody,
        },
        error => error,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn restrict_context_limits_native(
    pinned: Limits, admitted: Limits, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<Limits> {
    pinned.validate_with_native_work(work)?;
    admitted.validate_with_native_work(work)?;
    Ok(restrict_context_limits_fields(pinned, admitted))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) async fn verify_context_file_native(
    file: &FileRef, verifier: &dyn ContentResidencyVerifier,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if !verifier.supports_native_read(file.volume(), work)? {
        return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
            "context verifier has no original native capability"));
    }
    verifier.verify_with_native_work(file, file.descriptor().byte_length(), work)?.await
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
async fn verify_context_media_native(
    file: &FileRef, policy: &crate::model::FileProjectionPolicy,
    verifier: &dyn ContentResidencyVerifier, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    verify_context_file_native(file, verifier, work).await?;
    if let crate::model::FileProjectionPolicy::Native(policy) = policy
        && let Some(configuration) = &policy.configuration
    {
        verify_context_file_native(&configuration.configuration.content, verifier, work).await?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) async fn validate_context_refs_native(
    context: &Context, verifier: &dyn ContentResidencyVerifier, limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    validate_projected_context_native(context, limits, work)?;
    for message in &context.messages {
        for part in message.content.parts() {
            match part {
                ModelContentPart::File { file, policy } =>
                    verify_context_media_native(file, policy, verifier, work).await?,
                ModelContentPart::ToolResult { content: crate::model::ToolResultContent::Parts { parts }, .. } => {
                    for part in parts {
                        if let crate::model::ModelDataPart::File { file, policy } = part {
                            verify_context_media_native(file, policy, verifier, work).await?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for (name, file) in &context.metadata {
        if !name.contains('.') || name.len() > crate::COMPONENT_LABEL_MAX_BYTES
            || name.chars().any(char::is_control)
        {
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
                "durable context metadata key is invalid"));
        }
        verify_context_file_native(file, verifier, work).await?;
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
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
    ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
        Box::pin(async move {
            limits.validate()?;
            let tail = self.tail_revision().await?;
            if tail == 0 {
                return Ok(Vec::new());
            }
            Ok(self.revision_bounded(tail, limits).await?.context.messages)
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
            limits.validate_with_native_work(work)?;
            let tail = self.tail_revision_on_original_work(work).await?;
            if tail == 0 {
                return Ok((Vec::new(), work.retain_owned(0)
                    .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?));
            }
            let record = self.revision_on_original_work(tail, limits, work).await?;
            Ok((record.0.context.messages, record.1))
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
        Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
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
            validate_source_messages(&self.messages, limits)?;
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
            validate_source_messages_native(&self.messages, limits, work)?;
            clone_messages_native(&self.messages, work)
        })
    }
}

/// A source contributes messages only; its unrelated metadata is not imported.
pub(super) fn validate_source_messages(messages: &[ModelMessage], limits: Limits) -> Result<()> {
    limits.validate()?;
    let metadata = BTreeMap::new();
    BorrowedContext {
        messages: ContextMessages {
            first: messages,
            user: None,
            second: &[],
        },
        metadata: &metadata,
        current_input_index: None,
    }
    .validate_bounds(limits)?;
    for message in messages {
        message.content.validate_limits(limits)?;
    }
    Ok(())
}

/// A pinned provider allowance can narrow, never widen, the admitted request.
pub(super) fn restrict_context_limits(pinned: Limits, admitted: Limits) -> Result<Limits> {
    restrict_context_limits_with_io(pinned, admitted, None)
}

pub(crate) fn restrict_context_limits_with_io(
    pinned: Limits, admitted: Limits, io: Option<&ContextPreparationIo>,
) -> Result<Limits> {
    validate_context_limits_with_io(pinned, io)?;
    validate_context_limits_with_io(admitted, io)?;
    Ok(restrict_context_limits_fields(pinned, admitted))
}

fn restrict_context_limits_fields(pinned: Limits, admitted: Limits) -> Limits {
    Limits {
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
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
                "context source identity is invalid"));
        }
        if !source.supports_native() {
            return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
                "context source has no native capability"));
        }
        let mut name = clone_text_native(name, work)?;
        let mut revision = clone_text_native(revision, work)?;
        name.1.absorb(&mut revision.1).map_err(native_context_work_error)?;
        Ok((Self::new(name.0, revision.0, source, placement), name.1))
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
            validate_projected_context(&context, limits)?;
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
                native_context_boundary(crate::wire::ErrorCode::Invalid, "context contract storage overflows"))?;
        let owned = text_bytes.checked_add(3 * std::mem::size_of::<(String, Value)>() as u64)
            .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid,
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
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
                "context source identity is invalid"));
        }
        if !self.source.supports_native() {
            return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
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
            validate_projected_context_native(&context.0, limits, work)?;
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
            validate_projected_context(&context, limits)?;
            let compacted = DurableContextProvider::compact(
                &context,
                self.max_messages,
                self.summary.clone(),
                self.retention.clone(),
            )?
            .0;
            validate_projected_context(&compacted, limits)?;
            Ok(compacted)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.retention.validate_native()?;
        if self.max_messages == 0 {
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
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
            validate_projected_context_native(&context.0, limits, work)?;
            let summary = clone_summary_native(self.summary.as_ref(), work)?;
            let retention = clone_retention_native(&self.retention, work)?;
            io.absorb_native_custody(summary.1)?;
            io.absorb_native_custody(retention.1)?;
            let compacted = DurableContextProvider::compact_with_io(
                &context.0, self.max_messages, summary.0, retention.0, Some(&io))?.0;
            validate_projected_context_native(&compacted, limits, work)?;
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn validate_current_input_native(&self) -> Result<()> {
        if self.current_input_index.is_some_and(|index| index as usize >= self.messages.len()) {
            return Err(native_context_boundary(crate::wire::ErrorCode::Invalid,
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
    fn validate_bounds(&self, limits: Limits) -> Result<()> {
        self.validate_bounds_with_io(limits, None)
    }

    fn validate_bounds_with_io(&self, limits: Limits, io: Option<&ContextPreparationIo>) -> Result<()> {
        let count = self
            .messages
            .first
            .len()
            .checked_add(usize::from(self.messages.user.is_some()))
            .and_then(|count| count.checked_add(self.messages.second.len()))
            .filter(|count| *count <= limits.context_messages);
        if count.is_none() || self.metadata.len() > limits.attachments {
            return Err(context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                "context projection exceeded declared bounds"));
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            for message in self.messages.first.iter().chain(self.messages.second) {
                validate_context_content_with_io(&message.content, limits, false, Some(io))?;
            }
            if let Some(content) = self.messages.user {
                validate_context_content_with_io(content, limits, false, Some(io))?;
            }
        }
        validate_context_json_with_io(self, limits.render_bytes, io)
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
        io: Option<&ContextPreparationIo>,
    ) -> Result<Self> {
        let input = copy_context_content_with_io(&turn.input, io)?;
        let selected_context = match &turn.selected_context {
            Some(selected) => {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    let mut history = io.history_io();
                    let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                    let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
                    let mut message_ids = Vec::new();
                    let copied = (selected.selection.message_ids.len() * std::mem::size_of::<uuid::Uuid>()) as u64;
                    quota.work().as_deref().expect("observed selection").admit_copy(copied)?;
                    compaction::reserve_vector_with_work(&mut message_ids, selected.selection.message_ids.len(), quota.work())?;
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
                    Some(SelectedModelContext {
                        selection: crate::conversation::ModelContextSelection {
                            conversation_revision: selected.selection.conversation_revision,
                            message_ids, checkpoint,
                        },
                        messages,
                    })
                } else {
                    Some(selected.clone())
                }
                #[cfg(not(feature = "filesystem"))]
                { Some(selected.clone()) }
            }
            None => None,
        };
        Ok(Self { input, selected_context, step, prior_messages: copy_context_messages_with_io(prior, io)? })
    }
}

#[cfg(feature = "filesystem")]
pub(crate) type ContextPreparationIo = crate::executor::PreparationIo;
#[cfg(not(feature = "filesystem"))]
pub(crate) type ContextPreparationIo = ();

fn context_boundary_with_io(
    io: Option<&ContextPreparationIo>, code: crate::wire::ErrorCode, reason: &'static str,
) -> crate::Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(ContextPreparationIo::original_native_work).is_some() {
        return native_context_boundary(code, reason);
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unsupported => crate::Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Conflict => crate::Error::Conflict(reason.into()),
        _ => crate::Error::Invalid(reason.into()),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_context_boundary(code: crate::wire::ErrorCode, reason: &'static str) -> crate::Error {
    crate::Error::NativeBoundary { code, reason }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_context_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> crate::Error {
    crate::Error::NativeWork { cause, custody: None }
}

fn validate_context_limits_with_io(limits: Limits, io: Option<&ContextPreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return limits.validate_with_work(&mut Some(&mut history));
    }
    let _ = io;
    limits.validate()
}

fn validate_context_json_with_io<T: Serialize>(
    value: &T, maximum: u64, io: Option<&ContextPreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return crate::contract::validate_trusted_json_byte_bound(
            value, maximum, &mut Some(&mut history));
    }
    let _ = io;
    crate::contract::validate_json_byte_bound(value, maximum)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_context_json_native<T: Serialize>(
    value: &T, maximum: u64, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let mut observer = crate::store::history::native_work_io(work).map_err(native_context_work_error)?;
    crate::contract::validate_trusted_json_byte_bound(value, maximum, &mut Some(&mut observer))
}

fn validate_context_content_with_io(
    content: &ModelContent, limits: Limits, user: bool, io: Option<&ContextPreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        content.validate_limits_with_work(limits, &mut work)?;
        return if user { content.validate_user_input_with_work(&mut work) } else { Ok(()) };
    }
    let _ = io;
    content.validate_limits(limits)?;
    if user { content.validate_user_input()?; }
    Ok(())
}

fn validate_context_user_input_with_io(content: &ModelContent, io: Option<&ContextPreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return content.validate_user_input_with_work(&mut Some(&mut history));
    }
    let _ = io;
    content.validate_user_input()
}

fn validate_context_selection_with_io(input: &ContextInput, io: Option<&ContextPreparationIo>) -> Result<()> {
    if let Some(selected) = &input.selected_context {
        selected.validate_for_input(&input.input, io)?;
    }
    Ok(())
}

fn validate_context_projection_with_io(context: &Context, limits: Limits, io: Option<&ContextPreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return validate_projected_context_with_io(context, limits, Some(io));
    }
    let _ = io;
    validate_projected_context(context, limits)
}

fn copy_context_content_with_io(content: &ModelContent, io: Option<&ContextPreparationIo>) -> Result<ModelContent> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return crate::model::clone_content(content, &mut Some(&mut history)).map(|(copy, _)| copy);
    }
    let _ = io;
    Ok(content.clone())
}

fn copy_context_messages_with_io(messages: &[ModelMessage], io: Option<&ContextPreparationIo>) -> Result<Vec<ModelMessage>> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return compaction::clone_messages(messages, &mut Some(&mut history)).map(|(copy, _)| copy);
    }
    let _ = io;
    Ok(messages.to_vec())
}

pub(crate) fn context_clone_with_io(context: &Context, io: Option<&ContextPreparationIo>) -> Result<Context> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return compaction::clone_context(context, &mut Some(&mut history)).map(|(copy, _)| copy);
    }
    let _ = io;
    Ok(context.clone())
}

fn copy_context_base_messages_with_io(input: &ContextInput, io: Option<&ContextPreparationIo>) -> Result<Vec<ModelMessage>> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return compaction::clone_base_messages(
            input.selected_context.as_ref().map_or(&[], |selected| selected.messages.as_slice()),
            input.selected_context.is_none().then_some(&input.input), &input.prior_messages,
            &mut Some(&mut history)).map(|(copy, _)| copy);
    }
    let _ = io;
    let base = input.selected_context.as_ref().map_or_else(
        || vec![ModelMessage { role: ModelRole::User, content: input.input.clone() }],
        |selected| selected.messages.clone());
    Ok(base.into_iter().chain(input.prior_messages.iter().cloned()).collect())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn reserve_context_vec_native<T>(
    values: &mut Vec<T>, capacity: usize, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::NativeAllocation> {
    let mut guard = work.retain_owned(0).map_err(native_context_work_error)?;
    if capacity <= values.capacity() { return Ok(guard); }
    let old = values.capacity();
    let bytes = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    guard.resize_retained(bytes as u64).map_err(native_context_work_error)?;
    let copied = values.len().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid, "context vector copy overflows"))?;
    work.charge(acyclic_fs::WorkCounters { allocation_operations: 1, bytes_copied: copied as u64,
        ..acyclic_fs::WorkCounters::default() }).map_err(native_context_work_error)?;
    values.try_reserve_exact(capacity - values.len()).map_err(|cause| crate::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
    })?;
    let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    if let Err(cause) = guard.reconcile_capacity(actual as u64) {
        return Err(crate::Error::NativeWork { cause, custody: Some(guard) });
    }
    let retired = old.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| native_context_boundary(crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    if let Err(cause) = guard.release(retired as u64) {
        return Err(crate::Error::NativeWork { cause, custody: Some(guard) });
    }
    if let Err(cause) = work.remaining() {
        return Err(crate::Error::NativeWork { cause, custody: Some(guard) });
    }
    Ok(guard)
}

fn reserve_context_messages_with_io(
    messages: &mut Vec<ModelMessage>, capacity: usize, io: Option<&ContextPreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        return compaction::reserve_vector_with_work(messages, capacity, &mut Some(&mut history));
    }
    let _ = io;
    messages.reserve(capacity.saturating_sub(messages.len()));
    Ok(())
}

fn append_context_messages_with_io(
    first: &mut Vec<ModelMessage>, second: &mut Vec<ModelMessage>, io: Option<&ContextPreparationIo>,
) -> Result<()> {
    let capacity = first.len().checked_add(second.len()).ok_or_else(||
        context_boundary_with_io(io, crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
    reserve_context_messages_with_io(first, capacity, io)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let mut history = io.history_io();
        let copied = second.len().checked_mul(std::mem::size_of::<ModelMessage>()).ok_or_else(||
            context_boundary_with_io(Some(io), crate::wire::ErrorCode::Invalid, "context vector copy overflows"))?;
        use crate::store::history::HistoryIo as _;
        history.admit_copy(copied as u64)?;
        history.copy_bytes(copied as u64)?;
    }
    first.append(second);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let bytes = second.capacity().checked_mul(std::mem::size_of::<ModelMessage>()).ok_or_else(||
            context_boundary_with_io(Some(io), crate::wire::ErrorCode::Invalid, "context vector capacity overflows"))?;
        // The payloads moved, but the drained source vector's backing no longer has a physical user.
        drop(std::mem::take(second));
        io.release(bytes as u64)?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_source_messages_native(
    messages: &[ModelMessage], limits: Limits, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    limits.validate_with_native_work(work)?;
    if messages.len() > limits.context_messages {
        return Err(native_context_boundary(crate::wire::ErrorCode::Invalid, "context projection exceeded declared bounds"));
    }
    for message in messages { message.content.validate_limits_with_native_work(limits, work)?; }
    let metadata = BTreeMap::new();
    validate_context_json_native(&BorrowedContext {
        messages: ContextMessages { first: messages, user: None, second: &[] },
        metadata: &metadata, current_input_index: None,
    }, limits.render_bytes, work)?;
    Ok(())
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
        Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
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
        Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
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
        Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
            "context stage has no native capability"))
    }
}

/// Explicit ordered pipeline; no hidden framework stages are inserted.
#[derive(Clone, Default)]
pub struct ContextPipeline(Vec<Arc<dyn ContextStage>>);

impl ContextPipeline {
    /// Validates stage identities before installation or explicit reload.
    pub fn validate(&self) -> Result<()> {
        for stage in &self.0 {
            stage.validate()?;
        }
        Ok(())
    }

    /// Validates only the explicit native stage/source SPI before configuration copies or effects.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining().map_err(native_context_work_error)?;
        if self.0.iter().any(|stage| !stage.supports_native()) {
            return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
                "context stage has no native capability"));
        }
        for stage in &self.0 { stage.validate_native(work)?; }
        Ok(())
    }

    /// Appends to a caller-guarded native pipeline, returning only its actual backing-capacity delta.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn append_with_native_work(
        &mut self, stage: Arc<dyn ContextStage>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeAllocation> {
        work.remaining().map_err(native_context_work_error)?;
        if !stage.supports_native() {
            return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
                "context stage has no native capability"));
        }
        stage.validate_native(work)?;
        let capacity = self.0.len().checked_add(1).ok_or_else(||
            native_context_boundary(crate::wire::ErrorCode::Invalid, "context stage count overflows"))?;
        let guard = reserve_context_vec_native(&mut self.0, capacity, work)?;
        self.0.push(stage);
        Ok(guard)
    }

    /// Installs a structurally valid replacement for future operations only.
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
    pub fn base_context(input: &ContextInput, limits: Limits) -> Result<Context> {
        Self::base_context_with_io(input, limits, None)
    }

    pub(crate) fn base_context_with_io(
        input: &ContextInput,
        limits: Limits,
        io: Option<&ContextPreparationIo>,
    ) -> Result<Context> {
        validate_context_limits_with_io(limits, io)?;
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
            context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                "current input is missing from context")
        })?)
        .map_err(|_| context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
            "current input index exceeds portable count"))?;
        BorrowedContext {
            messages,
            metadata: &metadata,
            current_input_index: Some(current_input_index),
        }
        .validate_bounds_with_io(limits, io)?;
        validate_context_content_with_io(&input.input, limits, true, io)?;
        validate_context_selection_with_io(input, io)?;
        if input
            .selected_context
            .as_ref()
            .is_some_and(|selected| selected.selection.checkpoint.is_some())
        {
            return Err(context_boundary_with_io(io, crate::wire::ErrorCode::Unsupported,
                "canonical checkpoint requires owner-resolved base context"));
        }
        let messages = copy_context_base_messages_with_io(input, io)?;
        let context = Context { messages, metadata, current_input_index: Some(current_input_index) };
        validate_context_projection_with_io(&context, limits, io)?;
        Ok(context)
    }

    /// Adds an exact new delta to an owner-resolved base before running stages.
    pub fn continue_base(retained: Context, delta: Context, limits: Limits) -> Result<Context> {
        Self::continue_base_with_io(retained, delta, limits, None)
    }

    pub(crate) fn continue_base_with_io(
        mut retained: Context,
        delta: Context,
        limits: Limits,
        io: Option<&ContextPreparationIo>,
    ) -> Result<Context> {
        validate_context_projection_with_io(&retained, limits, io)?;
        if delta.current_input_index.is_none() {
            return Err(context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                "continued base has no current input"));
        }
        // The retained parent's old active input is historical in this admission.
        retained.current_input_index = None;
        selection::place_context(retained, delta, ContextPlacement::Append, limits, io)
    }

    /// Bounds the initial view and every intermediate projection, without truncation.
    pub async fn run_bounded(&self, input: &ContextInput, limits: Limits) -> Result<Context> {
        self.transform_bounded(input, Self::base_context(input, limits)?, limits)
            .await
    }

    /// Applies declared stages to an explicit base, including a retained canonical checkpoint plus its new history delta.
    pub async fn transform_bounded(
        &self,
        input: &ContextInput,
        context: Context,
        limits: Limits,
    ) -> Result<Context> {
        self.transform_bounded_with_io(input, context, limits, None).await
    }

    pub(crate) async fn transform_bounded_with_io(
        &self,
        input: &ContextInput,
        mut context: Context,
        limits: Limits,
        io: Option<&ContextPreparationIo>,
    ) -> Result<Context> {
        validate_context_limits_with_io(limits, io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = io.and_then(ContextPreparationIo::original_native_work);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native {
            self.validate_with_native_work(work)?;
        } else {
            self.validate()?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.validate()?;
        if self.0.len() > limits.context_messages {
            return Err(context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                "context stage count exceeds limit"));
        }
        validate_context_user_input_with_io(&input.input, io)?;
        validate_context_selection_with_io(input, io)?;
        validate_context_projection_with_io(&context, limits, io)?;
        if context.current_input_index.is_none() {
            return Err(context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
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
            validate_context_projection_with_io(&context, limits, io)?;
            if context.current_input_index.is_none() {
                return Err(context_boundary_with_io(io, crate::wire::ErrorCode::Invalid,
                    "context stage removed the current input marker"));
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

    /// Copies only the real stage-handle vector; pinned stage implementations remain shared.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut output = (Self(Vec::new()), work.retain_owned(0).map_err(native_context_work_error)?);
        let mut vector_guard = reserve_context_vec_native(&mut output.0.0, self.0.len(), work)?;
        output.1.absorb(&mut vector_guard).map_err(native_context_work_error)?;
        let copied = self.0.len().checked_mul(std::mem::size_of::<Arc<dyn ContextStage>>()).ok_or_else(||
            native_context_boundary(crate::wire::ErrorCode::Invalid, "context stage copy overflows"))?;
        work.charge(acyclic_fs::WorkCounters { bytes_copied: copied as u64,
            ..acyclic_fs::WorkCounters::default() }).map_err(native_context_work_error)?;
        output.0.0.extend(self.0.iter().cloned());
        Ok(output)
    }

    /// Produces each original immutable contract without invoking an ordinary custom callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn contracts_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<Value>, acyclic_fs::NativeAllocation)> {
        if self.0.iter().any(|stage| !stage.supports_native()) {
            return Err(native_context_boundary(crate::wire::ErrorCode::Unsupported,
                "context stage has no native capability"));
        }
        let mut output = (Vec::new(), work.retain_owned(0).map_err(native_context_work_error)?);
        let mut vector_guard = reserve_context_vec_native(&mut output.0, self.0.len(), work)?;
        output.1.absorb(&mut vector_guard).map_err(native_context_work_error)?;
        for stage in &self.0 {
            let mut source = stage.contract_with_native_work(work)?;
            output.1.absorb(&mut source.1).map_err(native_context_work_error)?;
            output.0.push(source.0);
        }
        Ok(output)
    }

    pub(crate) fn summary_source(
        source: Context, revision: String, io: Option<&ContextPreparationIo>,
    ) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let mut history = io.history_io();
            let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
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
                compaction::reserve_vector_with_work(&mut stages, 1, quota.work())?;
                quota.adopt((stages.capacity() * std::mem::size_of::<Arc<dyn ContextStage>>()) as u64)?;
                stages.push(stage);
                Ok(())
            })();
            if let Err(error) = made { drop(stages); return Err(error); }
            quota.transfer();
            return Ok(Self(stages));
        }
        let _ = io;
        Ok(Self::new([Arc::new(SourceStage::new(
            "summary-source", revision, Arc::new(source), ContextPlacement::Prepend)) as Arc<dyn ContextStage>]))
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
        validate_projected_context_with_io(&source, Limits::default(), Some(&io))?;
        assert_eq!(io.owned_bytes()?, 0);
        let mut budget = owned_context_budget();
        budget.items_examined = 0;
        let denied = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
        assert!(validate_projected_context_with_io(&source, Limits::default(), Some(&denied)).is_err());
        assert_eq!(denied.owned_bytes()?, 0);
        let cancelled = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
        cancellation.cancel();
        assert!(validate_projected_context_with_io(&source, Limits::default(), Some(&cancelled)).is_err());
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
        let output = message(ModelRole::Assistant, "summary")?.content.file_refs()[0].clone();
        let summary = ContextSummary { operation_id: crate::OperationId::new(), step: 0,
            source_messages: 2, source_digest: crate::contract::canonical_json_digest(&prefix)?, output };
        let (retained, reference) = DurableContextProvider::compact(
            &source, 3, Some(summary), CompactionRetention::default())?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
        validate_compaction_with_io(&reference, &source, &retained, Some(&io))?;
        assert_eq!(io.owned_bytes()?, 0);
        let mut changed = retained.clone();
        changed.messages.last_mut().expect("retained current input").content = ModelContent::Text("changed".into());
        assert!(validate_compaction_with_io(&reference, &source, &changed, Some(&io)).is_err());
        assert_eq!(io.owned_bytes()?, 0);
        let mut changed = reference.clone();
        changed.summary.as_mut().expect("summary proof").source_digest[0] ^= 1;
        assert!(validate_compaction_with_io(&changed, &source, &retained, Some(&io)).is_err());
        assert_eq!(io.owned_bytes()?, 0);
        let mut budget = owned_context_budget();
        budget.items_examined = 0;
        let denied = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
        assert!(validate_compaction_with_io(&reference, &source, &retained, Some(&denied)).is_err());
        assert_eq!(denied.owned_bytes()?, 0);
        let cancelled = crate::executor::PreparationIo::from_budget(owned_context_budget(), &cancellation)?;
        cancellation.cancel();
        assert!(validate_compaction_with_io(&reference, &source, &retained, Some(&cancelled)).is_err());
        assert_eq!(cancelled.owned_bytes()?, 0);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one fixture compares borrowed and owned wire bounds for both base variants and checkpoint continuation"
    )]
    fn borrowed_context_preflight_matches_base_and_continuation_wire() -> Result<()> {
        let user = ModelContent::Text("current 🦀 \"quoted\"\n".into());
        let message = |role, text: &str| ModelMessage {
            role,
            content: ModelContent::Text(text.into()),
        };
        let selected = SelectedModelContext {
            selection: crate::conversation::ModelContextSelection {
                conversation_revision: 2,
                message_ids: vec![uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2)],
                checkpoint: None,
            },
            messages: vec![
                message(ModelRole::System, "pinned instruction"),
                ModelMessage {
                    role: ModelRole::User,
                    content: user.clone(),
                },
            ],
        };
        for selected_context in [None, Some(selected)] {
            let input = ContextInput {
                input: user.clone(),
                selected_context,
                step: 0,
                prior_messages: vec![message(ModelRole::Assistant, "prior answer")],
            };
            let base = ContextPipeline::base_context(&input, Limits::default())?;
            let view = BorrowedContext {
                messages: ContextMessages {
                    first: input
                        .selected_context
                        .as_ref()
                        .map_or(&[], |selected| selected.messages.as_slice()),
                    user: input.selected_context.is_none().then_some(&input.input),
                    second: &input.prior_messages,
                },
                metadata: &base.metadata,
                current_input_index: base.current_input_index,
            };
            let encoded = crate::contract::canonical_json_bytes(&base)?;
            assert_eq!(crate::contract::canonical_json_bytes(&view)?, encoded);
            let exact = Limits {
                render_bytes: encoded.len() as u64,
                ..Limits::default()
            };
            assert_eq!(ContextPipeline::base_context(&input, exact)?, base);
            assert!(matches!(
                ContextPipeline::base_context(
                    &input,
                    Limits {
                        render_bytes: exact.render_bytes - 1,
                        ..exact
                    }
                ),
                Err(crate::Error::Invalid(_))
            ));
            assert!(matches!(
                ContextPipeline::base_context(
                    &input,
                    Limits {
                        context_messages: base.messages.len() - 1,
                        ..Limits::default()
                    }
                ),
                Err(crate::Error::Invalid(_))
            ));

            let retained = Context {
                messages: vec![message(ModelRole::System, "retained summary")],
                ..Default::default()
            };
            let expected = Context {
                messages: retained
                    .messages
                    .iter()
                    .chain(&base.messages)
                    .cloned()
                    .collect(),
                metadata: retained.metadata.clone(),
                current_input_index: base.current_input_index.map(|index| index + 1),
            };
            let view = BorrowedContext {
                messages: ContextMessages {
                    first: &retained.messages,
                    user: None,
                    second: &base.messages,
                },
                metadata: &retained.metadata,
                current_input_index: expected.current_input_index,
            };
            let encoded = crate::contract::canonical_json_bytes(&expected)?;
            assert_eq!(crate::contract::canonical_json_bytes(&view)?, encoded);
            let exact = Limits {
                render_bytes: encoded.len() as u64,
                ..Limits::default()
            };
            assert_eq!(
                ContextPipeline::continue_base(retained.clone(), base.clone(), exact)?,
                expected
            );
            assert!(matches!(
                ContextPipeline::continue_base(
                    retained.clone(),
                    base.clone(),
                    Limits {
                        render_bytes: exact.render_bytes - 1,
                        ..exact
                    }
                ),
                Err(crate::Error::Invalid(_))
            ));
            assert!(matches!(
                ContextPipeline::continue_base(
                    retained,
                    base,
                    Limits {
                        context_messages: expected.messages.len() - 1,
                        ..Limits::default()
                    }
                ),
                Err(crate::Error::Invalid(_))
            ));
        }
        Ok(())
    }
    use acyclic_stream::MemoryStream;
    use std::{future::Future, pin::Pin};

    fn native_retention_policy() -> FileProjectionPolicy {
        FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
            intent: crate::model::NativeMediaIntent::Document { maximum_pages: 1 },
            maximum_bytes: 1_048_576,
            maximum_work: 1,
            configuration: None,
        }))
    }

    fn validate_native_call_fixture(context: &Context) -> Result<()> {
        crate::model::PreparedModelRequest::prepare(
            crate::model::ModelRequest {
                model: crate::model::Model::new(
                    "test",
                    "native-retention",
                    "1",
                    serde_json::json!({}),
                )?,
                messages: context.messages.clone(),
                tools: vec![crate::tool::ToolDefinition {
                    name: "native.tool".into(),
                    revision: "1".into(),
                    description: "Native retention fixture".into(),
                    input_schema: serde_json::json!({"type":"null"}),
                    output_schema: serde_json::json!({"type":"null"}),
                    projection_schema: serde_json::json!({"oneOf": [
                        crate::tool::json_projection_schema(serde_json::json!({"type":"null"})),
                        {"type":"object", "required":["kind","parts"],
                         "properties":{"kind":{"const":"parts"}, "parts":{"type":"array", "minItems":1, "maxItems":1, "items":{"type":"object"}}},
                         "additionalProperties":false}
                    ]}),
                }],
                max_output_tokens: Some(128),
            },
            Limits::default(),
            None,
        )?;
        Ok(())
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
            validate_native_call_fixture(&source)?;
            let summary = ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: u32::try_from(source.messages.len())
                    .map_err(|_| crate::Error::Invalid("fixture count exceeds u32".into()))?,
                source_digest: crate::contract::canonical_json_digest(&source)?,
                output: file.clone(),
            };
            let (compacted, reference) = DurableContextProvider::compact(
                &source,
                results + 3,
                Some(summary.clone()),
                CompactionRetention::default(),
            )?;
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
            validate_compaction_with_io(&reference, &source, &compacted, None)?;
            validate_native_call_fixture(&compacted)?;
            assert!(
                DurableContextProvider::compact(
                    &source,
                    results + 2,
                    Some(summary.clone()),
                    CompactionRetention::default()
                )
                .is_err()
            );
            let retention = CompactionRetention {
                native_media: false,
                ..CompactionRetention::default()
            };
            let (without_media, _) =
                DurableContextProvider::compact(&source, 2, Some(summary), retention)?;
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
        let (prefix_compacted, prefix_reference) = DurableContextProvider::compact(
            &source,
            5,
            Some(prefix_summary.clone()),
            CompactionRetention::default(),
        )?;
        assert_eq!(prefix_compacted.messages.get(2..), source.messages.get(3..));
        validate_compaction_with_io(&prefix_reference, &source, &prefix_compacted, None)?;
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
            validate_compaction_with_io(&reference, &source, &projected, None)?;
            let mut corrupt = projected;
            corrupt
                .messages
                .last_mut()
                .ok_or_else(|| crate::Error::Invalid("empty context".into()))?
                .content = ModelContent::Text("substitution".into());
            assert!(validate_compaction_with_io(&reference, &source, &corrupt, None).is_err());
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
        validate_compaction_with_io(&reference, &configurable, &projected, None)?;
        reference.retention = CompactionRetention::default();
        assert!(validate_compaction_with_io(&reference, &configurable, &projected, None).is_err());
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

    struct SourceLimitSpy(std::sync::Mutex<Vec<Limits>>);

    impl ContextSource for SourceLimitSpy {
        fn load<'a>(
            &'a self,
            _: &'a ContextInput,
            limits: Limits,
        ) -> BoxFuture<'a, Result<Vec<ModelMessage>>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
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
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let projected = pipeline.run_bounded(&input, limits).await?;
        assert_eq!(projected.current_input_index, Some(1));
        assert_eq!(
            *source
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![limits]
        );
        assert_eq!(
            *stage
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![limits]
        );
        let base = ContextPipeline::base_context(&input, limits)?;
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
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let base = ContextPipeline::base_context(&input, admitted)?;
        assert!(matches!(
            stage.apply(&input, base.clone(), admitted).await,
            Err(crate::Error::Invalid(_))
        ));
        assert_eq!(verifier.count.load(Ordering::SeqCst), 0);
        assert!(
            renderer
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
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
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
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

        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
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
        validate_compaction_with_io(&reference, &source, &next, None)?;
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
        validate_compaction_with_io(&reference, &next, &compacted, None)?;
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
        validate_compaction_with_io(&reference, &source, &compacted, None)?;
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
        let raw = ContextPipeline::base_context(&input, Limits::default())?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(ContextPipeline::default().run(&input).await?, raw);
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
        validate_compaction_with_io(&reference, &projected, &compacted, None)?;
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
                    .decode_revision(0, &vec![b' '; MAX_RECORD_BYTES + 1], payload_limits())
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

        fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
            self.payloads.read(file)
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
        // Negative controls: replaying the current projection, or ignoring missing storage, must be rejected.
        let fresh: fn(State) -> Option<u8> = |state| state.intact.then_some(state.projection);
        let unchecked: fn(State) -> Option<u8> = |state| state.admitted;
        assert!(check(fresh).is_err());
        assert!(check(unchecked).is_err());
    }
}
