use super::*;

use crate::{executor::stock_boundary, wire::ErrorCode};

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

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for StoredContextRevision {
    fn maximum_slot_bytes() -> u64 {
        (<Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes())
            .max(std::mem::size_of::<Self>() as u64)
    }
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
        if maximum_page_records == 0 {
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
        let digest = crate::executor::json::canonical_json_digest(
            &(
                "harness:context-payload:v1",
                self.path.as_str(),
                &self.source,
                &self.source_revision,
                revision,
                key,
            ),
            u64::MAX,
            &mut None,
        )?;
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
            let source = self
                .read_revision(expected_revision, self.limits, None)
                .await?;
            validate_compaction(reference, &source.context, &context, None)?;
        }
        let (record, encoded) = self
            .stage_revision(revision, context, compaction, &key_bytes)
            .await?;
        self.commit(expected_revision, vec![encoded], key).await?;
        Ok(record)
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
        validate_compaction(&reference, &source, &compacted, None)?;
        validate_context_refs(&source, self.content_verifier.as_ref(), self.limits, None).await?;
        validate_context_refs(
            &compacted,
            self.content_verifier.as_ref(),
            self.limits,
            None,
        )
        .await?;
        let (source_record, source_bytes) = self
            .stage_revision(source_revision, source, None, &key_bytes)
            .await?;
        let (compacted_record, compacted_bytes) = self
            .stage_revision(compacted_revision, compacted, Some(reference), &key_bytes)
            .await?;
        self.commit(expected_revision, vec![source_bytes, compacted_bytes], key)
            .await?;
        Ok([source_record, compacted_record])
    }

    /// Captures the current continuing projection and its immutable revision.
    pub async fn latest_revision(&self) -> Result<Option<ContextRevision>> {
        let tail = self.tail_revision(None).await?;
        if tail == 0 {
            return Ok(None);
        }
        self.revision(tail).await.map(Some)
    }

    /// Returns the captured latest context.
    pub async fn latest(&self) -> Result<Context> {
        Ok(self
            .latest_revision()
            .await?
            .map_or_else(Context::default, |record| record.context))
    }

    /// Captures an immutable read boundary; later appends do not extend this revision.
    pub async fn tail_revision(&self, io: Option<&crate::executor::PreparationIo>) -> Result<u64> {
        crate::executor::with_history_io(io, |work| {
            work.as_deref_mut().map_or(Ok(()), |work| work.begin_call())
        })?;
        match self.provider.tail(self.path.clone()).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(context_stream_error(error, io)),
        }
    }

    /// Reads a bounded archival page through a captured revision.
    pub async fn revisions(&self, after: u64, through: u64) -> Result<Vec<ContextRevision>> {
        if after > through || through > self.tail_revision(None).await? {
            return Err(crate::Error::Invalid(
                "context page cursor is invalid".into(),
            ));
        }
        let count = (through - after).min(u64::from(self.maximum_page_records));
        let mut page = Vec::new();
        for offset in 0..count {
            page.push(self.revision(after + offset + 1).await?);
        }
        Ok(page)
    }

    async fn decode_revision(
        &self,
        sequence: u64,
        bytes: &[u8],
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ContextRevision> {
        let invalid = |reason| stock_boundary(io, ErrorCode::Storage, reason);
        let limits = restrict_context_limits(self.limits, limits, io)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("context record exceeds Stream limit"));
        }
        let revision: StoredContextRevision = decode_context_json(
            bytes,
            MAX_RECORD_BYTES as u64,
            io,
            "durable context identity, revision or canonical bytes are invalid",
        )?;
        let next = sequence
            .checked_add(1)
            .ok_or_else(|| stock_boundary(io, ErrorCode::Invalid, "revision exhausted"))?;
        if revision.format_version != 5
            || revision.revision != next
            || revision.source != self.source
            || revision.source_revision != self.source_revision
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Storage,
                "durable context identity, revision or canonical bytes are invalid",
            ));
        }
        crate::executor::with_history_io(io, |work| limits.validate_file(&revision.context, work))?;
        if revision.context.descriptor().media_type() != "application/json"
            || revision.context.descriptor().byte_length() > limits.render_bytes
        {
            return Err(invalid("context payload descriptor is invalid"));
        }
        verify_file(&revision.context, self.content_verifier.as_ref(), io).await?;
        if !self
            .content_verifier
            .supports_read(revision.context.volume(), io)?
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Unsupported,
                "context reader has no bounded capability",
            ));
        }
        let payload = self
            .content_verifier
            .read(
                &revision.context,
                revision.context.descriptor().byte_length(),
                io,
            )?
            .await?;
        #[cfg(feature = "filesystem")]
        let _payload_hold = io.map(|io| io.guard_bytes(&payload)).transpose()?;
        crate::executor::with_history_io(io, |work| {
            revision.context.descriptor().verify(&payload, work)
        })?;
        let context = decode_context_json(
            &payload,
            limits.render_bytes,
            io,
            "context payload is not canonical",
        )?;
        validate_context_refs(&context, self.content_verifier.as_ref(), limits, io).await?;
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

    /// Loads one pinned revision, checking its source proof with at most two record reads.
    pub async fn revision(&self, revision: u64) -> Result<ContextRevision> {
        self.revision_bounded(revision, self.limits, None).await
    }

    async fn revision_bounded(
        &self,
        revision: u64,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ContextRevision> {
        let record = self.read_revision(revision, limits, io).await?;
        if let Some(reference) = &record.compaction {
            #[cfg(feature = "filesystem")]
            let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
            #[cfg(feature = "filesystem")]
            let source_hold;
            let source = self.read_revision(revision - 1, limits, io).await?;
            #[cfg(feature = "filesystem")]
            {
                source_hold = io
                    .map(|io| io.guard_owned(io.owned_bytes()? - before))
                    .transpose()?;
            }
            validate_compaction(reference, &source.context, &record.context, io)?;
            drop(source);
            #[cfg(feature = "filesystem")]
            drop(source_hold);
        }
        Ok(record)
    }

    /// Compacts a context on the optional original preparation ledger.
    pub fn compact(
        context: &Context,
        max_messages: usize,
        summary: Option<ContextSummary>,
        retention: CompactionRetention,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Context, CompactionReference)> {
        let invalid = |reason| stock_boundary(io, ErrorCode::Invalid, reason);
        retention.validate(io)?;
        context.validate_current_input(io)?;
        if max_messages == 0 {
            return Err(invalid("compaction max_messages must be positive"));
        }
        let maximum_messages = u32::try_from(max_messages)
            .map_err(|_| invalid("compaction budget exceeds portable count"))?;
        let source_digest = crate::executor::preparation_digest(context, io)?;
        if let Some(summary) = &summary {
            let covered = context
                .messages
                .get(..summary.source_messages as usize)
                .ok_or_else(|| invalid("summary extent exceeds source projection"))?;
            let summarized = BorrowedContext {
                messages: ContextMessages {
                    first: covered,
                    user: None,
                    second: &[],
                },
                metadata: &context.metadata,
                current_input_index: context
                    .current_input_index
                    .filter(|index| *index < summary.source_messages),
            };
            if summary.source_messages == 0
                || summary.source_digest != crate::executor::preparation_digest(&summarized, io)?
                || summary.output.descriptor().media_type() != "text/plain"
            {
                return Err(invalid("summary does not bind its source prefix"));
            }
        }
        let (compacted, _) = crate::executor::with_history_io(io, |work| {
            if context.messages.len() > max_messages
                || (summary.is_some() && context.messages.len() == max_messages)
            {
                let summary = summary.as_ref().ok_or_else(|| {
                    invalid(
                        "compaction requires a durable summary; context cannot be silently dropped",
                    )
                })?;
                compaction::compact_context(context, max_messages, summary, &retention, work)
            } else {
                compaction::clone_context(context, None, work)
            }
        })?;
        Ok((
            compacted,
            CompactionReference {
                source_digest,
                maximum_messages,
                summary,
                retention,
            },
        ))
    }

    async fn read_revision(
        &self,
        revision: u64,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ContextRevision> {
        let limits = restrict_context_limits(self.limits, limits, io)?;
        let from = revision.checked_sub(1).ok_or_else(|| {
            stock_boundary(io, ErrorCode::Invalid, "context revision must be positive")
        })?;
        crate::executor::with_history_io(io, |work| {
            work.as_deref_mut().map_or(Ok(()), |work| work.begin_call())
        })?;
        let mut stream = self
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from,
                limit: 1,
            })
            .await
            .map_err(|error| context_stream_error(error, io))?;
        let missing = || stock_boundary(io, ErrorCode::Storage, "context revision is missing");
        let record = stream
            .next()
            .await
            .ok_or_else(missing)?
            .map_err(|error| context_stream_error(error, io))?;
        crate::executor::with_history_io(io, |work| {
            work.as_deref_mut().map_or(Ok(()), |work| {
                work.receive_record(record.value.len() as u64)
            })
        })?;
        #[cfg(feature = "filesystem")]
        let _record_hold = io
            .map(|io| io.guard_owned(record.value.len() as u64))
            .transpose()?;
        if let Some(extra) = stream.next().await {
            let extra = extra.map_err(|error| context_stream_error(error, io))?;
            crate::executor::with_history_io(io, |work| {
                work.as_deref_mut()
                    .map_or(Ok(()), |work| work.receive_record(extra.value.len() as u64))
            })?;
            #[cfg(feature = "filesystem")]
            let _extra_hold = io
                .map(|io| io.guard_owned(extra.value.len() as u64))
                .transpose()?;
            return Err(stock_boundary(
                io,
                ErrorCode::Storage,
                "context revision read returned an invalid range",
            ));
        }
        if record.sequence != from {
            return Err(stock_boundary(
                io,
                ErrorCode::Storage,
                "context revision read returned an invalid range",
            ));
        }
        self.decode_revision(record.sequence, &record.value, limits, io)
            .await
    }

    async fn commit(
        &self,
        expected: u64,
        records: Vec<Bytes>,
        key: StreamIdempotencyKey,
    ) -> Result<()> {
        let end = expected
            .checked_add(records.len() as u64)
            .ok_or_else(|| crate::Error::Invalid("revision exhausted".into()))?;
        match self
            .provider
            .append(AppendRequest {
                path: self.path.clone(),
                records: records.into_iter().map(Into::into).collect(),
                if_tail: Some(expected),
                idempotency_key: Some(key),
            })
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?
        {
            AppendOutcome::Committed(receipt)
                if receipt.start == expected && receipt.end == end && receipt.tail == end =>
            {
                Ok(())
            }
            AppendOutcome::Committed(_) => Err(crate::Error::Storage(
                "context append returned an invalid tail".into(),
            )),
            AppendOutcome::TailConflict { actual_tail } => Err(crate::Error::Conflict(format!(
                "context revision {expected} is stale; actual revision is {actual_tail}"
            ))),
        }
    }
}

fn context_stream_error(
    source: StreamError,
    io: Option<&crate::executor::PreparationIo>,
) -> crate::Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io
        .and_then(crate::executor::PreparationIo::original_native_work)
        .is_some()
    {
        return crate::Error::NativeStream {
            code: ErrorCode::Storage,
            source,
            custody: None,
        };
    }
    let _ = io;
    crate::Error::Storage(source.to_string())
}

fn decode_context_json<T: crate::executor::PreparationRecord + serde::Serialize>(
    bytes: &[u8],
    maximum: u64,
    io: Option<&crate::executor::PreparationIo>,
    reason: &'static str,
) -> Result<T> {
    let value = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData::<T>,
            bytes,
            ErrorCode::Storage,
        )?),
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
    if !canonical {
        return Err(stock_boundary(io, ErrorCode::Storage, reason));
    }
    Ok(value)
}

impl ContextSource for DurableContextProvider {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            crate::executor::with_history_io(io, |work| limits.validate(work))?;
            let tail = self.tail_revision(io).await?;
            if tail == 0 {
                return Ok(Vec::new());
            }
            Ok(self
                .revision_bounded(tail, limits, io)
                .await?
                .context
                .messages)
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        true
    }
}
