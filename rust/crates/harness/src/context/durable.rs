use super::*;

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
pub(super) fn context_stream_error(source: StreamError, io: Option<&crate::executor::PreparationIo>) -> crate::Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
        return crate::Error::NativeStream { code: crate::wire::ErrorCode::Storage, source, custody: None };
    }
    let _ = io;
    crate::Error::Storage(source.to_string())
}
pub(super) fn decode_context_json<T: crate::executor::PreparationRecord + serde::Serialize>(
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
impl ContextSource for DurableContextProvider {
    fn load<'a>(&'a self, _: &'a ContextInput, limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            crate::executor::with_history_io(io, |work| limits.validate(work))?;
            let tail = self.tail_revision(io).await?;
            if tail == 0 { return Ok(Vec::new()); }
            Ok(self.revision_bounded(tail, limits, io).await?.context.messages)
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool { true }
}
