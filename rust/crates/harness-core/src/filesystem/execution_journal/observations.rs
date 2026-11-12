use super::*;

#[derive(Serialize)]
struct BorrowedObservation<'a> {
    version: u32,
    operation_id: OperationId,
    turn_operation: OperationId,
    retry_digest: &'a str,
    event: &'a ExecutionEvent,
}

pub(super) fn decode_observation(bytes: &[u8], io: Option<&PreparationIo>) -> Result<Observation> {
    acyclic_auth::format::header(
        bytes,
        b"{\"version\":",
        b"5,",
        || {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "unsupported execution observation format",
            )
        },
        || {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "unsupported execution observation version",
            )
        },
    )?;
    crate::filesystem::decode_record(bytes, io)
}

fn encode_observation(
    value: &BorrowedObservation<'_>,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    if let Some(io) = io {
        return io.encode(value, acyclic_stream::MAX_RECORD_BYTES as u64, false);
    }
    let encoded = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
    if encoded.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Invalid(
            "execution journal observation exceeds Stream limit".into(),
        ));
    }
    Ok(encoded.into())
}

#[cfg(any(test, feature = "test-support"))]
pub(super) mod codec_fuzz {
    use super::*;

    fn encode(value: &Observation) -> Result<acyclic_stream::OwnedBytes> {
        encode_observation(
            &BorrowedObservation {
                version: value.version,
                operation_id: value.operation_id,
                turn_operation: value.turn_operation,
                retry_digest: &value.retry_digest,
                event: &value.event,
            },
            None,
        )
    }

    pub(in crate::filesystem::execution_journal) fn check(bytes: &[u8]) {
        let ordinary = decode_observation(bytes, None);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .expect("unbounded decoder work");
            let baseline = work.live_allocation_bytes();
            {
                let io = PreparationIo::from_native_work(&work).expect("original work is admitted");
                let funded = decode_observation(bytes, Some(&io));
                assert_eq!(
                    funded.as_ref().map(|_| ()).map_err(Error::code),
                    ordinary.as_ref().map(|_| ()).map_err(Error::code)
                );
                if let (Ok(funded), Ok(ordinary)) = (&funded, &ordinary) {
                    assert_eq!(encode(funded).unwrap(), encode(ordinary).unwrap());
                }
            }
            assert_eq!(
                work.live_allocation_bytes(),
                baseline,
                "decoder scratch retires on admission and refusal"
            );
        }
        if let Ok(value) = ordinary {
            let encoded = encode(&value).expect("admitted observation encodes");
            let restored = decode_observation(&encoded, None).expect("writer output decodes");
            assert_eq!(encode(&restored).unwrap(), encoded);
            let version = b"{\"version\":".len();
            for unsupported in [b'0', b'1', b'2', b'3', b'4', u8::MAX] {
                let mut header = encoded[..version + 2].to_vec();
                header[version] = unsupported;
                assert_eq!(
                    decode_observation(&header, None).err().unwrap().code(),
                    crate::wire::ErrorCode::Unsupported
                );
            }
            assert_ne!(
                decode_observation(&encoded[..version + 2], None)
                    .err()
                    .unwrap()
                    .code(),
                crate::wire::ErrorCode::Unsupported
            );
        }
    }

    pub(in crate::filesystem::execution_journal) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>>
    {
        let event = ExecutionEvent::Started {
            request_digest: [3; 32],
        };
        let operation_id = OperationId::from_bytes([1; 16]);
        let retry_digest =
            super::super::content::operation_digest(operation_id, "captured-retry", None)?
                .to_hex()
                .to_string();
        let value = BorrowedObservation {
            version: OBSERVATION_VERSION,
            operation_id,
            turn_operation: OperationId::from_bytes([2; 16]),
            retry_digest: &retry_digest,
            event: &event,
        };
        let bytes = encode_observation(&value, None)?;
        decode_observation(&bytes, None)?;
        Ok(vec![("execution-observation", bytes.to_vec())])
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn refresh_cached_summary(
        &self,
        operation: OperationId,
        cache: &mut SummaryCache,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        cache.bind(operation, io)?;
        if let Some(recovery) = cache.summary().recovery {
            let path = self.index_path(operation, "head", io)?;
            let mut history = io.map(PreparationIo::history_io);
            let latest = crate::store::operations::tail_optional_with_work(
                &self.stream,
                &path,
                &mut history
                    .as_mut()
                    .map(|io| io as &mut dyn crate::store::HistoryIo),
            )
            .await?
            .unwrap_or(0);
            if latest != recovery.head_tail {
                cache.reset(operation, io)?;
            }
        }
        let mut edit = cache.edit();
        let (summary, retained_io) = edit.parts();
        if summary.tail == 0
            && let Some((state, head_tail)) =
                self.load_index_checkpoint(operation, retained_io).await?
        {
            summary.install_checkpoint(state, head_tail);
        }
        self.refresh_summary(operation, summary, retained_io)
            .await?;
        edit.finish();
        Ok(())
    }

    pub(super) async fn refresh_summary(
        &self,
        operation: OperationId,
        summary: &mut ExecutionSummary,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if summary.operation != Some(operation) {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "execution summary is not bound to its operation",
            ));
        }
        let limits = Limits::default();
        loop {
            let before = io.map(PreparationIo::owned_bytes).transpose()?;
            let (page_hold, page) = {
                let page = match io {
                    #[cfg(not(target_arch = "wasm32"))]
                    Some(io) if io.original_native_work().is_some() => {
                        let work = io.original_native_work().ok_or_else(|| {
                            io.invalid("execution replay requires original native work")
                        })?;
                        acyclic_fs::NativeFutureBundle::new(work, || {
                            self.replay_page(
                                operation,
                                summary.tail,
                                EXECUTION_REPLAY_PAGE_RECORDS,
                                true,
                                Some(io),
                            )
                        })?
                        .await?
                    }
                    Some(io) => {
                        Box::pin(self.replay_page(
                            operation,
                            summary.tail,
                            EXECUTION_REPLAY_PAGE_RECORDS,
                            true,
                            Some(io),
                        ))
                        .await?
                    }
                    None => {
                        self.replay_page(
                            operation,
                            summary.tail,
                            EXECUTION_REPLAY_PAGE_RECORDS,
                            true,
                            None,
                        )
                        .await?
                    }
                };
                let page_hold = match (io, before) {
                    (Some(io), Some(before)) => Some(io.guard_owned(io.owned_since(before)?)?),
                    _ => None,
                };
                (page_hold, page)
            };
            if page.is_empty() {
                return Ok(());
            }
            for (record, model) in page {
                if summary.recovery.is_some_and(|recovery| recovery.indexed) {
                    let retry = crate::model::hash_bytes(record.idempotency_key.as_bytes(), io)?;
                    self.preload_summary(summary, retry, &record.event, io)
                        .await?;
                }
                summary.accept(&record, model.as_ref(), limits, io)?;
            }
            drop(page_hold);
        }
    }

    pub(super) fn append_contributed_if_tail<'a>(
        &'a self,
        operation: OperationId,
        expected_tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        contributions: &'a [indexed_publication::Contribution<'a>],
        confirmation: Option<&'a mut Option<acyclic_stream::CommitId>>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.append_projected(
            operation,
            Some(expected_tail),
            key,
            event,
            contributions,
            confirmation,
            io,
            |receipt| Ok(receipt.accepted),
        )
    }

    pub(super) fn append_projected<'a, T: 'a>(
        &'a self,
        operation: OperationId,
        expected_tail: Option<u64>,
        key: &'a str,
        event: &'a ExecutionEvent,
        contributions: &'a [indexed_publication::Contribution<'a>],
        confirmation: Option<&'a mut Option<acyclic_stream::CommitId>>,
        io: Option<&'a PreparationIo>,
        project: impl FnOnce(AppendObservation) -> Result<T> + acyclic_stream::ProviderTask + 'a,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, T>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let local = PreparationIo::from_native_work(work)?;
                let result = self
                    .append_contributed(
                        operation,
                        expected_tail,
                        key,
                        event,
                        contributions,
                        confirmation,
                        Some(&local),
                    )
                    .await;
                let retained = local.owned_bytes()?;
                drop(local.into_native_custody(retained)?);
                return result.and_then(project);
            }
            let span = match expected_tail {
                Some(rev) => {
                    crate::obs::obs_span!("acyclic.harness.journal.append_if_tail", rev = rev)
                }
                None => crate::obs::obs_span!("acyclic.harness.journal.append"),
            };
            crate::obs::traced_future(
                span,
                self.append_contributed(
                    operation,
                    expected_tail,
                    key,
                    event,
                    contributions,
                    confirmation,
                    io,
                ),
            )
            .await
            .and_then(project)
        })
    }

    async fn confirm_observation_record(
        &self,
        operation: OperationId,
        sequence: u64,
        expected: &acyclic_stream::OwnedBytes,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::CommitId> {
        let path = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(operation, work)
        })?;
        let path_hold = io
            .map(|io| io.guard_owned(path.as_str().len() as u64))
            .transpose()?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let position = sequence
            .checked_sub(1)
            .ok_or(Error::Indeterminate(operation))?;
        let record = crate::store::operations::one_record(&self.stream, &path, position, &mut work)
            .await?
            .ok_or(Error::Indeterminate(operation))?;
        let record_hold = io
            .map(|io| {
                io.guard_owned(crate::store::operations::sdk_record_retained_bytes(
                    &record, &work,
                ))
            })
            .transpose()?;
        if record.sequence != position || record.value != *expected {
            return Err(Error::Indeterminate(operation));
        }
        let commit = record.commit_id;
        drop(record);
        drop(record_hold);
        drop(path);
        drop(path_hold);
        Ok(commit)
    }

    pub(super) async fn append_contributed(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        claim_id: &str,
        event: &ExecutionEvent,
        contributions: &[indexed_publication::Contribution<'_>],
        confirmation: Option<&mut Option<acyclic_stream::CommitId>>,
        io: Option<&PreparationIo>,
    ) -> Result<AppendObservation> {
        self.append_observation_group(
            operation_id,
            expected_tail,
            &[ObservationEntry {
                key: claim_id,
                event,
                model: None,
            }],
            contributions,
            confirmation,
            io,
        )
        .await
    }

    pub(super) async fn append_observation_group(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        entries: &[ObservationEntry<'_>],
        contributions: &[indexed_publication::Contribution<'_>],
        mut confirmation: Option<&mut Option<acyclic_stream::CommitId>>,
        io: Option<&PreparationIo>,
    ) -> Result<AppendObservation> {
        use std::borrow::Cow;
        let scope = crate::filesystem::PreparationScope::new(io)?;
        let io = scope.io();
        self.require_operation(operation_id, io)?;
        if entries.is_empty() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "empty observation group",
            ));
        }
        let identity = indexed_publication::contribution_identity(contributions, io)?;
        if !contributions.is_empty() && (expected_tail.is_none() || entries.len() != 1) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "contributed observations require one original conditional cut",
            ));
        }
        let mut encoded = crate::model::vec_with_capacity(entries.len(), io)?;
        for entry in entries {
            let bound = match entry.event {
                ExecutionEvent::ConversationItem { publication, .. }
                | ExecutionEvent::ToolStarted { publication, .. } => *publication == identity,
                _ => identity.is_none(),
            };
            if !bound || entry.key.is_empty() {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution publication differs from its original contribution binding",
                ));
            }
            if let ExecutionEvent::ModelInline { event, .. } = entry.event {
                self.authorize_inline_event(event, VolumeOperation::Write, io)?;
            }
            let model = match entry.model {
                Some(model) => Some(Cow::Borrowed(model)),
                None => self
                    .verify_event_refs_with_io(operation_id, entry.event, io)
                    .await?
                    .map(Cow::Owned),
            };
            self.verify_canonical_publication_with_io(entry.event, Limits::default(), io)
                .await?;
            let digest = super::content::operation_digest(operation_id, entry.key, io)?;
            let retry_digest = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("{}", digest.to_hex()), work)
            })?;
            let observation = BorrowedObservation {
                version: OBSERVATION_VERSION,
                operation_id,
                turn_operation: self.turn_operation(operation_id),
                retry_digest: &retry_digest,
                event: entry.event,
            };
            let bytes = encode_observation(&observation, io)?;
            let retry_key = crate::model::hash_bytes(retry_digest.as_bytes(), io)?;
            let event_digest = crate::executor::with_history_io(io, |work| {
                crate::executor::json::canonical_json_digest(
                    entry.event,
                    Some(if io.is_some() {
                        acyclic_stream::MAX_RECORD_BYTES as u64
                    } else {
                        u64::MAX
                    }),
                    work,
                )
            })?;
            encoded.push(EncodedObservation {
                retry_key,
                event_digest,
                bytes,
                event: entry.event,
                model,
            });
        }
        let mut offset = 0;
        let mut receipt = AppendObservation::refused();
        while offset < encoded.len() {
            // Leave space for the checkpoint and at least one index page. This
            // chunks protocol frames; it never caps the call or its queue.
            let mut end = offset;
            let mut bytes = 0;
            while end < encoded.len() && end - offset < acyclic_stream::MAX_ITEMS {
                let next = bytes + encoded[end].bytes.len() + 16;
                if end > offset && next > acyclic_stream::MAX_COMMAND_BYTES / 2 {
                    break;
                }
                bytes = next;
                end += 1;
            }
            receipt = self
                .append_encoded_group(
                    operation_id,
                    expected_tail,
                    &encoded[offset..end],
                    contributions,
                    confirmation.as_deref_mut(),
                    io,
                )
                .await?;
            if !receipt.accepted {
                break;
            }
            offset = end;
        }
        Ok(receipt)
    }

    async fn append_encoded_group(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        encoded: &[EncodedObservation<'_>],
        contributions: &[indexed_publication::Contribution<'_>],
        mut confirmation: Option<&mut Option<acyclic_stream::CommitId>>,
        io: Option<&PreparationIo>,
    ) -> Result<AppendObservation> {
        let limits = Limits::default();
        let mut cache = self.verified.lock().await;
        loop {
            self.refresh_cached_summary(operation_id, &mut cache, io)
                .await?;
            if expected_tail.is_some_and(|tail| tail != cache.summary().tail) {
                // Exact retries are checked before deciding a stale tail below.
                let first = &encoded[0];
                let mut edit = cache.edit();
                let (summary, retained_io) = edit.parts();
                self.preload_summary(summary, first.retry_key, first.event, retained_io)
                    .await?;
                let previous = summary.retries.get(&first.retry_key, io)?;
                if previous.is_some_and(|(_, digest)| *digest != first.event_digest) {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "execution journal retry identity reused",
                    ));
                }
                let accepted = previous
                    .filter(|(sequence, _)| {
                        expected_tail.and_then(|tail| tail.checked_add(1)) == Some(*sequence)
                    })
                    .map(|(sequence, _)| *sequence);
                edit.finish();
                let Some(sequence) = accepted else {
                    return Ok(AppendObservation::refused());
                };
                if let Some(confirmation) = confirmation.as_deref_mut() {
                    *confirmation = Some(
                        self.confirm_observation_record(operation_id, sequence, &first.bytes, io)
                            .await?,
                    );
                }
                return Ok(AppendObservation {
                    accepted: true,
                    #[cfg(feature = "research")]
                    sequence: Some(sequence),
                });
            }
            let mut edit = cache.edit();
            let (summary, retained_io) = edit.parts();
            let mut records = crate::model::vec_with_capacity(encoded.len(), io)?;
            let mut last_new = None;
            let mut sequence = 0;
            for (index, entry) in encoded.iter().enumerate() {
                self.preload_summary(summary, entry.retry_key, entry.event, retained_io)
                    .await?;
                if let Some((original, digest)) = summary.retries.get(&entry.retry_key, io)? {
                    if *digest != entry.event_digest {
                        return Err(stock_boundary(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "execution journal retry identity reused",
                        ));
                    }
                    sequence = *original;
                    if last_new.is_some() {
                        return Err(stock_boundary(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "observation group repeats an already published suffix",
                        ));
                    }
                    continue;
                }
                summary.validate_next(entry.event, entry.model.as_deref(), limits, io)?;
                sequence = summary.tail.checked_add(1).ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "execution journal sequence overflows",
                    )
                })?;
                summary.accept_verified(
                    (sequence, entry.retry_key, entry.event_digest),
                    entry.event,
                    entry.model.as_deref(),
                    limits,
                    retained_io,
                )?;
                records.push(entry.bytes.clone());
                last_new = Some(index);
            }
            let Some(last) = last_new else {
                edit.finish();
                if let Some(confirmation) = confirmation.as_deref_mut() {
                    let last = encoded.last().ok_or_else(|| {
                        stock_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "observation group lost its final record",
                        )
                    })?;
                    *confirmation = Some(
                        self.confirm_observation_record(operation_id, sequence, &last.bytes, io)
                            .await?,
                    );
                }
                return Ok(AppendObservation {
                    accepted: true,
                    #[cfg(feature = "research")]
                    sequence: Some(sequence),
                });
            };
            let last = &encoded[last];
            let committed = self
                .publish_contributed_observations(
                    summary,
                    last.retry_key,
                    last.event,
                    &records,
                    contributions,
                    retained_io,
                )
                .await?;
            if let Some((recovery, commit_id)) = committed {
                #[cfg(any(test, feature = "test-support"))]
                tracing::trace!(target: "acyclic_harness::journal_work",
                    journal_appends = records.len() as u64, journal_publications = 1_u64,
                    "execution journal observation group accepted");
                summary.recovery = Some(recovery);
                summary.pending_steps.clear();
                edit.finish();
                if let Some(confirmation) = confirmation.as_deref_mut() {
                    *confirmation = Some(commit_id);
                }
                return Ok(AppendObservation {
                    accepted: true,
                    #[cfg(feature = "research")]
                    sequence: Some(sequence),
                });
            }
            drop(edit);
            if !contributions.is_empty() {
                return Ok(AppendObservation::refused());
            }
        }
    }
}

pub(super) struct ObservationEntry<'a> {
    pub(super) key: &'a str,
    pub(super) event: &'a ExecutionEvent,
    pub(super) model: Option<&'a crate::model::ModelEvent>,
}

struct EncodedObservation<'a> {
    retry_key: [u8; 32],
    event_digest: [u8; 32],
    bytes: acyclic_stream::OwnedBytes,
    event: &'a ExecutionEvent,
    model: Option<std::borrow::Cow<'a, crate::model::ModelEvent>>,
}

use crate::executor::stock_boundary;

pub(super) struct AppendObservation {
    pub(super) accepted: bool,
    #[cfg(feature = "research")]
    pub(super) sequence: Option<u64>,
}

impl AppendObservation {
    fn refused() -> Self {
        Self {
            accepted: false,
            #[cfg(feature = "research")]
            sequence: None,
        }
    }
}
