use super::*;

#[derive(Serialize)]
struct BorrowedObservation<'a> {
    version: u32,
    operation_id: OperationId,
    turn_operation: OperationId,
    retry_digest: &'a str,
    event: &'a ExecutionEvent,
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
                        crate::NativeFutureBundle::new(work, || {
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
        if io.is_none() {
            return crate::conversation::BoundedContentFuture::new(None, || async move {
                let span = crate::obs::obs_span!(
                    "acyclic.harness.journal.append_if_tail",
                    rev = expected_tail
                );
                crate::obs::traced_future(
                    span,
                    self.append_contributed(
                        operation,
                        Some(expected_tail),
                        key,
                        event,
                        contributions,
                        confirmation,
                        None,
                    ),
                )
                .await
            });
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let local = PreparationIo::from_native_work(work)?;
                let result = self
                    .append_contributed(
                        operation,
                        Some(expected_tail),
                        key,
                        event,
                        contributions,
                        confirmation,
                        Some(&local),
                    )
                    .await;
                let retained = local.owned_bytes()?;
                drop(local.into_native_custody(retained)?);
                return result;
            }
            let span = crate::obs::obs_span!(
                "acyclic.harness.journal.append_if_tail",
                rev = expected_tail
            );
            crate::obs::traced_future(
                span,
                self.append_contributed(
                    operation,
                    Some(expected_tail),
                    key,
                    event,
                    contributions,
                    confirmation,
                    io,
                ),
            )
            .await
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

    pub(super) async fn append_validated(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        claim_id: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        self.append_contributed(operation_id, expected_tail, claim_id, event, &[], None, io)
            .await
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
    ) -> Result<bool> {
        // Validation decoders and their scratch retire on success, refusal and
        // cancellation; the optional receipt is only a fixed-width identity.
        // authenticated caches retain their own independent ownership pools.
        let scope = crate::filesystem::PreparationScope::new(io)?;
        let io = scope.io();
        self.require_operation(operation_id, io)?;
        let identity = indexed_publication::contribution_identity(contributions, io)?;
        let bound = match event {
            ExecutionEvent::ConversationItem { publication, .. } => *publication == identity,
            _ => identity.is_none(),
        };
        if !bound || (!contributions.is_empty() && expected_tail.is_none()) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution publication differs from its original contribution binding",
            ));
        }
        if claim_id.is_empty() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution journal compare-and-append is invalid",
            ));
        }
        if let ExecutionEvent::ModelInline { event, .. } = event {
            self.authorize_inline_event(event, VolumeOperation::Write, io)?;
        }
        let model = self
            .verify_event_refs_with_io(operation_id, event, io)
            .await?;
        let limits = Limits::default();
        self.verify_canonical_publication_with_io(event, limits, io)
            .await?;
        let digest = super::content::operation_digest(operation_id, claim_id, io)?;
        let retry_hold = io.map(|io| io.reserve_decode(64)).transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(64)?;
            let copying = acyclic_fs::WorkCounters {
                bytes_copied: 64,
                ..Default::default()
            };
            io.preflight(copying)?;
            io.charge(copying)?;
        }
        let retry_digest = digest.to_hex().to_string();
        let observation = BorrowedObservation {
            version: OBSERVATION_VERSION,
            operation_id,
            turn_operation: self.turn_operation(operation_id),
            retry_digest: &retry_digest,
            event,
        };
        let body_hold;
        let bytes = if let Some(io) = io {
            let encoded =
                io.encode(&observation, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
            body_hold = io.guard_bytes(&encoded)?;
            encoded
        } else {
            body_hold = None;
            let encoded = serde_json::to_vec(&observation)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if encoded.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "execution journal observation exceeds Stream limit".into(),
                ));
            }
            encoded.into()
        };
        let retry_hashing = acyclic_fs::WorkCounters {
            bytes_hashed: 64,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(retry_hashing)?;
            io.charge(retry_hashing)?;
        }
        let retry_key = *blake3::hash(retry_digest.as_bytes()).as_bytes();
        let event_digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                event,
                Some(if io.is_some() {
                    acyclic_stream::MAX_RECORD_BYTES as u64
                } else {
                    u64::MAX
                }),
                work,
            )
        })?;
        let mut cache = self.verified.lock().await;
        loop {
            self.refresh_cached_summary(operation_id, &mut cache, io)
                .await?;
            let mut edit = cache.edit();
            let (summary, retained_io) = edit.parts();
            self.preload_summary(summary, retry_key, event, retained_io)
                .await?;
            edit.finish();
            let summary = cache.summary();
            if let Some((sequence, previous)) = summary.retries.get(&retry_key, io)? {
                if *previous != event_digest {
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.is_some() {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Conflict,
                            reason: "execution journal retry identity reused",
                        });
                    }
                    return Err(Error::Conflict(
                        "execution journal retry identity reused".into(),
                    ));
                }
                let accepted =
                    expected_tail.is_none_or(|tail| tail.checked_add(1) == Some(*sequence));
                if accepted && let Some(confirmation) = confirmation {
                    // The signed retry index already located this exact observation.
                    // Only callers consuming its receipt need the original commit;
                    // ordinary retries perform no additional provider read.
                    *confirmation = Some(
                        self.confirm_observation_record(operation_id, *sequence, &bytes, io)
                            .await?,
                    );
                }
                return Ok(accepted);
            }
            if expected_tail.is_some_and(|tail| tail != summary.tail) {
                return Ok(false);
            }
            summary.validate_next(event, model.as_ref(), limits, io)?;
            let tail = summary.tail;
            let sequence = tail.checked_add(1).ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution journal sequence overflows",
                )
            })?;
            // A dispatch claim must not commit until every fallible cache
            // allocation and admission has succeeded. The edit invalidates
            // this speculative state on conflict, error or cancellation.
            let mut edit = cache.edit();
            let (summary, retained_io) = edit.parts();
            summary.accept_verified(
                (sequence, retry_key, event_digest),
                event,
                model.as_ref(),
                limits,
                retained_io,
            )?;
            let committed = self
                .publish_contributed_observation(
                    summary,
                    retry_key,
                    event,
                    &bytes,
                    contributions,
                    retained_io,
                )
                .await?;
            if let Some((recovery, commit_id)) = committed {
                #[cfg(feature = "test-support")]
                tracing::trace!(target: "acyclic_harness::journal_work",
                    journal_appends = 1_u64, journal_append_bytes = bytes.len() as u64,
                    "execution journal observation accepted");
                summary.recovery = Some(recovery);
                summary.pending_steps.clear();
                edit.finish();
                if let Some(confirmation) = confirmation {
                    *confirmation = Some(commit_id);
                }
                drop(bytes);
                drop(body_hold);
                drop(retry_digest);
                drop(retry_hold);
                return Ok(true);
            }
            drop(edit);
            // A contributed fence can lose while the journal tail is unchanged.
            // Its original producer must settle that conditional refusal.
            if !contributions.is_empty() {
                return Ok(false);
            }
        }
    }
}

use crate::executor::stock_boundary;
