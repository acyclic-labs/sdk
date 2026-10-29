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

    pub(super) async fn append_validated(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        claim_id: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        // Appending returns no caller-owned payload. Validation decoders and
        // their scratch must retire on success, refusal and cancellation;
        // authenticated caches retain their own independent ownership pools.
        let scope = crate::filesystem::PreparationScope::new(io)?;
        let io = scope.io();
        self.require_operation(operation_id, io)?;
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
        let model = self.verify_event_refs_with_io(event, io).await?;
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
                if io.is_some() {
                    acyclic_stream::MAX_RECORD_BYTES as u64
                } else {
                    u64::MAX
                },
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
                return Ok(expected_tail.is_none_or(|tail| tail.checked_add(1) == Some(*sequence)));
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
                .publish_indexed_observation(summary, retry_key, event, &bytes, retained_io)
                .await?;
            if let Some(recovery) = committed {
                #[cfg(feature = "test-support")]
                tracing::trace!(target: "acyclic_harness::journal_work",
                    journal_appends = 1_u64, journal_append_bytes = bytes.len() as u64,
                    "execution journal observation accepted");
                summary.recovery = Some(recovery);
                summary.pending_steps.clear();
                edit.finish();
                drop(bytes);
                drop(body_hold);
                drop(retry_digest);
                drop(retry_hold);
                return Ok(true);
            }
            drop(edit);
        }
    }
}

use crate::executor::stock_boundary;
