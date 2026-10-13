use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn refresh_summary(
        &self,
        operation: OperationId,
        summary: &mut ExecutionSummary,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if summary.operation != Some(operation) {
            *summary = ExecutionSummary {
                operation: Some(operation),
                ..ExecutionSummary::default()
            };
        }
        let limits = self
            .owner
            .as_ref()
            .map_or_else(Limits::default, |(owner, _)| owner.input_limits());
        loop {
            let before = io.map(PreparationIo::owned_bytes).transpose()?;
            let page_hold;
            let page = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) if io.original_native_work().is_some() => {
                    let work = io
                        .original_native_work()
                        .expect("matched original native work");
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
            page_hold = match (io, before) {
                (Some(io), Some(before)) => Some(io.guard_owned(
                    io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "execution page custody underflow",
                        )
                    })?,
                )?),
                _ => None,
            };
            if page.is_empty() {
                return Ok(());
            }
            for (record, model) in page {
                summary.accept(&record, model.as_ref(), limits, io)?;
            }
            drop(page_hold);
        }
    }

    /// Checks settled dispatches under their original owner and optional IO.
    pub async fn is_quiescent(&self, io: Option<&PreparationIo>) -> Result<bool> {
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "quiescence requires a task-owned execution journal",
            )
        })?;
        self.require_operation(*operation, io)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo);
        owner.verify(true, &mut work).await?;
        let quiescent = {
            let mut shared = if io.is_none() {
                Some(self.verified.lock().await)
            } else {
                None
            };
            let summary_hold;
            let mut transient = ExecutionSummary::default();
            let summary = shared.as_deref_mut().unwrap_or(&mut transient);
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            self.refresh_summary(*operation, summary, io).await?;
            summary_hold = io
                .map(|io| {
                    io.guard_owned(
                        io.owned_bytes()?
                            .checked_sub(before)
                            .ok_or_else(|| io.invalid("execution summary custody underflow"))?,
                    )
                })
                .transpose()?;
            let quiescent = summary.quiescent(io)?;
            drop(transient);
            drop(summary_hold);
            quiescent
        };
        owner.verify(true, &mut work).await?;
        Ok(quiescent)
    }

    /// Releases only after both configured original journals prove passive workflow state and settled dispatches.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn release_if_quiescent_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
        workflow: &crate::filesystem::FilesystemWorkflowJournal<P, A, O>,
    ) -> Result<bool> {
        let io = PreparationIo::from_native_work(work)?;
        let (owner, operation) = self.owner.as_ref().ok_or_else(|| {
            Error::Unsupported("quiescence requires a task-owned execution journal".into())
        })?;
        workflow.require_execution_owner(owner, &self.volume, self.verifier.audience())?;
        let result: Result<_> = async {
            let mut borrowed = io.history_io();
            owner.verify(true, &mut Some(&mut borrowed)).await?;
            let tail = {
                let before = io.owned_bytes()?;
                let summary_hold;
                let mut summary = ExecutionSummary::default();
                self.refresh_summary(*operation, &mut summary, Some(&io))
                    .await?;
                summary_hold =
                    io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        Error::Invalid("execution summary custody underflow".into())
                    })?)?;
                if !summary.quiescent(Some(&io))? {
                    return Ok(None);
                }
                let tail = summary.tail;
                drop(summary);
                drop(summary_hold);
                tail
            };
            Ok(workflow
                .prove_release_with_io(&io)
                .await?
                .map(|pending| (tail, pending)))
        }
        .await;
        let Some((tail, pending)) = result? else {
            return Ok(false);
        };
        owner
            .release_quiescent_execution(*operation, tail, &pending, &mut Some(&mut io.history_io()))
            .await
    }

    /// Suspends only after the task-bound journal proves every dispatch settled.
    pub async fn suspend_workflow_command_if_quiescent(
        &self,
        revision: u64,
        command: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let span = obs_span!("acyclic.harness.journal.suspend", rev = revision);
        traced(span, async {
            let mut shared = if io.is_none() {
                Some(self.verified.lock().await)
            } else {
                None
            };
            let mut transient = ExecutionSummary::default();
            let summary = shared.as_deref_mut().unwrap_or(&mut transient);
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let result = async {
                let (owner, operation) = self.owner.as_ref().ok_or_else(|| {
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.is_some_and(|io| io.original_native_work().is_some()) {
                        return Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "quiescence requires a task-owned execution journal",
                        };
                    }
                    Error::Unsupported("quiescence requires a task-owned execution journal".into())
                })?;
                self.refresh_summary(*operation, summary, io).await?;
                if !summary.quiescent(io)? {
                    return Err(Error::Indeterminate(*operation));
                }
                let mut borrowed = io.map(PreparationIo::history_io);
                owner
                    .suspend_quiescent_execution(
                        *operation,
                        summary.tail,
                        revision,
                        command,
                        &mut borrowed
                            .as_mut()
                            .map(|io| io as &mut dyn crate::store::history::HistoryIo),
                    )
                    .await
            }
            .await;
            let summary_hold = io
                .map(|io| {
                    io.guard_owned(
                        io.owned_bytes()?
                            .checked_sub(before)
                            .ok_or_else(|| io.invalid("execution summary custody underflow"))?,
                    )
                })
                .transpose()?;
            drop(transient);
            drop(summary_hold);
            result
        })
        .await
    }

    pub(super) async fn append_validated(
        &self,
        operation_id: OperationId,
        expected_tail: Option<u64>,
        claim_id: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        self.require_operation(operation_id, io)?;
        if claim_id.is_empty() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution journal compare-and-append is invalid",
            ));
        }
        let owner = self.owner.as_ref().map(|(owner, _)| owner);
        let model = self.verify_event_refs_with_io(event, io).await?;
        let limits = owner.map_or_else(Limits::default, TaskJournalOwner::input_limits);
        self.verify_canonical_publication_with_io(operation_id, event, limits, io)
            .await?;
        #[derive(Serialize)]
        struct BorrowedObservation<'a> {
            version: u32,
            operation_id: OperationId,
            turn_operation: OperationId,
            retry_digest: &'a str,
            event: &'a ExecutionEvent,
        }
        let hash_input_bytes = 37_u64.checked_add(claim_id.len() as u64).ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution retry identity length overflows",
            )
        })?;
        let hash_input_hold = io
            .map(|io| io.reserve_decode(hash_input_bytes))
            .transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(hash_input_bytes)?;
            let copying = acyclic_fs::WorkCounters {
                bytes_copied: hash_input_bytes,
                ..Default::default()
            };
            io.preflight(copying)?;
            io.charge(copying)?;
        }
        let mut hash_input = String::with_capacity(hash_input_bytes as usize);
        use std::fmt::Write as _;
        write!(&mut hash_input, "{operation_id}:{claim_id}").map_err(|_| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution retry identity formatting failed",
            )
        })?;
        let hashing = acyclic_fs::WorkCounters {
            bytes_hashed: hash_input.len() as u64,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(hashing)?;
            io.charge(hashing)?;
        }
        let digest = blake3::hash(hash_input.as_bytes());
        drop(hash_input);
        drop(hash_input_hold);
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
        let bytes = match io {
            Some(io) => {
                let encoded =
                    io.encode(&observation, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
                body_hold = io.guard_bytes(&encoded)?;
                encoded
            }
            None => {
                body_hold = None;
                let encoded = serde_json::to_vec(&observation)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                if encoded.len() > acyclic_stream::MAX_RECORD_BYTES {
                    return Err(Error::Invalid(
                        "execution journal observation exceeds Stream limit".into(),
                    ));
                }
                encoded.into()
            }
        };
        let key_hold = io.map(|io| io.reserve_decode(32)).transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(32)?;
            let copying = acyclic_fs::WorkCounters {
                bytes_copied: 32,
                ..Default::default()
            };
            io.preflight(copying)?;
            io.charge(copying)?;
        }
        let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes())).map_err(|error| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() {
                return Error::NativeStream {
                    code: crate::wire::ErrorCode::Invalid,
                    source: error,
                    custody: None,
                };
            }
            Error::Invalid(error.to_string())
        })?;
        let retry_hashing = acyclic_fs::WorkCounters {
            bytes_hashed: 64,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(retry_hashing)?;
            io.charge(retry_hashing)?;
        }
        let retry_key = *blake3::hash(retry_digest.as_bytes()).as_bytes();
        let event_digest = crate::executor::with_history_io(io, |work|
            crate::executor::json::canonical_json_digest(event,
                if io.is_some() { acyclic_stream::MAX_RECORD_BYTES as u64 } else { u64::MAX }, work))?;
        let mut shared = if io.is_none() {
            Some(self.verified.lock().await)
        } else {
            None
        };
        let mut transient = ExecutionSummary::default();
        let summary = shared.as_deref_mut().unwrap_or(&mut transient);
        loop {
            self.refresh_summary(operation_id, summary, io).await?;
            if let Some(owner) = owner {
                let mut borrowed = io.map(PreparationIo::history_io);
                owner
                    .verify_execution(
                        operation_id,
                        event,
                        &mut borrowed
                            .as_mut()
                            .map(|io| io as &mut dyn crate::store::history::HistoryIo),
                    )
                    .await?;
            }
            if let Some((sequence, previous)) = summary.retries.get(&retry_key) {
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
            let committed = if let Some(owner) = owner {
                let mut borrowed = io.map(PreparationIo::history_io);
                owner
                    .append_execution(
                        operation_id,
                        summary.tail,
                        &key,
                        bytes.clone(),
                        event,
                        &mut borrowed
                            .as_mut()
                            .map(|io| io as &mut dyn crate::store::history::HistoryIo),
                    )
                    .await?
            } else {
                self.append_local_with_io(
                    operation_id,
                    summary.tail,
                    key.clone(),
                    bytes.clone(),
                    &retry_digest,
                    event,
                    io,
                )
                .await?
            };
            if committed {
                drop(key);
                drop(key_hold);
                drop(bytes);
                drop(body_hold);
                drop(retry_digest);
                drop(retry_hold);
                return Ok(true);
            }
        }
    }

    pub(super) async fn append_local_with_io(
        &self,
        operation_id: OperationId,
        tail: u64,
        key: StreamKey,
        bytes: acyclic_stream::OwnedBytes,
        retry_digest: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let path_hold = io
            .map(|io| io.reserve_decode(2 * ("harness/v2/execution/".len() + 36) as u64))
            .transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(("harness/v2/execution/".len() + 36) as u64)?;
            io.allocation_attempt(("harness/v2/execution/".len() + 36) as u64)?;
        }
        let stream = self.path(operation_id)?;
        let appended = match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                crate::distributed::append(
                    (&stream, &self.stream),
                    [bytes],
                    0,
                    Some(tail),
                    key,
                    operation_id,
                    &mut Some(&mut borrowed),
                )
                .await
            }
            None => stream
                .append_batch(vec![bytes], Some(tail), Some(key))
                .await
                .map_err(|error| match error {
                    StreamError::IdempotencyMismatch => {
                        Error::Conflict("execution journal retry identity reused".into())
                    }
                    _ => Error::Indeterminate(operation_id),
                }),
        };
        drop(stream);
        drop(path_hold);
        match appended {
            Ok(AppendOutcome::Committed(receipt))
                if receipt.start == tail && Some(receipt.end) == tail.checked_add(1) =>
            {
                Ok(true)
            }
            Ok(AppendOutcome::Committed(_)) => {
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "execution journal append receipt is invalid",
                    });
                }
                Err(Error::Storage(
                    "execution journal append receipt is invalid".into(),
                ))
            }
            Ok(AppendOutcome::TailConflict { .. }) => Ok(false),
            Err(Error::Indeterminate(_)) => {
                let records = self.replay_page(operation_id, tail, 1, true, io).await?;
                if records.iter().any(|(record, _)| {
                    record.idempotency_key == retry_digest && &record.event == event
                }) {
                    Ok(true)
                } else {
                    Err(Error::Indeterminate(operation_id))
                }
            }
            Err(error @ Error::Conflict(_)) => {
                let records = self.replay_page(operation_id, tail, 1, true, io).await?;
                if records.iter().any(|(record, _)| {
                    record.idempotency_key == retry_digest && &record.event == event
                }) {
                    Ok(true)
                } else {
                    Err(error)
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Err(
                error @ Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    ..
                },
            )
            | Err(
                error @ Error::NativeStream {
                    source: StreamError::IdempotencyMismatch,
                    ..
                },
            ) => {
                let records = self.replay_page(operation_id, tail, 1, true, io).await?;
                if records.iter().any(|(record, _)| {
                    record.idempotency_key == retry_digest && &record.event == event
                }) {
                    Ok(true)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }
}

use crate::executor::{stock_boundary};
