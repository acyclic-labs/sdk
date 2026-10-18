use super::*;

// Keep ordinary append allocation shape intact. Only model builds heap-place
// lazy child futures before polling them on Loom's coroutine stack.
macro_rules! journal_future {
    ($make:expr, $ordinary:expr) => {{
        #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
        {
            super::cache::model_future($make).await
        }
        #[cfg(not(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom)))]
        {
            $ordinary.await
        }
    }};
}

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
            let (page_hold, page, fence) = {
                let page = match io {
                    #[cfg(not(target_arch = "wasm32"))]
                    Some(io) if io.original_native_work().is_some() => {
                        let work = io.original_native_work().ok_or_else(|| {
                            io.invalid("execution replay requires original native work")
                        })?;
                        crate::NativeFutureBundle::new(work, || {
                            #[cfg(all(
                                feature = "model-checking",
                                acyclic_harness_loom,
                                acyclic_loom
                            ))]
                            {
                                let (tail, fence) = (summary.tail, summary.fence);
                                return super::cache::model_future(move || {
                                    self.replay_page_fenced(
                                        operation,
                                        tail,
                                        EXECUTION_REPLAY_PAGE_RECORDS,
                                        true,
                                        fence,
                                        Some(io),
                                    )
                                });
                            }
                            #[cfg(not(all(
                                feature = "model-checking",
                                acyclic_harness_loom,
                                acyclic_loom
                            )))]
                            self.replay_page_fenced(
                                operation,
                                summary.tail,
                                EXECUTION_REPLAY_PAGE_RECORDS,
                                true,
                                summary.fence,
                                Some(io),
                            )
                        })?
                        .await?
                    }
                    Some(io) => {
                        #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                        {
                            let (tail, fence) = (summary.tail, summary.fence);
                            super::cache::model_future(move || {
                                self.replay_page_fenced(
                                    operation,
                                    tail,
                                    EXECUTION_REPLAY_PAGE_RECORDS,
                                    true,
                                    fence,
                                    Some(io),
                                )
                            })
                            .await?
                        }
                        #[cfg(not(all(
                            feature = "model-checking",
                            acyclic_harness_loom,
                            acyclic_loom
                        )))]
                        {
                            Box::pin(self.replay_page_fenced(
                                operation,
                                summary.tail,
                                EXECUTION_REPLAY_PAGE_RECORDS,
                                true,
                                summary.fence,
                                Some(io),
                            ))
                            .await?
                        }
                    }
                    None => {
                        #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                        let (tail, fence) = (summary.tail, summary.fence);
                        journal_future!(
                            move || self.replay_page_fenced(
                                operation,
                                tail,
                                EXECUTION_REPLAY_PAGE_RECORDS,
                                true,
                                fence,
                                None,
                            ),
                            self.replay_page_fenced(
                                operation,
                                summary.tail,
                                EXECUTION_REPLAY_PAGE_RECORDS,
                                true,
                                summary.fence,
                                None,
                            )
                        )?
                    }
                };
                let (page, fence) = page;
                let page_hold = match (io, before) {
                    (Some(io), Some(before)) => Some(io.guard_owned(io.owned_since(before)?)?),
                    _ => None,
                };
                (page_hold, page, fence)
            };
            if page.is_empty() {
                return Ok(());
            }
            summary.accept_page(page, fence, limits, io)?;
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
            let mut transient = ExecutionSummary::default();
            if let Some(shared) = shared.as_deref_mut() {
                shared.bind(*operation, io)?;
            }
            let summary = shared
                .as_deref_mut()
                .map(|cache| &mut cache.summary)
                .unwrap_or(&mut transient);
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            self.refresh_summary(*operation, summary, io).await?;
            let summary_hold = io
                .map(|io| io.guard_owned(io.owned_since(before)?))
                .transpose()?;
            let quiescent = summary.quiescent(io);
            drop(transient);
            drop(summary_hold);
            quiescent?
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
        let result = crate::NativeFutureBundle::new(work, || async {
            let mut borrowed = io.history_io();
            owner.verify(true, &mut Some(&mut borrowed)).await?;
            let tail = {
                let before = io.owned_bytes()?;
                let mut summary = ExecutionSummary::default();
                self.refresh_summary(*operation, &mut summary, Some(&io))
                    .await?;
                let summary_hold = io.guard_owned(io.owned_since(before)?)?;
                let quiescent = summary.quiescent(Some(&io));
                let tail = summary.tail;
                drop(summary);
                drop(summary_hold);
                if !quiescent? {
                    return Ok(None);
                }
                tail
            };
            Ok(workflow
                .prove_release_with_io(&io)
                .await?
                .map(|pending| (tail, pending)))
        })?
        .await;
        let Some((tail, pending)) = result? else {
            return Ok(false);
        };
        owner
            .release_quiescent_execution(
                *operation,
                tail,
                &pending,
                &mut Some(&mut io.history_io()),
            )
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
            if let Some(shared) = shared.as_deref_mut() {
                shared.bind(
                    self.owner
                        .as_ref()
                        .map_or(command, |(_, operation)| *operation),
                    io,
                )?;
            }
            let summary = shared
                .as_deref_mut()
                .map(|cache| &mut cache.summary)
                .unwrap_or(&mut transient);
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
                .map(|io| io.guard_owned(io.owned_since(before)?))
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
        let digest = super::content::operation_digest(operation_id, claim_id, io)?;
        let _retry_hold = io.map(|io| io.reserve_decode(64)).transpose()?;
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
        #[cfg(not(target_arch = "wasm32"))]
        let cacheable = io.is_none() || io.and_then(PreparationIo::original_native_work).is_some();
        #[cfg(target_arch = "wasm32")]
        let cacheable = io.is_none();
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let cacheable = cacheable && !crate::test_support::accounted::append_scan_enabled();
        let mut shared = if cacheable {
            Some(self.verified.lock().await)
        } else {
            None
        };
        #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
        let _append_scope = {
            // Negative control shortens the actual production guard lifetime.
            if self.append_probe.release_early {
                drop(shared.take());
            }
            self.append_probe.enter()
        };
        let mut transient = VerifiedSummary::default();
        let cache = shared.as_deref_mut().unwrap_or(&mut transient);
        cache.bind(operation_id, io)?;
        let VerifiedSummary { summary, funding } = cache;
        let summary_io = funding.as_ref().or(io);
        let result = async {
            loop {
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let current = &mut *summary;
                journal_future!(
                    move || self.refresh_summary(operation_id, current, summary_io),
                    self.refresh_summary(operation_id, summary, summary_io)
                )?;
                if let Some(owner) = owner {
                    let mut borrowed = io.map(PreparationIo::history_io);
                    #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                    let mut history = borrowed
                        .as_mut()
                        .map(|io| io as &mut dyn crate::store::history::HistoryIo);
                    #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                    let work = &mut history;
                    journal_future!(
                        move || owner.verify_execution(operation_id, event, work),
                        owner.verify_execution(
                            operation_id,
                            event,
                            &mut borrowed
                                .as_mut()
                                .map(|io| io as &mut dyn crate::store::history::HistoryIo)
                        )
                    )?;
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
                    return Ok(
                        expected_tail.is_none_or(|tail| tail.checked_add(1) == Some(*sequence))
                    );
                }
                if expected_tail.is_some_and(|tail| tail != summary.tail) {
                    return Ok(false);
                }
                summary.validate_next(event, model.as_ref(), limits, io)?;
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let model_ref = model.as_ref();
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let preparing = super::cache::model_future(move || {
                    self.prepare_chunk(operation_id, event, model_ref, io)
                });
                #[cfg(not(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom)))]
                let preparing = self.prepare_chunk(operation_id, event, model.as_ref(), io);
                #[cfg(all(test, not(target_arch = "wasm32")))]
                let chunk = crate::test_support::accounted::observe_chunk(preparing, io).await;
                #[cfg(not(all(test, not(target_arch = "wasm32"))))]
                let chunk = preparing.await;
                let chunk = match chunk {
                    Err(Error::TaskFiles(error))
                        if matches!(*error, crate::task_volume::Error::SequenceConflict(_)) =>
                    {
                        continue;
                    }
                    result => result?,
                };
                let committed = if let Some(owner) = owner {
                    let mut borrowed = io.map(PreparationIo::history_io);
                    #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                    let mut history = borrowed
                        .as_mut()
                        .map(|io| io as &mut dyn crate::store::history::HistoryIo);
                    #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                    let work = &mut history;
                    #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                    let (tail, key_ref, content) = (summary.tail, &key, bytes.clone());
                    journal_future!(
                        move || owner.append_execution(
                            operation_id,
                            tail,
                            key_ref,
                            content,
                            event,
                            chunk,
                            work,
                        ),
                        owner.append_execution(
                            operation_id,
                            summary.tail,
                            &key,
                            bytes.clone(),
                            event,
                            chunk,
                            &mut borrowed
                                .as_mut()
                                .map(|io| io as &mut dyn crate::store::history::HistoryIo),
                        )
                    )?
                } else {
                    #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                    let (view, tail, key, content) =
                        (&observation, summary.tail, key.clone(), bytes.clone());
                    journal_future!(
                        move || self.append_local_with_io(view, tail, key, content, io),
                        self.append_local_with_io(
                            &observation,
                            summary.tail,
                            key.clone(),
                            bytes.clone(),
                            io
                        )
                    )?
                };
                if committed {
                    drop(key);
                    drop(key_hold);
                    drop(bytes);
                    drop(body_hold);
                    return Ok(true);
                }
            }
        }
        .await;
        if result.is_err() {
            cache.clear();
        }
        result
    }

    async fn append_local_with_io(
        &self,
        observation: &BorrowedObservation<'_>,
        tail: u64,
        key: StreamKey,
        bytes: acyclic_stream::OwnedBytes,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let BorrowedObservation {
            operation_id,
            retry_digest,
            event,
            ..
        } = *observation;
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
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let stream_ref = &stream;
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let mut history = Some(&mut borrowed as &mut dyn crate::store::history::HistoryIo);
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let work = &mut history;
                journal_future!(
                    move || crate::distributed::append(
                        (stream_ref, &self.stream),
                        [bytes],
                        0,
                        Some(tail),
                        key,
                        operation_id,
                        work,
                    ),
                    crate::distributed::append(
                        (&stream, &self.stream),
                        [bytes],
                        0,
                        Some(tail),
                        key,
                        operation_id,
                        &mut Some(&mut borrowed),
                    )
                )
            }
            None => {
                #[cfg(all(feature = "model-checking", acyclic_harness_loom, acyclic_loom))]
                let stream_ref = &stream;
                journal_future!(
                    move || stream_ref.append_batch(vec![bytes], Some(tail), Some(key)),
                    stream.append_batch(vec![bytes], Some(tail), Some(key))
                )
                .map_err(|error| match error {
                    StreamError::IdempotencyMismatch => {
                        Error::Conflict("execution journal retry identity reused".into())
                    }
                    _ => Error::Indeterminate(operation_id),
                })
            }
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
            Err(error) => {
                let error = match error {
                    Error::Indeterminate(_) => Error::Indeterminate(operation_id),
                    error @ Error::Conflict(_) => error,
                    #[cfg(not(target_arch = "wasm32"))]
                    error @ (Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        ..
                    }
                    | Error::NativeStream {
                        source: StreamError::IdempotencyMismatch,
                        ..
                    }) => error,
                    error => return Err(error),
                };
                let records = journal_future!(
                    move || self.replay_page(operation_id, tail, 1, true, io),
                    self.replay_page(operation_id, tail, 1, true, io)
                )?;
                if records.iter().any(|(record, _)| {
                    record.idempotency_key == retry_digest && &record.event == event
                }) {
                    Ok(true)
                } else {
                    Err(error)
                }
            }
        }
    }
}

use crate::executor::stock_boundary;
