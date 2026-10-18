use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn journal_read_stream_error(
        _native_io: Option<&PreparationIo>,
        code: crate::wire::ErrorCode,
        source: StreamError,
    ) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if _native_io
            .and_then(PreparationIo::original_native_work)
            .is_some()
        {
            return Error::NativeStream {
                code,
                source,
                custody: None,
            };
        }
        if code == crate::wire::ErrorCode::Invalid {
            Error::Invalid(source.to_string())
        } else {
            Error::Storage(source.to_string())
        }
    }

    pub(in crate::filesystem) async fn replay_page(
        &self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        retain_models: bool,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<(ExecutionRecord, Option<ModelEvent>)>> {
        self.replay_page_fenced(operation_id, after, maximum, retain_models, None, io)
            .await
            .map(|(records, _)| records)
    }

    pub(super) async fn replay_page_fenced(
        &self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        retain_models: bool,
        fence: Option<acyclic_stream::CommitId>,
        io: Option<&PreparationIo>,
    ) -> Result<(
        Vec<(ExecutionRecord, Option<ModelEvent>)>,
        Option<acyclic_stream::CommitId>,
    )> {
        #[cfg(not(target_arch = "wasm32"))]
        let recheck = io
            .and_then(PreparationIo::original_native_work)
            .map(|work| PreparationIo::from_native_work(&work.recheck_admitted()))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let check_io = recheck.as_ref().or(io);
        #[cfg(target_arch = "wasm32")]
        let check_io = io;
        let result = async {
            let io = check_io;
            self.require_operation(operation_id, io)?;
            if maximum == 0 {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution replay page bound is invalid",
                ));
            }
            let page_maximum = maximum.min(EXECUTION_REPLAY_PAGE_RECORDS);
            let capacity = page_maximum as usize;
            let mut result: Vec<(ExecutionRecord, Option<ModelEvent>)> =
                crate::model::vec_with_capacity(if io.is_some() { capacity } else { 0 }, io)?;
            let (_path_hold, path) = {
                let path = crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(
                        format_args!("harness/v2/execution/{operation_id}"),
                        work,
                    )
                })?;
                let _path_hold = io
                    .map(|io| io.guard_owned(path.capacity() as u64))
                    .transpose()?;
                (_path_hold, path)
            };
            let _stream_hold = if let Some(io) = io {
                let copying = acyclic_fs::WorkCounters {
                    bytes_copied: path.len() as u64,
                    ..Default::default()
                };
                io.preflight(copying)?;
                let hold = io.reserve_decode(path.len() as u64)?;
                io.allocation_attempt(path.len() as u64)?;
                io.charge(copying)?;
                Some(hold)
            } else {
                None
            };
            let stream =
                acyclic_stream::Stream::new(Arc::clone(&self.stream), &path).map_err(|error| {
                    Self::journal_read_stream_error(io, crate::wire::ErrorCode::Invalid, error)
                })?;
            let mut history = io.map(PreparationIo::history_io);
            let mut work = history
                .as_mut()
                .map(|io| io as &mut dyn crate::store::HistoryIo);
            let mut entries = match crate::store::operations::read_records(
                &self.stream,
                stream.path(),
                after.saturating_sub(u64::from(fence.is_some())),
                page_maximum + u32::from(fence.is_some()),
                &mut work,
            )
            .await?
            {
                Ok(entries) => entries,
                Err(StreamError::NotFound) => {
                    if let Some(io) = io {
                        io.check_cancellation()?;
                    }
                    if fence.is_some() {
                        return Err(canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "execution journal lost its retained cursor",
                        ));
                    }
                    return Ok((result, fence));
                }
                Err(error) => {
                    return Err(Self::journal_read_stream_error(
                        io,
                        crate::wire::ErrorCode::Storage,
                        error,
                    ));
                }
            };
            if let Some(expected) = fence {
                let anchor = entries.next(&mut work).await?.ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "execution journal lost its retained cursor",
                    )
                })?;
                if !super::cache::matches_anchor(after, expected, anchor.sequence, anchor.commit_id)
                {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "execution journal cursor was replaced",
                    ));
                }
            }
            let mut next_fence = fence;
            if io.is_none() {
                result
                    .try_reserve_exact(entries.size_hint().1.unwrap_or(capacity).min(capacity))
                    .map_err(Error::Allocation)?;
            }
            loop {
                let _record_hold;
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                let Some(record) = entries.next(&mut work).await? else {
                    if let Some(io) = io {
                        io.check_cancellation()?;
                    }
                    break;
                };
                _record_hold = if let Some(io) = io.filter(|io| io.original_native_work().is_none())
                {
                    io.charge(acyclic_fs::WorkCounters {
                        source_bytes_read: record.value.len() as u64,
                        ..Default::default()
                    })?;
                    Some(io.guard_owned(record.value.len() as u64)?)
                } else {
                    None
                };
                if record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "execution Stream record exceeds protocol bound",
                    ));
                }
                let sequence = after.checked_add(result.len() as u64).ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "execution replay sequence overflow",
                    )
                })?;
                if result.len() >= capacity || record.sequence != sequence {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "execution journal sequence or bound is invalid",
                    ));
                }
                let observation: Observation = crate::filesystem::decode_record(&record.value, io)?;
                if observation.version != OBSERVATION_VERSION {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "execution journal observation version is unsupported",
                    ));
                }
                if observation.operation_id != operation_id {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "execution journal identity is invalid",
                    ));
                }
                if observation.turn_operation != self.turn_operation(operation_id) {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "execution journal belongs to another original turn",
                    ));
                }
                for (previous, _) in &result {
                    crate::model::charge_items(io, 1)?;
                    if previous.idempotency_key == observation.retry_digest {
                        return Err(canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "execution journal identity is invalid",
                        ));
                    }
                }
                let before_model = io.map(PreparationIo::owned_bytes).transpose()?;
                let (_model_hold, model) = {
                    let model = self
                        .verify_event_refs_with_io(&observation.event, io)
                        .await?;
                    let _model_hold = if retain_models {
                        None
                    } else {
                        match (io, before_model) {
                            (Some(io), Some(before)) => {
                                Some(io.guard_owned(io.owned_since(before)?)?)
                            }
                            _ => None,
                        }
                    };
                    (_model_hold, model)
                };
                let model = if retain_models {
                    model
                } else {
                    drop(model);
                    None
                };
                // Provider hints size storage only; the independent page bound
                // above still rejects excess records, even for a false hint.
                if result.len() == result.capacity() {
                    result
                        .try_reserve_exact(capacity - result.len())
                        .map_err(Error::Allocation)?;
                }
                next_fence = Some(record.commit_id);
                result.push((
                    ExecutionRecord {
                        operation_id,
                        sequence: record.sequence.checked_add(1).ok_or_else(|| {
                            stock_boundary(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "execution replay sequence overflow",
                            )
                        })?,
                        idempotency_key: observation.retry_digest,
                        event: observation.event,
                    },
                    model,
                ));
            }
            if let Some((owner, _)) = &self.owner {
                owner
                    .verify_model_history(
                        operation_id,
                        result.iter().map(|(record, _)| record),
                        &mut work,
                    )
                    .await?;
            }
            Ok((result, next_fence))
        }
        .await;
        #[cfg(not(target_arch = "wasm32"))]
        if result.is_ok()
            && let Some(recheck) = recheck
        {
            let retained = recheck.owned_bytes()?;
            io.ok_or_else(|| recheck.invalid("execution replay observer is missing"))?
                .absorb_native_custody(recheck.into_native_custody(retained)?)?;
        }
        result
    }

    pub(super) async fn records(
        &self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<ExecutionRecord>> {
        let (page_slots, page) = {
            let page = self
                .replay_page(operation, after, maximum, io.is_none(), io)
                .await?;
            let page_bytes = page
                .capacity()
                .checked_mul(std::mem::size_of::<(ExecutionRecord, Option<ModelEvent>)>())
                .ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "execution replay vector ownership overflow",
                    )
                })? as u64;
            let page_slots = io.map(|io| io.guard_owned(page_bytes)).transpose()?;
            (page_slots, page)
        };
        let mut records = crate::model::vec_with_capacity(page.len(), io)?;
        for (record, _) in page {
            records.push(record);
        }
        drop(page_slots);
        Ok(records)
    }
}

use crate::executor::stock_boundary;
