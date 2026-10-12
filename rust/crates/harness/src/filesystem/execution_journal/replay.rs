use super::*;
use futures::TryStreamExt as _;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn journal_read_stream_error(
        io: Option<&PreparationIo>,
        code: crate::wire::ErrorCode,
        source: StreamError,
    ) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
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
        self.require_operation(operation_id, io)?;
        if maximum == 0 {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "execution replay page bound is invalid",
            ));
        }
        let capacity = maximum.min(EXECUTION_REPLAY_PAGE_RECORDS) as usize;
        let vector_bytes = capacity
            .checked_mul(std::mem::size_of::<(ExecutionRecord, Option<ModelEvent>)>())
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution replay vector ownership overflow",
                )
            })? as u64;
        if let Some(io) = io {
            io.retain_owned(vector_bytes)?;
            io.allocation_attempt(vector_bytes)?;
        }
        let mut result: Vec<(ExecutionRecord, Option<ModelEvent>)> = Vec::with_capacity(capacity);
        let _path_hold;
        let path = crate::contract::preparation_format(
            format_args!("harness/v2/execution/{operation_id}"),
            io,
        )?;
        _path_hold = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
        let _stream_hold;
        _stream_hold = if let Some(io) = io {
            let copying = acyclic_fs::WorkCounters {
                bytes_copied: path.len() as u64,
                items_examined: path.len() as u64,
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
        let _read_path_hold;
        _read_path_hold = if let Some(io) = io {
            let call = acyclic_fs::WorkCounters {
                backend_read_operations: 1,
                bytes_copied: path.len() as u64,
                ..Default::default()
            };
            io.preflight(call)?;
            let hold = io.reserve_decode(path.len() as u64)?;
            io.allocation_attempt(path.len() as u64)?;
            io.charge(call)?;
            Some(hold)
        } else {
            None
        };
        let mut entries = match stream.read(after, capacity as u32).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                return Ok(result);
            }
            Err(error) => {
                return Err(Self::journal_read_stream_error(
                    io,
                    crate::wire::ErrorCode::Storage,
                    error,
                ));
            }
        };
        loop {
            let _record_hold;
            if let Some(io) = io {
                io.check_cancellation()?;
            }
            let Some(record) = entries.try_next().await.map_err(|error| {
                Self::journal_read_stream_error(io, crate::wire::ErrorCode::Storage, error)
            })?
            else {
                if let Some(io) = io {
                    io.check_cancellation()?;
                }
                break;
            };
            _record_hold = if let Some(io) = io {
                let read = acyclic_fs::WorkCounters {
                    authority_records_read: 1,
                    authority_bytes_read: record.value.len() as u64,
                    source_bytes_read: record.value.len() as u64,
                    items_examined: 1,
                    ..Default::default()
                };
                io.charge(read)?;
                io.retain_owned(record.value.len() as u64)?;
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
            let observation: Observation = match io {
                Some(io) => crate::filesystem::retain_decoded(io.decode(
                    std::marker::PhantomData::<Observation>,
                    &record.value,
                    crate::wire::ErrorCode::Storage,
                )?),
                None => crate::contract::json_from_slice(&record.value).map_err(|error| {
                    Error::Storage(format!("execution journal record is invalid: {error}"))
                })?,
            };
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
            let _model_hold;
            let model = self
                .verify_event_refs_with_io(&observation.event, io)
                .await?;
            _model_hold = if retain_models {
                None
            } else {
                match (io, before_model) {
                    (Some(io), Some(before)) => Some(io.guard_owned(
                        io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                            stock_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "model observation custody underflow",
                            )
                        })?,
                    )?),
                    _ => None,
                }
            };
            let model = if retain_models {
                model
            } else {
                drop(model);
                None
            };
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
            if let Some(io) = io {
                let mut work = io.history_io();
                owner
                    .verify_model_history(
                        operation_id,
                        result.iter().map(|(record, _)| record),
                        &mut Some(&mut work),
                    )
                    .await?;
            } else {
                owner
                    .verify_model_history(
                        operation_id,
                        result.iter().map(|(record, _)| record),
                        &mut None,
                    )
                    .await?;
            }
        }
        Ok(result)
    }

    pub(super) async fn records(
        &self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<ExecutionRecord>> {
        let page_slots;
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
        page_slots = io.map(|io| io.guard_owned(page_bytes)).transpose()?;
        let output_bytes = page
            .len()
            .checked_mul(std::mem::size_of::<ExecutionRecord>())
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution replay vector ownership overflow",
                )
            })? as u64;
        if let Some(io) = io {
            io.retain_owned(output_bytes)?;
            io.allocation_attempt(output_bytes)?;
        }
        let mut records = Vec::with_capacity(page.len());
        for (record, _) in page {
            records.push(record);
        }
        drop(page_slots);
        Ok(records)
    }
}

use crate::executor::{stock_boundary};
