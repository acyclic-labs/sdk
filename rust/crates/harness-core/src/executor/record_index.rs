//! A driver validates the immutable journal prefix once and indexes new records.
//! This cache belongs to one stock execution, never to an authority or process.
use super::*;
use crate::model::DigestIndex;

fn grow_push<T>(values: &mut Vec<T>, value: T, io: Option<&PreparationIo>) -> Result<()> {
    if values.len() == values.capacity() {
        let capacity = values
            .capacity()
            .checked_mul(2)
            .map(|capacity| capacity.max(1))
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "execution index capacity overflows",
                )
            })?;
        stock_vec_reserve(values, capacity, io)?;
    }
    stock_vec_push(values, value, io)
}

#[cfg(feature = "filesystem")]
fn release_page_scratch(
    records: &[ExecutionRecord],
    capacity: usize,
    before: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let Some(io) = io else {
        return Ok(());
    };
    let overflow = || {
        stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Invalid,
            "execution page ownership overflows",
        )
    };
    let bytes = (capacity as u64)
        .checked_mul(std::mem::size_of::<ExecutionRecord>() as u64)
        .ok_or_else(overflow)?;
    let bytes = records.iter().try_fold(bytes, |bytes, record| {
        bytes
            .checked_add(record.payload_bytes(Some(io))?)
            .ok_or_else(overflow)
    })?;
    let held = io.owned_bytes()?.checked_sub(before).ok_or_else(overflow)?;
    io.release(held.checked_sub(bytes).ok_or_else(overflow)?)?;
    Ok(())
}

#[derive(Default)]
pub(super) struct RecordIndex<'a> {
    binding: Option<(OperationId, ExecutionJournalCacheId)>,
    tail: u64,
    keys: DigestIndex<usize>,
    steps: Vec<Vec<ExecutionRecord>>,
    // Values are dropped before the custody that funds them.
    #[cfg(feature = "filesystem")]
    quota: Option<crate::conversation::ContentHold<'a>>,
    #[cfg(not(feature = "filesystem"))]
    _quota: std::marker::PhantomData<&'a ()>,
}

impl ExecutionEvent {
    pub(crate) fn step(&self) -> Option<u32> {
        match self {
            Self::Started { .. } => None,
            Self::ContextPrepared { step, .. }
            | Self::ContextCompacted { step, .. }
            | Self::ModelStarted { step, .. }
            | Self::Model { step, .. }
            | Self::ModelInline { step, .. }
            | Self::ModelStopped { step, .. }
            | Self::ToolStarted { step, .. }
            | Self::ToolCompleted { step, .. }
            | Self::ToolFailed { step, .. } => Some(*step),
        }
    }

    pub(crate) fn copy_indexed(&self, io: Option<&PreparationIo>) -> Result<Self> {
        use ExecutionEvent::*;
        let file = |value: &FileRef| crate::model::clone_file(value, io);
        let text = |value: &str| stock_string_copy(value, io);
        Ok(match self {
            Started { request_digest } => Started {
                request_digest: *request_digest,
            },
            ContextPrepared { step, projection } => ContextPrepared {
                step: *step,
                projection: file(projection)?,
            },
            ContextCompacted {
                step,
                projection,
                compaction,
            } => ContextCompacted {
                step: *step,
                projection: file(projection)?,
                compaction: file(compaction)?,
            },
            ModelStarted {
                step,
                purpose,
                request_digest,
                request,
            } => ModelStarted {
                step: *step,
                purpose: *purpose,
                request_digest: *request_digest,
                request: file(request)?,
            },
            Model {
                step,
                purpose,
                event,
                usage,
            } => Model {
                step: *step,
                purpose: *purpose,
                event: file(event)?,
                usage: usage.as_ref().map(file).transpose()?,
            },
            ModelInline {
                step,
                purpose,
                event,
                usage,
            } => ModelInline {
                step: *step,
                purpose: *purpose,
                event: text(event)?,
                usage: usage.as_ref().map(file).transpose()?,
            },
            ModelStopped { step, purpose } => ModelStopped {
                step: *step,
                purpose: *purpose,
            },
            ToolStarted {
                step,
                call_id,
                invocation,
            } => ToolStarted {
                step: *step,
                call_id: text(call_id)?,
                invocation: file(invocation)?,
            },
            ToolCompleted {
                step,
                call_id,
                result,
                projection,
            } => ToolCompleted {
                step: *step,
                call_id: text(call_id)?,
                result: file(result)?,
                projection: file(projection)?,
            },
            ToolFailed {
                step,
                call_id,
                reason,
                cause,
            } => ToolFailed {
                step: *step,
                call_id: text(call_id)?,
                reason: *reason,
                cause: ToolFailureCause {
                    class: cause.class,
                    message: text(&cause.message)?,
                },
            },
        })
    }
}

impl<'a> RecordIndex<'a> {
    fn bucket(&self, step: Option<u32>, io: Option<&PreparationIo>) -> Result<&[ExecutionRecord]> {
        match self.keys.get(1, &step_key(step), io)? {
            None => Ok(&[]),
            Some(index) => self.steps.get(index).map(Vec::as_slice).ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "execution index bucket is missing",
                )
            }),
        }
    }

    async fn refresh(
        &mut self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        io: Option<&'a PreparationIo>,
    ) -> Result<()> {
        validate_journal(journal, io)?;
        // Addresses can be reused after an in-place journal replacement.
        // Providers without a stable binding identity always replay afresh.
        let binding = journal
            .cache_identity()
            .map(|identity| (operation, identity));
        if binding.is_none() || self.binding != binding {
            *self = Self::default();
            self.binding = binding;
        }
        loop {
            #[cfg(feature = "filesystem")]
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let result = self.read_page(journal, operation, io).await;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                let owned = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Storage,
                        "execution index ownership underflows",
                    )
                })?;
                let incoming = io.guard_owned(owned)?;
                if let Some(quota) = &mut self.quota {
                    quota.absorb(incoming)?;
                } else {
                    self.quota = Some(incoming);
                }
            }
            if !result? {
                return Ok(());
            }
        }
    }

    async fn read_page(
        &mut self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        #[cfg(feature = "filesystem")]
        let page_before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let page = journal
            .replay(operation, self.tail, EXECUTION_REPLAY_PAGE_RECORDS, io)?
            .await?;
        if page.len() > EXECUTION_REPLAY_PAGE_RECORDS as usize {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "execution replay page exceeds bound",
            ));
        }
        // Decoders may reserve conservative scratch space. Once the page is
        // materialized, only its vector and the moved records remain live.
        #[cfg(feature = "filesystem")]
        release_page_scratch(&page, page.capacity(), page_before, io)?;
        let mut keys = [[0; 32]; EXECUTION_REPLAY_PAGE_RECORDS as usize];
        for (index, record) in page.iter().enumerate() {
            crate::model::charge_items(io, index as u64)?;
            let key = crate::model::hash_bytes(record.idempotency_key.as_bytes(), io)?;
            if record.operation_id != operation
                || self.tail.checked_add(index as u64 + 1) != Some(record.sequence)
                || record.idempotency_key.is_empty()
                || self.keys.get(0, &key, io)?.is_some()
                || keys.get(..index).is_none_or(|prior| prior.contains(&key))
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "execution journal identity or sequence is invalid",
                ));
            }
            *keys.get_mut(index).ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "execution replay page exceeds bound",
                )
            })? = key;
        }
        #[cfg(feature = "filesystem")]
        let page_bytes = page.capacity() as u64 * std::mem::size_of::<ExecutionRecord>() as u64;
        let advanced = !page.is_empty();
        for (record, key) in page.into_iter().zip(keys) {
            let step = step_key(record.event.step());
            let bucket_index = if let Some(index) = self.keys.get(1, &step, io)? {
                index
            } else {
                let index = self.steps.len();
                grow_push(&mut self.steps, Vec::new(), io)?;
                self.keys.reserve_key(io)?;
                self.keys.set(1, step, Some(index), io)?;
                index
            };
            let sequence = record.sequence;
            let bucket = self.steps.get_mut(bucket_index).ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "execution index bucket is missing",
                )
            })?;
            grow_push(bucket, record, io)?;
            self.keys.reserve_key(io)?;
            self.keys.set(0, key, Some(0), io)?;
            self.tail = sequence;
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.release(page_bytes)?;
        }
        Ok(advanced)
    }

    fn select(
        &self,
        step: u32,
        include_started: bool,
        maximum: usize,
        select: impl Fn(&ExecutionEvent) -> bool,
        io: Option<&PreparationIo>,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        let mut selected = Vec::new();
        let started = if include_started {
            self.bucket(None, io)?
        } else {
            &[]
        };
        for record in started.iter().chain(self.bucket(Some(step), io)?.iter()) {
            crate::model::charge_items(io, 1)?;
            if select(&record.event) {
                if selected.len() >= maximum {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "selected execution records exceed bound",
                    ));
                }
                let copied = ExecutionRecord {
                    operation_id: record.operation_id,
                    sequence: record.sequence,
                    idempotency_key: stock_string_copy(&record.idempotency_key, io)?,
                    event: record.event.copy_indexed(io)?,
                };
                grow_push(&mut selected, copied, io)?;
            }
        }
        // Preserve wire order even when a malformed journal puts Started after
        // step records. Only the selected step is sorted, never its history.
        crate::model::charge_items(
            io,
            (selected.len() as u64)
                .saturating_mul(u64::from(usize::BITS - selected.len().leading_zeros())),
        )?;
        selected.sort_unstable_by_key(|record| record.sequence);
        Ok((self.tail, selected))
    }
}

fn step_key(step: Option<u32>) -> [u8; 32] {
    let mut key = [0; 32];
    if let Some(step) = step {
        key[..4].copy_from_slice(&step.to_be_bytes());
        key[4] = 1;
    }
    key
}

impl StockExecution<'_> {
    pub(super) async fn step_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        include_started: bool,
        maximum: usize,
        select: impl Fn(&ExecutionEvent) -> bool + Send + Sync,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        validate_journal(journal, self.io)?;
        let query = ExecutionRecordQuery {
            operation,
            step,
            include_started,
            maximum_step_records: ExecutionRecordQuery::step_bound(self.stock.limits),
            maximum,
            select: &select,
        };
        if let Some(future) = journal.indexed_records(query, self.io)?
            && let Some(result) = future.await?
        {
            // Keep the fallback validator's prefix untouched. A later raw append
            // must still pass complete sequence and retry-identity validation.
            return Ok(result);
        }
        let mut index = self.records.lock().await;
        if let Err(error) = index.refresh(journal, operation, self.io).await {
            // A cancelled/failed page is not an authenticated cache prefix.
            *index = Default::default();
            return Err(error);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let before = self
            .io
            .map(PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = self.io.filter(|_| stock_is_native(self.io)) {
            io.retain_owned(std::mem::size_of::<Vec<ExecutionRecord>>() as u64)?;
        }
        let (tail, records) = index.select(step, include_started, maximum, select, self.io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let bytes = records.iter().try_fold(
                native_size_add(
                    std::mem::size_of::<Vec<ExecutionRecord>>() as u64,
                    native_size_slots(records.capacity(), std::mem::size_of::<ExecutionRecord>())?,
                )?,
                |bytes, record| native_size_add(bytes, record.payload_bytes(self.io)?),
            )?;
            if let Some(hold) = super::payload::preparation_output(self.io, before, bytes)? {
                hold.into_retained()?;
            }
        }
        Ok((tail, records))
    }
}
