use super::*;

impl ExecutionReplay {
    /// Starts validation at the first observation of one exact execution.
    #[must_use]
    pub fn new(operation_id: OperationId) -> Self {
        Self {
            operation_id,
            after: 0,
            keys: Vec::new(),
        }
    }

    /// Last successfully validated one-based sequence, zero for an empty journal.
    #[must_use]
    pub const fn tail(&self) -> u64 {
        self.after
    }

    /// Reads one bounded page, or `None` at the current tail.
    pub async fn next_page(
        &mut self,
        journal: &dyn ExecutionJournal,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Vec<ExecutionRecord>>> {
        validate_journal(journal, io)?;
        validate_execution_page(EXECUTION_REPLAY_PAGE_RECORDS)?;
        let page = journal
            .replay(
                self.operation_id,
                self.after,
                EXECUTION_REPLAY_PAGE_RECORDS,
                io,
            )?
            .await?;
        self.observe_page(page.iter(), io)?;
        Ok((!page.is_empty()).then_some(page))
    }

    /// Validates a retained-model page without copying its execution records.
    pub(crate) fn observe_page<'a>(
        &mut self,
        page: impl ExactSizeIterator<Item = &'a ExecutionRecord> + Clone,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if page.len() > EXECUTION_REPLAY_PAGE_RECORDS as usize {
            return Err(Error::Storage("execution replay page exceeds bound".into()));
        }
        if page.len() == 0 {
            return Ok(());
        }
        let next_tail = self
            .after
            .checked_add(page.len() as u64)
            .ok_or_else(|| Error::Conflict("execution journal sequence overflows".into()))?;
        let mut keys = [[0; 32]; EXECUTION_REPLAY_PAGE_RECORDS as usize];
        for (index, record) in page.clone().enumerate() {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.preflight(acyclic_fs::WorkCounters {
                    bytes_hashed: record.idempotency_key.len() as u64,
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let key = *blake3::hash(record.idempotency_key.as_bytes()).as_bytes();
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters {
                    bytes_hashed: record.idempotency_key.len() as u64,
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            if record.operation_id != self.operation_id
                || record.sequence != self.after + index as u64 + 1
                || record.idempotency_key.is_empty()
                || self.keys.binary_search(&key).is_ok()
                || keys.iter().take(index).any(|prior| prior == &key)
            {
                return Err(Error::Conflict(
                    "execution journal identity or sequence is invalid".into(),
                ));
            }
            *keys
                .get_mut(index)
                .ok_or_else(|| Error::Storage("execution replay page exceeds bound".into()))? = key;
        }
        let previous_len = self.keys.len();
        for key in keys.iter().take(page.len()) {
            if let Err(error) = stock_vec_push(&mut self.keys, *key, io) {
                self.keys.truncate(previous_len);
                return Err(error);
            }
        }
        self.keys.sort_unstable();
        self.after = next_tail;
        Ok(())
    }
}

pub(crate) async fn replay_execution<'a>(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    maximum_selected: usize,
    select: impl Fn(&ExecutionEvent) -> bool,
    io: Option<&'a PreparationIo>,
) -> Result<(
    u64,
    Vec<ExecutionRecord>,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    validate_journal(journal, io)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let output_before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        io.retain_owned(std::mem::size_of::<Vec<ExecutionRecord>>() as u64)?;
    }
    let mut replay = ExecutionReplay::new(operation_id);
    let mut selected = Vec::new();
    loop {
        #[cfg(feature = "filesystem")]
        let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        #[cfg(feature = "filesystem")]
        let key_capacity_before = replay.keys.capacity();
        let Some(page) = replay.next_page(journal, io).await? else {
            break;
        };
        #[cfg(feature = "filesystem")]
        let held = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0) - before;
        #[cfg(feature = "filesystem")]
        let key_growth = (replay.keys.capacity() - key_capacity_before) as u64 * 32;
        #[cfg(feature = "filesystem")]
        let selected_before = selected.len();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let page_bytes = page.capacity() as u64 * std::mem::size_of::<ExecutionRecord>() as u64;
        for record in page {
            if select(&record.event) {
                if selected.len() >= maximum_selected {
                    return Err(Error::Invalid(
                        "selected execution records exceed bound".into(),
                    ));
                }
                stock_vec_push(&mut selected, record, io)?;
            }
        }
        #[cfg(feature = "filesystem")]
        if selected.len() == selected_before
            && let Some(io) = io
        {
            io.release(held.saturating_sub(key_growth))?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if selected.len() != selected_before
            && let Some(io) = io
            && io.original_native_work().is_some()
        {
            io.release(page_bytes)?;
        }
    }
    let tail = replay.tail();
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let key_bytes = replay.keys.capacity() as u64 * 32;
        drop(replay);
        io.release(key_bytes)?;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let hold = if stock_is_native(io) {
        let bytes = native_size_add(
            std::mem::size_of::<Vec<ExecutionRecord>>() as u64,
            native_size_slots(selected.capacity(), std::mem::size_of::<ExecutionRecord>())?,
        )?;
        let bytes = selected.iter().try_fold(bytes, |bytes, record| {
            native_size_add(bytes, record.payload_bytes()?)
        })?;
        super::payload::preparation_output(io, output_before, bytes)?
    } else {
        None
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let hold = None;
    Ok((tail, selected, hold))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ExecutionRecord {
    fn payload_bytes(&self) -> Result<u64> {
        use ExecutionEvent::*;
        let (call, files): (Option<&String>, [Option<&FileRef>; 3]) = match &self.event {
            Started { .. } | ModelStopped { .. } => (None, [None; 3]),
            ContextPrepared {
                projection,
                accounting,
                ..
            } => (None, [Some(projection), accounting.as_ref(), None]),
            ContextCompacted {
                projection,
                compaction,
                accounting,
                ..
            } => (None, [Some(projection), Some(compaction), Some(accounting)]),
            ModelStarted { request: file, .. } | Model { event: file, .. } => {
                (None, [Some(file), None, None])
            }
            ToolStarted {
                call_id,
                invocation,
                ..
            } => (Some(call_id), [Some(invocation), None, None]),
            ToolCompleted {
                call_id,
                result,
                projection,
                ..
            } => (Some(call_id), [Some(result), Some(projection), None]),
            ToolFailed { call_id, .. } => (Some(call_id), [None; 3]),
        };
        let bytes = native_size_add(
            self.idempotency_key.capacity() as u64,
            call.map_or(0, |call| call.capacity() as u64),
        )?;
        files.into_iter().flatten().try_fold(bytes, |bytes, file| {
            native_size_add(bytes, file.logical_owned_bytes()?)
        })
    }
}
