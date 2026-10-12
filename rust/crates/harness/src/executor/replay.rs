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
        validate_execution_page(EXECUTION_REPLAY_PAGE_RECORDS)?;
        let page = 'page: {
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.original_native_work() {
                    let (page, custody) = journal
                        .replay_with_native_work(
                            self.operation_id,
                            self.after,
                            EXECUTION_REPLAY_PAGE_RECORDS,
                            work,
                        )?
                        .await?;
                    if page.is_empty() {
                        drop(page);
                        drop(custody);
                        return Ok(None);
                    }
                    io.absorb_native_custody(custody)?;
                    break 'page page;
                }
                break 'page receive_preparation(
                    io,
                    journal
                        .replay_measured(
                            self.operation_id,
                            self.after,
                            EXECUTION_REPLAY_PAGE_RECORDS,
                            io.remaining()?,
                            io.cancellation(),
                        )
                        .await,
                )?;
            }
            break 'page journal
                .replay(self.operation_id, self.after, EXECUTION_REPLAY_PAGE_RECORDS)
                .await?;
        };
        if page.len() > EXECUTION_REPLAY_PAGE_RECORDS as usize {
            return Err(Error::Storage("execution replay page exceeds bound".into()));
        }
        if page.is_empty() {
            return Ok(None);
        }
        let next_tail = self
            .after
            .checked_add(page.len() as u64)
            .ok_or_else(|| Error::Conflict("execution journal sequence overflows".into()))?;
        let mut keys = [[0; 32]; EXECUTION_REPLAY_PAGE_RECORDS as usize];
        for (index, record) in page.iter().enumerate() {
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
                || keys[..index].contains(&key)
            {
                return Err(Error::Conflict(
                    "execution journal identity or sequence is invalid".into(),
                ));
            }
            keys[index] = key;
        }
        let previous_len = self.keys.len();
        for key in &keys[..page.len()] {
            if let Err(error) = stock_vec_push(&mut self.keys, *key, io) {
                self.keys.truncate(previous_len);
                return Err(error);
            }
        }
        self.keys.sort_unstable();
        self.after = next_tail;
        Ok(Some(page))
    }
}

pub(crate) async fn replay_execution(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    maximum_selected: usize,
    select: impl Fn(&ExecutionEvent) -> bool,
    io: Option<&PreparationIo>,
) -> Result<(u64, Vec<ExecutionRecord>)> {
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
        if selected.len() == selected_before {
            if let Some(io) = io {
                io.release(held.saturating_sub(key_growth))?;
            }
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
    Ok((tail, selected))
}
