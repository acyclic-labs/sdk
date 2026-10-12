use super::*;

#[derive(Default)]
pub(super) struct PreparedCapacity {
    pub(super) pending: AtomicUsize,
    pub(super) payload_bytes: AtomicUsize,
    pub(super) revision: AtomicU64,
}

impl PreparedCapacity {
    /// Fails new guarded acceptance after revision exhaustion.
    pub(super) fn advance_revision(&self) {
        let _ = self.revision.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.checked_add(1).unwrap_or(u64::MAX))
        });
    }

    pub(super) fn current_revision(&self) -> Result<u64, StreamError> {
        let revision = self.revision.load(Ordering::Acquire);
        if revision == u64::MAX { Err(StreamError::Capacity) } else { Ok(revision) }
    }
}

/// The genuine canonical mutation revision on the existing state lifetime.
pub(super) struct CommitStateVersion {
    pub(super) identity: Arc<PreparedCapacity>,
    pub(super) revision: u64,
    pub(super) reserved: [usize; 2],
}

impl CommitStateVersion {
    pub(super) fn reserved(capacity: &PreparedCapacity) -> [usize; 2] {
        [capacity.pending.load(Ordering::Acquire), capacity.payload_bytes.load(Ordering::Acquire)]
    }

    pub(super) fn capture(state: &State) -> Result<Self, StreamError> {
        Ok(Self {
            identity: Arc::clone(&state.prepared_capacity),
            revision: state.prepared_capacity.current_revision()?,
            reserved: Self::reserved(&state.prepared_capacity),
        })
    }

    pub(super) fn matches(&self, state: &State) -> Result<bool, StreamError> {
        Ok(Arc::ptr_eq(&self.identity, &state.prepared_capacity)
            && self.revision == state.prepared_capacity.current_revision()?
            && self.reserved == Self::reserved(&state.prepared_capacity))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct PreparedCapacityReservation {
    pub(super) capacity: Arc<PreparedCapacity>,
    pub(super) bytes: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PreparedCapacityReservation {
    fn drop(&mut self) {
        self.capacity.advance_revision();
        self.capacity.pending.fetch_sub(1, Ordering::AcqRel);
        self.capacity.payload_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        // Revision changes bracket the lock-free capacity refund.
        self.capacity.advance_revision();
    }
}

pub(super) fn reserve(
    state: &State,
    paths: &BTreeSet<&str>,
    records: usize,
    bytes: usize,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    if state.commits.len().saturating_add(state.prepared_capacity.pending.load(Ordering::Acquire)) >= limits.commits
        || state.paths.len().saturating_add(paths.len()) > limits.paths
        || state.path_bytes.saturating_add(paths.iter().map(|path| path.len()).sum()) > limits.path_bytes
        || state.record_count.saturating_add(state.prepared_capacity.pending.load(Ordering::Acquire))
            .saturating_add(records) > limits.records
        || state.payload_bytes.saturating_add(state.prepared_capacity.payload_bytes.load(Ordering::Acquire))
            .saturating_add(bytes) > limits.payload_bytes
    {
        Err(StreamError::Capacity)
    } else { Ok(()) }
}

/// Counts exactly the records and payload bytes the live paths retain, each shared history once.
pub(super) fn recount(state: &mut State, meter: WireMeter<'_>) -> MemoryResult<()> {
    let mut seen = MemoryOutput {
        value: HashSet::new(),
        allocation: allocate(meter, 0)?,
    };
    let mut records = 0_usize;
    let mut bytes = 0_usize;
    for path in state.paths.values() {
        admit_item(meter)?;
        let mut cursor = path.history.as_ref();
        record_item(meter)?;
        while let Some(node) = cursor {
            admit_item(meter)?;
            let pointer = Arc::as_ptr(node);
            if seen.value.contains(&pointer) {
                record_item(meter)?;
                break;
            }
            absorb(&mut seen.allocation, adopt(meter, std::mem::size_of::<*const History>())?)?;
            seen.value.insert(pointer);
            cursor = match node.as_ref() {
                History::Batch { parent, records: batch, .. } => {
                    records = records.saturating_add(batch.value.len());
                    for record in batch.value.iter() {
                        admit_item(meter)?;
                        bytes = bytes.saturating_add(record.value.len());
                        record_item(meter)?;
                    }
                    parent.as_ref()
                }
                History::Prefix { source, .. } => source.as_ref(),
            };
            record_item(meter)?;
        }
    }
    if state.record_count != records || state.payload_bytes != bytes {
        state.prepared_capacity.advance_revision();
    }
    state.record_count = records;
    state.payload_bytes = bytes;
    Ok(())
}

pub(super) fn admit_replay(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    if key.is_some() && state.replays.len()
        .saturating_add(state.prepared_capacity.pending.load(Ordering::Acquire)) >= limits.idempotency_results {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}
