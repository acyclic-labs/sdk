use super::*;

#[derive(Default)]
pub(super) struct State {
    pub(super) paths: BTreeMap<StreamPath, PathState>,
    pub(super) commits: BTreeMap<CommitId, Arc<Replay>>,
    pub(super) replays: BTreeMap<OwnedBytes, Arc<Replay>>,
    /// Indefinite receipt inventory used by the current snapshot codec.
    pub(super) retained: Vec<Retained>,
    pub(super) path_bytes: usize,
    /// Records and payload bytes retained, each counted once however many
    pub(super) record_count: usize,
    pub(super) payload_bytes: usize,
    /// High-water mark survives clock rollback and snapshot restoration.
    pub(super) last_committed_at_micros: u64,
    pub(super) hierarchy_version: CommitId,
    pub(super) retained_allocation: Allocation,
    pub(super) prepared_capacity: Arc<PreparedCapacity>,
}

impl Drop for State {
    fn drop(&mut self) {
        for (_, mut path) in std::mem::take(&mut self.paths) {
            let mut cursor = path.history.take();
            while let Some(node) = cursor {
                let Ok(history) = Arc::try_unwrap(node) else { break; };
                cursor = match history {
                    History::Batch { parent, .. } => parent,
                    History::Prefix { source, .. } => source,
                };
            }
        }
    }
}

/// One indefinitely retained snapshot entry.
#[cfg_attr(
    not(feature = "local"),
    allow(dead_code, reason = "only local snapshots read the retained fields")
)]
pub(super) struct Retained {
    pub(super) replay: Option<OwnedBytes>,
    pub(super) commit: Option<CommitId>,
    pub(super) _allocation: Allocation,
}

pub(super) struct PathState {
    pub(super) history: Option<Arc<History>>,
    pub(super) tail: u64,
    pub(super) changed: watch::Sender<u64>,
    pub(super) _allocation: Allocation,
}

pub(super) enum History {
    Batch {
        parent: Option<Arc<Self>>,
        records: Arc<MemoryOutput<Vec<Record>>>,
        _allocation: Allocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        tail: u64,
        _allocation: Allocation,
    },
}

pub(super) struct Replay {
    pub(super) digest: [u8; 32],
    pub(super) result: IdempotencyOutcome,
    pub(super) envelope: Option<CommittedEnvelope>,
    pub(super) _allocation: Allocation,
}
impl Replay {
    pub(super) fn envelope_mut(&mut self) -> Option<&mut CommittedEnvelope> {
        match &mut self.result {
            IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => Some(envelope),
            _ => self.envelope.as_mut(),
        }
    }

    pub(super) fn envelope(&self) -> Option<&CommittedEnvelope> {
        match &self.result {
            IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => Some(envelope),
            _ => self.envelope.as_ref(),
        }
    }
}

pub(super) fn cache_replay(
    meter: WireMeter<'_>, envelope: Option<CommittedEnvelope>, result: IdempotencyOutcome,
    mut allocation: Allocation,
) -> std::io::Result<Arc<Replay>> {
    absorb(&mut allocation, allocate(meter, std::mem::size_of::<Replay>())?)?;
    Ok(Arc::new(Replay { digest: [0; 32], result, envelope, _allocation: allocation }))
}
