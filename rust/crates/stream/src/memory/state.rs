#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    commits: BTreeMap<CommitId, usize>,
    replays: BTreeMap<Bytes, usize>,
    /// Indefinite receipt inventory used by the current snapshot codec.
    retained: VecDeque<Retained>,
    path_bytes: usize,
    /// Records and payload bytes retained, each counted once however many
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    /// High-water mark survives clock rollback and snapshot restoration.
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: Allocation,
    #[cfg(not(target_arch = "wasm32"))]
    reserved: Arc<std::sync::Mutex<(usize, usize)>>,
}

impl State {
    fn replay(&self, key: &[u8]) -> Option<&Retained> {
        self.retained.get(*self.replays.get(key)?)
    }

    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        self.retained.get(*self.commits.get(id)?)?.envelope()
    }
}
