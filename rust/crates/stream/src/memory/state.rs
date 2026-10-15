#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    retained: Vec<Retained>,
    path_bytes: usize,
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: Allocation,
    #[cfg(not(target_arch = "wasm32"))]
    reserved: Arc<std::sync::Mutex<(usize, usize)>>,
}

impl State {
    fn replay(&self, key: &[u8]) -> Option<&Retained> {
        self.retained.iter().rev().find(|entry| {
            entry
                .replay
                .as_ref()
                .is_some_and(|stored| stored.as_ref() == key)
        })
    }
    fn replay_count(&self) -> usize {
        self.retained
            .iter()
            .filter(|entry| entry.replay.is_some())
            .count()
    }
    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        self.retained
            .iter()
            .rev()
            .filter_map(Retained::envelope)
            .find(|envelope| envelope.commit_id == *id)
    }
}
