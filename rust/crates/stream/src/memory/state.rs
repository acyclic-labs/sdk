#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    replay_root: Option<usize>,
    replay_count: usize,
    retained: Vec<Retained>,
    path_bytes: usize,
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    hierarchy_changed: Option<watch::Sender<()>>,
    roots: ChildIndex,
    #[cfg(test)]
    child_visits: std::sync::atomic::AtomicUsize,
    retained_allocation: Allocation,
    #[cfg(not(target_arch = "wasm32"))]
    // Lock order: provider state -> reserved counters; counter release takes no other lock.
    reserved: Arc<crate::query_sync::SyncMutex<(usize, usize)>>,
}

impl State {
    fn replay<E>(
        &self,
        key: &[u8],
        mut probe: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<&Retained>, E> {
        let mut next = self.replay_root;
        while let Some(index) = next {
            probe()?;
            let entry = self.replay_entry(index);
            match entry.replay.as_deref().cmp(&Some(key)) {
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
                std::cmp::Ordering::Less => next = entry.replay_right,
                std::cmp::Ordering::Greater => next = entry.replay_left,
            }
        }
        Ok(None)
    }
    fn replay_count(&self) -> usize {
        self.replay_count
    }
    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        self.retained
            .iter()
            .rev()
            .filter_map(Retained::envelope)
            .find(|envelope| envelope.commit_id == *id)
    }
}
