#[derive(Default)]
struct State {
    paths: Namespace,
    signals: crate::interest::Interests<StreamPath, Signals>,
    replay_root: Option<RetainedIndexLink>,
    commit_root: Option<RetainedIndexLink>,
    replay_count: usize,
    retained: Vec<Retained>,
    path_bytes: usize,
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: Allocation,
    retained_growth: Arc<RetainedPool>,
    #[cfg(not(target_arch = "wasm32"))]
    // Lock order: provider state -> reserved counters; counter release takes no other lock.
    reserved: Arc<crate::query_sync::SyncMutex<(usize, usize)>>,
}

impl State {
    // Publication order is already the retained inventory's order. Seek that
    // owner directly instead of replaying history or registering request pins.
    fn namespace_at<E>(
        &self,
        index: u64,
        mut examine: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<&NamespaceCut>, E> {
        let (mut low, mut high) = (0, self.retained.len());
        while low < high {
            examine()?;
            let middle = low + (high - low) / 2;
            if self.retained[middle].index <= index {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low
            .checked_sub(1)
            .and_then(|at| self.retained.get(at))
            .filter(|fact| fact.index == index)
            .and_then(|fact| fact.namespace.get()))
    }
    /// The tail of `path`; `None` when it does not exist. Every planner reads
    /// presence and tails through this one lookup.
    fn tail(&self, path: &StreamPath) -> Option<u64> {
        self.paths.get(path).map(|path| path.tail)
    }

    fn replay_count(&self) -> usize {
        self.replay_count
    }
    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        match self.find_commit(id, |_| Ok::<_, std::convert::Infallible>(())) {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }
}

#[cfg(test)]
std::thread_local! { static COMMIT_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
