#[derive(Default)]
struct State {
    paths: Namespace,
    signals: crate::interest::Interests<StreamPath, Signals>,
    replay_root: Option<usize>,
    commit_root: Option<CommitLink>,
    replay_count: usize,
    retained: Vec<Retained>,
    path_bytes: usize,
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
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
    #[cfg(any(test, feature = "local"))]
    fn commit(&self, id: &CommitId) -> Option<&CommittedEnvelope> {
        match self.find_commit(id, || Ok::<_, std::convert::Infallible>(())) {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }
}

#[cfg(test)]
std::thread_local! { static COMMIT_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
