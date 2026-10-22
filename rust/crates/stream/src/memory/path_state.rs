struct PathState {
    history: Option<Arc<History>>,
    prefix: [Option<HistoryWindow>; 2],
    tail: u64,
    changed: watch::Sender<u64>,
    follows: Option<Arc<crate::query_sync::SyncMutex<crate::follow_index::FollowIndex>>>,
    children: ChildIndex,
    next_sibling: Option<StreamPath>,
    _allocation: Allocation,
}

impl PathState {
    fn prefix_window(&self, from: u64, end: u64) -> Option<HistoryWindow> {
        if from.checked_add(1) != Some(end) {
            return None;
        }
        self.prefix.get(usize::try_from(from).ok()?)?.clone()
    }
}
