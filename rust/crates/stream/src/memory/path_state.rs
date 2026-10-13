struct PathState {
    history: Option<Arc<History>>,
    tail: u64,
    changed: watch::Sender<u64>,
    _allocation: Allocation,
}
