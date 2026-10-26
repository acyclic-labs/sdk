#[derive(Clone)]
struct PathState {
    history: Option<Arc<History>>,
    prefix: [Option<HistoryWindow>; 2],
    tail: u64,
}

impl PathState {
    fn prefix_window(&self, from: u64, end: u64) -> Option<HistoryWindow> {
        if from.checked_add(1) != Some(end) {
            return None;
        }
        self.prefix.get(usize::try_from(from).ok()?)?.clone()
    }
}
