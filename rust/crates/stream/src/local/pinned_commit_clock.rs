/// Records the wall-clock sample in a frame and supplies that same sample on replay.
struct PinnedCommitClock {
    source: Arc<dyn UnixMillisClock>,
    pinned: Mutex<Option<u64>>,
}

impl PinnedCommitClock {
    fn pin(&self, at: u64) {
        *self.pinned.lock().unwrap_or_else(PoisonError::into_inner) = Some(at);
    }

    fn unpin(&self) {
        *self.pinned.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

impl UnixMillisClock for PinnedCommitClock {
    fn now_unix_millis(&self) -> u64 {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unwrap_or_else(|| self.source.now_unix_millis())
    }
}
